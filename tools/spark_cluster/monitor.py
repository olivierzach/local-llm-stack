"""Read-only Spark telemetry. Scrapes serve snapshots, never initiate SSH or inference."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import queue
import re
import signal
import stat
import threading
import time
from urllib.parse import urlsplit, urlunsplit
from urllib.request import Request, build_opener, HTTPRedirectHandler, ProxyHandler

from .cli import remote
from .config import read, validate_inventory, validate_saved_plan
from .gateway import from_plans
from .monitor_node import GPU_FIELDS, NET_COUNTERS, PHYSICAL_COUNTERS, RDMA_COUNTERS, RDMA_HARDWARE

SOURCE = Path(__file__).with_name("monitor_node.py")
MAX_BODY = 2 * 1024 * 1024
SAFE = re.compile(r"[a-zA-Z0-9_.:-]{1,128}\Z")
SAMPLE = re.compile(r'([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^\n]*\})?\s+([-+0-9.eE]+|[-+]?Inf|NaN)(?:\s+[0-9]+)?\Z')
LABEL = re.compile(r'\s*([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\[\\"n])*)"\s*(,|$)')
ENGINE_BASES = {
    "num_requests_running", "num_requests_waiting", "gpu_cache_usage_perc", "kv_cache_usage_perc",
    "prompt_tokens_total", "generation_tokens_total", "request_success_total",
    "time_to_first_token_seconds", "time_per_output_token_seconds", "inter_token_latency_seconds",
    "e2e_request_latency_seconds", "request_queue_time_seconds", "request_prefill_time_seconds",
    "request_decode_time_seconds", "request_prompt_tokens", "request_generation_tokens",
}
GATEWAY_BASES = {"requests_total", "errors_total", "inflight", "request_duration_seconds",
                 "time_to_first_token_seconds"}
HOST_FIELDS = {"memory_total_bytes", "memory_available_bytes", "swap_total_bytes", "swap_free_bytes",
               "load1", "cpu_count"} | {"cpu_" + mode + "_seconds_total" for mode in
               ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")}


def numeric(value):
    try:
        return type(value) in (int, float) and math.isfinite(value) and value >= 0
    except OverflowError:
        return False


def escape(value):
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


class Metrics:
    def __init__(self):
        self.lines = []
        self.types = {}

    def add(self, name, value, labels=None, kind=None):
        if not numeric(value):
            return
        kind = kind or ("counter" if name.endswith("_total") else "gauge")
        if name not in self.types:
            self.types[name] = kind
            self.lines.append(f"# TYPE {name} {kind}")
        suffix = "{" + ",".join(f'{k}="{escape(v)}"' for k, v in sorted((labels or {}).items())) + "}" if labels else ""
        self.lines.append(f"{name}{suffix} {value}")

    def render(self):
        return "\n".join(self.lines) + "\n"


def counter_rate(previous, current, elapsed):
    # Resets/wraps are unknown intervals, not negative rates or fictitious bursts.
    if not numeric(previous) or not numeric(current) or current < previous or elapsed <= 0:
        return None
    return (current - previous) / elapsed


def physical_mapping(inventory):
    """Map known Spark dual PCIe paths to one cable, never add their counters."""
    nodes = inventory["nodes"]
    pair = "--".join(sorted(nodes)) if len(nodes) == 2 else None
    known = {"enp1s0f0np0", "enP2p1s0f0np0"}
    result = []
    for node_id, node in sorted(nodes.items()):
        for rail in node["fabric"]:
            peer = rail.get("peer")
            if not peer and pair:
                peer = next(n for n in nodes if n != node_id)
            link = rail.get("physical_link")
            if not link and pair and rail["interface"] in known:
                link = "spark-qsfp-" + pair
            result.append({"node": node_id, "interface": rail["interface"], "rdma": rail["rdma"],
                           "physical_link": link or "unmapped-" + node_id + "-" + rail["interface"],
                           "peer": peer or "unknown", "mapped": bool(link)})
    return result


def canonical_physical_ports(mapping):
    """Choose a stable local NIC once per known physical port, independent of rail order."""
    ports = {}
    for row in sorted(mapping, key=lambda r: (r["physical_link"], r["node"], r["interface"].casefold(), r["interface"])):
        if row["mapped"]:
            ports.setdefault((row["node"], row["physical_link"]), row)
    return list(ports.values())


def physical_link_metrics(metrics, mapping, current, previous):
    """Export endpoint-scoped hardware counters and a single endpoint's cable view."""
    endpoints = {}
    for row in canonical_physical_ports(mapping):
        node, link, interface = row["node"], row["physical_link"], row["interface"]
        selected = link not in endpoints
        endpoints.setdefault(link, row)
        labels = {key: row[key] for key in ("node", "interface", "physical_link", "peer")}

        def sample(hosts):
            section = hosts.get(node, {}).get("physical", {})
            values = section.get(link, {}) if isinstance(section, dict) else {}
            if not isinstance(values, dict) or values.get("interface") != interface:
                return {}, {}
            counters = values.get("counters", {})
            return values, counters if isinstance(counters, dict) else {}

        values, counters = sample(current)
        old, old_counters = sample(previous)
        available = all(numeric(counters.get(direction + "_bytes")) for direction in ("tx", "rx"))
        metrics.add("spark_physical_port_available", int(available), labels)
        for key in PHYSICAL_COUNTERS.values():
            metrics.add("spark_physical_port_" + key + "_total", counters.get(key), labels)
        if selected:
            metrics.add("spark_fabric_endpoint_info", 1, labels)
            metrics.add("spark_fabric_aggregation_available", int(available), {"physical_link": link})
            nic = current.get(node, {}).get("interfaces", {}).get(interface, {})
            metrics.add("spark_fabric_negotiated_bits_per_second", nic.get("speed_bits_per_second"), labels)
        # Remote monotonic sample times exclude SSH scheduling jitter. A reboot or
        # either byte counter resetting invalidates the interval, never invents zero.
        sampled_at, old_time = values.get("sampled_at"), old.get("sampled_at")
        if not available or not numeric(sampled_at) or not numeric(old_time):
            continue
        rates = {direction: counter_rate(old_counters.get(direction + "_bytes"),
                 counters[direction + "_bytes"], sampled_at - old_time) for direction in ("tx", "rx")}
        if any(rate is None for rate in rates.values()):
            continue
        for direction, rate in rates.items():
            metrics.add("spark_physical_port_observed_bits_per_second", rate * 8, {**labels, "direction": direction})
            if selected:
                metrics.add("spark_fabric_physical_bits_per_second", rate * 8, {**labels, "direction": direction})
    for link in sorted({r["physical_link"] for r in mapping} - endpoints.keys()):
        metrics.add("spark_fabric_aggregation_available", 0, {"physical_link": link})


def private_key(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd) as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077 or not 1 <= info.st_size <= 8194:
            raise ValueError("gateway key must be a private owner-readable regular file")
        value = stream.read(8194).strip()
    if not re.fullmatch(r"[A-Za-z0-9_-]{24,8192}", value):
        raise ValueError("invalid gateway key")
    return value


def endpoint(url, path):
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname or parts.username or parts.password or parts.query or parts.fragment:
        raise ValueError("metrics URL must be HTTP(S) without credentials, query or fragment")
    _ = parts.port  # Reject malformed ports at startup, before collecting anything.
    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise ValueError("metrics redirects are not allowed")


def fetch(url, timeout, key=None):
    request = Request(url, headers={"Authorization": "Bearer " + key} if key else {})
    # Do not forward credentials to proxy environment variables or redirects.
    with build_opener(ProxyHandler({}), NoRedirect()).open(request, timeout=timeout) as response:
        body = response.read(MAX_BODY + 1)
    if len(body) > MAX_BODY:
        raise ValueError("metrics response too large")
    return body.decode("utf-8")


def parse_exposition(body, prefix, allowed, labels):
    """Retain only bounded native engine/gateway series; strip unsafe label dimensions."""
    result, seen, types = [], set(), {}
    for line in body.splitlines():
        if line.startswith("# TYPE "):
            parts = line.split()
            if len(parts) == 4 and parts[3] in ("counter", "gauge", "histogram", "summary", "untyped"):
                types[parts[2]] = parts[3]
            continue
        if not line or line.startswith("#"):
            continue
        match = SAMPLE.fullmatch(line)
        if not match:
            raise ValueError("malformed metrics sample")
        name, raw, value = match.groups()
        if not name.startswith(prefix):
            continue
        short = name[len(prefix):]
        base = re.sub(r"_(bucket|sum|count)$", "", short)
        if short not in allowed and base not in allowed:
            continue
        dimensions = dict(labels)
        if raw:
            text, pos = raw[1:-1], 0
            while pos < len(text):
                label = LABEL.match(text, pos)
                if not label:
                    raise ValueError("malformed metrics labels")
                key, encoded, comma = label.groups()
                decoded = json.loads('"' + encoded + '"')
                if key in ("le", "quantile"):
                    if not re.fullmatch(r"[-+0-9.eE]+|\+?Inf", decoded):
                        raise ValueError("invalid histogram boundary")
                    dimensions[key] = decoded
                elif key in ("engine", "engine_id") and re.fullmatch(r"[0-9]{1,6}", decoded):
                    dimensions[key] = decoded
                elif key in ("finished_reason", "finish_reason") and decoded in ("stop", "length", "abort", "error"):
                    dimensions[key] = decoded
                elif prefix == "spark_gateway_" and key in ("alias", "backend", "mode") and SAFE.fullmatch(decoded):
                    dimensions[key] = decoded
                pos = label.end()
                if not comma and pos != len(text):
                    raise ValueError("malformed metrics labels")
        identity = (name, tuple(sorted(dimensions.items())))
        if identity in seen:
            raise ValueError("ambiguous metrics after label filtering")
        seen.add(identity)
        if len(seen) > 10000:
            raise ValueError("too many metric samples")
        value = float(value)
        if not math.isfinite(value):
            continue
        family = prefix + base
        kind = types.get(name, types.get(family, "counter" if name.endswith("_total") else "gauge"))
        result.append((name, value, dimensions, family if kind in ("histogram", "summary") else name, kind))
    return result


def append_exposition(metrics, samples):
    for name, value, labels, family, kind in samples:
        if family not in metrics.types:
            metrics.types[family] = kind
            metrics.lines.append(f"# TYPE {family} {kind}")
        suffix = "{" + ",".join(f'{k}="{escape(v)}"' for k, v in sorted(labels.items())) + "}"
        metrics.lines.append(f"{name}{suffix} {value}")


def host_metrics(metrics, node_id, data, node, mapping, previous=None, elapsed=0):
    if not isinstance(data, dict) or data.get("version") != 1 or data.get("hostname") != node["hostname"] or data.get("architecture") != node["architecture"]:
        raise ValueError("invalid telemetry identity")
    for key in ("host", "disks", "interfaces", "rdma", "gpu"):
        if not isinstance(data.get(key), dict):
            raise ValueError("missing telemetry section")
    label = {"node": node_id}
    for key in HOST_FIELDS:
        if key.startswith("cpu_") and key.endswith("_seconds_total"):
            metrics.add("spark_host_cpu_seconds_total", data["host"].get(key),
                        {**label, "mode": key.removeprefix("cpu_").removesuffix("_seconds_total")})
        else:
            metrics.add("spark_host_" + key, data["host"].get(key), label)
    for mount in ("root", "cache"):
        disk = data["disks"].get(mount, {})
        if not isinstance(disk, dict):
            raise ValueError("invalid disk data")
        metrics.add("spark_disk_up", int(bool(disk)), {**label, "mount": mount})
        for key in ("size_bytes", "available_bytes"):
            metrics.add("spark_disk_" + key, disk.get(key), {**label, "mount": mount})
    gpu = data["gpu"]
    metrics.add("spark_gpu_query_up", int(gpu.get("up") is True), label)
    devices = gpu.get("devices", {})
    if not isinstance(devices, dict) or len(devices) > 32:
        raise ValueError("invalid GPU data")
    for index, values in sorted(devices.items()):
        if not isinstance(index, str) or not index.isdecimal() or not isinstance(values, dict):
            raise ValueError("invalid GPU device")
        for key, _ in GPU_FIELDS.values():
            labels = {**label, "gpu": index}
            metrics.add("spark_gpu_metric_available", int(numeric(values.get(key))), {**labels, "metric": key})
            metrics.add("spark_gpu_" + key, values.get(key), labels)
    by_interface = {row["interface"]: row for row in mapping if row["node"] == node_id}
    interfaces = data["interfaces"]
    if len(interfaces) > 128:
        raise ValueError("too many interfaces")
    for interface, values in sorted(interfaces.items()):
        if not isinstance(interface, str) or not re.fullmatch(r"[A-Za-z0-9_.:-]{1,32}", interface) or not isinstance(values, dict):
            raise ValueError("invalid interface data")
        labels = {**label, "interface": interface}
        if interface in by_interface:
            labels["physical_link"] = by_interface[interface]["physical_link"]
        metrics.add("spark_interface_present", int(values.get("present") is True), labels)
        metrics.add("spark_interface_carrier", values.get("carrier"), labels)
        metrics.add("spark_interface_speed_bits_per_second", values.get("speed_bits_per_second"), labels)
        counters = values.get("counters", {})
        for key in NET_COUNTERS:
            metrics.add("spark_interface_" + key + "_total", counters.get(key), labels)
    for row in (r for r in mapping if r["node"] == node_id):
        labels = {key: row[key] for key in ("node", "interface", "rdma", "physical_link", "peer")}
        metrics.add("spark_fabric_mapping_info", 1, labels)
        values = data["rdma"].get(row["rdma"], {})
        if not isinstance(values, dict) or not isinstance(values.get("counters", {}), dict):
            raise ValueError("invalid RDMA data")
        metrics.add("spark_rdma_present", int(values.get("present") is True), labels)
        if type(values.get("active")) is bool:
            metrics.add("spark_rdma_active", int(values["active"]), labels)
        counters = values.get("counters", {})
        for key in {v[0] for v in RDMA_COUNTERS.values()} | set(RDMA_HARDWARE):
            metrics.add("spark_rdma_" + key + "_total", counters.get(key), labels)
        for transport, section, device in (("rdma", "rdma", row["rdma"]), ("netdev", "interfaces", row["interface"])):
            current = data[section].get(device, {}).get("counters", {})
            old = (previous or {}).get(section, {}).get(device, {}).get("counters", {})
            for direction in ("tx", "rx"):
                rate = counter_rate(old.get(direction + "_bytes"), current.get(direction + "_bytes"), elapsed)
                metrics.add("spark_fabric_observed_bits_per_second", rate * 8 if rate is not None else None,
                            {**labels, "transport": transport, "direction": direction})


def recovery_metrics(metrics, data):
    if not isinstance(data, dict) or data.get("version") != 1:
        raise ValueError("invalid recovery summary")
    policy, phase = data.get("policy"), data.get("phase")
    if not isinstance(policy, str) or not SAFE.fullmatch(policy) or not isinstance(phase, str) or not SAFE.fullmatch(phase):
        raise ValueError("invalid recovery summary identity")
    labels = {"policy": policy}
    metrics.add("spark_recovery_phase_info", 1, {**labels, "phase": phase})
    selected = data.get("selected_plan_digest")
    if selected is not None:
        if not isinstance(selected, str) or not re.fullmatch(r"[a-f0-9]{64}", selected):
            raise ValueError("invalid selected deployment identity")
        metrics.add("spark_recovery_selected_deployment_info", 1, {**labels, "plan_digest": selected})
    for field, suffix in (("generation", "generation"), ("epoch", "epoch"), ("updated_at", "updated_timestamp_seconds"),
                          ("phase_since", "phase_since_timestamp_seconds"), ("retry_count", "retry_count"),
                          ("retry_at", "retry_timestamp_seconds"), ("last_transition_seconds", "last_transition_seconds")):
        metrics.add("spark_recovery_" + suffix, data.get(field), labels)
    if type(data.get("circuit_open")) is bool:
        metrics.add("spark_recovery_circuit_open", int(data["circuit_open"]), labels)
    if isinstance(data.get("quarantined_nodes"), list):
        metrics.add("spark_recovery_quarantined_nodes", len(data["quarantined_nodes"]), labels)
    events = data.get("event_counts", {})
    if not isinstance(events, dict) or len(events) > 64:
        raise ValueError("invalid recovery event counts")
    for event, count in sorted(events.items()):
        if SAFE.fullmatch(event):
            metrics.add("spark_recovery_events_total", count, {**labels, "event": event})


@dataclass(frozen=True)
class Snapshot:
    metrics: str
    status: bytes
    collected_at: float
    healthy: bool


class Collector:
    """At most one daemon task per target, even if an OS/DNS call outlives its deadline."""
    def __init__(self, inventory, plans=(), *, timeout=8, interval=10, recovery_state=None,
                 gateway_url=None, gateway_key=None, host_reader=None, http_reader=fetch, engine_reader=None):
        validate_inventory(inventory)
        if not 0.05 <= timeout <= 120 or not 0.05 <= interval <= 3600:
            raise ValueError("collection timeout/interval out of range")
        if len(inventory["nodes"]) > 64 or len(plans) > 64:
            raise ValueError("at most 64 nodes and 64 engine plans")
        self.inventory, self.timeout, self.interval = inventory, timeout, interval
        self.mapping = physical_mapping(inventory)
        def read_host(node):
            ports = [row for row in canonical_physical_ports(self.mapping)
                     if inventory["nodes"][row["node"]]["hostname"] == node["hostname"]]
            return remote(node, {"action": "monitor", "node": node, "physical_ports": ports},
                          source_path=SOURCE, timeout=timeout)
        self.host_reader = host_reader or read_host
        self.physical_previous = {}
        self.http_reader, self.pending, self.previous = http_reader, {}, {}
        self.last_success = {}
        self.collect_lock = threading.Lock()
        self.stop_event = threading.Event()
        self.thread = None
        self.targets = {}
        for node_id, node in inventory["nodes"].items():
            self.targets[("host", node_id)] = lambda n=node: self.host_reader(n)
        endpoints = {}
        endpoint_locks = {}
        digests = set()
        def read_state():
            with Path(recovery_state).open() as stream:
                body = stream.read(131073)
            if len(body) > 131072:
                raise ValueError("recovery summary too large")
            return json.loads(body)

        def selected_digest(state):
            if not isinstance(state, dict) or state.get("version") != 1:
                raise ValueError("invalid recovery summary")
            selected = state.get("selected_plan_digest")
            if not isinstance(selected, str) or selected not in digests:
                raise ValueError("recovery has no known selected deployment")
            return selected

        def read_engine(p):
            coordinator_id = p["deployment"]["coordinator"]
            worker = p["compose"][coordinator_id]["services"]["worker"]
            route = from_plans([p])["routes"][p["recipe"]["alias"]]
            engine = {"owner": p["owner"], "digest": p["digest"], "port": p["deployment"]["port"],
                      "alias": route["upstream_model"], "model_root": route["model_root"],
                      "context_tokens": route["context_tokens"],
                      "worker": {key: worker[key] for key in ("container_name", "image", "entrypoint", "command")}}
            return remote(inventory["nodes"][coordinator_id],
                          {"action": "monitor", "node": p["nodes"][coordinator_id],
                           "engine": engine, "timeout": timeout}, source_path=SOURCE, timeout=timeout)
        engine_reader = engine_reader or read_engine
        for plan in plans:
            validate_saved_plan(plan)
            if not set(plan["nodes"]) <= set(inventory["nodes"]):
                raise ValueError("engine plan contains nodes outside inventory")
            for node_id, node in plan["nodes"].items():
                if any(node[k] != inventory["nodes"][node_id][k] for k in ("hostname", "architecture")):
                    raise ValueError("engine plan host identity differs from inventory")
            url = endpoint(plan["endpoint"]["base_url"], "/metrics")
            coordinator = plan["nodes"][plan["deployment"]["coordinator"]]
            address = coordinator.get("serving", {}).get("address", coordinator["fabric"][0]["ip"])
            if url != f"http://{address}:{plan['deployment']['port']}/metrics":
                raise ValueError("engine endpoint differs from saved coordinator serving address")
            group = endpoints.setdefault(url, set())
            if plan["digest"] in group:
                raise ValueError("duplicate engine endpoint: duplicate exact plan")
            group.add(plan["digest"])
            digests.add(plan["digest"])
            lock = endpoint_locks.setdefault(url, threading.Lock())
            labels = {"deployment": plan["owner"], "plan_digest": plan["digest"], "alias": plan["recipe"]["alias"],
                      "coordinator": plan["deployment"]["coordinator"]}
            def collect_engine(state=None, p=plan, l=labels, group=group, lock=lock):
                # Alternatives sharing one socket use the same authority snapshot.
                # Independent endpoints remain independently observable.
                shared = len(group) > 1
                if shared and selected_digest(state) != p["digest"]:
                    return None
                if not lock.acquire(blocking=False):
                    raise ValueError("engine endpoint collection is still pending")
                try:
                    value = engine_reader(p)
                    if not isinstance(value, dict) or value.get("owner") != p["owner"] or value.get("digest") != p["digest"]:
                        raise ValueError("engine telemetry identity mismatch")
                    if shared and selected_digest(read_state()) != p["digest"]:
                        raise ValueError("selected deployment changed during collection")
                    return parse_exposition(value["metrics"], "vllm:", ENGINE_BASES, l)
                finally:
                    lock.release()
            self.targets[("engine", plan["owner"])] = collect_engine
        self.plans = tuple(plans)
        self.shared_engines = {plan["owner"] for plan in plans
                               if len(endpoints[endpoint(plan["endpoint"]["base_url"], "/metrics")]) > 1}
        if recovery_state:
            self.targets[("recovery", "authority")] = read_state
        if bool(gateway_url) != bool(gateway_key):
            raise ValueError("gateway URL and private key are required together")
        if gateway_url:
            metrics_url, status_url = endpoint(gateway_url, "/metrics"), endpoint(gateway_url, "/_spark/recovery")
            self.targets[("gateway", "ingress")] = lambda: parse_exposition(
                self.http_reader(metrics_url, timeout, gateway_key), "spark_gateway_", GATEWAY_BASES, {})
            self.targets[("gateway_recovery", "ingress")] = lambda: json.loads(
                self.http_reader(status_url, timeout, gateway_key))
        self.snapshot = Snapshot("", b'{"ready":false,"targets":[]}', 0, False)

    def collect(self):
        with self.collect_lock:
            start = time.monotonic()
            jobs = {}
            recovery_ready, recovery = threading.Event(), {}
            for key, collect in self.targets.items():
                previous = self.pending.get(key)
                if previous and previous[0].is_alive():
                    continue
                result = queue.Queue(maxsize=1)
                def run(fn=collect, out=result, key=key):
                    try:
                        if key[0] == "engine" and key[1] in self.shared_engines:
                            if not recovery_ready.wait(max(0, self.timeout - (time.monotonic() - start))):
                                raise ValueError("recovery selection unavailable")
                            payload = fn(recovery.get("state"))
                        else:
                            payload = fn()
                        if key == ("recovery", "authority"):
                            recovery["state"] = payload
                        out.put((True, payload))
                    except Exception:
                        out.put((False, None))
                    finally:
                        if key == ("recovery", "authority"):
                            recovery_ready.set()
                thread = threading.Thread(target=run, daemon=True, name="spark-monitor-target")
                self.pending[key] = jobs[key] = (thread, result)
                thread.start()
            metrics, status, success = Metrics(), [], True
            physical_current = {}
            inactive, available_engines = set(), set()
            now = time.time()
            for key in sorted(self.targets):
                kind, target = key
                error, payload = "timeout", None
                if key in jobs:
                    thread, result = jobs[key]
                    try:
                        ok, payload = result.get(timeout=max(0, self.timeout - (time.monotonic() - start)))
                        error = "" if ok else "collection_failed"
                    except queue.Empty:
                        pass
                if not error and kind == "engine" and payload is None:
                    error = "not_selected"
                    inactive.add(target)
                local = Metrics()
                if not error:
                    try:
                        if kind == "host":
                            old, old_time = self.previous.get(key, ({}, start))
                            host_metrics(local, target, payload, self.inventory["nodes"][target], self.mapping, old, start - old_time)
                            self.previous[key] = (payload, start)
                        elif kind in ("engine", "gateway"):
                            append_exposition(local, payload)
                        elif kind == "recovery":
                            recovery_metrics(local, payload)
                        else:
                            gateway = payload
                            if not isinstance(gateway, dict):
                                raise ValueError("invalid gateway status")
                            for field in ("generation", "highest_generation", "active_requests", "issued_at", "expires_at"):
                                local.add("spark_gateway_recovery_" + field, gateway.get(field))
                            for field in ("accepting", "fresh"):
                                if type(gateway.get(field)) is bool:
                                    local.add("spark_gateway_recovery_" + field, int(gateway[field]))
                        self.last_success[key] = now
                        if kind == "engine":
                            available_engines.add(target)
                        if kind == "host":
                            physical_current[target] = payload
                        # Shared TYPE declarations across hosts/engines appear only once.
                        for line in local.lines:
                            if line.startswith("# TYPE "):
                                name = line.split()[2]
                                if name in metrics.types:
                                    continue
                                metrics.types[name] = local.types[name]
                            metrics.lines.append(line)
                    except (ValueError, TypeError, KeyError, AttributeError):
                        error = "invalid_data"
                labels = {"kind": kind, "target": target}
                metrics.add("spark_monitor_target_up", int(not error), labels)
                metrics.add("spark_monitor_target_error", int(bool(error)), {**labels, "reason": error or "none"})
                metrics.add("spark_monitor_target_last_success_timestamp_seconds", self.last_success.get(key), labels)
                status.append({**labels, "up": not error, "error": error or None, "last_success": self.last_success.get(key)})
                success = success and (not error or error == "not_selected")
            physical_link_metrics(metrics, self.mapping, physical_current, self.physical_previous)
            self.physical_previous = physical_current
            for plan in self.plans:
                labels = {"deployment": plan["owner"], "alias": plan["recipe"]["alias"]}
                metrics.add("spark_engine_plan_info", 1, {**labels, "target": plan["owner"], "plan_digest": plan["digest"]})
                if plan["owner"] in self.shared_engines and plan["owner"] not in available_engines:
                    if plan["owner"] in inactive:
                        metrics.add("spark_engine_configured_replica_count", 0, labels)
                    continue
                for field in ("context_tokens", "max_output_tokens", "max_num_seqs"):
                    metrics.add("spark_engine_configured_" + field, plan["recipe"][field], labels)
                metrics.add("spark_engine_tensor_parallel_shards", plan["deployment"]["tensor_parallel"], labels)
                metrics.add("spark_engine_pipeline_parallel_shards", plan["deployment"]["pipeline_parallel"], labels)
                metrics.add("spark_engine_configured_replica_count", 1, labels)
            metrics.add("spark_monitor_collection_duration_seconds", time.monotonic() - start)
            completed = time.time()
            self.snapshot = Snapshot(metrics.render(), json.dumps({"ready": True, "all_targets_up": success,
                "collected_at": completed, "targets": status}, separators=(",", ":")).encode(), completed, success)
            return self.snapshot

    def start(self):
        if self.thread and self.thread.is_alive():
            raise RuntimeError("collector already running")
        self.stop_event.clear()
        def loop():
            while not self.stop_event.is_set():
                started = time.monotonic()
                self.collect()
                self.stop_event.wait(max(.05, self.interval - (time.monotonic() - started)))
        self.thread = threading.Thread(target=loop, daemon=True, name="spark-monitor-sampler")
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=self.timeout + 1)


class MonitorServer(ThreadingHTTPServer):
    daemon_threads = True
    block_on_close = False

    def __init__(self, address, collector):
        self.collector = collector
        super().__init__(address, MonitorHandler)

    def get_request(self):
        sock, address = super().get_request()
        sock.settimeout(5)
        return sock, address


class MonitorHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        collector = self.server.collector
        snapshot = collector.snapshot  # Immutable reference; no collector/SSH lock on HTTP paths.
        age = max(0, time.time() - snapshot.collected_at) if snapshot.collected_at else None
        fresh = age is not None and age <= 2 * collector.interval + collector.timeout
        code, content = 200, "application/json"
        if self.path == "/metrics":
            dynamic = Metrics()
            dynamic.add("spark_monitor_snapshot_timestamp_seconds", snapshot.collected_at)
            dynamic.add("spark_monitor_snapshot_age_seconds", age)
            dynamic.add("spark_monitor_snapshot_fresh", int(fresh))
            body = (snapshot.metrics + dynamic.render()).encode()
            content = "text/plain; version=0.0.4; charset=utf-8"
        elif self.path == "/status":
            data = json.loads(snapshot.status)
            data.update(fresh=fresh, age_seconds=age)
            body = json.dumps(data).encode()
        elif self.path == "/health":
            # Collector health != every target healthy: absent recovery is not an exporter outage.
            code = 200 if fresh else 503
            body = json.dumps({"healthy": fresh, "all_targets_up": snapshot.healthy}).encode()
        else:
            code, body = 404, b'{"error":"not_found"}'
        self.send_response(code)
        self.send_header("Content-Type", content)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError, TimeoutError):
            pass


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--plan", type=Path, action="append", default=[])
    parser.add_argument("--recovery-state", type=Path)
    parser.add_argument("--gateway-url")
    parser.add_argument("--gateway-key-file", type=Path)
    parser.add_argument("--bind", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=9842)
    parser.add_argument("--interval", type=float, default=10)
    parser.add_argument("--timeout", type=float, default=8)
    parser.add_argument("--once", action="store_true", help="collect once without listening; output metrics (exit 1 for any failed target)")
    args = parser.parse_args(argv)
    if not 0 <= args.port <= 65535:
        parser.error("port out of range")
    collector = Collector(read(args.inventory), [read(path) for path in args.plan],
        timeout=args.timeout, interval=args.interval, recovery_state=args.recovery_state,
        gateway_url=args.gateway_url, gateway_key=private_key(args.gateway_key_file) if args.gateway_key_file else None)
    if args.once:
        snapshot = collector.collect()
        print(snapshot.metrics, end="")
        return 0 if snapshot.healthy else 1
    server = MonitorServer((args.bind, args.port), collector)
    stopping = threading.Event()
    def stop(signum, frame):
        if not stopping.is_set():
            stopping.set()
            threading.Thread(target=server.shutdown, daemon=True).start()
    previous = {sig: signal.signal(sig, stop) for sig in (signal.SIGINT, signal.SIGTERM)}
    collector.start()
    try:
        server.serve_forever(poll_interval=.2)
    finally:
        collector.stop()
        server.server_close()
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError):
        # Configuration exceptions may contain private paths or supplied secret values.
        raise SystemExit("spark-monitor: invalid configuration or inaccessible input") from None
