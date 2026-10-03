"""Single-authority, closed-default recovery of trusted saved Spark deployments.

No automatic host takeover: preserve this directory and its authority/highwaters.
The policy ingress must be isolated before approved qualification or activation.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import copy
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import sys
import time
from urllib.parse import urlparse
import uuid

from . import cli, config, gateway, recovery_routes
from .config import ConfigError, fields, require

DEFAULT_TIMINGS = {
    "heartbeat": 5, "lease": 15, "observe": 3, "stable_return": 120,
    "minimum_dwell": 300, "drain": 120, "startup": 900,
    "backoff_initial": 30, "backoff_max": 600, "circuit_failures": 3,
    "prepare_interval": 60, "qualification_max_age": 2592000,
}
CHECKS = {"text", "tool_arguments", "tool_continuation", "sse_complete", "endpoint_identity", "independent_serving"}


def digest_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def hash_value(value):
    return hashlib.sha256(config.canonical(value).encode()).hexdigest()


def sha(value):
    require(isinstance(value, str) and re.fullmatch(r"[a-f0-9]{64}", value), "expected trusted SHA-256")


def reference(value, base, *, plan=False, fallback=False):
    fields(value, ("path", "sha256", "qualification") if plan else ("path", "sha256"), ("node",) if fallback else ())
    if fallback:
        require("node" in value, "fallback requires a node")
        config.name(value["node"])
    sha(value["sha256"])
    require(isinstance(value["path"], str) and value["path"], "reference path is required")
    result = {**value, "path": str((base / value["path"]).resolve())}
    if plan:
        load_plan(result)
        if value["qualification"] is not None:
            result["qualification"] = reference(value["qualification"], base)
            load_artifact(result["qualification"])
    return result


def load_artifact(ref):
    require(digest_file(ref["path"]) == ref["sha256"], "artifact SHA-256 mismatch")
    return config.read(ref["path"])


def load_plan(ref):
    p = config.read(ref["path"])
    config.validate_saved_plan(p, ref["sha256"])
    return p


def route_for(p):
    return gateway.from_plans([p])["routes"][p["recipe"]["alias"]]


def model_contract(p):
    return p["recipe"]


def load_policy(path):
    """Read strict policy; resolve references without requiring installed gateway files.

    Returns a JSON-compatible dict. load_plan(policy['preferred']) reads the exact
    trusted plan. Receipt absence is valid preparation, never activation approval.
    """
    path = Path(path).resolve()
    value = config.read(path)
    fields(value, ("version", "id", "alias", "preferred", "fallbacks", "management", "gateway", "timings"))
    require(type(value["version"]) is int and value["version"] == 1, "unsupported recovery policy")
    config.name(value["id"])
    require(value["alias"] == "local-auto", "only explicit local-auto may substitute models")
    result = copy.deepcopy(value)
    result["preferred"] = reference(value["preferred"], path.parent, plan=True)
    preferred = load_plan(result["preferred"])
    require(preferred["deployment"]["mode"] != "single" and len(preferred["nodes"]) >= 2, "preferred must be a distributed saved plan")
    require(isinstance(value["fallbacks"], list) and value["fallbacks"], "ordered per-node fallbacks required")
    result["fallbacks"] = [reference(r, path.parent, plan=True, fallback=True) for r in value["fallbacks"]]
    require(len({r["node"] for r in result["fallbacks"]}) == len(result["fallbacks"]), "duplicate fallback node")
    contracts = {}
    for ref in [result["preferred"], *result["fallbacks"]]:
        p = load_plan(ref)
        require(p["recipe"]["alias"] != "local-auto", "saved aliases must remain strict")
        alias = p["recipe"]["alias"]
        require(alias not in contracts or contracts[alias] == model_contract(p), "same strict alias changes model contract")
        contracts[alias] = model_contract(p)
        route_for(p)
        require(all(p["recipe"]["capabilities"].get(k) is True for k in ("text", "tools", "streaming")), "agent recovery requires text, tools and streaming")
        require(p["endpoint"]["capabilities"] == p["recipe"]["capabilities"] and
                p["endpoint"]["alias"] == alias and p["endpoint"]["context_tokens"] == p["recipe"]["context_tokens"] and
                p["endpoint"]["max_output_tokens"] == p["recipe"]["max_output_tokens"], "endpoint contract differs from pinned recipe")
        if "node" in ref:
            node = ref["node"]
            require(p["deployment"]["mode"] == "single" and set(p["nodes"]) == {node} and node in preferred["nodes"], "fallback membership mismatch")
            require({k: v for k, v in p["nodes"][node].items() if k != "serving"} ==
                    {k: v for k, v in preferred["nodes"][node].items() if k != "serving"},
                    "fallback changes trusted node identity, cache or collective topology")
            require("serving" not in preferred["nodes"][node] or
                    p["nodes"][node].get("serving") == preferred["nodes"][node]["serving"],
                    "fallback changes an explicit preferred serving binding")
            require("serving" in p["nodes"][node] and p["nodes"][node]["serving"]["interface"] not in {r["interface"] for r in p["nodes"][node]["fabric"]}, "fallback needs independent serving interface")
        coordinator = p["nodes"][p["deployment"]["coordinator"]]
        require(p["endpoint"]["base_url"] == f"http://{config.serving_address(coordinator)}:{p['deployment']['port']}/v1", "endpoint binding mismatch")
        if ref["qualification"] is not None:
            validate_qualification(load_artifact(ref["qualification"]), result, ref, check_age=False)
    fields(value["management"], (), tuple(preferred["nodes"]))
    require(set(value["management"]) == set(preferred["nodes"]), "every node needs a trusted independent management transport")
    for node, transport in value["management"].items():
        cli.transport_node(preferred["nodes"][node], transport)
    g = result["gateway"]
    fields(g, ("url", "key_file", "route_state", "registry", "isolation"))
    u = urlparse(g["url"])
    require(u.scheme in ("http", "https") and u.hostname and u.path in ("", "/") and not (u.username or u.password or u.query or u.fragment), "gateway URL must be an origin")
    g["url"] = g["url"].rstrip("/")
    for key in ("key_file", "route_state", "registry"):
        require(isinstance(g[key], str) and g[key], "gateway path required")
        g[key] = str((path.parent / g[key]).resolve())
    require(len({g[k] for k in ("key_file", "route_state", "registry")}) == 3, "gateway files must be distinct")
    if g["isolation"] is not None:
        g["isolation"] = reference(g["isolation"], path.parent)
        load_artifact(g["isolation"])
    fields(value["timings"], (), tuple(DEFAULT_TIMINGS))
    result["timings"] = t = {**DEFAULT_TIMINGS, **value["timings"]}
    for key, number in t.items():
        require(type(number) in (int, float) and math.isfinite(number) and 0 < number <= 31536000, f"invalid timing {key}")
    require(0 < t["observe"] < t["heartbeat"] and 2 * t["heartbeat"] < t["lease"] <= 60, "observation/heartbeat/lease bounds invalid")
    require(t["stable_return"] >= 2 * t["heartbeat"] and t["minimum_dwell"] >= t["stable_return"], "return hysteresis invalid")
    require(t["drain"] <= 3600 and t["startup"] <= 7200 and t["backoff_initial"] <= t["backoff_max"] <= 86400, "operation bounds invalid")
    require(type(t["circuit_failures"]) is int and 1 <= t["circuit_failures"] <= 100, "invalid circuit threshold")
    return result


def validate_qualification(receipt, policy, ref, *, now=None, check_age=True):
    fields(receipt, ("version", "policy", "plan_sha256", "deployment_digest", "nodes", "gateway_url", "route", "checks", "completed_at", "staging"))
    p = load_plan(ref)
    require(type(receipt["version"]) is int and receipt["version"] == 1 and receipt["policy"] == policy["id"], "qualification identity mismatch")
    require(receipt["plan_sha256"] == ref["sha256"] and receipt["deployment_digest"] == p["digest"] and receipt["nodes"] == sorted(p["nodes"]), "qualification belongs to another exact plan/node")
    require(receipt["gateway_url"].rstrip("/") == policy["gateway"]["url"].rstrip("/") and receipt["route"] == route_for(p), "qualification endpoint/contract mismatch")
    fields(receipt["checks"], tuple(CHECKS))
    require(all(receipt["checks"][k] is True for k in CHECKS - {"independent_serving"}), "qualification incomplete")
    independent = independent_serving(p)
    independent_check = receipt["checks"]["independent_serving"]
    require((independent and independent_check is True) or
            (not independent and p["deployment"]["mode"] != "single" and independent_check == "not-applicable-distributed"),
            "single qualification requires independent serving evidence")
    require(type(receipt["completed_at"]) in (int, float) and math.isfinite(receipt["completed_at"]) and receipt["completed_at"] > 0, "invalid qualification time")
    fields(receipt["staging"], tuple(p["nodes"]))
    for artifact in receipt["staging"].values():
        fields(artifact, ("path", "sha256"))
        require(Path(artifact["path"]).is_absolute(), "staging references must be absolute")
        load_artifact(artifact)
    if check_age:
        age = (time.time() if now is None else now) - receipt["completed_at"]
        require(0 <= age <= policy["timings"]["qualification_max_age"], "qualification expired or from future")
    return receipt


def eligible(policy, ref, now):
    if ref["qualification"] is None:
        return False
    try:
        validate_qualification(load_artifact(ref["qualification"]), policy, ref, now=now)
        return True
    except (ValueError, OSError, KeyError, TypeError):
        return False


def private_directory(path):
    path = Path(path)
    require(not any(p.is_symlink() for p in (path, *path.parents)), "managed state must not traverse symlinks")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077, "state directory must be private and owned, never a symlink")
    return path


@contextmanager
def lock(path, *, blocking=False):
    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077, "unsafe authority lock")
        fcntl.flock(fd, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        yield
    finally:
        os.close(fd)


class Journal:
    def __init__(self, directory, policy, *, clock=time.time):
        self.directory = private_directory(directory)
        self.path = self.directory / "journal.json"
        self.clock = clock
        self.policy = policy
        if self.path.exists():
            self.value = recovery_routes.private_json(self.path)
            s = self.value
            require(isinstance(s, dict) and s.get("version") == 1 and s.get("policy_hash") == hash_value(policy), "journal corrupt or policy changed: explicit disabled init adoption required")
            require(s.get("policy") == policy["id"] and recovery_routes.identifier(s.get("authority")), "journal authority mismatch")
            for k in ("epoch", "generation", "retry_count"):
                require(type(s.get(k)) is int and s[k] >= 0, "invalid journal highwater")
            require(type(s.get("enabled")) is bool and type(s.get("paused")) is bool and isinstance(s.get("quarantine"), dict) and isinstance(s.get("events"), dict), "invalid journal state")
            for k in ("phase_since", "retry_at", "active_since", "last_transition_seconds", "updated_at"):
                require(type(s.get(k)) in (int, float) and math.isfinite(s[k]) and s[k] >= 0, "invalid journal timestamp")
            require(s.get("stable_since") is None or (type(s["stable_since"]) in (int, float) and math.isfinite(s["stable_since"]) and s["stable_since"] > 0), "invalid stable return time")
            require(type(s.get("circuit_open")) is bool and isinstance(s.get("commands"), dict) and isinstance(s.get("receipts"), dict), "invalid journal controls")
            require(isinstance(s.get("phase"), str) and re.fullmatch(r"[a-z-]{1,64}", s["phase"]), "invalid journal phase")
            for ref in (s.get("preferred"), s.get("current"), s.get("selected_fallback")):
                if ref is not None:
                    load_plan(ref)
            require(s.get("preferred") is not None, "journal preferred plan missing")
            if s.get("route") is not None:
                recovery_routes.validate_state(s["route"], gateway.validate_registry)
                require(s["route"]["authority"] == s["authority"] and s["route"]["policy"] == s["policy"] and s["route"]["generation"] == s["generation"], "journal route/highwater mismatch")
            intent = s.get("intent")
            if intent is not None:
                fields(intent, ("id", "target", "previous", "voluntary", "stage", "started_at", "rollback"), ("rollback_pending",))
                require(recovery_routes.identifier(intent["id"]) and intent["stage"] in ("drain", "stop", "start", "open") and
                        type(intent["voluntary"]) is bool and type(intent["rollback"]) is bool, "invalid recovery intent")
                load_plan(intent["target"])
                if intent["previous"] is not None:
                    load_plan(intent["previous"])
        else:
            # Existing route/highwater files without the journal are NOT a new authority.
            require(not Path(policy["gateway"]["route_state"]).exists() and not Path(policy["gateway"]["route_state"] + ".fence.json").exists(), "missing authority journal with existing gateway history; cold fenced handoff required")
            self.value = {"version": 1, "policy": policy["id"], "policy_hash": hash_value(policy), "authority": str(uuid.uuid4()),
                          "epoch": 0, "generation": 0, "enabled": False, "paused": False, "phase": "disabled", "phase_since": clock(),
                          "intent": None, "current": None, "preferred": policy["preferred"], "selected_fallback": None,
                          "retry_count": 0, "retry_at": 0, "circuit_open": False, "quarantine": {}, "events": {},
                          "stable_since": None, "active_since": 0, "last_transition_seconds": 0, "route": None,
                          "receipts": {}, "commands": {}, "ownership": {}, "updated_at": clock()}
            self.save()

    def save(self):
        self.value["updated_at"] = self.clock()
        recovery_routes.atomic_json(self.path, self.value)
        s = self.value
        recovery_routes.atomic_json(self.directory / "status.json", {
            "version": 1, "policy": s["policy"], "phase": s["phase"], "generation": s["generation"], "epoch": s["epoch"],
            "updated_at": s["updated_at"], "phase_since": s["phase_since"], "retry_count": s["retry_count"],
            "retry_at": s["retry_at"], "circuit_open": s["circuit_open"], "quarantined_nodes": sorted(s["quarantine"]),
            "event_counts": s["events"], "last_transition_seconds": s["last_transition_seconds"],
            "selected_plan_digest": load_plan(s["current"])["digest"] if s["current"] else None})

    def phase(self, phase):
        if self.value["phase"] != phase:
            self.value["phase"] = phase
            self.value["phase_since"] = self.clock()
            self.value["events"][phase] = self.value["events"].get(phase, 0) + 1
        self.save()

    def receipt(self, key, value):
        recovery_routes.atomic_json(self.directory / "history" / (str(uuid.uuid4()) + ".json"),
                                    {"event": key, "at": self.clock(), "epoch": self.value["epoch"],
                                     "generation": self.value["generation"], "receipt": value})
        self.value["receipts"][key] = value
        self.save()


def queue_command(directory, policy, command, *, preferred=None):
    directory = private_directory(directory)
    require((directory / "journal.json").exists(), "initialize authority before controls")
    request_id = str(uuid.uuid4())
    with lock(directory / "commands.lock", blocking=True):
        queue = directory / "commands.json"
        pending = recovery_routes.private_json(queue) if queue.exists() else []
        require(isinstance(pending, list) and len(pending) < 256, "control queue full or corrupt")
        pending.append({"id": request_id, "policy_hash": hash_value(policy), "command": command, "preferred": preferred})
        recovery_routes.atomic_json(queue, pending)
    return request_id


class RealAdapter:
    """The only remote/inference boundary. No subprocess shell or retries of mutations."""
    def __init__(self, policy, directory):
        self.policy = policy
        self.directory = Path(directory)

    def observe(self):
        p = load_plan(self.policy["preferred"])
        nodes = copy.deepcopy(p["nodes"])
        for ref in self.policy["fallbacks"]:
            fallback = load_plan(ref)
            nodes[ref["node"]]["serving"] = fallback["nodes"][ref["node"]]["serving"]
        return cli.observe(nodes, timeout=self.policy["timings"]["observe"], transports=self.policy["management"])["nodes"]

    def invoke(self, ref, node, action, context):
        p = load_plan(ref)
        return cli.call(p, node, action, recovery=context, timeout=min(240, self.policy["timings"]["startup"]), transport=self.policy["management"][node])

    def fence(self, ref, node, context, digests):
        p = load_plan(ref)
        request = cli.request(p, node, "fence", recovery=context)
        request["allowed_digests"] = digests
        return cli.remote(cli.transport_node(p["nodes"][node], self.policy["management"][node]), request, timeout=self.policy["timings"]["observe"])

    def start(self, ref, context):
        return cli.up(load_plan(ref), self.policy["timings"]["startup"], self.directory / "attempts" / context["operation_id"] / ref["sha256"],
                      recovery=context, transports=self.policy["management"], expected_sha256=ref["sha256"], saved_plan=True)

    def prepare(self, ref):
        p = load_plan(ref)
        for node in p["nodes"]:
            result = cli.call(p, node, "preflight", timeout=self.policy["timings"]["observe"], transport=self.policy["management"][node])
            checks = {v["check"]: v["passed"] for v in result["checks"]}
            required = {"runtime-image", "model-cache", "host-inspection"}
            if p["deployment"]["mode"] != "single":
                required.add("rdma-devices")
            if p["recipe"].get("nccl_library"):
                required.add("nccl-library")
            if p["recipe"].get("deepseek_v4", {}).get("source_overlays") or p["recipe"].get("glm53", {}).get("source_overlays"):
                required.add("source-overlays")
            if p["recipe"].get("speculative_config", {}).get("method") == "dflash":
                required.add("draft-cache")
            if not all(checks.get(k) is True for k in required):
                return False
        return True

    def session(self):
        import requests
        key = Path(self.policy["gateway"]["key_file"])
        info = key.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077, "gateway key must be private regular file")
        secret = key.read_text().strip()
        require(len(secret) >= 24 and not any(c.isspace() for c in secret), "invalid gateway key")
        session = requests.Session()
        session.trust_env = False
        session.headers["Authorization"] = "Bearer " + secret
        return session

    def gateway_status(self):
        with self.session() as session:
            response = session.get(self.policy["gateway"]["url"] + "/_spark/recovery", timeout=self.policy["timings"]["observe"])
            response.raise_for_status()
            return response.json()

    def endpoint(self, p):
        with self.session() as session:
            response = session.get(self.policy["gateway"]["url"] + "/v1/models", timeout=self.policy["timings"]["observe"])
            response.raise_for_status()
            matches = [v for v in response.json()["data"] if v["id"] == "local-auto"]
            if len(matches) != 1:
                return False
            m = matches[0]
            route = route_for(p)
            return (all(m.get(k) == route[k] for k in ("deployment_digest", "model_root", "max_output_tokens", "capabilities")) and
                    m.get("context_length") == route["context_tokens"] and m.get("backend_alias") == p["recipe"]["alias"])

    def isolation(self, refs):
        g = self.policy["gateway"]
        require(g["isolation"] is not None, "explicit independently pinned ingress isolation receipt required")
        r = load_artifact(g["isolation"])
        fields(r, ("version", "policy", "gateway_url", "registry", "route_state", "plan_sha256", "exclusive_ingress", "legacy_endpoints_blocked", "independent_management", "independent_serving", "approved_by", "evidence"))
        require(type(r["version"]) is int and r["version"] == 1 and r["policy"] == self.policy["id"], "isolation identity mismatch")
        require(r["gateway_url"] == g["url"] and r["registry"] == g["registry"] and r["route_state"] == g["route_state"], "isolation is for another ingress")
        require(set(r["plan_sha256"]) >= {v["sha256"] for v in refs}, "isolation does not cover all exact plans")
        require(all(r[k] is True for k in ("exclusive_ingress", "legacy_endpoints_blocked", "independent_management", "independent_serving")), "unfenced bypass or dependent management/serving")
        require(isinstance(r["approved_by"], str) and r["approved_by"].strip() and isinstance(r["evidence"], list) and r["evidence"], "physical isolation approval and evidence required")
        for evidence in r["evidence"]:
            fields(evidence, ("path", "sha256"))
            require(Path(evidence["path"]).is_absolute(), "isolation evidence references must be absolute")
            load_artifact(evidence)
        registry = recovery_routes.private_json(g["registry"])
        gateway.validate_registry(registry)
        allowed = [route_for(load_plan(ref)) for ref in refs]
        require(registry["routes"] and all(alias != "local-auto" and route in allowed and alias == route["upstream_model"] for alias, route in registry["routes"].items()), "dedicated ingress registry contains unapproved routes/providers")
        # Authenticated recovery endpoint proves this is a policy-aware ingress.
        self.gateway_status()
        return r


def reached(observation, node):
    return (isinstance(observation, dict) and observation.get("hostname") == node["hostname"] and
            observation.get("architecture") == node["architecture"] and
            all(k in observation and observation[k] is not None for k in ("containers", "gpu_containers", "gpu_processes")) and
            "reservation" in observation and "recovery_fence" in observation and
            not set(observation.get("errors", {})) & {"reservation", "recovery_fence", "containers", "gpu_processes"})


def network_ready(p, observations, *, fabric=False):
    for node, identity in p["nodes"].items():
        o = observations.get(node)
        if not reached(o, identity):
            return False
        serving = o.get("serving") if "serving" in identity else (o.get("fabric") or [None])[0]
        if not serving or serving.get("ready") is not True:
            return False
        if fabric and (len(o.get("fabric", [])) != len(identity["fabric"]) or
                       not all(r.get("ready") is True for r in o["fabric"])):
            return False
    return True


def healthy(p, observations):
    if not network_ready(p, observations, fabric=p["deployment"]["mode"] != "single"):
        return False
    for node in p["nodes"]:
        o = observations[node]
        r = o["reservation"]
        containers = o["gpu_containers"]
        if (not r or r.get("owner") != p["owner"] or r.get("digest") != p["digest"] or r.get("phase") != "started" or
                len(containers) != len(p["compose"][node]["services"]) or
                {c.get("id") for c in containers} != set(r.get("container_ids", []))):
            return False
        if not all(c.get("owner") == p["owner"] and c.get("digest") == p["digest"] and
                   c.get("state") == "running" and c.get("health") == "healthy" and
                   c.get("image") == p["recipe"]["image"] and c.get("image_id") and
                   c.get("started_at") and type(c.get("restart_count")) is int for c in containers):
            return False
    return True


class StaleObservation(RuntimeError):
    """Observation evidence expired before it could authorize admission."""


class Controller:
    def __init__(self, policy, journal, adapter, *, wall=time.time, monotonic=time.monotonic, sleep=time.sleep):
        self.policy, self.journal, self.adapter = policy, journal, adapter
        self.s = journal.value
        self.wall, self.monotonic, self.sleep = wall, monotonic, sleep
        self.t = policy["timings"]
        self.boot = True
        self.last_wall, self.last_mono = wall(), monotonic()
        self.prepared_at = None
        self.prepared = False
        self.extra_refs = []
        self.observation_started = None
        route_path = Path(policy["gateway"]["route_state"])
        if route_path.exists():
            route = recovery_routes.private_json(route_path)
            require(route.get("authority") == self.s["authority"] and route.get("policy") == policy["id"] and
                    type(route.get("generation")) is int and route["generation"] <= self.s["generation"],
                    "foreign or unjournaled route highwater")

    def refs(self):
        refs = [self.s["preferred"], *self.policy["fallbacks"], *self.extra_refs]
        for ref in (self.policy["preferred"], self.s["current"]):
            if ref and ref not in refs:
                refs.append(ref)
        if self.s["intent"]:
            for ref in (self.s["intent"]["target"], self.s["intent"]["previous"]):
                if ref and ref not in refs:
                    refs.append(ref)
        return refs

    def context(self):
        return {"policy": self.policy["id"], "authority": self.s["authority"], "generation": self.s["epoch"],
                "operation_id": self.s["intent"]["id"] if self.s["intent"] else str(uuid.uuid4())}

    def advance_epoch(self):
        self.s["epoch"] += 1
        self.journal.save()

    def publish(self, ref, accepting, reason):
        if accepting:
            self.require_fresh_observation()
        route = route_for(load_plan(ref)) if ref else None
        mode = ("preferred" if ref["sha256"] == self.s["preferred"]["sha256"] else "fallback") if ref else "unavailable"
        previous = self.s["route"]
        now = self.wall()
        if previous and now < previous["issued_at"]:
            # Do not forge future leases after a wall-clock regression.
            if accepting:
                raise RuntimeError("clock must pass persisted heartbeat before reopening")
            now = max(now, 0.001)
        contract = {"version": 1, "policy": self.policy["id"], "authority": self.s["authority"],
                    "alias": "local-auto", "accepting": accepting, "route": route, "mode": mode}
        changed = not previous or any(previous[k] != v for k, v in contract.items())
        if changed or (previous and now < previous["issued_at"]):
            self.s["generation"] += 1
        value = {**contract, "generation": self.s["generation"], "reason": reason,
                 "issued_at": now, "expires_at": now + self.t["lease"]}
        self.s["route"] = value
        # The journal's highwater is durable before the externally visible route.
        self.journal.save()
        if accepting:
            registry_path = self.policy["gateway"]["registry"]
            registry = recovery_routes.private_json(registry_path)
            gateway.validate_registry(registry)
            allowed = [route_for(load_plan(r)) for r in self.refs()]
            require(all(alias == item["upstream_model"] and item in allowed for alias, item in registry["routes"].items()),
                    "dedicated registry acquired foreign binding")
            registry["routes"][route["upstream_model"]] = route
            recovery_routes.atomic_json(registry_path, registry)
        recovery_routes.write_state(self.policy["gateway"]["route_state"], value, gateway.validate_registry)
        return value

    def ack(self, *, drained=False):
        status = self.adapter.gateway_status()
        return (status.get("policy") == self.policy["id"] and status.get("authority") == self.s["authority"] and
                status.get("generation") == self.s["generation"] and
                status.get("accepting") == self.s["route"]["accepting"] and
                (not drained or (status.get("accepting") is False and status.get("active_requests") == 0)))

    def drain(self, reason):
        self.publish(self.s["current"], False, reason)
        deadline = self.monotonic() + self.t["drain"]
        while True:
            try:
                if self.ack(drained=True):
                    return True
            except Exception:
                pass
            if self.monotonic() >= deadline:
                return False
            self.sleep(min(self.t["heartbeat"], max(0, deadline - self.monotonic())))
            self.publish(self.s["current"], False, reason)

    def require_fresh_observation(self):
        # Wall time catches suspend on platforms whose monotonic clock pauses.
        # Keep the acquisition deadline through fencing and lease publication.
        if self.observation_started is not None:
            wall, mono = self.observation_started
            wall_elapsed, mono_elapsed = self.wall() - wall, self.monotonic() - mono
            bound = self.t["observe"] + self.t["heartbeat"]
            if (0 <= wall_elapsed <= bound and 0 <= mono_elapsed <= bound and
                    abs(wall_elapsed - mono_elapsed) <= self.t["heartbeat"]):
                return
        self.publish(self.s["current"], False, "stalled-observation")
        self.s["stable_since"] = None
        self.journal.phase("unavailable")
        raise StaleObservation("fresh observation required before admission")

    def observations(self):
        self.observation_started = self.wall(), self.monotonic()
        result = self.adapter.observe()
        self.require_fresh_observation()
        preferred = load_plan(self.s["preferred"])
        changed = False
        for node, identity in preferred["nodes"].items():
            o = result.get(node)
            if not reached(o, identity):
                if node not in self.s["quarantine"]:
                    self.s["quarantine"][node] = {"since": self.wall(), "epoch": self.s["epoch"], "reason": "unreachable-ownership-retained",
                                                 "last_known": self.s.get("ownership", {}).get(node)}
                    changed = True
                continue
            fence = o.get("recovery_fence")
            if fence:
                require(fence.get("policy") == self.policy["id"] and fence.get("authority") == self.s["authority"],
                        "competing node authority requires explicit cold handoff")
                require(type(fence.get("generation")) is int and fence["generation"] <= self.s["epoch"], "node epoch exceeds durable journal")
            r = o["reservation"]
            matches = [load_plan(ref) for ref in self.refs() if node in load_plan(ref)["nodes"]]
            require(not r or any(r.get("owner") == p["owner"] and r.get("digest") == p["digest"] for p in matches), "foreign ownership; never stop it")
            if r:
                ids = set(r.get("container_ids", []))
                require(r.get("phase") in ("reserved", "creating", "started"), "unsupported reservation phase")
                for c in o["gpu_containers"]:
                    require(c.get("owner") == r.get("owner") and c.get("digest") == r.get("digest") and
                            (c.get("id") in ids or (not ids and r["phase"] == "creating")), "container identity replaced or foreign")
            else:
                require(not o["gpu_containers"] and not o["gpu_processes"], "unowned GPU occupancy")
            self.s.setdefault("ownership", {})[node] = {"observed_at": self.wall(), "reservation": r,
                                                       "containers": o["gpu_containers"], "recovery_fence": fence}
            changed = True
        if changed:
            self.journal.save()
        self.require_fresh_observation()
        return result

    def stop_owned(self, observations):
        """Only durable, exact live ownership; dead ranks retain their tombstones."""
        allowed = sorted({load_plan(ref)["digest"] for ref in self.refs()})
        preferred = load_plan(self.s["preferred"])
        for node, identity in preferred["nodes"].items():
            o = observations.get(node)
            if not reached(o, identity):
                continue
            ref = next((r for r in self.refs() if node in load_plan(r)["nodes"]), None)
            self.journal.receipt("before-fence:" + node, {"epoch": self.s["epoch"], "reservation": o["reservation"], "containers": o["gpu_containers"]})
            receipt = self.adapter.fence(ref, node, self.context(), allowed)
            self.journal.receipt("fence:" + node, receipt)
            reservation = o["reservation"]
            if reservation:
                owned_ref = next(r for r in self.refs() if load_plan(r)["owner"] == reservation["owner"] and load_plan(r)["digest"] == reservation["digest"])
                self.journal.receipt("before-stop:" + node, reservation)
                receipt = self.adapter.invoke(owned_ref, node, "stop", self.context())
                self.journal.receipt("stop:" + node, receipt)
            # A responding node is removed from quarantine only after exact cleanup.
            fresh = self.observations().get(node)
            require(reached(fresh, identity) and fresh["reservation"] is None and not fresh["gpu_containers"] and not fresh["gpu_processes"], "GPU not proven idle after exact stop")
            self.s["quarantine"].pop(node, None)
            self.journal.save()

    def failure(self, reason):
        self.s["retry_count"] += 1
        self.s["retry_at"] = self.wall() + min(self.t["backoff_max"], self.t["backoff_initial"] * 2 ** min(30, self.s["retry_count"] - 1))
        self.s["circuit_open"] = self.s["retry_count"] >= self.t["circuit_failures"]
        self.s["stable_since"] = None
        self.journal.phase(reason)

    def open_healthy(self, ref, observations, *, reset_retry=False):
        require(eligible(self.policy, ref, self.wall()) and healthy(load_plan(ref), observations), "refusing unhealthy or unqualified route")
        for node in load_plan(ref)["nodes"]:
            fence = observations[node].get("recovery_fence")
            if not fence or not fence.get("active") or fence.get("generation") != self.s["epoch"]:
                self.journal.receipt("reopen-fence:" + node,
                                     self.adapter.fence(ref, node, self.context(), sorted({load_plan(r)["digest"] for r in self.refs()})))
        try:
            self.publish(ref, True, "reconciled")
        except StaleObservation:
            return False
        try:
            acknowledged = self.ack() and self.adapter.endpoint(load_plan(ref))
        except Exception:
            acknowledged = False
        if not acknowledged:
            self.publish(ref, False, "gateway-unavailable")
            self.journal.phase("gateway-unavailable")
            return False
        if not self.s["current"] or self.s["current"]["sha256"] != ref["sha256"]:
            self.s["active_since"] = self.wall()
        self.s["current"] = ref
        if ref["sha256"] != self.s["preferred"]["sha256"]:
            self.s["selected_fallback"] = ref
        if reset_retry:
            self.s["retry_count"], self.s["retry_at"], self.s["circuit_open"] = 0, 0, False
        self.journal.phase("preferred" if ref["sha256"] == self.s["preferred"]["sha256"] else "fallback")
        return True

    def begin(self, target, *, voluntary=False):
        require(eligible(self.policy, target, self.wall()), "target lacks current qualification")
        self.s["intent"] = {"id": str(uuid.uuid4()), "target": target, "previous": self.s["current"],
                            "voluntary": voluntary, "stage": "drain", "started_at": self.wall(), "rollback": False}
        self.journal.phase("draining")
        self.transition()

    def transition(self):
        intent = self.s["intent"]
        target = intent["target"]
        if intent["stage"] == "drain":
            if not self.drain("transition-drain"):
                previous = intent["previous"]
                self.failure("drain-timeout")
                # No worker mutation has happened. Voluntary abort may reopen the
                # exact still-healthy fallback, and retries survive a restart.
                self.s["intent"] = None
                self.journal.save()
                if previous:
                    try:
                        observations = self.observations()
                    except StaleObservation:
                        return
                    if healthy(load_plan(previous), observations):
                        self.open_healthy(previous, observations)
                return
            intent["stage"] = "stop"
            self.advance_epoch()
        try:
            if intent["stage"] == "stop":
                # Every stop, including rollback after an attempted route-open,
                # needs a fresh global drain. Persisted earlier proof is stale.
                require(self.drain("mutation-drain"), "no fresh global drain proof")
                self.journal.phase("stopping")
                self.stop_owned(self.observations())
                intent["stage"] = "start"
                self.journal.phase("starting")
            if intent["stage"] == "start":
                observations = self.observations()
                # Re-fence even a healthy completed start after response loss:
                # delayed cleanup from an older epoch cannot stop its workers.
                require(network_ready(load_plan(target), observations, fabric=load_plan(target)["deployment"]["mode"] != "single"), "target network unavailable")
                for node in load_plan(target)["nodes"]:
                    self.journal.receipt("start-fence:" + node, self.adapter.fence(target, node, self.context(), sorted({load_plan(r)["digest"] for r in self.refs()})))
                if not healthy(load_plan(target), observations):
                    for node in load_plan(target)["nodes"]:
                        o = observations[node]
                        require(o["reservation"] is None and not o["gpu_containers"] and not o["gpu_processes"], "partial start requires reconciliation before retry")
                    self.journal.receipt("before-start", {"plan_sha256": target["sha256"], "epoch": self.s["epoch"], "operation_id": intent["id"]})
                    self.require_fresh_observation()
                    result = self.adapter.start(target, self.context())
                    self.journal.receipt("start", result)
                    observations = self.observations()
                require(healthy(load_plan(target), observations), "startup readiness not proven")
                intent["stage"] = "open"
                self.journal.save()
            if intent["stage"] == "open":
                observations = self.observations()
                if self.open_healthy(target, observations, reset_retry=not intent["rollback"] and target["sha256"] == self.s["preferred"]["sha256"]):
                    self.s["last_transition_seconds"] = max(0, self.wall() - intent["started_at"])
                    self.s["intent"] = None
                    self.journal.save()
        except StaleObservation:
            # Preserve the exact pending stage; a fresh cycle reconciles it.
            return
        except Exception as exc:
            # Persist failure BEFORE cleanup; a lost response never triggers a
            # blind repetition. Next cycle freshly reconciles durable ownership.
            self.journal.receipt("transition-error", {"type": type(exc).__name__, "stage": intent["stage"]})
            self.publish(None, False, "transition-failed")
            if isinstance(exc, cli.AmbiguousMutationError):
                # A fresh observation decides whether start completed, cleanup
                # completed, or a partial exact attempt needs rollback. Never
                # repeat a dispatched mutation merely because its reply was lost.
                intent["stage"] = "start" if intent["stage"] == "start" else "stop"
                intent["rollback_pending"] = True
                self.advance_epoch()
                return
            self.failure("rollback" if intent["previous"] and not intent["rollback"] else "unavailable")
            previous = intent["previous"]
            fallback = (previous if previous and (intent["voluntary"] or load_plan(previous)["deployment"]["mode"] == "single")
                        else self.s["selected_fallback"])
            if fallback and fallback["sha256"] != target["sha256"] and eligible(self.policy, fallback, self.wall()) and not intent["rollback"]:
                intent["target"], intent["rollback"], intent["stage"] = fallback, True, "stop"
                intent["rollback_pending"] = True
            else:
                # Retain the exact failed intent for safe cleanup after backoff.
                intent["stage"] = "stop"
                intent["rollback_pending"] = False
            self.advance_epoch()

    def process_commands(self):
        queue = self.journal.directory / "commands.json"
        with lock(self.journal.directory / "commands.lock", blocking=True):
            commands = recovery_routes.private_json(queue) if queue.exists() else []
        for command in commands:
            request_id = command["id"]
            if request_id in self.s["commands"]:
                continue
            require(command["policy_hash"] == self.s["policy_hash"], "queued policy hash mismatch")
            action = command["command"]
            try:
                if action in ("enable", "resume"):
                    require(all(eligible(self.policy, r, self.wall()) for r in [self.s["preferred"], *self.policy["fallbacks"]]), "all selected plans need per-node current qualification")
                    self.journal.receipt("isolation", self.adapter.isolation(self.refs()))
                    self.s["enabled"], self.s["paused"] = True, False
                elif action in ("pause", "disable"):
                    if action == "pause":
                        self.s["paused"] = True
                    else:
                        self.s["enabled"] = False
                    # Journal desired control before waiting for external ACK.
                    self.journal.save()
                    drained = self.drain(action)
                    self.journal.phase(("paused" if action == "pause" else "disabled") if drained else action + "-draining")
                elif action == "clear-circuit":
                    self.s["circuit_open"], self.s["retry_count"], self.s["retry_at"] = False, 0, 0
                elif action == "reset":
                    self.s["enabled"], self.s["paused"] = False, False
                    self.journal.save()
                    require(self.drain("reset"), "reset cannot interrupt undrained requests")
                    self.advance_epoch()
                    self.stop_owned(self.observations())
                    self.s["current"], self.s["intent"], self.s["selected_fallback"] = None, None, None
                    self.s["stable_since"] = None
                    self.publish(None, False, "reset")
                    self.journal.phase("disabled")
                elif action == "switch-coordinator":
                    target = command["preferred"]
                    validate_coordinator(self.policy, self.s["preferred"], target, self.wall())
                    require(self.s["enabled"] and not self.s["paused"] and not self.s["intent"], "coordinator switch requires enabled reconciled authority")
                    self.adapter.isolation([*self.refs(), target])
                    require(self.adapter.prepare(target), "coordinator target artifacts/network are not prepared")
                    self.s["preferred"] = target
                    self.s["stable_since"] = None
                    self.journal.save()
                    self.begin(target, voluntary=True)
                else:
                    raise ConfigError("unknown queued control")
                self.s["commands"][request_id] = {"command": action, "state": "applied", "at": self.wall()}
            except Exception as exc:
                self.s["commands"][request_id] = {"command": action, "state": "refused", "error": type(exc).__name__,
                                                 "reason": str(exc) if isinstance(exc, ConfigError) else "external operation unavailable; ownership retained",
                                                 "at": self.wall()}
            self.journal.save()
        with lock(self.journal.directory / "commands.lock", blocking=True):
            current = recovery_routes.private_json(queue) if queue.exists() else []
            recovery_routes.atomic_json(queue, [c for c in current if c["id"] not in self.s["commands"]])

    def tick(self):
        now, mono = self.wall(), self.monotonic()
        discontinuity = now < self.last_wall or abs((now - self.last_wall) - (mono - self.last_mono)) > self.t["heartbeat"] or mono - self.last_mono > self.t["lease"]
        self.last_wall, self.last_mono = now, mono
        if self.boot or discontinuity:
            self.publish(self.s["current"], False, "restart-or-clock-discontinuity")
            if self.s["epoch"] > 0:
                self.advance_epoch()
            self.s["stable_since"] = None
            self.prepared_at = None
            self.boot = False
            self.journal.save()
        self.process_commands()
        if not self.s["enabled"] or self.s["paused"]:
            self.publish(self.s["current"], False, "paused" if self.s["paused"] else "disabled")
            self.journal.phase("paused" if self.s["paused"] else "disabled")
            return
        try:
            observations = self.observations()
        except StaleObservation:
            return
        if self.s["intent"]:
            target = self.s["intent"]["target"]
            target_plan = load_plan(target)
            if not self.s["circuit_open"] and not network_ready(target_plan, observations, fabric=target_plan["deployment"]["mode"] != "single"):
                alternative = next((r for r in self.policy["fallbacks"]
                                    if r["sha256"] != target["sha256"] and eligible(self.policy, r, self.wall()) and network_ready(load_plan(r), observations)), None)
                if alternative:
                    self.s["intent"].update(target=alternative, stage="stop", rollback_pending=True)
                    self.advance_epoch()
            if self.s["intent"]["stage"] == "open" or self.s["intent"].get("rollback_pending") or (not self.s["circuit_open"] and self.wall() >= self.s["retry_at"]):
                self.s["intent"]["rollback_pending"] = False
                self.journal.save()
                self.transition()
            else:
                self.publish(None, False, "retry-or-circuit")
            return
        current = self.s["current"]
        if current and healthy(load_plan(current), observations) and eligible(self.policy, current, self.wall()):
            if not self.open_healthy(current, observations):
                return  # gateway-only failure is not a GPU failure.
            if current["sha256"] == self.s["preferred"]["sha256"]:
                return
            preferred = load_plan(self.s["preferred"])
            if not network_ready(preferred, observations, fabric=True):
                self.s["stable_since"] = None
                self.prepared = False
                self.journal.save()
                return
            if self.prepared_at is None or self.wall() - self.prepared_at >= self.t["prepare_interval"]:
                # Preflight is structural only: no collective/GPU probe while
                # fallback owns the GPU. Never renew independently during it.
                try:
                    self.prepared = self.adapter.prepare(self.s["preferred"])
                except Exception:
                    self.prepared = False
                self.prepared_at = self.wall()
            if not self.prepared:
                self.s["stable_since"] = None
            elif self.s["stable_since"] is None:
                self.s["stable_since"] = self.wall()
            self.journal.save()
            if (self.prepared and self.s["stable_since"] is not None and
                    self.wall() - self.s["stable_since"] >= self.t["stable_return"] and
                    self.wall() - self.s["active_since"] >= self.t["minimum_dwell"] and
                    self.wall() >= self.s["retry_at"] and not self.s["circuit_open"] and
                    eligible(self.policy, self.s["preferred"], self.wall())):
                self.begin(self.s["preferred"], voluntary=True)
            return
        self.publish(current, False, "worker-or-fabric-unavailable")
        self.s["stable_since"] = None
        self.journal.save()
        if self.s["circuit_open"] or self.wall() < self.s["retry_at"]:
            self.journal.phase("unavailable")
            return
        # Adopt a healthy exact deployment on initial activation, never launch
        # the preferred group merely because an unhealthy node answered SSH.
        preferred = self.s["preferred"]
        if healthy(load_plan(preferred), observations) and eligible(self.policy, preferred, self.wall()):
            self.advance_epoch()
            for node in load_plan(preferred)["nodes"]:
                self.journal.receipt("adopt-fence:" + node, self.adapter.fence(preferred, node, self.context(), sorted({load_plan(r)["digest"] for r in self.refs()})))
            self.open_healthy(preferred, observations, reset_retry=True)
            return
        for ref in self.policy["fallbacks"]:
            if eligible(self.policy, ref, self.wall()) and network_ready(load_plan(ref), observations):
                self.begin(ref)
                return
        self.journal.phase("unavailable")


def validate_coordinator(policy, current, target, now, *, require_qualified=True):
    old, new = load_plan(current), load_plan(target)
    require(old["nodes"] == new["nodes"] and old["recipe"] == new["recipe"], "coordinator switch must retain node/model contract")
    require(old["deployment"]["coordinator"] != new["deployment"]["coordinator"], "coordinator did not change")
    require({k: v for k, v in old["deployment"].items() if k not in ("name", "coordinator")} ==
            {k: v for k, v in new["deployment"].items() if k not in ("name", "coordinator")}, "coordinator switch changes deployment contract")
    if require_qualified:
        require(eligible(policy, target, now), "coordinator target requires independently trusted exact qualification")


def probe_chat(session, base, body):
    """Reuse the existing tool probe's parser, including mandatory SSE [DONE].

    Its Linux-only memory measurement is deliberately omitted: qualification is
    hosted on the selected Mac, and memory is not protocol acceptance evidence.
    """
    import runpy
    from types import SimpleNamespace
    chat = runpy.run_path(str(cli.ROOT / "scripts/qwen38-verify.py"))["chat"]
    chat.__globals__["memory"] = lambda: {}
    chat.__globals__["requests"] = SimpleNamespace(post=session.post)
    return chat(base, body, None)


def independent_serving(p):
    return all("serving" in n and n["serving"]["interface"] not in {r["interface"] for r in n["fabric"]} for n in p["nodes"].values())


def qualify(controller, ref, staging, output, *, rounds=1, chat=probe_chat):
    """Explicit approved inference on an already started, exact plan; never up()."""
    require(type(rounds) is int and 1 <= rounds <= 10, "qualification rounds must be 1..10")
    require(not controller.s["enabled"] and not controller.s["intent"], "qualification requires disabled authority without pending transition")
    require(not Path(output).exists(), "qualification output must be a fresh filename")
    p = load_plan(ref)
    if not any(ref["sha256"] == r["sha256"] for r in controller.refs()):
        validate_coordinator(controller.policy, controller.s["preferred"], ref, controller.wall(), require_qualified=False)
        controller.extra_refs.append(ref)
    fields(staging, tuple(p["nodes"]))
    for artifact in staging.values():
        fields(artifact, ("path", "sha256"))
        require(Path(artifact["path"]).is_absolute(), "staging references must be absolute")
        load_artifact(artifact)
    controller.adapter.isolation(controller.refs())
    observations = controller.observations()
    require(healthy(p, observations), "qualification requires already started exact workers and endpoint binding")
    require(independent_serving(p) or p["deployment"]["mode"] != "single",
            "single qualification requires independent serving binding")
    checks = {k: False for k in CHECKS}
    receipt = {"version": 1, "policy": controller.policy["id"], "plan_sha256": ref["sha256"],
               "deployment_digest": p["digest"], "nodes": sorted(p["nodes"]), "gateway_url": controller.policy["gateway"]["url"],
               "route": route_for(p), "checks": checks, "completed_at": controller.wall(), "staging": staging}
    # Keep failure receipts, but never mistake them for an eligible artifact.
    recovery_routes.atomic_json(output, receipt)
    tools = [{"type": "function", "function": {
        "name": "lookup_value", "description": "Look up the secret verification value for a key.",
        "parameters": {"type": "object", "properties": {"key": {"type": "string"}}, "required": ["key"], "additionalProperties": False}}}]
    old_current = controller.s["current"]
    try:
        require(controller.drain("qualification"), "qualification requires globally drained ingress")
        with controller.adapter.session() as session:
            def exercise(body, finish):
                require(healthy(p, controller.observations()), "qualification workers changed")
                controller.publish(ref, True, "approved-qualification")
                require(controller.ack() and controller.adapter.endpoint(p), "gateway does not acknowledge exact qualification route")
                record, message = chat(session, controller.policy["gateway"]["url"] + "/v1", body)
                headers = record["guard"]
                require(headers.get("x-spark-deployment") == p["digest"] and
                        headers.get("x-spark-backend") == p["recipe"]["alias"] and
                        headers.get("x-spark-authority") == controller.s["authority"] and
                        headers.get("x-spark-generation") == str(controller.s["generation"]), "actual response identity mismatch")
                require(record["finish_reason"] == finish, "unexpected completion finish")
                return message

            token = "verified_" + uuid.uuid4().hex
            body = {"model": "local-auto", "temperature": 0, "max_tokens": 256,
                    "messages": [{"role": "user", "content": "Reply with exactly: " + token}], "stream": False}
            text = exercise(body, "stop")
            require(token in (text.get("content") or "") and not text.get("tool_calls"), "text verification failed")
            checks["text"] = True
            for _ in range(rounds):
                for mode in ("auto", "named", "required"):
                    for stream in (False, True):
                        key = "key_" + uuid.uuid4().hex[:12]
                        choice = {"type": "function", "function": {"name": "lookup_value"}} if mode == "named" else mode
                        body = {"model": "local-auto", "temperature": 0, "max_tokens": 256,
                                "messages": [{"role": "user", "content": f"Call lookup_value with key {key}. Then reply only with its returned value."}],
                                "tools": tools, "tool_choice": choice, "stream": stream}
                        if stream:
                            body["stream_options"] = {"include_usage": True}
                        assistant = exercise(body, "stop" if mode == "named" else "tool_calls")
                        calls = assistant.get("tool_calls", [])
                        require(len(calls) == 1 and calls[0].get("id") and calls[0]["function"]["name"] == "lookup_value" and
                                json.loads(calls[0]["function"]["arguments"]) == {"key": key}, "tool arguments verification failed")
                        value = "verified_" + uuid.uuid4().hex
                        body["messages"] += [assistant, {"role": "tool", "tool_call_id": calls[0]["id"], "content": value}]
                        body["tool_choice"] = "none"
                        final = exercise(body, "stop")
                        require(not final.get("tool_calls") and value in (final.get("content") or ""), "actual tool result continuation failed")
            checks.update({k: True for k in CHECKS})
            checks["independent_serving"] = True if independent_serving(p) else "not-applicable-distributed"
            receipt["completed_at"] = controller.wall()
            validate_qualification(receipt, controller.policy, ref, now=controller.wall())
    finally:
        controller.s["current"] = old_current
        controller.publish(None, False, "qualification-ended")
        # Close/ACK accounts for all policy aliases. No worker cleanup is implied.
        drained = controller.drain("qualification-ended")
        if not drained:
            checks["sse_complete"] = False
        recovery_routes.atomic_json(output, receipt)
    require(all(checks.values()), "qualification incomplete; retained non-eligible receipt")
    return {"path": str(Path(output).resolve()), "sha256": digest_file(output)}


def initialize(policy, directory, *, adopt_policy=False):
    directory = private_directory(directory)
    with lock(directory / "authority.lock"):
        path = directory / "journal.json"
        if adopt_policy and path.exists():
            saved = recovery_routes.private_json(path)
            require(saved.get("enabled") is False and saved.get("intent") is None and saved.get("policy") == policy["id"],
                    "policy adoption requires same disabled, reconciled authority")
            old_route = saved.get("route")
            require(not old_route or old_route.get("accepting") is False, "policy adoption cannot replace accepting route")
            # Receipt enrichment only: deployment/trust/ingress/timing changes
            # require a separate cold, explicitly fenced migration.
            old_policy = recovery_routes.private_json(directory / "policy.json")
            def without_receipts(p):
                p = copy.deepcopy(p)
                p["gateway"]["isolation"] = None
                for ref in [p["preferred"], *p["fallbacks"]]:
                    ref["qualification"] = None
                return p
            require(without_receipts(old_policy) == without_receipts(policy), "only qualification/isolation receipt enrichment is allowed")
            saved["policy_hash"] = hash_value(policy)
            # A queued coordinator switch is durable authority state, not a
            # policy edit. Enrich known receipts without reverting that choice.
            for key in ("preferred", "current", "selected_fallback"):
                ref = saved.get(key)
                if ref:
                    replacement = next((r for r in [policy["preferred"], *policy["fallbacks"]]
                                        if r["sha256"] == ref["sha256"]), None)
                    if replacement:
                        ref["qualification"] = replacement["qualification"]
            recovery_routes.atomic_json(path, saved)
        journal = Journal(directory, policy)
        recovery_routes.atomic_json(directory / "policy.json", policy)
        return {"policy": policy["id"], "authority": journal.value["authority"], "enabled": journal.value["enabled"]}


def run(policy, directory, *, once=False, adapter=None):
    directory = private_directory(directory)
    with lock(directory / "authority.lock"):
        require((directory / "journal.json").exists(), "explicit init required before supervised run")
        journal = Journal(directory, policy)
        controller = Controller(policy, journal, adapter or RealAdapter(policy, directory))
        import signal
        import threading
        old_handler = None
        if threading.current_thread() is threading.main_thread():
            def terminate(signum, frame):
                raise SystemExit(0)
            old_handler = signal.signal(signal.SIGTERM, terminate)
        try:
            while True:
                started = controller.monotonic()
                try:
                    controller.tick()
                except (OSError, ValueError, RuntimeError, KeyError, TypeError) as exc:
                    # Persist only a sanitized class, never remote stderr/secrets.
                    # If disk has failed this also fails: no independent heartbeat
                    # exists, so the last published lease expires closed.
                    controller.publish(controller.s["current"], False, "reconciliation-failed")
                    journal.receipt("reconcile-error", {"type": type(exc).__name__})
                    journal.phase("unavailable")
                if once:
                    return 0
                controller.sleep(max(0.001, controller.t["heartbeat"] - (controller.monotonic() - started)))
        finally:
            try:
                controller.publish(controller.s["current"], False, "controller-stopped")
            finally:
                if old_handler is not None:
                    signal.signal(signal.SIGTERM, old_handler)


APPROVALS = {
    "enable": "automatic-recovery", "resume": "automatic-recovery", "pause": "close-ingress",
    "disable": "close-ingress", "reset": "stop-exact-owned-workers", "clear-circuit": "retry-owned-transitions",
    "switch-coordinator": "saved-coordinator-drain-rollback",
}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("init", "validate", "status", "run", "qualify", *APPROVALS):
        p = sub.add_parser(command)
        p.add_argument("--policy", type=Path, required=True)
        if command != "validate":
            p.add_argument("--state-dir", type=Path, required=True)
        if command == "run":
            p.add_argument("--once", action="store_true", help="one bounded reconciliation, never implicit enablement")
        if command in APPROVALS or command in ("init", "qualify"):
            p.add_argument("--apply", action="store_true")
        if command in APPROVALS:
            p.add_argument("--approve", choices=[APPROVALS[command]], required=True)
        if command == "init":
            p.add_argument("--adopt-receipts", action="store_true", help="explicit receipt-only enrichment of same disabled policy")
        if command in ("qualify", "switch-coordinator"):
            p.add_argument("--plan", type=Path, required=True)
            p.add_argument("--plan-sha256", required=True)
        if command == "switch-coordinator":
            p.add_argument("--qualification", type=Path, required=True)
            p.add_argument("--qualification-sha256", required=True)
        if command == "qualify":
            p.add_argument("--approve-inference", action="store_true")
            p.add_argument("--staging", type=Path, required=True, help="JSON per-node independently trusted staging artifact references")
            p.add_argument("--output", type=Path, required=True)
            p.add_argument("--rounds", type=int, default=1)
    args = parser.parse_args(argv)
    policy = load_policy(args.policy)
    if args.command == "validate":
        result = {"valid": True, "policy": policy["id"], "qualified": all(eligible(policy, r, time.time()) for r in [policy["preferred"], *policy["fallbacks"]]),
                  "activation_requires": "current exact receipts, isolated dedicated gateway, explicit enable approval"}
    elif args.command == "init":
        require(args.apply, "init requires --apply (local private authority files only)")
        result = initialize(policy, args.state_dir, adopt_policy=args.adopt_receipts)
    elif args.command == "status":
        result = recovery_routes.private_json(args.state_dir / "status.json")
        saved = recovery_routes.private_json(args.state_dir / "journal.json")
        require(saved["policy_hash"] == hash_value(policy), "status policy differs from journal")
        result.update(enabled=saved["enabled"], paused=saved["paused"], commands=saved["commands"])
    elif args.command == "run":
        return run(policy, args.state_dir, once=args.once)
    elif args.command == "qualify":
        require(args.apply and args.approve_inference, "qualification requires --apply --approve-inference")
        directory = private_directory(args.state_dir)
        with lock(directory / "authority.lock"):
            journal = Journal(directory, policy)
            ref = reference({"path": str(args.plan.resolve()), "sha256": args.plan_sha256, "qualification": None}, Path.cwd(), plan=True)
            controller = Controller(policy, journal, RealAdapter(policy, directory))
            result = qualify(controller, ref, config.read(args.staging), args.output, rounds=args.rounds)
    else:
        require(args.apply, "operator controls require --apply")
        preferred = None
        if args.command == "switch-coordinator":
            preferred = reference({"path": str(args.plan.resolve()), "sha256": args.plan_sha256,
                                   "qualification": {"path": str(args.qualification.resolve()), "sha256": args.qualification_sha256}},
                                  Path.cwd(), plan=True)
            saved = recovery_routes.private_json(args.state_dir / "journal.json")
            require(saved["policy_hash"] == hash_value(policy), "command policy differs from journal")
            validate_coordinator(policy, saved["preferred"], preferred, time.time())
        request_id = queue_command(args.state_dir, policy, args.command, preferred=preferred)
        # A supervised daemon consumes this queue safely under its authority
        # lock. Controls never compete for its lock or mutate node state directly.
        result = {"queued": request_id, "command": args.command, "note": "daemon or explicit run --once applies the durable request"}
    print(json.dumps(result, sort_keys=True))
    return 0
