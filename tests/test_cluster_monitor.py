"""Consumer telemetry contracts; no GPU, SSH, live model or installed exporter needed."""
import copy
from dataclasses import replace
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster import monitor, monitor_node


@pytest.fixture
def inventory():
    return json.loads((ROOT / "cluster/inventory.json").read_text())


def host(node, value=100):
    return {"version": 1, "hostname": node["hostname"], "architecture": node["architecture"],
            "collected_at": time.time(), "host": {"memory_available_bytes": 1048576},
            "disks": {"root": {"size_bytes": 200, "available_bytes": 100}},
            "gpu": {"up": True, "devices": {"0": {"busy_ratio": .5}}},
            "interfaces": {rail["interface"]: {"present": True, "carrier": 1,
                "speed_bits_per_second": 200000000000, "counters": {"tx_bytes": value, "rx_bytes": value}}
                for rail in node["fabric"]},
            "rdma": {rail["rdma"]: {"present": True, "active": True,
                "counters": {"tx_bytes": value, "rx_bytes": value}}
                for rail in node["fabric"]}}


@pytest.fixture
def exported(inventory):
    collector = monitor.Collector(inventory, host_reader=host, timeout=.1, interval=.1)
    collector.collect()
    server = monitor.MonitorServer(("127.0.0.1", 0), collector)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", collector
    server.shutdown()
    server.server_close()
    collector.stop()
    thread.join(timeout=2)


def get(url):
    with urlopen(url, timeout=2) as response:
        return response.status, response.read().decode()


def test_http_snapshots_do_not_collect_and_health_tracks_staleness(exported):
    base, collector = exported
    collector.host_reader = lambda _: pytest.fail("HTTP scrape must not collect")
    status, body = get(base + "/metrics")
    assert status == 200
    assert 'spark_host_memory_available_bytes{node="66f1"} 1048576' in body
    assert 'spark_host_memory_available_bytes{node="e8f1"} 1048576' in body
    assert body.count("# TYPE spark_host_memory_available_bytes gauge") == 1
    assert json.loads(get(base + "/status")[1])["all_targets_up"] is True
    assert json.loads(get(base + "/health")[1])["healthy"] is True
    collector.snapshot = replace(collector.snapshot, collected_at=time.time() - 60)
    assert json.loads(get(base + "/status")[1])["fresh"] is False
    with pytest.raises(HTTPError) as exc:
        get(base + "/health")
    assert exc.value.code == 503
    assert "spark_monitor_snapshot_fresh 0" in get(base + "/metrics")[1]


def test_partial_host_failures_and_unsupported_gpu_do_not_invent_zeros(inventory):
    def reader(node):
        if node["hostname"] == "spark-66f1":
            return {"secret": "not telemetry"}
        return host(node)
    snapshot = monitor.Collector(inventory, host_reader=reader, timeout=.1).collect()
    statuses = {v["target"]: v for v in json.loads(snapshot.status)["targets"]}
    assert statuses["66f1"]["error"] == "invalid_data"
    assert statuses["e8f1"]["up"] is True
    assert 'spark_gpu_busy_ratio{gpu="0",node="e8f1"} 0.5' in snapshot.metrics
    assert "spark_gpu_memory_total_bytes{" not in snapshot.metrics
    assert 'spark_gpu_metric_available{gpu="0",metric="memory_total_bytes",node="e8f1"} 0' in snapshot.metrics
    assert "not telemetry" not in snapshot.metrics + snapshot.status.decode()


def test_missing_nic_does_not_hide_cpu_or_other_host(inventory):
    def reader(node):
        result = host(node)
        result["interfaces"] = {node["fabric"][0]["interface"]: {"present": False, "counters": {}}}
        result["rdma"] = {}
        return result
    snapshot = monitor.Collector(inventory, host_reader=reader, timeout=.1).collect()
    assert snapshot.healthy
    assert "spark_host_memory_available_bytes" in snapshot.metrics
    assert "spark_interface_carrier" not in snapshot.metrics
    assert "spark_rdma_tx_bytes_total" not in snapshot.metrics
    assert "spark_rdma_present" in snapshot.metrics


def test_timeout_is_bounded_and_does_not_spawn_overlapping_target(inventory):
    release = threading.Event()
    entered = []
    def slow(node):
        entered.append(node["hostname"])
        release.wait(2)
        return host(node)
    collector = monitor.Collector(inventory, host_reader=slow, timeout=.05)
    try:
        started = time.monotonic()
        snapshot = collector.collect()
        assert time.monotonic() - started < .5
        assert {v["error"] for v in json.loads(snapshot.status)["targets"]} == {"timeout"}
        collector.collect()
        assert sorted(entered) == ["spark-66f1", "spark-e8f1"]
    finally:
        release.set()
        collector.stop()


def test_counter_resets_and_escaping():
    assert monitor.counter_rate(10, 20, 2) == 5
    assert monitor.counter_rate(20, 10, 2) is None
    assert monitor.counter_rate(None, 20, 2) is None
    assert monitor.counter_rate(10, 20, 0) is None
    metrics = monitor.Metrics()
    metrics.add("example", 1, {"label": 'quote" slash\\ newline\n'})
    assert 'label="quote\\" slash\\\\ newline\\n"' in metrics.render()
    assert metrics.render().count("\n") == 2


def test_known_spark_paths_map_one_cable_without_claiming_aggregate(inventory):
    collector = monitor.Collector(inventory, host_reader=host, timeout=.1)
    assert len({row["physical_link"] for row in collector.mapping}) == 1
    collector.collect()
    snapshot = collector.collect()
    samples = [line for line in snapshot.metrics.splitlines() if line.startswith("spark_fabric_aggregation_available{")]
    assert samples == ['spark_fabric_aggregation_available{physical_link="spark-qsfp-66f1--e8f1"} 0']
    observed = [line for line in snapshot.metrics.splitlines() if line.startswith("spark_fabric_observed_bits_per_second{")]
    assert len(observed) == 16  # two nodes x two paths x RX/TX x netdev/RDMA, never a sum.
    assert all('interface="' in line and 'node="' in line for line in observed)


def test_explicit_physical_links_override_known_topology(inventory):
    inventory = copy.deepcopy(inventory)
    inventory["nodes"]["66f1"]["fabric"][1].update(physical_link="second-cable", peer="third-node")
    mapping = monitor.physical_mapping(inventory)
    assert next(r for r in mapping if r["node"] == "66f1" and r["interface"] == "enP2p1s0f0np0")["physical_link"] == "second-cable"


def physical_host(inventory, node_id, tx=1000, rx=2000, sampled_at=10):
    data = host(inventory["nodes"][node_id])
    data["physical"] = {
        row["physical_link"]: {"interface": row["interface"], "sampled_at": sampled_at,
                              "counters": {"tx_bytes": tx, "rx_bytes": rx, "link_down_events": 3}}
        for row in monitor.canonical_physical_ports(monitor.physical_mapping(inventory))
        if row["node"] == node_id
    }
    return data


def metric_samples(body, family):
    result = []
    for line in body.splitlines():
        match = monitor.SAMPLE.fullmatch(line)
        if match and match[1] == family:
            labels = {label[1]: label[2] for label in monitor.LABEL.finditer((match[2] or "{}")[1:-1])}
            value = int(match[3]) if match[3].isdecimal() else float(match[3])
            result.append((labels, value))
    return result


def test_physical_parser_excludes_vport_ring_and_acceleration_counters():
    output = """NIC statistics:
     tx_bytes_phy: 12512566605440
     rx_bytes_phy: 12675730417676
     rx_crc_errors_phy: 2
     rx_discards_phy: 5
     link_down_events_phy: 7
     rx_corrected_bits_phy: 9007199254740993
     rx_pause_ctrl_phy: 11
     tx_pause_ctrl_phy: 13
     tx_vport_rdma_unicast_bytes: 999999999999
     rx0_bytes: 888888888888
     tx_bytes: 777777777777
     rx_bytes_phy_extra: 666666666666
"""
    assert monitor_node.parse_physical_counters(output) == {
        "tx_bytes": 12512566605440, "rx_bytes": 12675730417676, "rx_crc_errors": 2,
        "rx_discards": 5, "link_down_events": 7, "rx_corrected_bits": 9007199254740993,
        "rx_pause_ctrl": 11, "tx_pause_ctrl": 13,
    }
    assert monitor_node.parse_physical_counters(
        "tx_bytes_phy: -1\nrx_bytes_phy: N/A\nrx_crc_errors_phy: 18446744073709551616") == {}
    with pytest.raises(ValueError):
        monitor_node.parse_physical_counters("tx_bytes_phy: 10\ntx_bytes_phy: 20")


def test_physical_collection_uses_one_canonical_nic_per_port(inventory, monkeypatch):
    mapping = monitor.physical_mapping(inventory)
    ports = monitor.canonical_physical_ports(list(reversed(mapping)))
    assert {(p["node"], p["interface"]) for p in ports} == {
        ("66f1", "enp1s0f0np0"), ("e8f1", "enp1s0f0np0")}
    calls = []
    def query(interface, timeout):
        calls.append(interface)
        return "tx_bytes_phy: 12\nrx_bytes_phy: 34\n"
    monkeypatch.setattr(monitor_node, "bounded_ethtool", query)
    local = [p for p in ports if p["node"] == "66f1"]
    # Even a repeated descriptor cannot cause a second logical-path query or sum.
    data = monitor_node.physical_metrics(local + local)
    assert calls == ["enp1s0f0np0"]
    assert data[local[0]["physical_link"]]["counters"] == {"tx_bytes": 12, "rx_bytes": 34}
    explicit = copy.deepcopy(inventory)
    explicit["nodes"]["66f1"]["fabric"][1]["physical_link"] = "second-cable"
    assert len([p for p in monitor.canonical_physical_ports(monitor.physical_mapping(explicit))
                if p["node"] == "66f1"]) == 2


def test_physical_collection_deadline_limits_all_ports(monkeypatch):
    clock, calls = [100.0], []
    monkeypatch.setattr(monitor_node.time, "monotonic", lambda: clock[0])
    def timeout(interface, duration):
        calls.append((interface, duration))
        clock[0] += duration
        raise monitor_node.subprocess.TimeoutExpired("ethtool", duration)
    monkeypatch.setattr(monitor_node, "bounded_ethtool", timeout)
    ports = [{"interface": f"eth{i}", "physical_link": f"cable{i}"} for i in range(3)]
    result = monitor_node.physical_metrics(ports, budget=1, per_call_timeout=.75)
    assert calls == [("eth0", .75), ("eth1", .25)]
    assert all(value["counters"] == {} for value in result.values())
    assert all("sampled_at" not in value for value in result.values())


@pytest.mark.parametrize("script", [
    "import sys; print('tx_bytes_phy: 9\\nrx_bytes_phy: 10'); sys.exit(1)",
    "import time; print('tx_bytes_phy: 9\\nrx_bytes_phy: 10', flush=True); time.sleep(10)",
    "import sys; sys.stdout.write('x' * (256 * 1024 + 1))",
])
def test_physical_subprocess_errors_timeout_and_output_bound(script, monkeypatch):
    real_popen = monitor_node.subprocess.Popen
    def process(args, **kwargs):
        return real_popen([sys.executable, "-c", script], **kwargs)
    monkeypatch.setattr(monitor_node.subprocess, "Popen", process)
    started = time.monotonic()
    result = monitor_node.physical_metrics(
        [{"interface": "eth0", "physical_link": "cable"}], budget=.5, per_call_timeout=.1)
    assert result["cable"]["counters"] == {}
    assert "sampled_at" not in result["cable"]
    assert time.monotonic() - started < 2


def test_missing_ethtool_does_not_fail_host(inventory, monkeypatch):
    def missing(*args, **kwargs):
        raise FileNotFoundError("ethtool")
    monkeypatch.setattr(monitor_node.subprocess, "Popen", missing)
    ports = monitor.canonical_physical_ports(monitor.physical_mapping(inventory))
    def reader(node):
        data = host(node)
        local = [p for p in ports if inventory["nodes"][p["node"]]["hostname"] == node["hostname"]]
        data["physical"] = monitor_node.physical_metrics(local)
        return data
    snapshot = monitor.Collector(inventory, host_reader=reader, timeout=.1).collect()
    assert snapshot.healthy
    assert {value for _, value in metric_samples(snapshot.metrics, "spark_physical_port_available")} == {0}
    assert metric_samples(snapshot.metrics, "spark_physical_port_tx_bytes_total") == []
    assert metric_samples(snapshot.metrics, "spark_fabric_physical_bits_per_second") == []
    assert len(metric_samples(snapshot.metrics, "spark_host_memory_available_bytes")) == 2


def test_physical_cable_rates_select_one_endpoint_without_summing(inventory):
    mapping = monitor.physical_mapping(inventory)
    previous = {node: physical_host(inventory, node) for node in inventory["nodes"]}
    current = {
        "66f1": physical_host(inventory, "66f1", tx=1500, rx=3000, sampled_at=12),
        "e8f1": physical_host(inventory, "e8f1", tx=9000, rx=19000, sampled_at=12),
    }
    metrics = monitor.Metrics()
    monitor.physical_link_metrics(metrics, mapping, current, previous)
    body = metrics.render()
    cable = metric_samples(body, "spark_fabric_physical_bits_per_second")
    assert {(labels["node"], labels["interface"], labels["direction"], value) for labels, value in cable} == {
        ("66f1", "enp1s0f0np0", "tx", 2000), ("66f1", "enp1s0f0np0", "rx", 4000)}
    assert len(metric_samples(body, "spark_physical_port_observed_bits_per_second")) == 4
    assert [v for _, v in metric_samples(body, "spark_fabric_negotiated_bits_per_second")] == [200000000000]
    assert [v for _, v in metric_samples(body, "spark_fabric_aggregation_available")] == [1]
    assert [v for _, v in metric_samples(body, "spark_physical_port_link_down_events_total")] == [3, 3]
    # The peer remains observable, but does not silently replace the stable cable endpoint.
    metrics = monitor.Metrics()
    monitor.physical_link_metrics(metrics, mapping, {"e8f1": current["e8f1"]}, previous)
    body = metrics.render()
    assert metric_samples(body, "spark_fabric_physical_bits_per_second") == []
    assert [v for _, v in metric_samples(body, "spark_fabric_aggregation_available")] == [0]
    assert {labels["node"] for labels, _ in metric_samples(body, "spark_physical_port_observed_bits_per_second")} == {"e8f1"}


def test_physical_rates_require_two_complete_samples_and_skip_reset(inventory):
    mapping = monitor.physical_mapping(inventory)
    old = {"66f1": physical_host(inventory, "66f1")}
    new = {"66f1": physical_host(inventory, "66f1", tx=1500, rx=3000, sampled_at=12)}
    link = next(iter(new["66f1"]["physical"]))
    partial = copy.deepcopy(new)
    del partial["66f1"]["physical"][link]["counters"]["rx_bytes"]
    partial_old = copy.deepcopy(old)
    del partial_old["66f1"]["physical"][link]["counters"]["rx_bytes"]
    reset = {"66f1": physical_host(inventory, "66f1", tx=1, rx=3000, sampled_at=12)}
    reboot = {"66f1": physical_host(inventory, "66f1", tx=1500, rx=3000, sampled_at=1)}
    for current, previous in ((new, {}), (partial, old), (new, partial_old), (reset, old), (reboot, old)):
        metrics = monitor.Metrics()
        monitor.physical_link_metrics(metrics, mapping, current, previous)
        assert metric_samples(metrics.render(), "spark_fabric_physical_bits_per_second") == []
    metrics = monitor.Metrics()
    monitor.physical_link_metrics(metrics, mapping, partial, old)
    availability = metric_samples(metrics.render(), "spark_physical_port_available")
    assert dict((labels["node"], value) for labels, value in availability)["66f1"] == 0
    assert metric_samples(metrics.render(), "spark_physical_port_rx_bytes_total") == []
    recovered = {"66f1": physical_host(inventory, "66f1", tx=101, rx=3200, sampled_at=14)}
    metrics = monitor.Metrics()
    monitor.physical_link_metrics(metrics, mapping, recovered, reset)
    assert {labels["direction"]: value for labels, value in metric_samples(
        metrics.render(), "spark_fabric_physical_bits_per_second")} == {"tx": 400, "rx": 800}


def test_physical_host_failure_breaks_rate_interval_but_not_other_host(inventory):
    samples = {node: physical_host(inventory, node) for node in inventory["nodes"]}
    def reader(node):
        node_id = next(key for key, value in inventory["nodes"].items() if value["hostname"] == node["hostname"])
        if node_id not in samples:
            raise OSError("host inaccessible")
        return samples[node_id]
    collector = monitor.Collector(inventory, host_reader=reader, timeout=.1)
    collector.collect()
    samples["66f1"] = physical_host(inventory, "66f1", tx=1200, rx=2400, sampled_at=12)
    assert len(metric_samples(collector.collect().metrics, "spark_fabric_physical_bits_per_second")) == 2
    del samples["66f1"]
    failed = collector.collect()
    assert [value for _, value in metric_samples(failed.metrics, "spark_fabric_aggregation_available")] == [0]
    assert dict((labels["target"], value) for labels, value in metric_samples(
        failed.metrics, "spark_monitor_target_up")) == {"66f1": 0, "e8f1": 1}
    samples["66f1"] = physical_host(inventory, "66f1", tx=1400, rx=2800, sampled_at=14)
    assert metric_samples(collector.collect().metrics, "spark_fabric_physical_bits_per_second") == []


def test_native_engine_histograms_preserve_engine_not_shard_and_strip_secrets():
    body = '''# TYPE vllm:time_to_first_token_seconds histogram
vllm:time_to_first_token_seconds_bucket{model_name="secret-model-path",engine="0",le="0.1"} 3
vllm:time_to_first_token_seconds_bucket{model_name="secret-model-path",engine="0",le="+Inf"} 4
vllm:time_to_first_token_seconds_sum{model_name="secret-model-path",engine="0"} 0.6
vllm:time_to_first_token_seconds_count{model_name="secret-model-path",engine="0"} 4
vllm:prompt_tokens_total{model_name="secret-model-path",engine="0",request_id="secret-request"} 32
process_secret{token="private-token"} 42
'''
    metrics = monitor.Metrics()
    monitor.append_exposition(metrics, monitor.parse_exposition(body, "vllm:", monitor.ENGINE_BASES, {"deployment": "exact-plan"}))
    text = metrics.render()
    assert "secret" not in text and "private-token" not in text
    assert 'deployment="exact-plan",engine="0",le="+Inf"' in text
    assert "# TYPE vllm:time_to_first_token_seconds histogram" in text
    assert "# TYPE vllm:time_to_first_token_seconds_bucket" not in text
    assert "vllm:prompt_tokens_total" in text
    with pytest.raises(ValueError, match="ambiguous"):
        monitor.parse_exposition('vllm:num_requests_running{request_id="a"} 1\nvllm:num_requests_running{request_id="b"} 2', "vllm:", monitor.ENGINE_BASES, {})


def test_exception_and_unknown_recovery_fields_never_expose_sensitive_data(inventory, tmp_path):
    state = tmp_path / "recovery.json"
    state.write_text(json.dumps({"version": 1, "policy": "local-auto", "phase": "SINGLE_SERVING", "generation": 2,
        "updated_at": 123, "event_counts": {"failover": 1}, "reason": "private bearer secret",
        "last_error": "prompt-content", "environment": {"TOKEN": "credential"}}))
    def failing(node):
        raise RuntimeError("credential private bearer secret")
    snapshot = monitor.Collector(inventory, host_reader=failing, timeout=.1, recovery_state=state).collect()
    exposed = snapshot.metrics + snapshot.status.decode()
    assert not any(secret in exposed for secret in ("credential", "private bearer", "prompt-content", "TOKEN"))
    assert 'spark_recovery_phase_info{phase="SINGLE_SERVING",policy="local-auto"} 1' in exposed
    assert 'spark_recovery_events_total{event="failover",policy="local-auto"} 1' in exposed
    state.unlink()
    snapshot = monitor.Collector(inventory, host_reader=host, timeout=.1, recovery_state=state).collect()
    assert 'spark_monitor_target_up{kind="recovery",target="authority"} 0' in snapshot.metrics
    assert 'spark_monitor_target_up{kind="host",target="66f1"} 1' in snapshot.metrics


def test_gateway_key_permissions_and_symlinks(tmp_path):
    path = tmp_path / "key"
    key = "private-gateway-credential-0000000000"
    path.write_text(key)
    path.chmod(0o600)
    assert monitor.private_key(path) == key
    path.chmod(0o644)
    with pytest.raises(ValueError):
        monitor.private_key(path)
    link = tmp_path / "symlink"
    link.symlink_to(path)
    with pytest.raises(OSError):
        monitor.private_key(link)
    with pytest.raises(ValueError):
        monitor.endpoint("https://username:credential@example.org/v1", "/metrics")


def test_rdma_word_conversion_and_missing_fields(inventory, tmp_path, monkeypatch):
    node = inventory["nodes"]["66f1"]
    def fake_path(path):
        candidate = Path(path)
        return candidate if candidate.is_relative_to(tmp_path) else tmp_path / str(path).lstrip("/")
    monkeypatch.setattr(monitor_node, "Path", fake_path)
    monkeypatch.setattr(monitor_node.platform, "node", lambda: node["hostname"])
    monkeypatch.setattr(monitor_node.platform, "machine", lambda: node["architecture"])
    monkeypatch.setattr(monitor_node, "gpu_metrics", lambda: {"up": False, "devices": {}})
    monkeypatch.setattr(monitor_node.os, "statvfs", lambda _: SimpleNamespace(f_blocks=20, f_frsize=4096, f_bavail=10))
    root = fake_path("/sys/class/infiniband/rocep1s0f0/ports/1")
    (root / "counters").mkdir(parents=True)
    (root / "counters/port_xmit_data").write_text("123")
    (root / "counters/port_rcv_data").write_text("456")
    (root / "counters/port_xmit_packets").write_text("7")
    (root / "state").write_text("4: ACTIVE")
    proc = fake_path("/proc/meminfo")
    proc.parent.mkdir(parents=True)
    proc.write_text("MemTotal: 1024 kB\nMemAvailable: 512 kB\n")
    result = monitor_node.collect(node)
    counters = result["rdma"]["rocep1s0f0"]["counters"]
    assert counters == {"tx_bytes": 492, "rx_bytes": 1824, "tx_packets": 7}
    assert result["host"]["memory_available_bytes"] == 512 * 1024
    assert result["rdma"]["roceP2p1s0f0"]["present"] is False
    assert result["interfaces"]["enp1s0f0np0"]["present"] is False


def test_gpu_unsupported_field_does_not_hide_other_supported_metrics(monkeypatch):
    def query(args, **kwargs):
        field = args[1].split(",", 1)[1]
        if field == "power.draw":
            return SimpleNamespace(returncode=1, stdout="", stderr="unsupported")
        return SimpleNamespace(returncode=0, stdout="0, N/A\n" if field.startswith("memory.") else "0, 50\n")
    monkeypatch.setattr(monitor_node.subprocess, "run", query)
    result = monitor_node.gpu_metrics()
    assert result["devices"]["0"] == {"busy_ratio": .5, "temperature_celsius": 50}
    assert result["up"] is True


def test_ingress_metrics_survive_absent_recovery_endpoint(inventory):
    def upstream(url, timeout, key=None):
        if url.endswith("/_spark/recovery"):
            raise HTTPError(url, 404, "not configured", {}, None)
        return 'spark_gateway_requests_total{alias="local-coder",backend="local-coder",mode="strict"} 7\n'
    snapshot = monitor.Collector(inventory, host_reader=host, http_reader=upstream, timeout=.1,
        gateway_url="http://127.0.0.1:4110", gateway_key="private-credential").collect()
    assert 'spark_gateway_requests_total{alias="local-coder",backend="local-coder",mode="strict"} 7.0' in snapshot.metrics
    assert 'spark_monitor_target_up{kind="gateway",target="ingress"} 1' in snapshot.metrics
    assert 'spark_monitor_target_up{kind="gateway_recovery",target="ingress"} 0' in snapshot.metrics
    assert "private-credential" not in snapshot.metrics + snapshot.status.decode()


def test_saved_plan_targets_cannot_redirect_collection_or_duplicate_replicas(inventory):
    from spark_cluster import config
    inv, recipe, deployment = config.load(ROOT, ROOT / "cluster/inventory.json",
                                         ROOT / "cluster/deployments/coder-e8f1.json")
    plan = config.plan(inv, recipe, deployment)
    with pytest.raises(ValueError, match="duplicate engine endpoint"):
        monitor.Collector(inventory, [plan, plan], host_reader=host)
    forged = copy.deepcopy(plan)
    forged["endpoint"]["base_url"] = "http://unrelated.example:80/v1"
    with pytest.raises(ValueError, match="saved coordinator"):
        monitor.Collector(inventory, [forged], host_reader=host)


@pytest.fixture
def engine_endpoint(monkeypatch, inventory):
    node = copy.deepcopy(inventory["nodes"]["e8f1"])
    node["serving"] = {"address": "127.0.0.1"}
    worker = {"container_name": "spark-owned", "image": "pinned-image",
              "entrypoint": ["python3"], "command": ["serve"]}
    engine = {"owner": "owned", "digest": "a" * 64, "alias": "local-coder",
              "model_root": "/cache/model/snapshots/pinned", "context_tokens": 32768, "worker": worker}
    identity = {"id": "container-one", "name": "/spark-owned", "running": True,
                "started": "first-start", "restarts": 0,
                "labels": {"io.spark.owner": engine["owner"], "io.spark.digest": engine["digest"]},
                "image": worker["image"], "command": worker["command"], "entrypoint": worker["entrypoint"]}
    state = {"root": engine["model_root"], "restart_on_metrics": False}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_GET(self):
            if self.path == "/v1/models":
                body = json.dumps({"data": [{"id": engine["alias"], "root": state["root"],
                                             "max_model_len": engine["context_tokens"]}]}).encode()
            elif self.path == "/metrics":
                body = b"vllm:num_requests_running 2\n"
                if state["restart_on_metrics"]:
                    identity["started"] = "second-start"
                    identity["restarts"] = 1
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    engine["port"] = server.server_port
    monkeypatch.setattr(monitor_node.platform, "node", lambda: node["hostname"])
    monkeypatch.setattr(monitor_node.platform, "machine", lambda: node["architecture"])
    monkeypatch.setattr(monitor_node.subprocess, "run",
                        lambda *args, **kwargs: SimpleNamespace(stdout=json.dumps(identity)))
    try:
        yield node, engine, identity, state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_engine_metrics_require_live_model_and_container_contract(engine_endpoint):
    node, engine, identity, state = engine_endpoint
    state["root"] = "/cache/model/snapshots/different-revision"
    with pytest.raises(ValueError, match="model identity"):
        monitor_node.collect_engine(node, engine, 2)
    state["root"] = engine["model_root"]
    identity["command"] = ["serve", "--different-settings"]
    with pytest.raises(ValueError, match="saved plan"):
        monitor_node.collect_engine(node, engine, 2)
    identity["command"] = engine["worker"]["command"]
    assert monitor_node.collect_engine(node, engine, 2)["metrics"] == "vllm:num_requests_running 2\n"


def test_engine_restart_during_scrape_cannot_be_attributed_to_old_instance(engine_endpoint):
    node, engine, identity, state = engine_endpoint
    state["restart_on_metrics"] = True
    with pytest.raises(ValueError, match="changed during collection"):
        monitor_node.collect_engine(node, engine, 2)


def test_foreign_engine_receipt_cannot_publish_saved_plan_metrics(inventory):
    from spark_cluster import config
    inv, recipe, deployment = config.load(ROOT, ROOT / "cluster/inventory.json",
                                         ROOT / "cluster/deployments/coder-e8f1.json")
    plan = config.plan(inv, recipe, deployment)
    def foreign_engine(_):
        return {"owner": "another-deployment", "digest": plan["digest"],
                "metrics": "vllm:num_requests_running 7\n"}
    snapshot = monitor.Collector(inventory, [plan], host_reader=host, engine_reader=foreign_engine, timeout=.1).collect()
    assert "vllm:num_requests_running" not in snapshot.metrics
    assert f'spark_monitor_target_up{{kind="engine",target="{plan["owner"]}"}} 0' in snapshot.metrics
