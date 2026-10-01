"""Independent serving and management paths never weaken collective admission."""
import copy
import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster import cli, config, node


@pytest.fixture
def inputs():
    inv, recipe, deployment = config.load(ROOT, ROOT / "cluster/inventory.json",
                                          ROOT / "cluster/deployments/fast-e8f1.json")
    inv["nodes"]["e8f1"]["serving"] = {"address": "192.168.50.21", "interface": "wlan0"}
    inv["nodes"]["e8f1"]["management"] = {"ssh_targets": ["spark-wired", "spark-wifi"]}
    return inv, recipe, deployment


def test_serving_bind_health_and_endpoint_are_independent(inputs):
    inv, recipe, deployment = inputs
    p = config.plan(*inputs)
    worker = p["compose"]["e8f1"]["services"]["worker"]
    address = inv["nodes"]["e8f1"]["serving"]["address"]
    assert worker["command"][worker["command"].index("--host") + 1] == address
    assert address in worker["healthcheck"]["test"][-1]
    assert p["endpoint"]["base_url"] == f"http://{address}:{deployment['port']}/v1"
    assert "NCCL_SOCKET_IFNAME" not in worker["environment"]
    config.validate_saved_plan(p)


def test_distributed_health_uses_serving_but_collectives_keep_fabric(inputs):
    inv, _, _ = inputs
    _, recipe, deployment = config.load(ROOT, ROOT / "cluster/inventory.json",
                                        ROOT / "cluster/deployments/fast-tp2.json")
    deployment["coordinator"] = "e8f1"
    p = config.plan(inv, recipe, deployment)
    for node_id, compose in p["compose"].items():
        worker = compose["services"]["worker"]
        assert worker["environment"]["VLLM_HOST_IP"] == inv["nodes"][node_id]["fabric"][0]["ip"]
        assert worker["command"][worker["command"].index("--master-addr") + 1] == inv["nodes"]["e8f1"]["fabric"][0]["ip"]
        assert "192.168.50.21" in worker["healthcheck"]["test"][-1]


def test_cable_absent_single_reserves_but_tensor_refuses(inputs, tmp_path, monkeypatch):
    p = config.plan(*inputs)
    req = cli.request(p, "e8f1", "reserve")
    monkeypatch.setattr(node, "STATE", tmp_path / "state")
    report = {"gpu_processes": [], "gpu_containers": [], "research_window": None,
              "memory_mib": {"MemAvailable": 999999}, "fabric": [], "serving": {"ready": True}}
    monkeypatch.setattr(node, "doctor", lambda n: copy.deepcopy(report))
    monkeypatch.setattr(node, "validate_cached_snapshot", lambda p: None)
    monkeypatch.setattr(node, "run", lambda args: json.dumps([{"Architecture": "arm64"}]))
    bound = []
    monkeypatch.setattr(node, "check_port", lambda address, port: bound.append((address, port)))
    node.reserve(req)
    assert node.reservation()["digest"] == p["digest"]
    assert bound == [("192.168.50.21", p["deployment"]["port"])]
    node.durable_unlink(node.STATE / "gpu.json")
    req["deployment"] = {**req["deployment"], "mode": "tensor", "master_port": 29500}
    with pytest.raises(RuntimeError):
        node.reserve(req)
    assert node.reservation() is None
    req["deployment"]["mode"] = "single"
    report["serving"] = None
    with pytest.raises(RuntimeError):
        node.reserve(req)
    assert node.reservation() is None


@pytest.mark.parametrize("management", [{"ssh_targets": []}, {"ssh_targets": ["one", "one"]},
                                         {"ssh_targets": ["-oProxyCommand=bad"]},
                                         {"ssh_targets": ["one"], "command": "bad"}])
def test_management_rejects_unsafe_or_ambiguous_targets(inputs, management):
    inputs[0]["nodes"]["e8f1"]["management"] = management
    with pytest.raises(config.ConfigError):
        config.validate_inventory(inputs[0])


def test_read_transport_falls_back_without_editing_plan(inputs, monkeypatch):
    p = config.plan(*inputs)
    before = config.plan_sha256(p)
    calls = []
    monkeypatch.setattr(cli.socket, "gethostname", lambda: "controller")

    def execute(command, **kwargs):
        calls.append(command)
        assert "StrictHostKeyChecking=yes" in command
        if command[-2] == "spark-wired":
            raise subprocess.TimeoutExpired(command, kwargs["timeout"])
        return SimpleNamespace(returncode=0, stdout=json.dumps({"ok": True, "result": {"complete": True}}))

    monkeypatch.setattr(cli.subprocess, "run", execute)
    assert cli.call(p, "e8f1", "observe", timeout=0.5)["complete"]
    assert [c[-2] for c in calls] == ["spark-wired", "spark-wifi"]
    assert config.plan_sha256(p) == before


def test_lost_mutation_reply_is_not_replayed_on_backup(inputs, monkeypatch):
    p = config.plan(*inputs)
    actions = []
    monkeypatch.setattr(cli.socket, "gethostname", lambda: "controller")

    def execute(command, **kwargs):
        action = json.loads(kwargs["input"])["action"]
        actions.append((command[-2], action))
        if action == "identity":
            return SimpleNamespace(returncode=0, stdout=json.dumps({"ok": True, "result": {}}))
        raise subprocess.TimeoutExpired(command, kwargs["timeout"])

    monkeypatch.setattr(cli.subprocess, "run", execute)
    with pytest.raises(cli.AmbiguousMutationError):
        cli.call(p, "e8f1", "start", timeout=1)
    assert actions == [("spark-wired", "identity"), ("spark-wired", "start")]


def test_transport_override_cannot_change_pinned_host(inputs):
    original = inputs[0]["nodes"]["e8f1"]
    with pytest.raises(config.ConfigError):
        cli.transport_node(original, {"hostname": "other", "ssh": "backup"})
    with pytest.raises(config.ConfigError):
        cli.transport_node(original, {"serving": {"address": "192.168.1.1"}})
    selected = cli.transport_node(original, {"ssh": "approved-backup"})
    assert config.ssh_targets(selected) == ["approved-backup"]
    assert original["management"]["ssh_targets"] == ["spark-wired", "spark-wifi"]
