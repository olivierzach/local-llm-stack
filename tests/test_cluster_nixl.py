"""External recipes retain controller ownership and fail-closed NIXL admission."""
import copy
import json
import os
from pathlib import Path
import socket
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster import cli, config, node


@pytest.fixture
def inputs():
    inv, recipe, deployment = config.load(
        ROOT, ROOT / "cluster/inventory.json", ROOT / "cluster/deployments/fast-e8f1.json")
    recipe["extra_args"] = [arg for arg in recipe["extra_args"] if arg != "--enable-prefix-caching"]
    recipe["prefix_caching"] = False
    recipe["kv_transfer"] = {"role": "kv_producer", "side_channel_port": 19001,
                             "buffer_device": "cuda", "transport": "rdma"}
    recipe["parallelism"] = ["single"]
    return inv, recipe, deployment


@pytest.mark.parametrize("field,value", [
    ("role", "kv_both"), ("side_channel_port", True), ("side_channel_port", 1023),
    ("side_channel_port", 65536), ("buffer_device", "auto"), ("transport", "auto"),
    ("environment", {"UCX_TLS": "all"}),
])
def test_invalid_transfer_options_rejected(inputs, field, value):
    recipe = inputs[1]
    recipe["kv_transfer"][field] = value
    with pytest.raises(config.ConfigError):
        config.validate_recipe(recipe)


@pytest.mark.parametrize("change", ["prefix-type", "prefix-conflict", "speculation", "parallelism"])
def test_unsupported_option_combinations_rejected(inputs, change):
    recipe = inputs[1]
    if change == "prefix-type":
        recipe["prefix_caching"] = "false"
    elif change == "prefix-conflict":
        recipe["extra_args"].append("--enable-prefix-caching")
    elif change == "speculation":
        recipe["speculative_config"] = {"method": "mtp", "num_speculative_tokens": 2}
    else:
        recipe["parallelism"].append("tensor")
    with pytest.raises(config.ConfigError):
        config.validate_recipe(recipe)


@pytest.mark.parametrize("change", ["api-port", "master-port", "gpus", "tensor", "pipeline", "nodes"])
def test_invalid_transfer_placement_rejected(inputs, change):
    inv, recipe, deployment = inputs
    if change == "api-port":
        deployment["port"] = recipe["kv_transfer"]["side_channel_port"]
    elif change == "master-port":
        deployment["master_port"] = recipe["kv_transfer"]["side_channel_port"]
    elif change == "gpus":
        inv["nodes"]["e8f1"]["gpus"] = 2
    elif change == "nodes":
        deployment["nodes"].append("66f1")
    else:
        deployment[change + "_parallel"] = 2
    with pytest.raises(config.ConfigError):
        config.plan(inv, recipe, deployment)


@pytest.mark.parametrize("transport,role,buffer,prefix", [
    ("rdma", "kv_producer", "cuda", False),
    ("tcp", "kv_consumer", "cpu", True),
])
def test_nixl_launch_is_fabric_bound_and_offline(inputs, transport, role, buffer, prefix):
    inv, recipe, deployment = inputs
    recipe["kv_transfer"].update(transport=transport, role=role, buffer_device=buffer)
    recipe["prefix_caching"] = prefix
    config.validate_recipe(recipe)
    p = config.plan(inv, recipe, deployment)
    worker = p["compose"]["e8f1"]["services"]["worker"]
    command, environment = worker["command"], worker["environment"]
    rail = inv["nodes"]["e8f1"]["fabric"][0]
    transfer = json.loads(command[command.index("--kv-transfer-config") + 1])
    assert transfer == {"kv_connector": "NixlConnector", "kv_role": role,
                        "kv_buffer_device": buffer, "kv_load_failure_policy": "fail",
                        "kv_connector_extra_config": {"backends": ["UCX"]}}
    assert ("--enable-prefix-caching" in command) == prefix
    assert ("--no-enable-prefix-caching" in command) != prefix
    assert environment["UCX_TLS"] == (("rc,ud" if transport == "rdma" else "tcp") +
                                      ",sm,self,cuda_copy,cuda_ipc")
    assert environment["UCX_NET_DEVICES"] == (rail["rdma"] + ":1" if transport == "rdma" else rail["interface"])
    assert environment["VLLM_NIXL_SIDE_CHANNEL_HOST"] == rail["ip"]
    assert environment["VLLM_NIXL_SIDE_CHANNEL_PORT"] == "19001"
    assert command[command.index("--host") + 1] == rail["ip"]
    assert command[command.index("--model") + 1] == (
        "/cache/hub/models--" + recipe["model"].replace("/", "--") + "/snapshots/" + recipe["revision"])
    assert environment["HF_HUB_OFFLINE"] == environment["TRANSFORMERS_OFFLINE"] == "1"
    assert worker["devices"] == ["/dev/infiniband:/dev/infiniband"]
    assert worker["cap_add"] == ["IPC_LOCK"]
    assert worker["ulimits"]["memlock"] == {"soft": -1, "hard": -1}
    assert worker["labels"] == {"io.spark.owner": p["owner"], "io.spark.digest": p["digest"]}
    assert f"http://{rail['ip']}:{deployment['port']}/health" in worker["healthcheck"]["test"][-1]


def test_external_recipe_render_and_saved_recovery(inputs, tmp_path, monkeypatch):
    inv, recipe, deployment = inputs
    deployment["recipe"] = "external-only"
    recipe_path, deployment_path = tmp_path / "recipe.json", tmp_path / "deployment.json"
    recipe_path.write_text(json.dumps(recipe))
    deployment_path.write_text(json.dumps(deployment))
    output = tmp_path / "rendered"
    assert cli.main(["render", "--deployment", str(deployment_path), "--recipe-file", str(recipe_path),
                     "--output", str(output)]) == 0
    rendered = config.read(output / "plan.json")
    expected = config.plan(inv, recipe, deployment)
    assert rendered == expected
    recipe_path.unlink()
    deployment_path.unlink()
    stopped = []
    def call(plan, node_id, action):
        stopped.append((plan["owner"], node_id, action))
        return {"released": True}
    monkeypatch.setattr(cli, "call", call)
    assert cli.main(["down", "--saved-plan", str(output / "plan.json")]) == 0
    assert stopped == [(rendered["owner"], "e8f1", "stop")]


def test_recipe_source_cannot_override_saved_recovery():
    with pytest.raises(SystemExit) as error:
        cli.main(["down", "--saved-plan", "missing.json", "--recipe-file", "other.json"])
    assert error.value.code == 2
    with pytest.raises(config.ConfigError):
        cli.main(["render", "--recipe-file", "missing.json"])


@pytest.fixture
def admission(inputs, tmp_path, monkeypatch):
    p = config.plan(*inputs)
    request = cli.request(p, "e8f1", "reserve")
    request["node"]["cache"] = str(tmp_path / "cache")
    request["node"]["fabric"][0]["ip"] = "127.0.0.1"
    snapshot = (Path(request["node"]["cache"]) / "hub" /
                ("models--" + request["recipe"]["model"].replace("/", "--")) /
                "snapshots" / request["recipe"]["revision"])
    snapshot.mkdir(parents=True)
    (snapshot / "config.json").write_text("{}")
    (snapshot / "model.safetensors").write_bytes(b"fixture")
    devices = tmp_path / "dev/infiniband"
    devices.mkdir(parents=True)
    for name in ("rdma_cm", "uverbs0"):
        (devices / name).symlink_to(os.devnull)
    sysfs = tmp_path / "sys/class/infiniband"
    verbs = sysfs / request["node"]["fabric"][0]["rdma"] / "device/infiniband_verbs"
    verbs.mkdir(parents=True)
    (verbs / "uverbs0").mkdir()
    def mapped_path(value):
        return {"/dev/infiniband": devices, "/sys/class/infiniband": sysfs}.get(str(value), Path(value))
    monkeypatch.setattr(node, "Path", mapped_path)
    monkeypatch.setattr(node, "STATE", tmp_path / "state")
    report = {"reservation": None, "research_window": "released", "gpu_processes": [],
              "gpu_containers": [], "memory_mib": {"MemAvailable": 120000}, "fabric": []}
    monkeypatch.setattr(node, "doctor", lambda _: copy.deepcopy(report))
    monkeypatch.setattr(node, "run", lambda args: json.dumps([{"Architecture": "arm64"}]))
    request["deployment"]["port"] = 0
    request["recipe"]["kv_transfer"]["side_channel_port"] = 0
    return request, devices, verbs


def test_occupied_side_channel_blocks_preflight_and_reserve(admission):
    request, _, _ = admission
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        request["recipe"]["kv_transfer"]["side_channel_port"] = listener.getsockname()[1]
        report = node.preflight(request)
        assert not report["launchable"]
        assert {check["check"] for check in report["checks"] if not check["passed"]} == {"kv-side-channel-port"}
        with pytest.raises(OSError):
            node.reserve(request)
        assert node.reservation() is None
    assert node.preflight(request)["launchable"]
    assert node.reserve(request)["reserved"]
    assert node.reservation()["owner"] == request["owner"]


@pytest.mark.parametrize("missing", ["selected-hca", "verbs-device", "rdma-cm", "permissions"])
def test_inaccessible_devices_block_both_admission_paths(admission, monkeypatch, missing):
    request, devices, verbs = admission
    if missing == "selected-hca":
        (verbs / "uverbs0").rmdir()
    elif missing == "verbs-device":
        (devices / "uverbs0").unlink()
    elif missing == "rdma-cm":
        (devices / "rdma_cm").unlink()
    else:
        monkeypatch.setattr(node.os, "access", lambda *args: False)
    report = node.preflight(request)
    assert not report["launchable"]
    assert {check["check"] for check in report["checks"] if not check["passed"]} == {"rdma-devices"}
    with pytest.raises(RuntimeError):
        node.reserve(request)
    assert node.reservation() is None
