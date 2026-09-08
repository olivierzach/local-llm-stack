"""Contract, admission and recovery tests without SSH or GPU dependencies."""
import copy
import importlib
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster import cli, config, node


@pytest.fixture
def inputs():
    return config.load(ROOT, ROOT / "cluster/inventory.json", ROOT / "cluster/deployments/fast-e8f1.json")


@pytest.fixture
def planned(inputs):
    return config.plan(*inputs)


@pytest.fixture
def req(planned, tmp_path, monkeypatch):
    monkeypatch.setattr(node, "STATE", tmp_path)
    return cli.request(planned, "e8f1", "reserve")


def save_reservation(req, phase="started", ids=None):
    node.atomic(node.STATE / "gpu.json", {"owner": req["owner"], "digest": req["digest"],
                "phase": phase, "container_ids": ids or []})


def container(req, identity="original", owner=None, digest=None):
    return {"Id": identity, "Config": {"Labels": {
        "io.spark.owner": owner or req["owner"], "io.spark.digest": digest or req["digest"]}}}


def test_deterministic_and_isolated(inputs, planned):
    assert planned == config.plan(*copy.deepcopy(inputs))
    service = planned["compose"]["e8f1"]["services"]["worker"]
    assert service["container_name"].startswith("spark-fast-e8f1-")
    assert service["pull_policy"] == "never"
    assert service["restart"] == "no"
    assert service["volumes"][0]["read_only"]
    assert planned["recipe"]["revision"] in " ".join(service["command"])
    assert planned["endpoint"]["ready"] is False


def test_saved_plan_recovery_survives_renderer_change(planned, monkeypatch):
    monkeypatch.setattr(config, "plan", lambda *args: {"different": "future-render"})
    config.validate_saved_plan(planned)


def test_saved_plan_rejects_changed_ownership(planned):
    planned["compose"]["e8f1"]["services"]["worker"]["labels"]["io.spark.owner"] = "foreign"
    with pytest.raises(config.ConfigError, match="ownership"):
        config.validate_saved_plan(planned)


def test_placement_keeps_model_contract(inputs, planned):
    inv, recipe, d = copy.deepcopy(inputs)
    d.update(nodes=["66f1"], coordinator="66f1")
    moved = config.plan(inv, recipe, d)
    assert moved["owner"] != planned["owner"]
    for key in ("alias", "capabilities", "context_tokens", "max_output_tokens"):
        assert moved["endpoint"][key] == planned["endpoint"][key]
    assert moved["endpoint"]["base_url"] != planned["endpoint"]["base_url"]


@pytest.mark.parametrize("field,value", [("image", "vllm/vllm-openai:latest"), ("revision", "main"),
    ("extra_args", ["--host", "0.0.0.0"]), ("max_num_seqs", True), ("gpu_memory_utilization", 1.0),
    ("max_output_tokens", 8192), ("parallelism", ["magic"]), ("alias", "../escape")])
def test_invalid_recipe_rejected(inputs, field, value):
    recipe = copy.deepcopy(inputs[1])
    recipe[field] = value
    with pytest.raises(config.ConfigError): config.validate_recipe(recipe)


def test_duplicate_json_keys_rejected(tmp_path):
    p = tmp_path / "bad.json"
    p.write_text('{"version": 1, "version": 2}')
    with pytest.raises(config.ConfigError, match="duplicate JSON"): config.read(p)


def test_inventory_rejects_aliasing_one_host(inputs):
    inv = copy.deepcopy(inputs[0])
    inv["nodes"]["e8f1"]["hostname"] = inv["nodes"]["66f1"]["hostname"]
    with pytest.raises(config.ConfigError, match="duplicate hostname"): config.validate_inventory(inv)


def test_no_implicit_distributed_support(inputs):
    inv, recipe, d = copy.deepcopy(inputs)
    d.update(mode="tensor", nodes=["66f1", "e8f1"], tensor_parallel=2)
    with pytest.raises(config.ConfigError, match="does not support"): config.plan(inv, recipe, d)


def test_explicit_model_template_defaults_are_typed_and_rendered(inputs):
    inv, recipe, deployment = copy.deepcopy(inputs)
    recipe['default_chat_template_kwargs'] = {'enable_thinking': False}
    config.validate_recipe(recipe)
    rendered = config.plan(inv,recipe,deployment)
    command = rendered['compose']['e8f1']['services']['worker']['command']
    assert json.loads(command[command.index('--default-chat-template-kwargs')+1]) == {'enable_thinking':False}
    recipe['default_chat_template_kwargs']['enable_thinking'] = 'false'
    with pytest.raises(config.ConfigError,match='boolean'): config.validate_recipe(recipe)


@pytest.mark.parametrize("coordinator", ["66f1", "e8f1"])
def test_distributed_placement_uses_fabric_and_explicit_ranks(coordinator):
    inv, recipe, d = config.load(ROOT, ROOT / "cluster/inventory.json", ROOT / "cluster/deployments/fast-tp2.json")
    d["coordinator"] = coordinator
    p = config.plan(inv, recipe, d)
    for n, compose in p["compose"].items():
        worker = compose["services"]["worker"]
        argv = worker["command"]
        assert worker["entrypoint"] == ["vllm"]
        assert argv[argv.index("--node-rank") + 1] == ("0" if n == coordinator else "1")
        assert argv[argv.index("--master-addr") + 1] == inv["nodes"][coordinator]["fabric"][0]["ip"]
        assert ("--headless" in argv) == (n != coordinator)
        assert worker["environment"]["VLLM_HOST_IP"] == inv["nodes"][n]["fabric"][0]["ip"]
        assert worker["devices"] == ["/dev/infiniband:/dev/infiniband"]


def test_exclusive_owner(req):
    save_reservation(req, "reserved")
    with pytest.raises(RuntimeError, match="ownership mismatch"):
        node.reserve({**req, "owner": "different-deployment"})
    assert node.reservation()["owner"] == req["owner"]


def test_same_reservation_is_idempotent(req):
    save_reservation(req, "reserved")
    assert node.reserve(req) == {"reserved": True, "existing": True}


def test_busy_gpu_refuses_without_mutation(req, monkeypatch):
    monkeypatch.setattr(node, "doctor", lambda n: {"gpu_processes": ["4321"], "gpu_containers": []})
    with pytest.raises(RuntimeError, match="GPU is busy"): node.reserve(req)
    assert node.reservation() is None


def test_pending_gpu_container_blocks():
    c = {"State": {"Status": "created"}, "HostConfig": {"DeviceRequests": [{"Count": -1}]}}
    assert node.gpu_containers([c]) == [c]
    c["State"]["Status"] = "exited"
    assert node.gpu_containers([c]) == []


@pytest.mark.parametrize("change", ["replacement", "digest", "owner"])
def test_cleanup_preserves_mismatched_containers(req, monkeypatch, change):
    save_reservation(req, ids=["original"])
    c = container(req)
    if change == "replacement": c["Id"] = "replacement"
    if change == "digest": c["Config"]["Labels"]["io.spark.digest"] = "wrong"
    if change == "owner": c["Config"]["Labels"]["io.spark.owner"] = "other"
    monkeypatch.setattr(node, "containers", lambda: [c])
    mutations = []
    monkeypatch.setattr(node, "run", lambda argv, **kw: mutations.append(argv))
    with pytest.raises(RuntimeError, match="cleanup refused"): node.stop(req)
    assert not mutations
    assert node.reservation()


def test_owned_cleanup_leaves_other_workload(req, monkeypatch):
    save_reservation(req, ids=["original"])
    items = [container(req), container(req, identity="foreign", owner="another")]
    monkeypatch.setattr(node, "containers", lambda: items[:])
    def remove(argv, **kw):
        assert argv == ["docker", "rm", "-f", "original"]
        items[:] = [items[1]]
        return ""
    monkeypatch.setattr(node, "run", remove)
    assert node.stop(req)["released"]
    assert items[0]["Id"] == "foreign"
    assert node.reservation() is None


def test_create_reply_loss_can_be_recovered(req, monkeypatch):
    save_reservation(req, phase="creating")
    items = [container(req)]
    monkeypatch.setattr(node, "containers", lambda: items[:])
    monkeypatch.setattr(node, "run", lambda args, **kw: items.clear())
    assert node.stop(req)["released"]
    assert not items


def test_start_failure_rolls_back_new_reservation(planned, tmp_path, monkeypatch):
    actions = []
    def call(p, n, action):
        actions.append(action)
        if action == "status": return {"reservation": None}
        if action == "reserve": return {"reserved": True}
        if action == "start": raise RuntimeError("injected startup failure")
        return {"released": True}
    monkeypatch.setattr(cli, "call", call)
    with pytest.raises(RuntimeError, match="injected"): cli.up(planned, 10, tmp_path)
    assert actions == ["status", "reserve", "start", "stop"]
    assert (tmp_path / "plan.json").exists()
    assert not (tmp_path / "endpoint.json").exists()


def test_failed_retry_does_not_stop_existing_deployment(planned, tmp_path, monkeypatch):
    actions = []
    def call(p, n, action):
        actions.append(action)
        if action == "status": return {"reservation": {"owner": p["owner"]}}
        if action == "reserve": return {"existing": True}
        raise RuntimeError("lost connection")
    monkeypatch.setattr(cli, "call", call)
    with pytest.raises(RuntimeError, match="lost connection"): cli.up(planned, 10, tmp_path)
    assert "stop" not in actions


def test_ready_requires_successful_completion(planned, tmp_path, monkeypatch):
    def call(p, n, action):
        if action == "status": return {"reservation": None}
        if action == "probe": raise RuntimeError("generation failed")
        return {}
    monkeypatch.setattr(cli, "call", call)
    monkeypatch.setattr(cli, "inspect", lambda p: {"healthy": True})
    with pytest.raises(RuntimeError, match="generation failed"): cli.up(planned, 10, tmp_path)
    assert not (tmp_path / "endpoint.json").exists()


def test_collective_records_survive_nccl_log_prefix():
    from spark_cluster.profile import parse_records
    raw = 'NCCL buffered log{"rank": 0, "correct": true}\nNCCL noise\n{"rank": 1, "correct": true}trailing log\n'
    assert parse_records(raw) == [{"rank": 0, "correct": True}, {"rank": 1, "correct": True}]


@pytest.mark.parametrize("busy", ["process", "container", "window"])
def test_workload_lease_requires_idle_gpu(req, monkeypatch, busy):
    report = {"gpu_processes": [], "gpu_containers": [], "research_window": None,
              "memory_mib": {"MemAvailable": 64000}}
    if busy == "process": report["gpu_processes"] = ["123"]
    if busy == "container": report["gpu_containers"] = [{"state": "created"}]
    if busy == "window": report["research_window"] = "entered"
    monkeypatch.setattr(node, "doctor", lambda _: report)
    with pytest.raises(RuntimeError): node.workload_reservation(req)
    assert node.reservation() is None


def test_workload_lease_is_exclusive_and_cannot_be_released_while_running(req, monkeypatch):
    report = {"gpu_processes": [], "gpu_containers": [], "research_window": None,
              "memory_mib": {"MemAvailable": 64000}}
    monkeypatch.setattr(node, "doctor", lambda _: report)
    node.workload_reservation(req)
    other = {**req, "owner": "different"}
    with pytest.raises(RuntimeError, match="ownership"): node.reserve(other)
    with pytest.raises(RuntimeError, match="project-specific"): node.stop(req)
    report["gpu_processes"] = ["123"]
    with pytest.raises(RuntimeError): node.workload_reservation(req, release=True)
    assert node.reservation()["phase"] == "workload"
    report["gpu_processes"] = []
    assert node.workload_reservation(req, release=True)["released"]
    assert node.reservation() is None


def test_tool_capability_requires_parser(inputs):
    r = copy.deepcopy(inputs[1])
    r["capabilities"]["tools"] = True
    with pytest.raises(config.ConfigError, match="explicit supported parser"): config.validate_recipe(r)
    r["tool_call_parser"] = "hermes"
    config.validate_recipe(r)
    p = config.plan(inputs[0], r, inputs[2])
    command = p["compose"]["e8f1"]["services"]["worker"]["command"]
    assert command[-3:] == ["--enable-auto-tool-choice", "--tool-call-parser", "hermes"]


def test_vision_recipe_bounds_render_and_validate():
    inv, recipe, deployment = config.load(ROOT, ROOT/'cluster/inventory.json', ROOT/'cluster/deployments/vision-e8f1.json')
    planned = config.plan(inv, recipe, deployment)
    command = planned['compose']['e8f1']['services']['worker']['command']
    assert json.loads(command[command.index('--limit-mm-per-prompt')+1]) == {'image': 2, 'video': 0}
    assert json.loads(command[command.index('--mm-processor-kwargs')+1]) == {'min_pixels': 65536, 'max_pixels': 1048576}
    for field, value in [('max_images', 0), ('max_images', True), ('max_pixels', 1024), ('max_pixels', 16777217)]:
        changed = {**recipe, 'image_processing': {**recipe['image_processing'], field: value}}
        with pytest.raises(ValueError): config.validate_recipe(changed)
    changed = {**recipe, 'capabilities': {**recipe['capabilities'], 'vision': False}}
    with pytest.raises(ValueError, match='vision capability'): config.validate_recipe(changed)
