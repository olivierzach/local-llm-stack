"""CPU regressions for persistent fencing, truthful observation and exact restore."""
import copy
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import stat
import sys
import threading

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster import cli, config, node


@pytest.fixture
def setup(tmp_path, monkeypatch):
    p = config.plan(*config.load(ROOT, ROOT / "cluster/inventory.json",
                                ROOT / "cluster/deployments/fast-e8f1.json"))
    monkeypatch.setattr(node, "STATE", tmp_path / "node-state")
    monkeypatch.setattr(node, "verify_host", lambda n: None)
    monkeypatch.setattr(node, "containers", lambda: [])
    monkeypatch.setattr(node, "run", lambda args, **kwargs: "")
    return p, cli.request(p, "e8f1", "fence", recovery={"policy": "local-auto", "authority": "authority-a", "generation": 1})


def establish(req):
    return node.main({**req, "action": "fence", "allowed_digests": [req["digest"]]})


@pytest.mark.parametrize("action", ["reserve", "start", "stop", "reserve-batch", "reserve-workload",
                                    "release-workload", "cache-clear", "probe"])
def test_fenced_node_rejects_every_legacy_gpu_path(setup, action):
    _, req = setup
    establish(req)
    legacy = {k: v for k, v in req.items() if k != "recovery"}
    with pytest.raises(RuntimeError):
        node.main({**legacy, "action": action})
    assert node.recovery_fence()["active"]
    assert node.reservation() is None


def test_generation_persists_after_stop_and_rejects_stale_controller(setup):
    _, req = setup
    initial = establish(req)
    assert establish(req) == initial
    newer = {**req, "recovery": {**req["recovery"], "generation": 2}}
    establish(newer)
    with pytest.raises(RuntimeError):
        establish(req)
    node.main({**newer, "action": "stop"})
    # Loading again from disk, not an in-memory cache, retains the fence.
    assert node.recovery_fence()["generation"] == 2
    with pytest.raises(RuntimeError):
        node.main({**req, "action": "stop"})
    with pytest.raises(RuntimeError):
        establish({**newer, "recovery": {**newer["recovery"], "authority": "authority-b"}})


def test_concurrent_authorities_cannot_both_acquire_node(setup):
    _, req = setup
    barrier = threading.Barrier(2)

    def acquire(authority):
        barrier.wait()
        try:
            establish({**req, "recovery": {**req["recovery"], "authority": authority}})
            return authority
        except RuntimeError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        accepted = list(pool.map(acquire, ["authority-a", "authority-b"]))
    winners = [value for value in accepted if value]
    assert len(winners) == 1
    assert node.recovery_fence()["authority"] == winners[0]


def test_foreign_reservation_and_native_gpu_process_prevent_initial_fence(setup, monkeypatch):
    _, req = setup
    node.atomic(node.STATE / "gpu.json", {"owner": "unrelated", "digest": "f" * 64})
    with pytest.raises(RuntimeError):
        establish(req)
    assert node.recovery_fence() is None
    node.durable_unlink(node.STATE / "gpu.json")
    monkeypatch.setattr(node, "run", lambda args, **kwargs: "1234")
    with pytest.raises(RuntimeError):
        establish(req)
    assert node.recovery_fence() is None


def test_release_requires_idle_matching_context_and_retains_generation_tombstone(setup):
    _, req = setup
    establish(req)
    node.atomic(node.STATE / "gpu.json", {"owner": req["owner"], "digest": req["digest"],
                                          "phase": "reserved", "container_ids": []})
    with pytest.raises(RuntimeError):
        node.main({**req, "action": "release-fence"})
    node.main({**req, "action": "stop"})
    assert node.main({**req, "action": "release-fence"})["active"] is False
    with pytest.raises(RuntimeError):
        establish(req)
    assert establish({**req, "recovery": {**req["recovery"], "generation": 2}})["active"]


def test_corrupt_gate_is_not_treated_as_unfenced(setup):
    _, req = setup
    node.atomic(node.STATE / "recovery-fence.json", None)
    legacy = {k: v for k, v in req.items() if k != "recovery"}
    with pytest.raises(RuntimeError):
        node.main({**legacy, "action": "stop"})


def test_observe_reports_partial_failure_without_any_writes_or_mutex(setup, monkeypatch):
    _, req = setup
    def forbidden(*args, **kwargs):
        raise AssertionError("observation must not mutate")
    monkeypatch.setattr(node, "locked", forbidden)
    monkeypatch.setattr(node, "atomic", forbidden)
    monkeypatch.setattr(node, "check_port", forbidden)
    monkeypatch.setattr(node, "memory", lambda: {"MemAvailable": 100})
    monkeypatch.setattr(node, "containers", lambda: (_ for _ in ()).throw(RuntimeError("secret environment")))
    monkeypatch.setattr(node, "interface_health", lambda rail: {**rail, "available": False, "ready": False, "errors": {"carrier": "unavailable"}})
    result = node.main({**req, "action": "observe"})
    assert not result["complete"]
    assert result["containers"] is None
    assert result["memory_mib"]["MemAvailable"] == 100
    assert result["reservation"] is None
    assert "secret environment" not in json.dumps(result)
    assert not node.STATE.exists()


def test_cli_observation_keeps_reachable_nodes_and_writes_exclusive_private_evidence(setup, tmp_path, monkeypatch):
    p, _ = setup
    nodes = {"good": p["nodes"]["e8f1"], "bad": {**p["nodes"]["e8f1"], "hostname": "absent"}}
    def remote(n, request, **kwargs):
        if n["hostname"] == "absent":
            raise TimeoutError("unreachable")
        return {"complete": True, "hostname": n["hostname"]}
    monkeypatch.setattr(cli, "remote", remote)
    result = cli.observe(nodes, timeout=0.5)
    assert not result["complete"] and result["nodes"]["good"]["complete"]
    assert result["nodes"]["bad"]["error"]
    target = tmp_path / "observation.json"
    cli.save_exclusive(target, result)
    assert stat.S_IMODE(target.stat().st_mode) == 0o600
    original = target.read_bytes()
    with pytest.raises(FileExistsError):
        cli.save_exclusive(target, {"wrong": True})
    assert target.read_bytes() == original


@pytest.mark.parametrize("changed", ["compose", "endpoint"])
def test_full_plan_hash_rejects_nonidentity_tampering_before_activation(setup, tmp_path, monkeypatch, changed):
    p, _ = setup
    expected = config.plan_sha256(p)
    altered = copy.deepcopy(p)
    if changed == "compose":
        altered["compose"]["e8f1"]["services"]["worker"]["command"] += ["--evil"]
    else:
        altered["endpoint"]["base_url"] = "http://192.168.1.99:9999/v1"
    assert altered["digest"] == p["digest"]
    monkeypatch.setattr(cli, "call", lambda *args, **kwargs: pytest.fail("tampered plan reached a node"))
    with pytest.raises(config.ConfigError):
        cli.up(altered, 10, tmp_path / "output", expected_sha256=expected, saved_plan=True)
    assert not (tmp_path / "output").exists()


def test_exact_activation_never_rerenders_or_rewrites_old_plan(setup, tmp_path, monkeypatch):
    p, _ = setup
    saved = tmp_path / "plan.json"
    saved.write_text(json.dumps(p, separators=(",", ":")))
    original = saved.read_bytes()
    monkeypatch.setattr(cli, "plan", lambda *args: pytest.fail("saved activation re-rendered"))
    reservation = {"owner": p["owner"], "digest": p["digest"], "container_ids": ["worker-id"], "phase": "started"}
    live = {"id": "worker-id", "owner": p["owner"], "digest": p["digest"], "state": "running", "health": "healthy",
            "image": p["recipe"]["image"], "image_id": "sha256:actual", "started_at": "2026-09-15T01:00:00Z", "restart_count": 0}
    def call(plan, node_id, action, **kwargs):
        if action in ("status", "start"):
            return {"reservation": reservation, "containers": [live]}
        if action == "reserve":
            return {"existing": True}
        if action == "probe":
            return {"ready": True}
        pytest.fail("healthy existing deployment must not be stopped")
    monkeypatch.setattr(cli, "call", call)
    assert cli.main(["up", "--saved-plan", str(saved), "--plan-sha256", config.plan_sha256(p)]) == 0
    assert saved.read_bytes() == original
    assert json.loads((tmp_path / "endpoint.json").read_text())["ready"]


def test_lost_reply_cleanup_reconciles_and_never_stops_foreign_owner(setup, tmp_path, monkeypatch):
    p, req = setup
    state = {"reservation": None, "foreign_stopped": False}
    def call(plan, node_id, action, **kwargs):
        if action == "status":
            return {"reservation": state["reservation"]}
        if action == "reserve":
            state["reservation"] = {"owner": p["owner"], "digest": p["digest"]}
            return {"reserved": True}
        if action == "start":
            state["reservation"] = {"owner": "different", "digest": "e" * 64}
            raise cli.AmbiguousMutationError("reply lost")
        if action == "stop":
            state["foreign_stopped"] = True
    monkeypatch.setattr(cli, "call", call)
    with pytest.raises(cli.AmbiguousMutationError):
        cli.up(p, 10, tmp_path / "out", recovery=req["recovery"], expected_sha256=config.plan_sha256(p))
    assert not state["foreign_stopped"]
    assert not (tmp_path / "out" / "endpoint.json").exists()


def test_health_cannot_promote_wrong_image_or_replaced_container(setup, monkeypatch):
    p, _ = setup
    response = {"reservation": {"owner": p["owner"], "digest": p["digest"], "container_ids": ["old"]},
                "containers": [{"id": "new", "state": "running", "health": "healthy", "owner": p["owner"],
                                "digest": p["digest"], "image": "untrusted", "image_id": "x", "started_at": "now", "restart_count": 0}]}
    monkeypatch.setattr(cli, "call", lambda *args, **kwargs: response)
    assert not cli.inspect(p)["healthy"]


def test_observation_overall_deadline_returns_unknown_not_green(setup, monkeypatch):
    p, _ = setup
    release = threading.Event()

    def blocked(*args, **kwargs):
        release.wait()
        return {"complete": True}

    monkeypatch.setattr(cli, "remote", blocked)
    try:
        result = cli.observe(p["nodes"], timeout=0.02)
        assert not result["complete"]
        assert result["nodes"]["e8f1"]["error"]
    finally:
        release.set()


def test_status_does_not_take_mutation_lock(setup, monkeypatch):
    _, req = setup

    def forbidden():
        pytest.fail("status attempted mutation lock")

    monkeypatch.setattr(node, "locked", forbidden)
    result = node.main({**req, "action": "status"})
    assert result["reservation"] is None and result["containers"] == []
    assert not node.STATE.exists()
