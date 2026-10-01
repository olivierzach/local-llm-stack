"""Deterministic ownership, crash and hysteresis regressions; no hosts or inference."""
import copy
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster import config, recovery


class Clock:
    def __init__(self):
        self.now = 100000.0
        self.elapsed = 1000.0

    def wall(self):
        return self.now

    def mono(self):
        return self.elapsed

    def advance(self, seconds):
        self.now += seconds
        self.elapsed += seconds


class Crash(BaseException):
    pass


class MemoryNodes:
    """External boundary only; Controller/Journal/registry/lease files remain real."""
    def __init__(self, policy, clock):
        self.policy, self.clock = policy, clock
        preferred = recovery.load_plan(policy["preferred"])
        self.nodes = {}
        for node, identity in preferred["nodes"].items():
            self.nodes[node] = {"hostname": identity["hostname"], "architecture": identity["architecture"],
                                "reservation": None, "containers": [], "gpu_containers": [], "gpu_processes": [],
                                "recovery_fence": None, "serving": {"ready": True, "available": True},
                                "fabric": [{"ready": True, "available": True} for _ in identity["fabric"]], "errors": {}}
        self.offline = set()
        self.active_requests = 0
        self.gateway_down = False
        self.prepared = True
        self.events = []
        self.crash_after = None
        self.fail_start = set()
        self.lose_start_reply = False
        self.observe_delay = 0

    def event(self, action, node=None):
        self.events.append((action, node))
        if action == self.crash_after:
            self.crash_after = None
            raise Crash(action)

    def observe(self):
        self.clock.advance(self.observe_delay)
        return {n: ({"complete": False} if n in self.offline else copy.deepcopy(o)) for n, o in self.nodes.items()}

    def install(self, ref):
        p = recovery.load_plan(ref)
        for node in p["nodes"]:
            container = {"id": p["digest"][:12] + node, "owner": p["owner"], "digest": p["digest"],
                         "state": "running", "health": "healthy", "image": p["recipe"]["image"],
                         "image_id": "sha256:runtime", "started_at": "2026-01-01T00:00:00Z", "restart_count": 0}
            self.nodes[node].update(reservation={"owner": p["owner"], "digest": p["digest"], "phase": "started", "container_ids": [container["id"]]},
                                    containers=[container], gpu_containers=[container], gpu_processes=["123"])

    def fence(self, ref, node, context, digests):
        assert node not in self.offline
        old = self.nodes[node]["recovery_fence"]
        if old:
            assert old["authority"] == context["authority"] and old["generation"] <= context["generation"]
        self.nodes[node]["recovery_fence"] = {**context, "allowed_digests": digests, "active": True}
        self.event("fence", node)
        return self.nodes[node]["recovery_fence"]

    def invoke(self, ref, node, action, context):
        assert action == "stop" and node not in self.offline
        p = recovery.load_plan(ref)
        reservation = self.nodes[node]["reservation"]
        assert reservation["owner"] == p["owner"] and reservation["digest"] == p["digest"]
        assert self.nodes[node]["recovery_fence"]["generation"] == context["generation"]
        self.nodes[node].update(reservation=None, containers=[], gpu_containers=[], gpu_processes=[])
        self.event("stop", node)
        return {"released": True}

    def start(self, ref, context):
        p = recovery.load_plan(ref)
        assert all(n not in self.offline and self.nodes[n]["reservation"] is None for n in p["nodes"])
        self.install(ref)
        if ref["sha256"] in self.fail_start:
            for node in p["nodes"]:
                self.nodes[node]["gpu_containers"][0]["health"] = "unhealthy"
            self.event("failed-start", ref["sha256"])
            raise RuntimeError("startup failed after partial owned creation")
        self.event("start", ref["sha256"])
        if self.lose_start_reply:
            self.lose_start_reply = False
            raise recovery.cli.AmbiguousMutationError("dispatched reply lost")
        return {"ready": True}

    def gateway_status(self):
        if self.gateway_down:
            raise OSError("gateway unavailable")
        state = recovery.recovery_routes.private_json(self.policy["gateway"]["route_state"])
        return {**state, "active_requests": self.active_requests, "fresh": state["expires_at"] > self.clock.wall()}

    def endpoint(self, p):
        registry = recovery.recovery_routes.private_json(self.policy["gateway"]["registry"])
        return registry["routes"].get(p["recipe"]["alias"]) == recovery.route_for(p)

    def prepare(self, ref):
        return self.prepared

    def isolation(self, refs):
        return {"verified": True}


@pytest.fixture
def rig(tmp_path):
    clock = Clock()
    inv, recipe, deployment = config.load(ROOT, ROOT / "cluster/inventory.json",
                                          ROOT / "cluster/deployments/large-tp2-mtp2-tools-nccl2307-e8f1.json")
    for index, node in enumerate(sorted(inv["nodes"])):
        inv["nodes"][node]["serving"] = {"address": "192.168.50." + str(21 + index), "interface": "wlan0"}
    plans = [config.plan(inv, recipe, deployment)]
    for node in ("e8f1", "66f1"):
        _, recipe, deployment = config.load(ROOT, ROOT / "cluster/inventory.json", ROOT / f"cluster/deployments/coder-{node}.json")
        plans.append(config.plan(inv, recipe, deployment))
    refs = []
    for index, plan in enumerate(plans):
        path = tmp_path / f"plan-{index}.json"
        recovery.recovery_routes.atomic_json(path, plan)
        refs.append({"path": str(path), "sha256": config.plan_sha256(plan), "qualification": None})
    for ref, node in zip(refs[1:], ("e8f1", "66f1")):
        ref["node"] = node
    policy = {"version": 1, "id": "test-recovery", "alias": "local-auto", "preferred": refs[0], "fallbacks": refs[1:],
              "management": {node: {"ssh": "independent-" + node} for node in inv["nodes"]},
              "gateway": {"url": "http://127.0.0.1:19842", "key_file": str(tmp_path / "ingress/key"),
                          "registry": str(tmp_path / "ingress/registry.json"), "route_state": str(tmp_path / "ingress/route.json"), "isolation": None},
              "timings": {"stable_return": 10, "minimum_dwell": 10, "drain": 10, "backoff_initial": 10, "backoff_max": 40}}
    for index, ref in enumerate(refs):
        p = recovery.load_plan(ref)
        staging = {}
        for node in p["nodes"]:
            path = tmp_path / f"staging-{index}-{node}.json"
            recovery.recovery_routes.atomic_json(path, {"node": node, "verified_weights": True})
            staging[node] = {"path": str(path), "sha256": recovery.digest_file(path)}
        receipt = {"version": 1, "policy": policy["id"], "plan_sha256": ref["sha256"], "deployment_digest": p["digest"],
                   "nodes": sorted(p["nodes"]), "gateway_url": policy["gateway"]["url"], "route": recovery.route_for(p),
                   "checks": dict.fromkeys(recovery.CHECKS, True), "completed_at": clock.wall(), "staging": staging}
        path = tmp_path / f"qualified-{index}.json"
        recovery.recovery_routes.atomic_json(path, receipt)
        ref["qualification"] = {"path": str(path), "sha256": recovery.digest_file(path)}
    path = tmp_path / "policy.json"
    recovery.recovery_routes.atomic_json(path, policy)
    policy = recovery.load_policy(path)
    recovery.recovery_routes.atomic_json(policy["gateway"]["registry"], recovery.gateway.from_plans([plans[0]]))
    directory = tmp_path / "authority"
    journal = recovery.Journal(directory, policy, clock=clock.wall)
    journal.value["enabled"] = True
    journal.save()
    adapter = MemoryNodes(policy, clock)
    adapter.install(policy["preferred"])
    def reopen():
        fresh = recovery.Journal(directory, policy, clock=clock.wall)
        return recovery.Controller(policy, fresh, adapter, wall=clock.wall, monotonic=clock.mono, sleep=clock.advance)
    controller = reopen()
    return controller, adapter, clock, reopen, path


def step(controller, clock, count=1):
    for _ in range(count):
        clock.advance(5)
        controller.tick()


@pytest.mark.parametrize("failed", ["e8f1", "66f1"])
def test_symmetric_loss_and_stable_automatic_return(rig, failed):
    c, nodes, clock, reopen, _ = rig
    c.tick()
    old = copy.deepcopy(nodes.nodes[failed]["reservation"])
    nodes.offline.add(failed)
    step(c, clock)
    assert c.s["phase"] == "fallback"
    assert c.s["current"]["node"] != failed
    assert nodes.nodes[failed]["reservation"] == old
    assert failed in c.s["quarantine"]
    assert ("stop", failed) not in nodes.events
    selected = recovery.load_plan(c.s["current"])
    assert nodes.endpoint(selected)
    nodes.offline.clear()
    step(c, clock, 4)
    assert c.s["phase"] == "preferred"
    assert c.s["current"]["sha256"] == c.policy["preferred"]["sha256"]
    assert not c.s["quarantine"]
    assert all(o["reservation"]["digest"] == recovery.load_plan(c.policy["preferred"])["digest"] for o in nodes.nodes.values())


def test_cable_failure_uses_independent_single_and_restores_exact_group(rig):
    c, nodes, clock, _, _ = rig
    c.tick()
    for o in nodes.nodes.values():
        for rail in o["fabric"]:
            rail["ready"] = False
    step(c, clock)
    assert c.s["phase"] == "fallback"
    assert sum(o["reservation"] is not None for o in nodes.nodes.values()) == 1
    for o in nodes.nodes.values():
        for rail in o["fabric"]:
            rail["ready"] = True
    step(c, clock, 4)
    assert c.s["phase"] == "preferred"


def fallback(rig):
    c, nodes, clock, reopen, path = rig
    c.tick()
    nodes.offline.add("66f1")
    step(c, clock)
    nodes.offline.clear()
    return c, nodes, clock, reopen, path


def test_drain_timeout_reopens_exact_single_and_retry_survives_restart(rig):
    c, nodes, clock, reopen, _ = fallback(rig)
    nodes.active_requests = 1
    before = len([e for e in nodes.events if e[0] == "stop"])
    step(c, clock, 4)
    assert c.s["phase"] == "fallback" and c.s["retry_count"] == 1
    assert c.s["route"]["accepting"] is True
    assert len([e for e in nodes.events if e[0] == "stop"]) == before
    retry = c.s["retry_at"]
    c = reopen()
    assert c.s["retry_at"] == retry
    nodes.active_requests = 0
    step(c, clock, 5)
    assert c.s["phase"] == "preferred"


def test_failed_promotion_cleans_partial_under_new_epoch_and_rolls_back(rig):
    c, nodes, clock, _, _ = fallback(rig)
    previous = c.s["current"]["sha256"]
    old_epoch = c.s["epoch"]
    nodes.fail_start.add(c.s["preferred"]["sha256"])
    step(c, clock, 3)
    assert c.s["intent"]["rollback"]
    step(c, clock)
    assert c.s["phase"] == "fallback" and c.s["current"]["sha256"] == previous
    assert c.s["epoch"] > old_epoch + 1
    assert c.s["retry_count"] == 1
    assert sum(o["reservation"] is not None for o in nodes.nodes.values()) == 1


def test_failed_rollback_stays_closed_and_circuit_persists(rig):
    c, nodes, clock, reopen, _ = fallback(rig)
    nodes.fail_start.update([c.s["preferred"]["sha256"], c.s["current"]["sha256"]])
    step(c, clock, 20)
    assert c.s["circuit_open"] and c.s["route"]["accepting"] is False
    events = len(nodes.events)
    c = reopen()
    step(c, clock, 20)
    assert c.s["circuit_open"] and len(nodes.events) == events


@pytest.mark.parametrize("boundary", ["fence", "stop", "start"])
def test_crash_after_side_effect_reconciles_without_duplicate_start(rig, boundary):
    c, nodes, clock, reopen, _ = rig
    c.tick()
    nodes.offline.add("66f1")
    nodes.crash_after = boundary
    with pytest.raises(Crash):
        step(c, clock)
    generation = c.s["generation"]
    c = reopen()
    step(c, clock)
    assert c.s["phase"] == "fallback"
    assert c.s["generation"] > generation
    assert len([e for e in nodes.events if e[0] == "start"]) == 1
    assert nodes.nodes["66f1"]["reservation"] is not None


def test_dropped_start_reply_reconciles_completed_start_under_new_epoch(rig):
    c, nodes, clock, reopen, _ = rig
    c.tick()
    nodes.offline.add("66f1")
    nodes.lose_start_reply = True
    step(c, clock)
    assert c.s["route"]["accepting"] is False
    epoch = c.s["epoch"]
    c = reopen()
    step(c, clock, 4)
    assert c.s["phase"] == "fallback"
    assert c.s["epoch"] >= epoch
    assert len([e for e in nodes.events if e[0] == "start"]) == 1


def test_gateway_only_failure_never_restarts_workers(rig):
    c, nodes, clock, _, _ = rig
    c.tick()
    events = list(nodes.events)
    nodes.gateway_down = True
    step(c, clock)
    assert c.s["phase"] == "gateway-unavailable" and not c.s["route"]["accepting"]
    assert nodes.events == events
    nodes.gateway_down = False
    step(c, clock)
    assert c.s["phase"] == "preferred" and nodes.events == events


def test_foreign_container_or_authority_never_gets_cleaned(rig):
    c, nodes, clock, _, _ = rig
    c.tick()
    nodes.nodes["e8f1"]["gpu_containers"][0]["id"] = "foreign-replacement"
    before = list(nodes.events)
    with pytest.raises(config.ConfigError):
        step(c, clock)
    assert nodes.events == before


def test_stalled_observation_and_sleep_close_until_fresh_reconciliation(rig):
    c, nodes, clock, _, _ = rig
    c.tick()
    nodes.observe_delay = 20
    step(c, clock)
    assert not c.s["route"]["accepting"] and c.s["stable_since"] is None
    nodes.observe_delay = 0
    clock.now += 3600  # macOS suspend may advance wall without monotonic.
    step(c, clock)
    assert c.s["phase"] == "preferred"  # only after this tick's fresh observation
    assert not [e for e in nodes.events if e[0] in ("start", "stop")]


def test_missing_qualification_cannot_activate_or_select_single(rig):
    c, nodes, clock, _, _ = rig
    c.s["enabled"] = False
    c.journal.save()
    Path(c.policy["fallbacks"][0]["qualification"]["path"]).unlink()
    request = recovery.queue_command(c.journal.directory, c.policy, "enable")
    c.tick()
    assert c.s["commands"][request]["state"] == "refused"
    assert not c.s["enabled"] and not c.s["route"]["accepting"]
    assert not nodes.events


def test_singleton_queue_and_reset_preserve_highwaters_and_dead_ownership(rig):
    c, nodes, clock, reopen, _ = rig
    c.tick()
    nodes.offline.add("66f1")
    step(c, clock)
    epoch, generation = c.s["epoch"], c.s["generation"]
    with recovery.lock(c.journal.directory / "authority.lock"):
        with pytest.raises(BlockingIOError):
            with recovery.lock(c.journal.directory / "authority.lock"):
                pass
        request = recovery.queue_command(c.journal.directory, c.policy, "reset")
    step(c, clock)
    assert c.s["commands"][request]["state"] == "applied"
    assert not c.s["enabled"] and nodes.nodes["e8f1"]["reservation"] is None
    assert nodes.nodes["66f1"]["reservation"] is not None and "66f1" in c.s["quarantine"]
    c = reopen()
    assert c.s["epoch"] > epoch and c.s["generation"] > generation


def test_receipt_cannot_qualify_other_node_or_changed_compose(rig):
    c, _, clock, _, _ = rig
    ref = c.policy["fallbacks"][0]
    receipt = recovery.load_artifact(ref["qualification"])
    with pytest.raises(config.ConfigError):
        recovery.validate_qualification(receipt, c.policy, c.policy["fallbacks"][1], now=clock.wall())
    plan = recovery.load_plan(ref)
    plan["compose"][ref["node"]]["services"]["worker"]["command"].append("--changed")
    recovery.recovery_routes.atomic_json(ref["path"], plan)
    with pytest.raises(config.ConfigError):
        recovery.load_plan(ref)


def test_stable_return_resets_on_flap_or_failed_preparation(rig):
    c, nodes, clock, _, _ = fallback(rig)
    step(c, clock)
    nodes.offline.add("66f1")
    step(c, clock)
    assert c.s["stable_since"] is None
    nodes.offline.clear()
    nodes.prepared = False
    c.prepared_at = None
    step(c, clock, 5)
    assert c.s["phase"] == "fallback" and c.s["stable_since"] is None
