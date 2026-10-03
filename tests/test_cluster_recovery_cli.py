"""Operator approval, pinned artifacts and real qualification protocol boundaries."""
from contextlib import nullcontext
import copy
import json
from pathlib import Path

import pytest

from test_cluster_recovery import rig, step, Crash
from spark_cluster import config, recovery


def test_prepared_policy_without_receipts_is_valid_but_never_enabled(rig, tmp_path, capsys):
    c, nodes, clock, _, policy_path = rig
    policy = config.read(policy_path)
    for ref in [policy["preferred"], *policy["fallbacks"]]:
        ref["qualification"] = None
    recovery.recovery_routes.atomic_json(policy_path, policy)
    assert recovery.main(["validate", "--policy", str(policy_path)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["valid"] and not report["qualified"]
    destination = tmp_path / "new-authority"
    # Use distinct prepared ingress; existing authority history must not be adopted.
    policy["gateway"]["route_state"] = str(tmp_path / "new-ingress/route.json")
    recovery.recovery_routes.atomic_json(policy_path, policy)
    with pytest.raises(config.ConfigError):
        recovery.main(["init", "--policy", str(policy_path), "--state-dir", str(destination)])
    assert not destination.exists()
    recovery.main(["init", "--policy", str(policy_path), "--state-dir", str(destination), "--apply"])
    saved = recovery.recovery_routes.private_json(destination / "journal.json")
    assert saved["enabled"] is False and saved["epoch"] == 0
    assert nodes.events == []


@pytest.mark.parametrize("change", ["bool-timing", "nan-timing", "unknown-policy", "alias-contract"])
def test_policy_rejects_ambiguous_safety_contracts(rig, change):
    c, _, _, _, path = rig
    policy = config.read(path)
    if change == "bool-timing":
        policy["timings"]["heartbeat"] = True
    elif change == "nan-timing":
        policy["timings"]["observe"] = float("nan")
    elif change == "unknown-policy":
        policy["allow_unfenced"] = True
    else:
        ref = policy["fallbacks"][1]
        p = recovery.load_plan(ref)
        p["recipe"]["tool_call_parser"] = "different-parser"
        # Preserve a structurally valid plan identity; the external hash changes,
        # but the named alias must still not represent two different contracts.
        identity = {k: p[k] for k in ("recipe", "deployment", "nodes")}
        if "renderer" in p:
            identity["renderer"] = p["renderer"]
        p["digest"] = recovery.hash_value(identity)
        p["owner"] = p["deployment"]["name"] + "-" + p["digest"][:12]
        for compose in p["compose"].values():
            compose["name"] = "spark-" + p["owner"]
            worker = compose["services"]["worker"]
            worker["container_name"] = "spark-" + p["owner"]
            worker["labels"] = {"io.spark.owner": p["owner"], "io.spark.digest": p["digest"]}
        recovery.recovery_routes.atomic_json(ref["path"], p)
        ref["sha256"] = config.plan_sha256(p)
        ref["qualification"] = None
    path.write_text(json.dumps(policy))
    with pytest.raises(config.ConfigError):
        recovery.load_policy(path)


def test_pause_disable_and_resume_queue_while_authority_lock_is_owned(rig):
    c, nodes, clock, _, path = rig
    c.tick()
    owned = copy.deepcopy({n: o["reservation"] for n, o in nodes.nodes.items()})
    with recovery.lock(c.journal.directory / "authority.lock"):
        recovery.main(["pause", "--policy", str(path), "--state-dir", str(c.journal.directory), "--apply", "--approve", "close-ingress"])
        step(c, clock)
        assert c.s["paused"] and not c.s["route"]["accepting"]
        recovery.queue_command(c.journal.directory, c.policy, "resume")
        step(c, clock)
        assert not c.s["paused"] and c.s["route"]["accepting"]
        recovery.queue_command(c.journal.directory, c.policy, "disable")
        step(c, clock)
        assert not c.s["enabled"] and not c.s["route"]["accepting"]
    assert owned == {n: o["reservation"] for n, o in nodes.nodes.items()}
    assert not [e for e in nodes.events if e[0] in ("start", "stop")]


def test_explicit_clear_circuit_preserves_ownership_and_highwaters(rig):
    c, nodes, clock, _, _ = rig
    c.tick()
    c.s.update(circuit_open=True, retry_count=3, retry_at=clock.wall() + 600)
    c.journal.save()
    epoch = c.s["epoch"]
    request = recovery.queue_command(c.journal.directory, c.policy, "clear-circuit")
    step(c, clock)
    assert c.s["commands"][request]["state"] == "applied"
    assert not c.s["circuit_open"] and c.s["epoch"] == epoch
    assert all(o["reservation"] for o in nodes.nodes.values())


def test_corrupt_or_foreign_journal_cannot_be_reset_into_new_authority(rig):
    c, _, _, _, _ = rig
    c.tick()
    path = c.journal.path
    saved = recovery.recovery_routes.private_json(path)
    saved["generation"] = -1
    recovery.recovery_routes.atomic_json(path, saved)
    with pytest.raises(config.ConfigError):
        recovery.initialize(c.policy, c.journal.directory)
    path.unlink()
    with pytest.raises(config.ConfigError):
        recovery.initialize(c.policy, c.journal.directory)


def test_isolation_receipt_must_cover_every_exact_plan_and_private_registry(rig, monkeypatch):
    c, _, _, _, _ = rig
    adapter = recovery.RealAdapter(c.policy, c.journal.directory)
    monkeypatch.setattr(adapter, "gateway_status", lambda: {"accepting": False})
    with pytest.raises(config.ConfigError):
        adapter.isolation(c.refs())
    evidence = Path(c.policy["preferred"]["qualification"]["path"])
    receipt = {"version": 1, "policy": c.policy["id"], "gateway_url": c.policy["gateway"]["url"],
               "registry": c.policy["gateway"]["registry"], "route_state": c.policy["gateway"]["route_state"],
               "plan_sha256": [r["sha256"] for r in c.refs()], "exclusive_ingress": True,
               "legacy_endpoints_blocked": True, "independent_management": True, "independent_serving": True,
               "approved_by": "test-operator", "evidence": [{"path": str(evidence), "sha256": recovery.digest_file(evidence)}]}
    path = c.journal.directory / "isolation.json"
    recovery.recovery_routes.atomic_json(path, receipt)
    c.policy["gateway"]["isolation"] = {"path": str(path), "sha256": recovery.digest_file(path)}
    adapter.isolation(c.refs())
    receipt["plan_sha256"].pop()
    recovery.recovery_routes.atomic_json(path, receipt)
    c.policy["gateway"]["isolation"]["sha256"] = recovery.digest_file(path)
    with pytest.raises(config.ConfigError):
        adapter.isolation(c.refs())


def test_runbook_adopts_isolation_before_qualification(rig, tmp_path, monkeypatch):
    original, nodes, clock, _, _ = rig
    staging = recovery.load_artifact(original.policy["preferred"]["qualification"])["staging"]
    policy = copy.deepcopy(original.policy)
    for ref in [policy["preferred"], *policy["fallbacks"]]:
        ref["qualification"] = None
    policy_path = tmp_path / "runbook-policy.json"
    directory = tmp_path / "runbook-authority"
    recovery.recovery_routes.atomic_json(policy_path, policy)
    recovery.main(["init", "--policy", str(policy_path), "--state-dir", str(directory), "--apply"])

    def controller():
        loaded = recovery.load_policy(policy_path)
        nodes.policy = loaded
        journal = recovery.Journal(directory, loaded, clock=clock.wall)
        return recovery.Controller(loaded, journal, nodes, wall=clock.wall,
                                   monotonic=clock.mono, sleep=clock.advance)

    current = controller()
    current.publish(None, False, "preparing-qualification")
    before = {key: current.s[key] for key in ("authority", "epoch", "generation")}

    def isolation(refs):
        adapter = recovery.RealAdapter(current.policy, directory)
        monkeypatch.setattr(adapter, "gateway_status", nodes.gateway_status)
        return adapter.isolation(refs)

    monkeypatch.setattr(nodes, "isolation", isolation)
    monkeypatch.setattr(nodes, "session", lambda: nullcontext(None), raising=False)

    class InferenceReached(Exception):
        pass

    def chat(*args):
        raise InferenceReached

    output = directory / "qualification.json"
    with pytest.raises(config.ConfigError):
        recovery.qualify(current, current.policy["preferred"], staging, output, chat=chat)
    assert not output.exists()
    receipt = {"version": 1, "policy": policy["id"], "gateway_url": policy["gateway"]["url"],
               "registry": policy["gateway"]["registry"], "route_state": policy["gateway"]["route_state"],
               "plan_sha256": [ref["sha256"] for ref in current.refs()], "exclusive_ingress": True,
               "legacy_endpoints_blocked": True, "independent_management": True,
               "independent_serving": True, "approved_by": "test-operator",
               "evidence": list(staging.values())}
    isolation_path = directory / "approved-isolation.json"
    recovery.recovery_routes.atomic_json(isolation_path, receipt)
    policy["gateway"]["isolation"] = {"path": str(isolation_path),
                                     "sha256": recovery.digest_file(isolation_path)}
    recovery.recovery_routes.atomic_json(policy_path, policy)
    recovery.main(["init", "--policy", str(policy_path), "--state-dir", str(directory),
                   "--adopt-receipts", "--apply"])
    current = controller()
    assert {key: current.s[key] for key in before} == before
    with pytest.raises(InferenceReached):
        recovery.qualify(current, current.policy["preferred"], staging, output, chat=chat)
    assert not current.s["enabled"] and not current.s["route"]["accepting"]
    assert not recovery.load_artifact({"path": str(output),
                                      "sha256": recovery.digest_file(output)})["checks"]["text"]
    assert nodes.events == []


def test_qualification_rejects_wrong_tool_arguments_and_retains_failed_receipt(rig, monkeypatch):
    c, nodes, clock, _, _ = rig
    c.s["enabled"] = False
    c.journal.save()
    monkeypatch.setattr(nodes, "session", lambda: nullcontext(None), raising=False)
    ref = c.policy["preferred"]
    staging = recovery.load_artifact(ref["qualification"])["staging"]
    output = c.journal.directory / "new-qualification.json"
    def chat(session, base, body):
        record = {"guard": {"x-spark-deployment": recovery.load_plan(ref)["digest"],
                            "x-spark-backend": recovery.load_plan(ref)["recipe"]["alias"],
                            "x-spark-authority": c.s["authority"], "x-spark-generation": str(c.s["generation"])}}
        if "tools" not in body:
            record["finish_reason"] = "stop"
            return record, {"role": "assistant", "content": body["messages"][0]["content"].split(": ", 1)[1]}
        record["finish_reason"] = "tool_calls"
        return record, {"role": "assistant", "tool_calls": [{"id": "tool-1", "function": {"name": "lookup_value", "arguments": '{"key":"wrong"}'}}]}
    with pytest.raises(config.ConfigError):
        recovery.qualify(c, ref, staging, output, chat=chat)
    receipt = recovery.recovery_routes.private_json(output)
    assert receipt["checks"]["text"] and not receipt["checks"]["tool_arguments"]
    assert not c.s["route"]["accepting"]
    assert not nodes.events


def test_existing_probe_parser_rejects_truncated_sse_before_tool_execution():
    class Response:
        headers = {}
        def __enter__(self):
            return self
        def __exit__(self, *args):
            pass
        def raise_for_status(self):
            pass
        def iter_lines(self, **kwargs):
            yield b'data: {"choices":[{"delta":{"content":"partial"},"finish_reason":"stop"}]}'
            # A finish frame without [DONE] is not a complete response.
    class Session:
        def post(self, *args, **kwargs):
            return Response()
    with pytest.raises(RuntimeError):
        recovery.probe_chat(Session(), "http://unused/v1", {"stream": True})


def test_crash_between_journal_publication_and_route_write_reopens_safely(rig, monkeypatch):
    c, nodes, clock, reopen, _ = rig
    c.tick()
    nodes.offline.add("66f1")
    write = recovery.recovery_routes.write_state
    def interrupted(path, value, validate):
        write(path, value, validate)
        if value["accepting"] and value["mode"] == "fallback":
            raise Crash("published-before-ack")
    monkeypatch.setattr(recovery.recovery_routes, "write_state", interrupted)
    with pytest.raises(Crash):
        step(c, clock)
    monkeypatch.setattr(recovery.recovery_routes, "write_state", write)
    c = reopen()
    step(c, clock)
    assert c.s["phase"] == "fallback" and c.s["route"]["accepting"]
    assert len([e for e in nodes.events if e[0] == "start"]) == 1


def test_discovery_adapter_checks_public_context_length_and_rejects_wrong_identity(rig, monkeypatch):
    c, _, _, _, _ = rig
    p = recovery.load_plan(c.policy["preferred"])
    route = recovery.route_for(p)
    model = {"id": "local-auto", "backend_alias": route["upstream_model"], "context_length": route["context_tokens"],
             **{k: route[k] for k in ("deployment_digest", "model_root", "max_output_tokens", "capabilities")}}
    class Response:
        def raise_for_status(self):
            pass
        def json(self):
            return {"data": [model]}
    class Session:
        def get(self, *args, **kwargs):
            return Response()
    adapter = recovery.RealAdapter(c.policy, c.journal.directory)
    monkeypatch.setattr(adapter, "session", lambda: nullcontext(Session()))
    assert adapter.endpoint(p)
    model["context_length"] -= 1
    assert not adapter.endpoint(p)
    model["context_length"] += 1
    model["deployment_digest"] = "0" * 64
    assert not adapter.endpoint(p)


def test_legacy_exact_preferred_accepts_independent_fallback_without_rewriting_plan(rig, monkeypatch):
    c, nodes, _, _, path = rig
    root = Path(__file__).resolve().parents[1]
    original = config.plan(*config.load(root, root / "cluster/inventory.json",
                                       root / "cluster/deployments/large-tp2-mtp2-tools-nccl2307-e8f1.json"))
    preferred_path = path.parent / "original-preferred.json"
    recovery.recovery_routes.atomic_json(preferred_path, original)
    original_bytes = preferred_path.read_bytes()
    policy = config.read(path)
    policy["preferred"] = {"path": str(preferred_path), "sha256": config.plan_sha256(original), "qualification": None}
    recovery.recovery_routes.atomic_json(path, policy)
    loaded = recovery.load_policy(path)
    adapter = recovery.RealAdapter(loaded, c.journal.directory)
    def observe(inventory, **kwargs):
        result = copy.deepcopy(nodes.nodes)
        for node, identity in inventory.items():
            result[node]["serving"] = {"ready": "serving" in identity, "available": True}
            for rail in result[node]["fabric"]:
                rail["ready"] = False
        return {"nodes": result}
    monkeypatch.setattr(recovery.cli, "observe", observe)
    assert recovery.network_ready(recovery.load_plan(loaded["fallbacks"][0]), adapter.observe())
    assert not recovery.network_ready(original, adapter.observe(), fabric=True)
    assert preferred_path.read_bytes() == original_bytes


def test_policy_rejects_fallback_that_changes_an_explicit_serving_binding(rig):
    _, _, _, _, path = rig
    policy = config.read(path)
    ref = policy["fallbacks"][0]
    p = recovery.load_plan(ref)
    p["nodes"][ref["node"]]["serving"]["address"] = "192.168.50.99"
    changed = config.plan({"version": 1, "nodes": p["nodes"]}, p["recipe"], p["deployment"])
    recovery.recovery_routes.atomic_json(ref["path"], changed)
    ref.update(sha256=config.plan_sha256(changed), qualification=None)
    recovery.recovery_routes.atomic_json(path, policy)
    with pytest.raises(config.ConfigError, match="serving"):
        recovery.load_policy(path)


@pytest.mark.parametrize("failed_target", ["preferred", "fallback"])
def test_real_start_can_restore_another_plan_within_the_same_transition(rig, monkeypatch, failed_target):
    c, _, _, _, _ = rig
    failed = c.policy["preferred"] if failed_target == "preferred" else c.policy["fallbacks"][0]
    target = c.policy["fallbacks"][-1]
    failed_plan = recovery.load_plan(failed)
    target_plan = recovery.load_plan(target)
    workers = {}

    def call(p, node, action, **kwargs):
        if action == "status":
            return workers.get(node, {"reservation": None, "containers": []})
        if action == "reserve":
            workers.setdefault(node, {"reservation": {"owner": p["owner"], "digest": p["digest"],
                                                      "phase": "reserved"}, "containers": []})
        elif action == "start":
            if p["digest"] == failed_plan["digest"]:
                raise RuntimeError("simulated worker startup failure")
            container = {"id": "worker-" + node, "owner": p["owner"], "digest": p["digest"],
                         "state": "running", "health": "healthy", "image": p["recipe"]["image"],
                         "image_id": "sha256:runtime", "started_at": "2026-09-30T00:00:00Z", "restart_count": 0}
            workers[node] = {"reservation": {"owner": p["owner"], "digest": p["digest"],
                                             "phase": "started", "container_ids": [container["id"]]},
                             "containers": [container]}
        elif action == "stop":
            workers.pop(node, None)
        elif action == "probe":
            return {"ready": True}
        else:
            pytest.fail("unexpected node operation: " + action)
        return {"ok": True}

    monkeypatch.setattr(recovery.cli, "call", call)
    adapter = recovery.RealAdapter(c.policy, c.journal.directory)
    context = {"authority": c.s["authority"], "epoch": 1, "operation_id": "one-transition"}
    with pytest.raises(RuntimeError, match="simulated worker startup failure"):
        adapter.start(failed, context)
    saved = next((c.journal.directory / "attempts").rglob("plan.json"))
    original_bytes = saved.read_bytes()
    restored = adapter.start(target, context)
    assert restored["endpoint"]["ready"] and restored["endpoint"]["digest"] == target_plan["digest"]
    assert saved.read_bytes() == original_bytes
    assert all(w["reservation"]["digest"] == target_plan["digest"] for w in workers.values())


def test_restart_refences_completed_start_before_delayed_old_stop_can_arrive(rig, monkeypatch):
    from spark_cluster import node
    c, nodes, clock, reopen, _ = rig
    c.tick()
    nodes.offline.add("66f1")
    nodes.crash_after = "start"
    with pytest.raises(Crash):
        step(c, clock)
    old_context = copy.deepcopy(nodes.nodes["e8f1"]["recovery_fence"])
    c = reopen()
    step(c, clock)
    assert c.s["phase"] == "fallback"
    assert nodes.nodes["e8f1"]["recovery_fence"]["generation"] > old_context["generation"]
    monkeypatch.setattr(node, "recovery_fence", lambda: nodes.nodes["e8f1"]["recovery_fence"])
    request = recovery.cli.request(recovery.load_plan(c.s["current"]), "e8f1", "stop", recovery={
        key: old_context[key] for key in ("policy", "authority", "generation", "operation_id")})
    with pytest.raises(RuntimeError):
        node.check_fence(request)
    assert nodes.nodes["e8f1"]["reservation"]["digest"] == recovery.load_plan(c.s["current"])["digest"]


def test_failed_route_open_requires_new_global_drain_before_retry_stop(rig, monkeypatch):
    c, nodes, clock, _, _ = rig
    c.tick()
    nodes.offline.add("66f1")
    endpoint = nodes.endpoint
    def reject_after_admission(p):
        nodes.active_requests = 1
        return False
    monkeypatch.setattr(nodes, "endpoint", reject_after_admission)
    step(c, clock)
    assert c.s["intent"]["stage"] == "open"
    stopped = [e for e in nodes.events if e[0] == "stop"]
    nodes.nodes["e8f1"]["gpu_containers"][0]["health"] = "unhealthy"
    step(c, clock, 3)
    assert [e for e in nodes.events if e[0] == "stop"] == stopped
    assert not c.s["route"]["accepting"]
    monkeypatch.setattr(nodes, "endpoint", endpoint)
    nodes.active_requests = 0
    step(c, clock, 8)
    assert c.s["phase"] == "fallback" and c.s["route"]["accepting"]


def test_selected_single_disappearing_during_start_uses_other_qualified_survivor(rig, monkeypatch):
    c, nodes, clock, _, _ = rig
    c.tick()
    for observation in nodes.nodes.values():
        for rail in observation["fabric"]:
            rail["ready"] = False
    start = nodes.start
    def disappear(ref, context):
        result = start(ref, context)
        if ref.get("node") == "e8f1":
            nodes.offline.add("e8f1")
            raise RuntimeError("node disappeared after dispatch")
        return result
    monkeypatch.setattr(nodes, "start", disappear)
    step(c, clock)
    step(c, clock)
    assert c.s["phase"] == "fallback" and c.s["current"]["node"] == "66f1"
    assert "e8f1" in c.s["quarantine"]
    assert nodes.nodes["e8f1"]["reservation"] is not None
    assert c.s["retry_count"] == 1


def coordinator_target(c):
    original = recovery.load_plan(c.s["preferred"])
    deployment = {**original["deployment"], "coordinator": "66f1", "name": "approved-coordinator-66f1"}
    target = config.plan({"version": 1, "nodes": original["nodes"]}, original["recipe"], deployment)
    target_path = c.journal.directory / "approved-coordinator.json"
    recovery.recovery_routes.atomic_json(target_path, target)
    receipt = recovery.load_artifact(c.s["preferred"]["qualification"])
    receipt.update(plan_sha256=config.plan_sha256(target), deployment_digest=target["digest"], route=recovery.route_for(target))
    receipt_path = c.journal.directory / "approved-coordinator-qualification.json"
    recovery.recovery_routes.atomic_json(receipt_path, receipt)
    return {"path": str(target_path), "sha256": config.plan_sha256(target),
            "qualification": {"path": str(receipt_path), "sha256": recovery.digest_file(receipt_path)}}


@pytest.mark.parametrize("current", ["preferred", "fallback"])
def test_receipt_adoption_preserves_switched_coordinator_and_selected_fallback(rig, current):
    c, nodes, clock, _, policy_path = rig
    recovery.initialize(c.policy, c.journal.directory)
    c.tick()
    target = coordinator_target(c)
    request = recovery.queue_command(c.journal.directory, c.policy, "switch-coordinator", preferred=target)
    step(c, clock)
    assert c.s["commands"][request]["state"] == "applied"
    assert c.s["current"] == target and c.s["preferred"] == target
    if current == "fallback":
        nodes.offline.add("66f1")
        step(c, clock)
        assert c.s["current"] == c.s["selected_fallback"]
        assert c.s["current"]["node"] == "e8f1"
    recovery.queue_command(c.journal.directory, c.policy, "disable")
    step(c, clock)
    before = copy.deepcopy(c.s)
    events = list(nodes.events)

    enriched = copy.deepcopy(c.policy)
    for index, ref in enumerate([enriched["preferred"], *enriched["fallbacks"]]):
        receipt = recovery.load_artifact(ref["qualification"])
        receipt["completed_at"] = clock.wall()
        path = c.journal.directory / f"enriched-qualification-{index}.json"
        recovery.recovery_routes.atomic_json(path, receipt)
        ref["qualification"] = {"path": str(path), "sha256": recovery.digest_file(path)}
    recovery.recovery_routes.atomic_json(policy_path, enriched)
    enriched = recovery.load_policy(policy_path)
    recovery.initialize(enriched, c.journal.directory, adopt_policy=True)
    journal = recovery.Journal(c.journal.directory, enriched, clock=clock.wall)
    saved = journal.value

    expected = copy.deepcopy(before)
    expected["policy_hash"] = recovery.hash_value(enriched)
    if current == "fallback":
        qualification = enriched["fallbacks"][0]["qualification"]
        expected["current"]["qualification"] = qualification
        expected["selected_fallback"]["qualification"] = qualification
    assert saved == expected
    assert nodes.events == events
    restarted = recovery.Controller(enriched, journal, nodes, wall=clock.wall, monotonic=clock.mono, sleep=clock.advance)
    recovery.queue_command(c.journal.directory, enriched, "enable")
    step(restarted, clock)
    assert restarted.s["preferred"] == target
    assert restarted.s["current"]["sha256"] == before["current"]["sha256"]
    assert restarted.s["route"]["accepting"]
    assert not [event for event in nodes.events[len(events):] if event[0] in ("start", "stop")]


@pytest.mark.parametrize("failure", [None, "startup", "preparation"])
def test_trusted_coordinator_switch_restores_exact_target_or_exact_previous(rig, failure, monkeypatch, capsys):
    c, nodes, clock, _, policy_path = rig
    monkeypatch.setattr(recovery.time, "time", clock.wall)
    c.tick()
    original = recovery.load_plan(c.s["preferred"])
    ref = coordinator_target(c)
    target = recovery.load_plan(ref)
    original_hash = c.s["preferred"]["sha256"]
    if failure == "startup":
        nodes.fail_start.add(ref["sha256"])
    if failure == "preparation":
        nodes.prepared = False
    def switch(target_ref):
        recovery.main(["switch-coordinator", "--policy", str(policy_path), "--state-dir", str(c.journal.directory),
                       "--plan", target_ref["path"], "--plan-sha256", target_ref["sha256"],
                       "--qualification", target_ref["qualification"]["path"],
                       "--qualification-sha256", target_ref["qualification"]["sha256"],
                       "--apply", "--approve", "saved-coordinator-drain-rollback"])
        return json.loads(capsys.readouterr().out)["queued"]

    request = switch(ref)
    step(c, clock)
    if failure == "preparation":
        assert c.s["commands"][request]["state"] == "refused"
        assert c.s["preferred"]["sha256"] == original_hash and c.s["current"]["sha256"] == original_hash
        assert c.s["route"]["accepting"]
        assert not any(event[0] in ("start", "stop") for event in nodes.events)
        return
    assert c.s["commands"][request]["state"] == "applied"
    assert c.s["preferred"]["sha256"] == ref["sha256"]
    assert c.s["current"]["sha256"] == (original_hash if failure == "startup" else ref["sha256"])
    assert c.s["route"]["accepting"]
    expected = original["digest"] if failure == "startup" else target["digest"]
    assert all(o["reservation"]["digest"] == expected for o in nodes.nodes.values())
    assert recovery.load_plan(ref) == target
    if failure is None:
        request = switch(c.policy["preferred"])
        step(c, clock)
        assert c.s["commands"][request]["state"] == "applied"
        assert c.s["current"]["sha256"] == original_hash and c.s["preferred"]["sha256"] == original_hash
        assert c.s["route"]["accepting"]


def test_real_ingress_tracks_controller_failover_failback_and_monitoring(rig, monkeypatch):
    """Only GPU/SSH and backend generation are simulated; ingress HTTP is real."""
    from http.server import ThreadingHTTPServer
    import socket
    import threading
    import requests
    from test_cluster_gateway import Backend, KEY
    from spark_cluster import gateway, monitor

    c, nodes, clock, _, policy_path = rig
    monkeypatch.setenv("NO_PROXY", "*")
    monkeypatch.setattr(recovery.recovery_routes.time, "time", clock.wall)

    class ModelBackend(Backend):
        def do_GET(self):
            if self.path == "/v1/models":
                p = self.server.selected
                self.reply({"data": [{"id": p["recipe"]["alias"], "root": recovery.route_for(p)["model_root"],
                                      "max_model_len": p["recipe"]["context_tokens"]}]})
            else:
                super().do_GET()

    backend = ThreadingHTTPServer(("127.0.0.1", 0), ModelBackend)
    backend.received = []
    backend.selected = recovery.load_plan(c.policy["preferred"])
    addresses = {config.serving_address(n) for n in backend.selected["nodes"].values()}
    resolve = socket.getaddrinfo
    def local_only(host, port, *args, **kwargs):
        if host in addresses:
            return resolve("127.0.0.1", backend.server_port, *args, **kwargs)
        assert host in ("127.0.0.1", "localhost"), "simulation must not contact real hosts"
        return resolve(host, port, *args, **kwargs)
    monkeypatch.setattr(socket, "getaddrinfo", local_only)
    key_path = Path(c.policy["gateway"]["key_file"])
    key_path.write_text(KEY)
    key_path.chmod(0o600)
    ingress = gateway.serve(c.policy["gateway"]["registry"], "127.0.0.1", 0, KEY,
                            recovery_state=c.policy["gateway"]["route_state"])
    base = "http://127.0.0.1:" + str(ingress.server_port)
    c.policy["gateway"]["url"] = base
    for ref in c.refs():
        artifact = ref["qualification"]
        receipt = recovery.load_artifact(artifact)
        receipt["gateway_url"] = base
        recovery.recovery_routes.atomic_json(artifact["path"], receipt)
        artifact["sha256"] = recovery.digest_file(artifact["path"])
    c.s["policy_hash"] = recovery.hash_value(c.policy)
    c.journal.save()
    recovery.recovery_routes.atomic_json(policy_path, c.policy)
    adapter = recovery.RealAdapter(c.policy, c.journal.directory)
    nodes.gateway_status, nodes.endpoint = adapter.gateway_status, adapter.endpoint
    start = nodes.start
    def start_backend(ref, context):
        result = start(ref, context)
        backend.selected = recovery.load_plan(ref)
        return result
    nodes.start = start_backend
    threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in (backend, ingress)]
    for thread in threads:
        thread.start()
    headers = {"Authorization": "Bearer " + KEY}
    def generate(alias="local-auto"):
        return requests.post(base + "/v1/chat/completions", headers=headers, timeout=5,
                             json={"model": alias, "messages": [{"role": "user", "content": "ready"}],
                                   "max_tokens": 16, "stream": True})
    try:
        step(c, clock)
        first = generate()
        assert first.status_code == 200 and "data: [DONE]" in first.text
        preferred_digest = backend.selected["digest"]
        assert first.headers["X-Spark-Deployment"] == preferred_digest
        nodes.offline.add("66f1")
        step(c, clock)
        fallback = generate()
        assert fallback.status_code == 200 and "data: [DONE]" in fallback.text
        assert fallback.headers["X-Spark-Backend"] == "local-coder"
        assert fallback.headers["X-Spark-Deployment"] != preferred_digest
        assert generate(recovery.load_plan(c.policy["preferred"])["recipe"]["alias"]).status_code == 503
        nodes.offline.clear()
        step(c, clock, 4)
        restored = generate()
        assert restored.status_code == 200 and restored.headers["X-Spark-Deployment"] == preferred_digest
        assert int(restored.headers["X-Spark-Generation"]) > int(fallback.headers["X-Spark-Generation"])
        collector = monitor.Collector({"version": 1, "nodes": backend.selected["nodes"]},
                                      host_reader=lambda node: {}, gateway_url=base, gateway_key=KEY,
                                      recovery_state=c.journal.directory / "status.json", timeout=2)
        snapshot = collector.collect()
        targets = {(t["kind"], t["target"]): t for t in json.loads(snapshot.status)["targets"]}
        assert targets["gateway", "ingress"]["up"] and targets["gateway_recovery", "ingress"]["up"]
        assert 'spark_recovery_phase_info{phase="preferred"' in snapshot.metrics
        assert 'spark_gateway_recovery_active_requests 0' in snapshot.metrics
    finally:
        for server in (ingress, backend):
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join(timeout=2)
