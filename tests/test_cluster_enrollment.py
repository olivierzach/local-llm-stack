"""Enrollment cannot silently widen trust, erase peers, or qualify cached artifacts."""
import base64
import copy
import importlib.util
import json
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster import cli, config, enrollment, node, peer_ssh


def key(number):
    wire = b"\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20" + bytes([number]) * 32
    return "ssh-ed25519 " + base64.b64encode(wire).decode()


def peer(number):
    return {"hostname": "spark-peer" + str(number), "architecture": "aarch64",
            "ssh": "spark-peer" + str(number) + "-wired",
            "fabric": [{"ip": "10.10.20." + str(number), "interface": "eth0", "rdma": "mlx5_0"}]}


def idle(node):
    return {"hostname": node["hostname"], "architecture": node["architecture"],
            "reservation": None, "recovery_fence": None, "containers": [],
            "gpu_containers": [], "gpu_processes": [], "errors": {}}


def request(number):
    return {"action": "configure", "peer": peer(number), "peer_host_key": key(number),
            "trusted_host_key": key(number), "peer_public_key": key(number + 30)}


def guarded_removal(number):
    guard = {"owner": "peer-removal-" + "a" * 32, "digest": "b" * 64}
    return {**request(number), "action": "remove", "removal_guard": guard,
            "removal": {**idle(peer(number)),
                        "reservation": {**guard, "phase": "workload", "container_ids": []}}}


@pytest.fixture
def ssh_directory(tmp_path):
    directory = tmp_path / ".ssh"
    directory.mkdir()
    original = {"config": "Host company\n  HostName bastion.example\n",
                "config.spark-cluster": "# unrelated managed-file comment\n",
                "known_hosts_spark_controller": "company " + key(90) + "\n",
                "authorized_keys": key(91) + " personal-key\n"}
    for name, text in original.items():
        (directory / name).write_text(text)
    return directory, original


def configure(directory, req):
    before = peer_ssh.read_files(directory)
    after = peer_ssh.changes(directory, req, before)
    peer_ssh.commit(directory, before, after)
    return after


def test_incremental_peers_preserve_existing_trust_and_removal_is_scoped(ssh_directory):
    directory, original = ssh_directory
    first = configure(directory, request(2))
    first_block = peer_ssh.peer_block(first["config.spark-cluster"], peer(2)["hostname"])[1]
    configure(directory, request(3))
    both = peer_ssh.read_files(directory)
    assert first_block in both["config.spark-cluster"]
    for name, text in original.items():
        assert text in both[name]
    assert configure(directory, request(2)) == both
    removal = guarded_removal(3)
    removed = configure(directory, removal)
    assert removed == first
    assert configure(directory, removal) == removed


def test_trust_conflict_leaves_every_file_unchanged(ssh_directory):
    directory, _ = ssh_directory
    configure(directory, request(2))
    before = peer_ssh.read_files(directory)
    changed = {**request(2), "peer_host_key": key(99)}
    with pytest.raises(RuntimeError, match="independently trusted"):
        configure(directory, changed)
    assert peer_ssh.read_files(directory) == before
    changed["trusted_host_key"] = key(99)
    with pytest.raises(RuntimeError, match="trusted controller key"):
        configure(directory, changed)
    assert peer_ssh.read_files(directory) == before


def test_write_failure_rolls_back_authorization_config_and_host_trust(ssh_directory, monkeypatch):
    directory, _ = ssh_directory
    configure(directory, request(2))
    before = peer_ssh.read_files(directory)
    write = peer_ssh.write
    failed = False
    def fail_once(path, text):
        nonlocal failed
        if path.name == "authorized_keys" and not failed:
            failed = True
            raise OSError("injected disk failure after other files were replaced")
        write(path, text)
    monkeypatch.setattr(peer_ssh, "write", fail_once)
    with pytest.raises(OSError):
        configure(directory, request(3))
    assert peer_ssh.read_files(directory) == before
    assert not (directory / "spark-cluster-transaction.json").exists()


def leave_pending_transaction(directory, monkeypatch):
    before = peer_ssh.read_files(directory)
    proposed = peer_ssh.changes(directory, request(3), before)
    write = peer_ssh.write

    def interrupt_write(path, text):
        write(path, text)
        if path.name == "known_hosts_spark_controller":
            raise OSError("interrupted after replacing host trust")

    def interrupt_restore(*args):
        raise OSError("rollback also interrupted")

    with monkeypatch.context() as patch:
        patch.setattr(peer_ssh, "write", interrupt_write)
        patch.setattr(peer_ssh, "restore", interrupt_restore)
        with pytest.raises(OSError):
            peer_ssh.commit(directory, before, proposed)
    return before


def test_interrupted_transaction_is_recovered_before_another_peer_is_added(ssh_directory, monkeypatch):
    directory, _ = ssh_directory
    configure(directory, request(2))
    before = leave_pending_transaction(directory, monkeypatch)
    peer_ssh.recover(directory)
    assert peer_ssh.read_files(directory) == before
    result = configure(directory, request(4))
    assert "spark-peer2-cluster " in result["known_hosts_spark_controller"]
    assert "spark-peer3-cluster " not in result["known_hosts_spark_controller"]
    assert "spark-peer4-cluster " in result["known_hosts_spark_controller"]


def test_pending_ssh_repair_preserves_unrelated_operator_edit(ssh_directory, monkeypatch):
    directory, _ = ssh_directory
    configure(directory, request(2))
    leave_pending_transaction(directory, monkeypatch)
    authorized = directory / "authorized_keys"
    authorized.write_text(authorized.read_text() + key(92) + " new-operator-key\n")
    current = peer_ssh.read_files(directory)
    journal = directory / "spark-cluster-transaction.json"
    evidence = journal.read_bytes()
    with pytest.raises(RuntimeError):
        peer_ssh.recover(directory)
    assert peer_ssh.read_files(directory) == current
    assert journal.read_bytes() == evidence


def test_immediate_ssh_rollback_preserves_unrelated_operator_edit(ssh_directory, monkeypatch):
    directory, _ = ssh_directory
    configure(directory, request(2))
    write = peer_ssh.write
    current = {}

    def fail_with_operator_edit(path, text):
        write(path, text)
        if path.name == "known_hosts_spark_controller":
            authorized = directory / "authorized_keys"
            authorized.write_text(authorized.read_text() + key(92) + " new-operator-key\n")
            current.update(peer_ssh.read_files(directory))
            raise OSError("write interrupted while operator updates authorization")

    monkeypatch.setattr(peer_ssh, "write", fail_with_operator_edit)
    with pytest.raises(RuntimeError):
        configure(directory, request(3))
    assert peer_ssh.read_files(directory) == current
    assert (directory / "spark-cluster-transaction.json").exists()


def test_interrupted_ssh_rollback_can_resume_without_losing_prior_peers(ssh_directory, monkeypatch):
    directory, _ = ssh_directory
    configure(directory, request(2))
    before = leave_pending_transaction(directory, monkeypatch)
    pending = peer_ssh.read_files(directory)
    write = peer_ssh.write

    def interrupt_restore(path, text):
        write(path, text)
        if path.name == "config.spark-cluster":
            raise OSError("interrupted during rollback")

    with monkeypatch.context() as patch:
        patch.setattr(peer_ssh, "write", interrupt_restore)
        with pytest.raises(OSError):
            peer_ssh.recover(directory)
    partial = peer_ssh.read_files(directory)
    assert partial["config.spark-cluster"] == before["config.spark-cluster"]
    assert partial["known_hosts_spark_controller"] == pending["known_hosts_spark_controller"]
    peer_ssh.recover(directory)
    assert peer_ssh.read_files(directory) == before
    assert not (directory / "spark-cluster-transaction.json").exists()


def test_before_only_ssh_journal_requires_manual_recovery(ssh_directory):
    directory, _ = ssh_directory
    before = configure(directory, request(2))
    journal = directory / "spark-cluster-transaction.json"
    journal.write_text(json.dumps(before))
    authorized = directory / "authorized_keys"
    authorized.write_text(authorized.read_text() + key(92) + " new-operator-key\n")
    current, evidence = peer_ssh.read_files(directory), journal.read_bytes()
    with pytest.raises(RuntimeError):
        peer_ssh.recover(directory)
    assert peer_ssh.read_files(directory) == current
    assert journal.read_bytes() == evidence


def test_corrupt_ssh_transaction_evidence_cannot_restore_files(ssh_directory, monkeypatch):
    directory, _ = ssh_directory
    configure(directory, request(2))
    leave_pending_transaction(directory, monkeypatch)
    journal = directory / "spark-cluster-transaction.json"
    transaction = json.loads(journal.read_text())
    transaction["before"]["authorized_keys"] += key(92) + " unverified-key\n"
    journal.write_text(json.dumps(transaction))
    current, evidence = peer_ssh.read_files(directory), journal.read_bytes()
    with pytest.raises(RuntimeError):
        peer_ssh.recover(directory)
    assert peer_ssh.read_files(directory) == current
    assert journal.read_bytes() == evidence


def test_independent_management_alias_is_usable_without_fabric(ssh_directory):
    if not shutil.which("ssh"):
        pytest.skip("OpenSSH configuration parser is unavailable")
    directory, _ = ssh_directory
    req = {**request(2), "management": [{"alias": "spark-peer2-management", "address": "192.0.2.22"}],
           "source_addresses": ["192.0.2.22"]}
    result = configure(directory, req)
    # ssh -G reads actual OpenSSH configuration, without connecting to a host.
    parsed = subprocess.run(["ssh", "-G", "-F", str(directory / "config"), "spark-peer2-management"],
                            check=True, capture_output=True, text=True)
    fields = dict(line.split(maxsplit=1) for line in parsed.stdout.splitlines() if " " in line)
    assert fields["hostname"] == "192.0.2.22"
    assert fields["hostkeyalias"] == "spark-peer2-cluster"
    assert fields["stricthostkeychecking"] in ("true", "yes")
    assert fields["identitiesonly"] in ("true", "yes")
    assert 'from="10.10.20.2,192.0.2.22"' in result["authorized_keys"]
    with pytest.raises(RuntimeError, match="approved source"):
        configure(directory, {**req, "source_addresses": []})
    assert peer_ssh.read_files(directory) == result


def test_alias_and_authorized_key_collisions_do_not_overwrite_other_owners(ssh_directory):
    directory, _ = ssh_directory
    before = configure(directory, request(2))
    collision = {**request(3), "peer_public_key": request(2)["peer_public_key"]}
    with pytest.raises(RuntimeError, match="different ownership"):
        configure(directory, collision)
    collision = request(3)
    collision["peer"]["ssh"] = peer(2)["ssh"]
    with pytest.raises(RuntimeError, match="another managed peer"):
        configure(directory, collision)
    with pytest.raises(RuntimeError, match="outside its managed block"):
        peer_ssh.changes(directory, request(3), before, resolve=lambda alias: "unrelated.example")
    assert peer_ssh.read_files(directory) == before


def test_old_pair_file_migrates_without_losing_first_peer(ssh_directory):
    directory, _ = ssh_directory
    first = configure(directory, request(2))
    block = peer_ssh.peer_block(first["config.spark-cluster"], peer(2)["hostname"])[1]
    legacy = "# Managed by local-llm-stack configure-spark-peer-ssh.py\n" + "\n".join(block.splitlines()[1:-1]) + "\n"
    (directory / "config.spark-cluster").write_text(legacy)
    merged = configure(directory, request(3))
    assert "Host " + peer(2)["ssh"] + "\n" in merged["config.spark-cluster"]
    assert "Host " + peer(3)["ssh"] + "\n" in merged["config.spark-cluster"]
    assert merged["known_hosts_spark_controller"].startswith(first["known_hosts_spark_controller"])


@pytest.mark.parametrize("change", [
    {"reservation": {"owner": "live-deployment"}},
    {"recovery_fence": {"active": True}},
    {"recovery_fence": {"generation": 9}},
    {"errors": {"recovery_fence": "unreadable"}},
    {"gpu_processes": ["123"]},
    {"containers": [{"state": "running"}]},
    {"gpu_containers": None},
])
def test_active_reserved_or_unknown_peer_cannot_be_removed(ssh_directory, change):
    directory, _ = ssh_directory
    before = configure(directory, request(2))
    req = guarded_removal(2)
    req["removal"].update(change)
    with pytest.raises(RuntimeError):
        configure(directory, req)
    assert peer_ssh.read_files(directory) == before


def test_idle_snapshot_without_admission_guard_cannot_revoke_trust(ssh_directory):
    directory, _ = ssh_directory
    before = configure(directory, request(2))
    with pytest.raises(RuntimeError):
        configure(directory, {**request(2), "action": "remove", "removal": idle(peer(2))})
    assert peer_ssh.read_files(directory) == before


@pytest.fixture
def inputs():
    return config.load(ROOT, ROOT / "cluster/inventory.json", ROOT / "cluster/deployments/fast-e8f1.json")


def candidate_from(inventory):
    third = copy.deepcopy(inventory["nodes"]["e8f1"])
    third.update(hostname="spark-third", ssh="spark-third-wired",
                 management={"ssh_targets": ["spark-third-management", "spark-third-wired"]},
                 serving={"address": "192.0.2.33", "interface": "wlan0"})
    for i, rail in enumerate(third["fabric"]):
        rail["ip"] = "10.10." + str(20 + i) + ".3"
    return {"version": 1, "nodes": {"third": third}}


def test_third_node_and_new_recipe_preserve_old_plans_and_remain_unqualified(inputs, tmp_path, capsys):
    inv, recipe, deployment = inputs
    previous_plan = config.plan(inv, recipe, deployment)
    saved = tmp_path / "old-plan.json"
    saved.write_text(json.dumps(previous_plan))
    previous = saved.read_bytes()
    candidate = candidate_from(inv)
    base_path, candidate_path = tmp_path / "base.json", tmp_path / "candidate.json"
    base_path.write_text(json.dumps(inv))
    candidate_path.write_text(json.dumps(candidate))
    output = tmp_path / "admitted.json"
    recipes = tmp_path / "cluster/recipes"
    recipes.mkdir(parents=True)
    new_recipe = {**recipe, "alias": "local-new-candidate"}
    (recipes / "candidate.json").write_text(json.dumps(new_recipe))
    placement = {**deployment, "name": "third-candidate", "recipe": "candidate",
                 "nodes": ["third"], "coordinator": "third"}
    deployment_path, new_plan_path = tmp_path / "placement.json", tmp_path / "new-plan.json"
    deployment_path.write_text(json.dumps(placement))
    argv = ["apply", "--inventory", str(candidate_path), "--base-inventory", str(base_path),
            "--output", str(output), "--approve-inventory-sha256", enrollment.digest(candidate), "--apply",
            "--root", str(tmp_path), "--deployment", str(deployment_path), "--plan-output", str(new_plan_path)]
    for ip in ["10.10.20.3", "10.10.21.3", "192.0.2.33"]:
        argv.extend(["--approve-stable-address", ip])
    assert enrollment.main(argv) == 0
    report = json.loads(capsys.readouterr().out)
    admitted = config.read(output)
    assert admitted["nodes"]["e8f1"] == inv["nodes"]["e8f1"]
    assert admitted["nodes"]["66f1"] == inv["nodes"]["66f1"]
    assert admitted["nodes"]["third"] == candidate["nodes"]["third"]
    assert not report["qualified"] and not report["eligible_automation"] and not report["recovery_enrolled"]
    admitted_plan = config.read(new_plan_path)
    config.validate_saved_plan(admitted_plan, report["plan_sha256"])
    assert admitted_plan["recipe"] == new_recipe
    assert set(admitted_plan["nodes"]) == {"third"}
    assert enrollment.main(argv) == 0
    capsys.readouterr()
    assert config.read(new_plan_path) == admitted_plan
    assert saved.read_bytes() == previous
    candidate["nodes"]["e8f1"] = copy.deepcopy(inv["nodes"]["e8f1"])
    candidate["nodes"]["e8f1"]["cache"] += "/new"
    with pytest.raises(config.ConfigError, match="existing node"):
        enrollment.merge_candidate(inv, candidate)


def test_discovered_dynamic_address_does_not_become_stable_inventory(inputs):
    inventory = candidate_from(inputs[0])
    before = copy.deepcopy(inventory)
    def resolver(*args):
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.199", 0))]
    def observer(nodes, **kwargs):
        return {"nodes": {name: idle(node) for name, node in nodes.items()}}
    report = enrollment.discovery(inventory, resolver=resolver, observer=observer)
    assert report["candidate_inventory"] == before and inventory == before
    assert report["discovery"]["third"][0]["address_status"] == "needs-explicit-approval-and-stability"
    with pytest.raises(config.ConfigError, match="stability approval"):
        enrollment.approve(inventory, enrollment.digest(inventory), ["192.0.2.199"])
    assert not report["eligible_automation"]


def test_inventory_removal_rejects_incomplete_ownership_but_not_unused_cable(inputs):
    inventory = inputs[0]
    report = idle(inventory["nodes"]["e8f1"])
    report.update(complete=False, errors={"interface:unused-fabric": "unavailable"})
    def observer(*args, **kwargs):
        return {"nodes": {"e8f1": report}}
    result, _ = enrollment.remove_candidate(inventory, "e8f1", observer=observer)
    assert set(result["nodes"]) == {"66f1"}
    report.pop("recovery_fence")
    with pytest.raises(RuntimeError, match="unknown ownership"):
        enrollment.remove_candidate(inventory, "e8f1", observer=observer)
    assert set(inventory["nodes"]) == {"66f1", "e8f1"}


def test_ineligible_topology_and_tensor_shapes_are_rejected(inputs):
    inventory = enrollment.merge_candidate(inputs[0], candidate_from(inputs[0]))
    for name, node in inventory["nodes"].items():
        for rail in node["fabric"]:
            rail["peer"] = "e8f1" if name == "66f1" else "66f1"
    deployment = {**inputs[2], "mode": "tensor", "nodes": ["66f1", "e8f1", "third"], "coordinator": "e8f1",
                  "tensor_parallel": 3}
    with pytest.raises(config.ConfigError, match="not reciprocal"):
        enrollment.topology(inventory, deployment)
    with pytest.raises(ValueError, match="attention heads"):
        enrollment.tensor_shape({"num_attention_heads": 32, "num_key_value_heads": 8}, 3)
    with pytest.raises(ValueError, match="KV heads"):
        enrollment.tensor_shape({"num_attention_heads": 24, "num_key_value_heads": 5}, 3)
    with pytest.raises(ValueError, match="unknown"):
        enrollment.tensor_shape({}, 2)


def test_prepared_artifacts_with_absent_fabric_are_not_gpu_qualification(inputs, tmp_path, monkeypatch):
    inv, recipe, deployment = inputs
    node = inv["nodes"]["e8f1"]
    node["cache"] = str(tmp_path / "cache")
    node["serving"] = {"address": "192.0.2.22", "interface": "wlan0"}
    p = config.plan(inv, recipe, deployment)
    snapshot = Path(node["cache"]) / "hub" / ("models--" + recipe["model"].replace("/", "--")) / "snapshots" / recipe["revision"]
    snapshot.mkdir(parents=True)
    (snapshot / "config.json").write_text(json.dumps({"num_attention_heads": 32}))
    (snapshot / "model.safetensors").write_bytes(b"structural-fixture-not-inference")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    monkeypatch.setattr(platform, "node", lambda: node["hostname"])
    monkeypatch.setattr(platform, "machine", lambda: node["architecture"])
    def command(argv, **kwargs):
        if argv[:3] == ["docker", "image", "inspect"]:
            output = json.dumps([{"Architecture": "arm64", "Id": "sha256:" + "a" * 64}])
        elif argv[:3] == ["docker", "ps", "-aq"] or argv[0] == "nvidia-smi":
            output = ""
        elif argv[0] == "dpkg-query":
            output = "installed"
        else:
            raise AssertionError("unexpected external operation: " + repr(argv))
        return subprocess.CompletedProcess(argv, 0, output, "")
    monkeypatch.setattr(subprocess, "run", command)
    req = cli.request(p, "e8f1", "observe")
    req.update(node_source=cli.NODE_SOURCE.read_text(), packages=["curl"])
    result = enrollment.host(req)
    assert result["prepared"]
    assert not result["qualified"] and not result["eligible_automation"]
    assert not (tmp_path / ".local/state/local-llm-cluster").exists()
    (snapshot / "model.safetensors").unlink()
    assert not enrollment.host(req)["prepared"]


def test_lost_staging_receipt_stops_without_replay_or_automatic_enrollment():
    inv, recipe, deployment = config.load(ROOT, ROOT / "cluster/inventory.json",
                                         ROOT / "cluster/deployments/fast-tp2-e8f1.json")
    p = config.plan(inv, recipe, deployment)
    attempted = []
    def remote(node, req, source, **kwargs):
        attempted.append(node["hostname"])
        raise cli.AmbiguousMutationError("receipt lost after pull")
    result = enrollment.inspect_candidates(inv, p, apply=True, pull_image=True, remote_fn=remote)
    assert len(attempted) == 1  # No replay or staging on the next node after an ambiguous mutation.
    assert any(report.get("requires_reconciliation") for report in result["nodes"].values())
    assert not result["prepared"] and not result["eligible_automation"]


def prepare_args(evidence):
    return ["prepare", "--inventory", str(ROOT / "cluster/inventory.json"),
            "--deployment", str(ROOT / "cluster/deployments/fast-e8f1.json"),
            "--pull-image", "--apply", "--evidence-output", str(evidence)]


def test_prepare_existing_evidence_rejects_before_staging(tmp_path, monkeypatch):
    evidence = tmp_path / "evidence.json"
    original = b"original immutable evidence\n"
    evidence.write_bytes(original)
    staging = []
    def remote(*args, **kwargs):
        staging.append(args)
        return {"prepared": True}
    monkeypatch.setattr(cli, "remote", remote)
    with pytest.raises(config.ConfigError):
        enrollment.main(prepare_args(evidence))
    assert staging == []
    assert evidence.read_bytes() == original


@pytest.mark.parametrize("parent_kind", ["missing", "file"])
def test_prepare_unusable_evidence_parent_rejects_before_staging(tmp_path, monkeypatch, parent_kind):
    parent = tmp_path / "unusable"
    if parent_kind == "file":
        parent.write_bytes(b"not a directory")
    evidence = parent / "evidence.json"
    staging = []
    def remote(*args, **kwargs):
        staging.append(args)
        return {"prepared": True}
    monkeypatch.setattr(cli, "remote", remote)
    with pytest.raises(config.ConfigError):
        enrollment.main(prepare_args(evidence))
    assert staging == []
    assert not evidence.exists()
    if parent_kind == "file":
        assert parent.read_bytes() == b"not a directory"


@pytest.mark.parametrize("failure", ["collision", "write"])
def test_prepare_evidence_publication_failure_reports_actual_receipts(tmp_path, inputs, monkeypatch, capsys, failure):
    evidence = tmp_path / "evidence.json"
    concurrent = b"concurrent immutable evidence\n"
    receipt = {"image": inputs[1]["image"]}
    staging = []
    def remote(node, req, source, **kwargs):
        staging.append(node["hostname"])
        if failure == "collision":
            evidence.write_bytes(concurrent)
        return {"prepared": True, "staging_receipts": receipt,
                "qualified": False, "eligible_automation": False}
    monkeypatch.setattr(cli, "remote", remote)
    if failure == "write":
        def failed_write(*args):
            raise OSError("credential=do-not-expose")
        monkeypatch.setattr(cli, "save_exclusive", failed_write)
    assert enrollment.main(prepare_args(evidence)) == 1
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    assert staging == [inputs[0]["nodes"]["e8f1"]["hostname"]]
    assert report["ok"] is False and report["requires_reconciliation"]
    assert report["prepared"] and report["nodes"]["e8f1"]["staging_receipts"] == receipt
    assert report["evidence_error"] == ("FileExistsError" if failure == "collision" else "OSError")
    assert "do-not-expose" not in captured.out + captured.err
    if failure == "collision":
        assert evidence.read_bytes() == concurrent


@pytest.mark.parametrize("destination", ["existing", "same", "parent-alias"])
def test_remove_evidence_conflicts_reject_before_inventory_publication(inputs, tmp_path, monkeypatch, destination):
    output = tmp_path / "removed.json"
    evidence = tmp_path / "evidence.json"
    original = b"original immutable evidence\n"
    if destination == "existing":
        evidence.write_bytes(original)
    elif destination == "same":
        evidence = output
    else:
        alias = tmp_path / "alias"
        alias.symlink_to(tmp_path, target_is_directory=True)
        evidence = alias / output.name
    def observer(nodes, **kwargs):
        return {"nodes": {name: idle(node) for name, node in nodes.items()}}
    monkeypatch.setattr(cli, "observe", observer)
    with pytest.raises(config.ConfigError):
        enrollment.main(["remove", "--inventory", str(ROOT / "cluster/inventory.json"),
                         "--node", "e8f1", "--output", str(output), "--apply",
                         "--approve-inventory-sha256", enrollment.digest(inputs[0]),
                         "--evidence-output", str(evidence)])
    assert not output.exists()
    if destination == "existing":
        assert evidence.read_bytes() == original
    else:
        assert not evidence.exists()


def test_remove_evidence_failure_reports_published_inventory(inputs, tmp_path, monkeypatch, capsys):
    output, evidence = tmp_path / "removed.json", tmp_path / "evidence.json"
    def observer(nodes, **kwargs):
        return {"nodes": {name: idle(node) for name, node in nodes.items()}}
    save = cli.save_exclusive
    def publish(path, value):
        if path == evidence:
            raise OSError("credential=do-not-expose")
        save(path, value)
    monkeypatch.setattr(cli, "observe", observer)
    monkeypatch.setattr(cli, "save_exclusive", publish)
    assert enrollment.main(["remove", "--inventory", str(ROOT / "cluster/inventory.json"),
                            "--node", "e8f1", "--output", str(output), "--apply",
                            "--approve-inventory-sha256", enrollment.digest(inputs[0]),
                            "--evidence-output", str(evidence)]) == 1
    captured = capsys.readouterr()
    report = json.loads(captured.out)
    published = config.read(output)
    assert set(published["nodes"]) == {"66f1"}
    assert report["candidate_inventory"] == published and report["removed"] == "e8f1"
    assert report["ok"] is False and report["requires_reconciliation"]
    assert report["evidence_error"] == "OSError"
    assert "do-not-expose" not in captured.out + captured.err


def test_staging_requires_approval_and_exact_selected_manifest_before_any_command(inputs, monkeypatch):
    recipe = inputs[1]
    manifest = {"version": 1, "repo": recipe["model"], "revision": "f" * 40,
                "image": recipe["image"], "hub_version": "0.34.4",
                "files": {"config.json": {"size": 1, "sha256": "a" * 64},
                          "model.safetensors": {"size": 1, "sha256": "b" * 64}}}
    req = {"recipe": recipe, "pull_image": True, "model_manifest": manifest,
           "fetch_source": (ROOT / "scripts/fetch-pinned-spark-model.py").read_text(), "max_download_bytes": 2}
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: pytest.fail("no external action is authorized"))
    with pytest.raises(ValueError, match="explicit apply"):
        enrollment.stage(req)
    with pytest.raises(ValueError, match="differs from recipe"):
        enrollment.stage({**req, "apply": True})
    manifest["revision"] = recipe["revision"]
    with pytest.raises(ValueError, match="byte budget"):
        enrollment.stage({**req, "apply": True, "max_download_bytes": 1})


@pytest.fixture
def bootstrap(monkeypatch):
    spec = importlib.util.spec_from_file_location("spark_peer_bootstrap", ROOT / "scripts/configure-spark-peer-ssh.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(socket, "gethostname", lambda: "isolated-test-controller")
    monkeypatch.setattr(module, "ambient_remote", lambda *a, **k: pytest.fail("independent trust must not fall back to ambient SSH"))
    return module


class AuthenticatedSSHBoundary:
    """Isolated SSH authentication boundary, including forged pre-auth stdout."""
    def __init__(self, host_keys, fail_mutation=False):
        self.host_keys = host_keys
        self.fail_mutation = fail_mutation
        self.attempts = []
        self.mutations = []
        self.pin_paths = []
        self.reservations = {}

    def __call__(self, argv, *, input, **kwargs):
        assert argv[0] == "ssh"
        options = dict(argv[i + 1].split("=", 1) for i, value in enumerate(argv[:-1]) if value == "-o")
        assert options["StrictHostKeyChecking"] == "yes"
        assert options["GlobalKnownHostsFile"] == "none"
        assert options["KnownHostsCommand"] == "none"
        assert options["HostKeyAlgorithms"] == "ssh-ed25519"
        assert options["VerifyHostKeyDNS"] == options["UpdateHostKeys"] == "no"
        assert options["ControlPath"] == "none" and options["ControlMaster"] == "no"
        path = Path(options["UserKnownHostsFile"])
        assert path.stat().st_mode & 0o077 == 0
        self.pin_paths.append(path)
        alias, pin = path.read_text().strip().split(" ", 1)
        assert options["HostKeyAlias"] == alias
        req = json.loads(input)
        action, target = req["action"], argv[-2]
        self.attempts.append((action, target))
        mutation = action not in ("identity", "observe")
        # The hostile host lies about /etc/ssh and supplies its own controller key.
        # SSH must reject it using the actual handshake key before trusting JSON.
        reply = {"host_key": pin, "public_key": key(99), "transaction_pending": False}
        authenticated = self.host_keys[target] == pin and not (self.fail_mutation and mutation)
        if not authenticated:
            return subprocess.CompletedProcess(argv, 255, json.dumps({"ok": True, "result": reply}), "host key mismatch")
        if mutation:
            self.mutations.append((action, target))
            reply = {"configured": True}
            if action == "reserve-workload":
                self.reservations[target] = {k: req[k] for k in ("owner", "digest")}
                self.reservations[target].update(phase="workload", container_ids=[])
            elif action == "release-workload":
                self.reservations.pop(target, None)
        elif action == "observe":
            reply = idle(req["node"])
            reply["reservation"] = self.reservations.get(target)
        return subprocess.CompletedProcess(argv, 0, json.dumps({"ok": True, "result": reply}), "")


def test_bootstrap_wrong_handshake_pin_cannot_inject_self_reported_controller_key(bootstrap, inputs, tmp_path, monkeypatch):
    inv = inputs[0]
    inventory_path, trust_path = tmp_path / "inventory.json", tmp_path / "trust.json"
    inventory_path.write_text(json.dumps(inv))
    trust_path.write_text(json.dumps({"version": 1, "nodes": {name: {"host_key": key(1)} for name in inv["nodes"]}}))
    # Ambient trust could accept this different key; independent trust must not.
    boundary = AuthenticatedSSHBoundary({node["ssh"]: key(2) for node in inv["nodes"].values()})
    monkeypatch.setattr(subprocess, "run", boundary)
    with pytest.raises(ConnectionError, match="independently pinned transport"):
        bootstrap.main(["--inventory", str(inventory_path), "--trust", str(trust_path), "--apply"])
    assert boundary.mutations == []
    assert all(action == "identity" for action, _ in boundary.attempts)
    assert all(not path.exists() for path in boundary.pin_paths)


def test_each_fallback_alias_reauthenticates_same_independent_pin(bootstrap, monkeypatch):
    node = {**peer(2), "management": {"ssh_targets": ["wrong-host", "independent-path"]}}
    boundary = AuthenticatedSSHBoundary({"wrong-host": key(3), "independent-path": key(2)})
    monkeypatch.setattr(subprocess, "run", boundary)
    helper = ROOT / "tools/spark_cluster/peer_ssh.py"
    bootstrap.pinned_remote(node, {"action": "configure", "node": node}, helper, key(2))
    assert boundary.attempts == [("identity", "wrong-host"), ("identity", "independent-path"),
                                 ("configure", "independent-path")]
    assert boundary.mutations == [("configure", "independent-path")]
    assert all(not path.exists() for path in boundary.pin_paths)


def test_pinned_mutation_failure_never_replays_on_another_alias(bootstrap, monkeypatch):
    node = {**peer(2), "management": {"ssh_targets": ["first-path", "second-path"]}}
    boundary = AuthenticatedSSHBoundary({"first-path": key(2), "second-path": key(2)}, fail_mutation=True)
    monkeypatch.setattr(subprocess, "run", boundary)
    with pytest.raises(cli.AmbiguousMutationError):
        bootstrap.pinned_remote(node, {"action": "prepare", "node": node},
                                ROOT / "tools/spark_cluster/peer_ssh.py", key(2))
    assert boundary.attempts == [("identity", "first-path"), ("prepare", "first-path")]
    assert boundary.mutations == []


def test_peer_removal_ownership_observation_uses_independent_handshake_pin(bootstrap, inputs, tmp_path, monkeypatch, capsys):
    inv = inputs[0]
    inventory_path, trust_path = tmp_path / "inventory.json", tmp_path / "trust.json"
    inventory_path.write_text(json.dumps(inv))
    trust_path.write_text(json.dumps({"version": 1, "nodes": {name: {"host_key": key(1)} for name in inv["nodes"]}}))
    boundary = AuthenticatedSSHBoundary({node["ssh"]: key(1) for node in inv["nodes"].values()})
    monkeypatch.setattr(subprocess, "run", boundary)
    assert bootstrap.main(["--inventory", str(inventory_path), "--trust", str(trust_path),
                           "--remove", "e8f1", "--apply"]) == 0
    capsys.readouterr()
    assert ("observe", inv["nodes"]["e8f1"]["ssh"]) in boundary.attempts
    assert boundary.mutations == [("reserve-workload", inv["nodes"]["e8f1"]["ssh"]),
                                  ("remove", inv["nodes"]["66f1"]["ssh"]),
                                  ("release-workload", inv["nodes"]["e8f1"]["ssh"])]


@pytest.fixture
def removal_cluster(bootstrap, inputs, tmp_path, monkeypatch):
    """Real admission and SSH transactions, with only transport/hardware replaced."""
    inv = enrollment.merge_candidate(inputs[0], candidate_from(inputs[0]))
    inventory_path, trust_path = tmp_path / "inventory.json", tmp_path / "trust.json"
    inventory_path.write_text(json.dumps(inv))
    trust_path.write_text(json.dumps({"version": 1, "nodes":
                                    {name: {"host_key": key(1)} for name in inv["nodes"]}}))
    monkeypatch.setattr(node, "STATE", tmp_path / "node-state")
    monkeypatch.setattr(node, "verify_host", lambda value: None)
    monkeypatch.setattr(node, "containers", lambda: [])
    monkeypatch.setattr(node, "run", lambda *args, **kwargs: "")
    monkeypatch.setattr(node, "doctor", lambda value:
                        {"gpu_processes": [], "gpu_containers": [], "memory_mib": {"MemAvailable": 65536}})
    directories = {}
    target = inv["nodes"]["e8f1"]
    trust = {"action": "configure", "peer": target, "peer_host_key": key(1),
             "trusted_host_key": key(1), "peer_public_key": key(99)}
    for name in inv["nodes"]:
        if name != "e8f1":
            directory = tmp_path / name
            directory.mkdir()
            configure(directory, trust)
            directories[inv["nodes"][name]["hostname"]] = directory
    state = {"after_observe": None, "fail_peer": None, "lose_acquire_reply": False,
             "remove_calls": [], "release_calls": 0}

    def transport(host, req, source, host_key, **kwargs):
        action = req["action"]
        if action == "identity":
            return {"host_key": key(1), "public_key": key(99), "transaction_pending": False}
        if action == "observe":
            report = {**idle(host), "reservation": node.reservation(),
                      "recovery_fence": node.recovery_fence()}
            if state["after_observe"]:
                state["after_observe"]()
            return report
        if action == "remove":
            state["remove_calls"].append(host["hostname"])
            if host["hostname"] == state["fail_peer"]:
                raise cli.AmbiguousMutationError("lost revoke reply")
            configure(directories[host["hostname"]], req)
            return {"removed": True}
        if action == "release-workload":
            state["release_calls"] += 1
        result = node.main(req)
        if action == "reserve-workload" and state["lose_acquire_reply"]:
            raise cli.AmbiguousMutationError("lost acquire reply")
        return result

    monkeypatch.setattr(bootstrap, "pinned_remote", transport)
    plan = config.plan(*inputs)
    request = cli.request(plan, "e8f1", "reserve-workload")
    args = ["--inventory", str(inventory_path), "--trust", str(trust_path),
            "--remove", "e8f1", "--apply"]
    return bootstrap, args, request, directories, state


@pytest.mark.parametrize("action", ["reserve", "reserve-workload", "reserve-batch", "fence"])
def test_removal_guard_blocks_admission_after_idle_observation(removal_cluster, action):
    bootstrap, args, req, directories, state = removal_cluster
    rejected = []

    def race():
        competing = {**req, "action": action}
        if action == "fence":
            competing.update(allowed_digests=[req["digest"]],
                             recovery={"policy": "local-auto", "authority": "authority-a", "generation": 1})
        try:
            node.main(competing)
        except RuntimeError:
            rejected.append(action)

    state["after_observe"] = race
    assert bootstrap.main(args) == 0
    assert rejected == [action, action]
    assert node.reservation() is None
    assert all(key(99) not in peer_ssh.read_files(directory)["authorized_keys"]
               for directory in directories.values())


def test_partial_removal_retains_guard_and_explicit_retry_completes(removal_cluster, capsys):
    bootstrap, args, req, directories, state = removal_cluster
    state["fail_peer"] = "spark-third"
    assert bootstrap.main(args) == 1
    receipt = json.loads(capsys.readouterr().out)
    guard = node.reservation()
    assert guard["owner"] == receipt["removal_guard"]["owner"]
    assert receipt["requires_reconciliation"] and not receipt["guard_release_confirmed"]
    assert state["release_calls"] == 0
    assert key(99) in peer_ssh.read_files(directories["spark-third"])["authorized_keys"]
    assert any(key(99) not in peer_ssh.read_files(directory)["authorized_keys"]
               for directory in directories.values())
    with pytest.raises(RuntimeError):
        node.main(req)
    # A fresh invocation cannot steal the retained lease.
    assert bootstrap.main(args) == 1
    capsys.readouterr()
    assert node.reservation() == guard
    state["fail_peer"] = None
    assert bootstrap.main([*args, "--removal-id", receipt["removal_id"]]) == 0
    assert node.reservation() is None and state["release_calls"] == 1
    assert all(key(99) not in peer_ssh.read_files(directory)["authorized_keys"]
               for directory in directories.values())


def test_lost_removal_guard_reply_retains_reconcilable_ownership(removal_cluster, capsys):
    bootstrap, args, _, directories, state = removal_cluster
    state["lose_acquire_reply"] = True
    assert bootstrap.main(args) == 1
    captured = capsys.readouterr()
    receipt, intent = json.loads(captured.out), json.loads(captured.err)
    assert receipt["removal_guard"] == intent["removal_guard"]
    assert node.reservation()["owner"] == receipt["removal_guard"]["owner"]
    assert state["remove_calls"] == [] and state["release_calls"] == 0
    assert all(key(99) in peer_ssh.read_files(directory)["authorized_keys"]
               for directory in directories.values())
    state["lose_acquire_reply"] = False
    assert bootstrap.main([*args, "--removal-id", receipt["removal_id"]]) == 0
    assert node.reservation() is None


@pytest.mark.parametrize("obstacle", ["reservation", "active-fence", "unknown-fence"])
def test_removal_does_not_revoke_or_release_foreign_state(removal_cluster, obstacle):
    bootstrap, args, req, directories, state = removal_cluster
    if obstacle == "reservation":
        node.main(req)
        path = node.STATE / "gpu.json"
    else:
        path = node.STATE / "recovery-fence.json"
        node.atomic(path, None if obstacle == "unknown-fence" else
                    {"active": True, "policy": "local-auto", "authority": "foreign",
                     "generation": 1, "allowed_digests": [req["digest"]]})
    original = path.read_bytes()
    assert bootstrap.main(args) == 1
    assert path.read_bytes() == original
    assert state["remove_calls"] == [] and state["release_calls"] == 0
    assert all(key(99) in peer_ssh.read_files(directory)["authorized_keys"]
               for directory in directories.values())
