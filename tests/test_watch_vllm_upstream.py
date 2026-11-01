"""Watcher interruption, durable triage, and opt-in alert delivery boundaries."""
import json
import runpy
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_stop_during_poll_preserves_last_success(tmp_path):
    status = {"ok": True, "last_success_at": "2026-10-04T00:00:00+00:00"}
    (tmp_path / "status.json").write_text(json.dumps(status))
    # Use a child so real SIGTERM handling cannot change the test runner's
    # handlers. Model the interrupted gh subprocess observed during hub stop.
    program = '''
import runpy
import signal
import sys
script, directory = sys.argv[1:]
module = runpy.run_path(script)
def interrupted_poll(_directory):
    signal.raise_signal(signal.SIGTERM)
    raise RuntimeError("in-flight GitHub GET interrupted")
module["main"].__globals__["poll"] = interrupted_poll
sys.argv = [script, "--state-dir", directory, "--no-notify"]
raise SystemExit(module["main"]())
'''
    result = subprocess.run(
        [sys.executable, "-c", program,
         str(ROOT / "scripts/watch-vllm-upstream.py"), str(tmp_path)],
        capture_output=True, text=True, timeout=10, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads((tmp_path / "status.json").read_text()) == status
    events = [json.loads(line)["event"]
              for line in (tmp_path / "events.jsonl").read_text().splitlines()]
    assert "poll_failed" not in events
    assert "watcher_stopped" in events


@pytest.fixture
def watcher():
    return runpy.run_path(str(ROOT / "scripts/watch-vllm-upstream.py"))["poll"].__globals__


@pytest.fixture
def route():
    return {"command": "openclaw", "account": "test-account", "target": "123456"}


def upstream(revision):
    return {
        "url": "https://github.com/vllm-project/vllm/pull/53597",
        "metadata": {"state": "open", "head": revision, "body": "untrusted PR text"},
        "comments": {}, "reviews": {}, "review_comments": {},
    }


def change_upstream(watcher, monkeypatch, tmp_path, route=None):
    monkeypatch.setitem(watcher, "snapshot", lambda _number: upstream("before"))
    watcher["poll"](tmp_path, route)
    monkeypatch.setitem(watcher, "snapshot", lambda _number: upstream("after"))
    watcher["poll"](tmp_path, route)


def test_failed_delivery_retries_without_another_change(watcher, route, monkeypatch, tmp_path):
    change_upstream(watcher, monkeypatch, tmp_path, route)
    request = (tmp_path / "triage-request.json").read_bytes()
    attempts = []
    outcomes = iter((False, True))

    def send(_route, message):
        attempts.append(message)
        return next(outcomes)

    monkeypatch.setitem(watcher, "send_telegram", send)
    watcher["retry_telegram"](tmp_path, route)
    state = json.loads((tmp_path / "telegram-delivery.json").read_text())
    assert next(iter(state.values()))["accepted_at"] is None
    assert watcher["poll"](tmp_path, route) is None
    watcher["retry_telegram"](tmp_path, route)
    # Reloading the module models a watcher restart; durable acceptance, not an
    # in-memory flag, must suppress the third attempt.
    restarted = runpy.run_path(str(ROOT / "scripts/watch-vllm-upstream.py"))["poll"].__globals__
    monkeypatch.setitem(restarted, "send_telegram", send)
    restarted["retry_telegram"](tmp_path, route)
    assert len(attempts) == 2
    delivery = next(iter(json.loads((tmp_path / "telegram-delivery.json").read_text()).values()))
    assert delivery["accepted_at"] is not None
    assert delivery["human_receipt"] == "unconfirmed"
    assert (tmp_path / "triage-request.json").read_bytes() == request
    events = [json.loads(line) for line in (tmp_path / "events.jsonl").read_text().splitlines()]
    assert sum(event["event"] == "upstream_changed" for event in events) == 1
    assert "untrusted PR text" not in attempts[0]


def test_pending_alerts_survive_later_change_and_route_mismatch(
    watcher, route, monkeypatch, tmp_path,
):
    change_upstream(watcher, monkeypatch, tmp_path, route)
    monkeypatch.setitem(watcher, "snapshot", lambda _number: upstream("third"))
    watcher["poll"](tmp_path, route)
    sent = []
    monkeypatch.setitem(watcher, "send_telegram", lambda _route, message: sent.append(message) or True)
    watcher["retry_telegram"](tmp_path, {**route, "target": "654321"})
    assert sent == []
    watcher["retry_telegram"](tmp_path, route)
    assert len(sent) == 2
    assert all(record["accepted_at"] for record in json.loads(
        (tmp_path / "telegram-delivery.json").read_text()
    ).values())


@pytest.mark.parametrize("failure", ["timeout", "zero_exit_failure", "dry_run", "invalid_json"])
def test_cli_failure_is_not_recorded_as_acceptance(
    watcher, route, monkeypatch, tmp_path, failure,
):
    change_upstream(watcher, monkeypatch, tmp_path, route)

    def run(command, **_kwargs):
        if failure == "timeout":
            raise subprocess.TimeoutExpired(command, 60)
        response = {
            "action": "send", "channel": "telegram", "dryRun": False,
            "messageId": "test-message",
        }
        if failure == "zero_exit_failure":
            response.update(ok=False, deliveryStatus="failed")
        elif failure == "dry_run":
            response["dryRun"] = True
        return subprocess.CompletedProcess(
            command, 0, "not JSON" if failure == "invalid_json" else json.dumps(response), "",
        )

    monkeypatch.setattr(subprocess, "run", run)
    watcher["retry_telegram"](tmp_path, route)
    assert next(iter(json.loads(
        (tmp_path / "telegram-delivery.json").read_text()
    ).values()))["accepted_at"] is None


def test_acceptance_survives_event_log_failure(watcher, route, monkeypatch, tmp_path):
    change_upstream(watcher, monkeypatch, tmp_path, route)
    sent = []

    def accepted_cli(command, **_kwargs):
        sent.append(command)
        return subprocess.CompletedProcess(command, 0, json.dumps({
            "action": "send", "channel": "telegram", "dryRun": False,
            "messageId": "accepted-message",
        }), "")

    monkeypatch.setattr(subprocess, "run", accepted_cli)

    def failed_log(*_args, **_kwargs):
        raise OSError("log disk unavailable")

    monkeypatch.setitem(watcher, "emit", failed_log)
    with pytest.raises(OSError):
        watcher["retry_telegram"](tmp_path, route)
    watcher["retry_telegram"](tmp_path, route)
    assert len(sent) == 1


@pytest.mark.parametrize("blocked_file", ["triage-request.json", "telegram-delivery.json"])
def test_consumer_write_failure_preserves_baseline(
    watcher, route, monkeypatch, tmp_path, blocked_file,
):
    monkeypatch.setitem(watcher, "snapshot", lambda _number: upstream("before"))
    watcher["poll"](tmp_path, route)
    baseline = (tmp_path / "snapshot.json").read_bytes()
    assert not (tmp_path / "triage-request.json").exists()
    monkeypatch.setitem(watcher, "snapshot", lambda _number: upstream("after"))
    write_json = watcher["write_json"]

    def fail_consumer(path, value):
        if path.name == blocked_file:
            raise OSError("consumer state unavailable")
        write_json(path, value)

    monkeypatch.setitem(watcher, "write_json", fail_consumer)
    with pytest.raises(OSError):
        watcher["poll"](tmp_path, route)
    assert (tmp_path / "snapshot.json").read_bytes() == baseline
    monkeypatch.setitem(watcher, "write_json", write_json)
    watcher["poll"](tmp_path, route)
    request = json.loads((tmp_path / "triage-request.json").read_text())
    assert request["prs"] == [53597, 58738]
    assert request["schema_version"] == 1
    assert "untrusted PR text" not in json.dumps(request)
    assert (tmp_path / "snapshot.json").read_bytes() != baseline
    assert request["event_id"] in json.loads((tmp_path / "telegram-delivery.json").read_text())


def test_snapshot_failure_replays_same_event_without_resending(
    watcher, route, monkeypatch, tmp_path,
):
    monkeypatch.setitem(watcher, "snapshot", lambda _number: upstream("before"))
    watcher["poll"](tmp_path, route)
    monkeypatch.setitem(watcher, "snapshot", lambda _number: upstream("after"))
    write_json = watcher["write_json"]

    def fail_snapshot(path, value):
        if path.name == "snapshot.json":
            raise OSError("baseline unavailable")
        write_json(path, value)

    monkeypatch.setitem(watcher, "write_json", fail_snapshot)
    with pytest.raises(OSError):
        watcher["poll"](tmp_path, route)
    request = json.loads((tmp_path / "triage-request.json").read_text())
    sent = []
    monkeypatch.setitem(watcher, "send_telegram", lambda _route, message: sent.append(message) or True)
    watcher["retry_telegram"](tmp_path, route)
    monkeypatch.setitem(watcher, "write_json", write_json)
    watcher["poll"](tmp_path, route)
    watcher["retry_telegram"](tmp_path, route)
    replay = json.loads((tmp_path / "triage-request.json").read_text())
    assert replay["event_id"] == request["event_id"]
    assert len(sent) == 1


def test_revisited_head_delivers_a_new_alert(watcher, route, monkeypatch, tmp_path):
    sent = []
    monkeypatch.setitem(watcher, "send_telegram", lambda _route, message: sent.append(message) or True)
    monkeypatch.setitem(watcher, "snapshot", lambda _number: upstream("before"))
    watcher["poll"](tmp_path, route)
    observed = []
    for revision in ("after", "before", "after"):
        monkeypatch.setitem(watcher, "snapshot", lambda _number, revision=revision: upstream(revision))
        watcher["poll"](tmp_path, route)
        observed.append(json.loads((tmp_path / "triage-request.json").read_text())["event_id"])
        watcher["retry_telegram"](tmp_path, route)
    assert len(sent) == 3
    assert len(set(observed)) == 3
