#!/usr/bin/env python3
"""Watch the two remote-worker-failure PRs without posting or changing code.

Requires authenticated GitHub CLI (gh). State and events stay in ignored logs/.
macOS notifications are best-effort; status.json and events.jsonl are authoritative.
"""
import argparse
import fcntl
import hashlib
import json
import math
import os
import secrets
import signal
import subprocess
import sys
import threading
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
REPOSITORY = "vllm-project/vllm"
PULLS = (53597, 58738)
DEFAULT_STATE = ROOT / "logs/incidents/2026-10-01-vllm-worker-failure/upstream-watch"
NOTIFICATION_SCRIPT = '''on run argv
    display notification (item 1 of argv) with title "vLLM upstream watcher"
end run'''


def now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w") as output:
        os.chmod(temporary, 0o600)
        output.write(json.dumps(value, indent=2) + "\n")
        output.flush()
        os.fsync(output.fileno())
    temporary.replace(path)


def emit(directory, event, **fields):
    record = {"time": now(), "event": event, **fields}
    with (directory / "events.jsonl").open("a") as output:
        output.write(json.dumps(record) + "\n")
    print(json.dumps({k: v for k, v in record.items() if k != "changes"}), flush=True)


def notify(message):
    if sys.platform != "darwin":
        return False
    # Pass notification text as data, never interpolate GitHub content into code.
    result = subprocess.run(
        ["osascript", "-e", NOTIFICATION_SCRIPT, message],
        capture_output=True, text=True, timeout=15, check=False,
    )
    return result.returncode == 0


def queue_telegram(directory, request, route):
    path = directory / "telegram-delivery.json"
    deliveries = json.loads(path.read_text()) if path.exists() else {}
    event_id = request["event_id"]
    if event_id not in deliveries:
        deliveries[event_id] = {
            "summary": request["summary"], "observed_at": request["observed_at"],
            "account": route["account"], "target": route["target"],
            "accepted_at": None, "human_receipt": "unconfirmed", "attempts": 0,
        }
        write_json(path, deliveries)


def send_telegram(route, message):
    result = subprocess.run(
        [route["command"], "message", "send", "--channel", "telegram",
         "--account", route["account"], "--target", route["target"],
         "--message", message, "--json"],
        capture_output=True, text=True, timeout=60, check=False,
    )
    # A zero exit status alone can also describe suppressed/failed delivery.
    # Never expose CLI output: it can include credentials or routing details.
    if result.returncode:
        return False
    try:
        response = json.loads(result.stdout)
    except ValueError:
        return False
    return (
        isinstance(response, dict) and response.get("action") == "send"
        and response.get("channel") == "telegram"
        and response.get("dryRun") is False and response.get("ok") is not False
        and response.get("deliveryStatus") not in ("failed", "partial_failed", "suppressed")
        and bool(response.get("messageId"))
    )


def retry_telegram(directory, route):
    # Use the poll lock for queue updates too, including concurrent --once runs.
    with (directory / ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = directory / "telegram-delivery.json"
        if not path.exists():
            return
        deliveries = json.loads(path.read_text())
        for event_id, delivery in deliveries.items():
            if delivery["accepted_at"] or any(
                delivery[field] != route[field] for field in ("account", "target")
            ):
                continue
            # Only generated field names/counts leave the machine, never PR text,
            # local paths, hostnames, or serving/network topology.
            message = "vLLM upstream update: " + delivery["summary"]
            delivery["attempts"] += 1
            delivery["last_attempt_at"] = now()
            write_json(path, deliveries)
            try:
                accepted = send_telegram(route, message)
            except (OSError, subprocess.TimeoutExpired):
                accepted = False
            if accepted:
                delivery["accepted_at"] = now()
            # Commit acceptance before logging so an event-log failure cannot
            # cause a known accepted send to be retried on the next poll.
            write_json(path, deliveries)
            emit(directory, "telegram_notification_requested", event_id=event_id,
                 accepted=accepted, human_receipt="unconfirmed")


def fetch(endpoint, paginated=False):
    command = ["gh", "api", "--method", "GET", f"repos/{REPOSITORY}/{endpoint}"]
    if paginated:
        command += ["--paginate", "--slurp"]
    result = subprocess.run(command, capture_output=True, text=True, timeout=60, check=False)
    if result.returncode:
        raise RuntimeError(f"GitHub GET {endpoint}: {result.stderr.strip()}")
    data = json.loads(result.stdout)
    return [item for page in data for item in page] if paginated else data


def snapshot(number):
    pull = fetch(f"pulls/{number}")
    result = {
        "url": pull["html_url"],
        "metadata": {
            "state": pull["state"], "merged": pull["merged"],
            "draft": pull["draft"], "head": pull["head"]["sha"],
            "base_branch": pull["base"]["ref"],
            "title": pull["title"], "body": pull["body"],
        },
    }
    for kind, endpoint in (
        ("comments", f"issues/{number}/comments?per_page=100"),
        ("reviews", f"pulls/{number}/reviews?per_page=100"),
        ("review_comments", f"pulls/{number}/comments?per_page=100"),
    ):
        result[kind] = {
            str(item["id"]): {
                "author": (item.get("user") or {}).get("login"),
                **{key: item.get(key) for key in (
                    "body", "state", "html_url", "updated_at", "submitted_at",
                    "commit_id", "path", "line",
                )},
            }
            for item in fetch(endpoint, paginated=True)
        }
    return result


def difference(before, after):
    return {
        "added": {key: after[key] for key in after.keys() - before.keys()},
        "edited": {key: {"before": before[key], "after": after[key]}
                   for key in before.keys() & after.keys() if before[key] != after[key]},
        "removed": {key: before[key] for key in before.keys() - after.keys()},
    }


def poll(directory, telegram=None):
    # Serialize --once and loop polls, including their snapshot/event writes.
    with (directory / ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        path = directory / "snapshot.json"
        previous = json.loads(path.read_text()) if path.exists() else None
        intent_path = directory / "transition-intent.json"
        current = {str(number): snapshot(number) for number in PULLS}
        changes = {}
        if previous is not None:
            for number in current:
                for kind in ("metadata", "comments", "reviews", "review_comments"):
                    delta = difference(previous[number][kind], current[number][kind])
                    if any(delta.values()):
                        changes.setdefault(number, {})[kind] = delta
        if changes:
            summaries = []
            for number, kinds in changes.items():
                descriptions = []
                for kind, delta in kinds.items():
                    if kind == "metadata":
                        descriptions.append(", ".join(sorted(delta["edited"])))
                    else:
                        descriptions.append(
                            f"{kind}: +{len(delta['added'])}/"
                            f"edited {len(delta['edited'])}/removed {len(delta['removed'])}"
                        )
                summaries.append(f"#{number}: {'; '.join(descriptions)}")
            summary = " | ".join(summaries)
            transition = hashlib.sha256(
                json.dumps([previous, current], sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            intent = json.loads(intent_path.read_text()) if intent_path.exists() else None
            if intent is None or intent["transition"] != transition:
                intent = {
                    "transition": transition,
                    "request": {
                        "schema_version": 1, "event_id": secrets.token_hex(32),
                        "observed_at": now(), "summary": summary, "prs": list(PULLS),
                    },
                }
                write_json(intent_path, intent)
            request = intent["request"]
            event_id = request["event_id"]
            # Persist every consumer's work before advancing the baseline. A
            # crash can duplicate a log entry, but cannot discard an update.
            emit(directory, "upstream_changed", event_id=event_id,
                 summary=summary, changes=changes)
            if telegram:
                queue_telegram(directory, request, telegram)
            write_json(directory / "triage-request.json", request)
        else:
            summary = "Baseline saved" if previous is None else "No upstream changes"
        write_json(path, current)
        # Retain an intent only until its baseline commits: retries deduplicate,
        # but a later return to the same upstream head is a new observation.
        intent_path.unlink(missing_ok=True)
        checked_at = now()
        write_json(directory / "status.json", {
            "checked_at": checked_at, "last_success_at": checked_at,
            "ok": True, "summary": summary,
            "pulls": {
                key: {
                    "url": item["url"],
                    **{field: value for field, value in item["metadata"].items()
                       if field != "body"},
                }
                for key, item in current.items()
            },
        })
        emit(directory, "poll_complete", summary=summary, changed=bool(changes))
        return summary if changes else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--interval-seconds", type=float, default=3600)
    parser.add_argument("--state-dir", type=Path, default=DEFAULT_STATE)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--no-notify", action="store_true",
                        help="Disable desktop and Telegram notification delivery")
    parser.add_argument("--telegram-account", help="Explicit existing OpenClaw account ID")
    parser.add_argument("--telegram-target", help="Verified existing Telegram chat target")
    parser.add_argument("--openclaw-command", default="openclaw",
                        help="OpenClaw executable path (default: openclaw)")
    args = parser.parse_args()
    if not math.isfinite(args.interval_seconds) or args.interval_seconds <= 0:
        parser.error("--interval-seconds must be positive and finite")
    if bool(args.telegram_account) != bool(args.telegram_target):
        parser.error("--telegram-account and --telegram-target must be supplied together")
    telegram = {
        "account": args.telegram_account, "target": args.telegram_target,
        "command": args.openclaw_command,
    } if args.telegram_account and not args.no_notify else None
    os.umask(0o077)
    args.state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    stop = threading.Event()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_: stop.set())
    failed = False
    while not stop.is_set():
        try:
            message = poll(args.state_dir, telegram) if telegram else poll(args.state_dir)
            if failed and not message:
                message = "GitHub checks recovered; see local-llm-stack status.json."
            failed = False
        except (OSError, ValueError, KeyError, RuntimeError, subprocess.TimeoutExpired) as error:
            if stop.is_set():
                break
            previous_status = args.state_dir / "status.json"
            last_success = None
            if previous_status.exists():
                last_success = json.loads(previous_status.read_text()).get("last_success_at")
            write_json(previous_status, {
                "checked_at": now(), "last_success_at": last_success,
                "ok": False, "error": str(error),
            })
            emit(args.state_dir, "poll_failed", error=str(error))
            message = None if failed else "GitHub check failed; see local-llm-stack status.json."
            failed = True
        if message and not args.no_notify:
            try:
                accepted = notify(message)
                emit(args.state_dir, "notification_requested", accepted=accepted)
            except (OSError, subprocess.TimeoutExpired) as error:
                emit(args.state_dir, "notification_failed", error=str(error))
        if telegram and not stop.is_set():
            try:
                retry_telegram(args.state_dir, telegram)
            except (OSError, ValueError, KeyError) as error:
                # Notification failures must not misreport a GitHub outage.
                emit(args.state_dir, "telegram_notification_failed",
                     error=type(error).__name__)
        if args.once:
            return 1 if failed else 0
        stop.wait(args.interval_seconds)
    emit(args.state_dir, "watcher_stopped")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
