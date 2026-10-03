"""Private leased route publication and single-ingress admission fencing (stdlib only)."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
import threading
import time


class RouteUnavailable(ValueError):
    pass


def private_json(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd) as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("route files must be private regular files owned by the gateway user")
        if not 0 < info.st_size <= 1024 * 1024:
            raise ValueError("invalid route file size")
        return json.load(stream)


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd, temporary = tempfile.mkstemp(prefix="." + path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            os.fchmod(stream.fileno(), 0o600)
            json.dump(value, stream, allow_nan=False, sort_keys=True, separators=(",", ":"))
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def identifier(value):
    return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value) is not None


def validate_state(value, validate_registry):
    required = {"version", "policy", "authority", "generation", "alias", "accepting", "route", "mode", "reason", "issued_at", "expires_at"}
    if not isinstance(value, dict) or set(value) != required:
        raise ValueError("invalid recovery route fields")
    if type(value["version"]) is not int or value["version"] != 1 or value["alias"] != "local-auto":
        raise ValueError("unsupported recovery route version or alias")
    if not identifier(value["policy"]) or not identifier(value["authority"]):
        raise ValueError("invalid policy or authority")
    if type(value["generation"]) is not int or not 1 <= value["generation"] <= 2**53 - 1:
        raise ValueError("invalid route generation")
    if type(value["accepting"]) is not bool or value["mode"] not in ("preferred", "fallback", "unavailable"):
        raise ValueError("invalid route admission or mode")
    if not isinstance(value["reason"], str) or len(value["reason"]) > 1024 or any(ord(c) < 32 for c in value["reason"]):
        raise ValueError("invalid route reason")
    for field in ("issued_at", "expires_at"):
        if type(value[field]) not in (int, float) or not 0 < value[field] <= 2**53 - 1:
            raise ValueError("invalid lease timestamp")
    if not 0 < value["expires_at"] - value["issued_at"] <= 60:
        raise ValueError("lease duration must be positive and at most 60 seconds")
    route = value["route"]
    if route is None:
        if value["accepting"] or value["mode"] != "unavailable":
            raise ValueError("absent route must be unavailable and closed")
    else:
        validate_registry({"version": 1, "routes": {"local-auto": route}})
        if value["mode"] == "unavailable" or "replicas" in route or route.get("upstream_key_env"):
            raise ValueError("policy routes must select one pinned local deployment")
        if not isinstance(route.get("deployment_digest"), str) or not re.fullmatch(r"[a-f0-9]{64}", route["deployment_digest"]):
            raise ValueError("policy route requires exact deployment digest")
        if not route.get("model_root") or not identifier(route["upstream_model"]) or route["upstream_model"] == "local-auto":
            raise ValueError("policy route requires pinned model identity")
        base = route["base_url"].removesuffix("/v1")
        if route.get("tokenizer_base_url") != base or route.get("health_url") != base + "/health":
            raise ValueError("policy tokenizer and health must belong to the selected backend")
    return value


def write_state(path, value, validate_registry):
    """Publish one validated lease; the authority owns ordering and heartbeat renewal."""
    validate_state(value, validate_registry)
    atomic_json(path, value)


def contract_digest(state):
    contract = {k: v for k, v in state.items() if k not in ("issued_at", "expires_at", "reason")}
    return hashlib.sha256(json.dumps(contract, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class RecoveryRoutes:
    def __init__(self, path, validate_registry):
        self.path = Path(path)
        self.validate_registry = validate_registry
        self.fence_path = self.path.with_name(self.path.name + ".fence.json")
        self.lock = threading.RLock()
        self.active = 0
        self.started_at = time.time()
        self.last_wall = self.started_at
        self.lease = None
        self.lease_deadline = 0
        self.fence = None
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(str(self.path) + ".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        self.process_lock = os.fdopen(fd, "a")
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise ValueError("unsafe gateway recovery lock")
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if self.fence_path.exists():
                self.fence = private_json(self.fence_path)
                f = self.fence
                if not isinstance(f, dict) or set(f) != {"policy", "authority", "generation", "digest", "issued_at", "expires_at"}:
                    raise ValueError("invalid persisted gateway fence")
                if not identifier(f["policy"]) or not identifier(f["authority"]) or type(f["generation"]) is not int or not 1 <= f["generation"] <= 2**53 - 1:
                    raise ValueError("invalid persisted gateway identity")
                if not isinstance(f["digest"], str) or not re.fullmatch(r"[a-f0-9]{64}", f["digest"]):
                    raise ValueError("invalid persisted gateway digest")
                if any(type(f[k]) not in (int, float) or not 0 < f[k] <= 2**53 - 1 for k in ("issued_at", "expires_at")) or not 0 < f["expires_at"] - f["issued_at"] <= 60:
                    raise ValueError("invalid persisted gateway lease")
            # Neither a durable fence nor an unaccepted startup route is a new heartbeat,
            # even if the wall clock regressed below its timestamp across restart.
            self.restart_heartbeat = max(self.started_at, self.fence["issued_at"] if self.fence else 0)
            try:
                startup_state = validate_state(private_json(self.path), self.validate_registry)
            except (OSError, ValueError, TypeError, KeyError):
                pass
            else:
                self.restart_heartbeat = max(self.restart_heartbeat, startup_state["issued_at"])
        except Exception:
            self.process_lock.close()
            raise

    def close(self):
        self.process_lock.close()

    def _read(self):
        state = validate_state(private_json(self.path), self.validate_registry)
        now = time.time()
        monotonic = time.monotonic()
        if now < self.last_wall:
            raise RouteUnavailable("gateway clock regressed; fresh heartbeat required")
        self.last_wall = now
        digest = contract_digest(state)
        if self.fence:
            if (state["policy"], state["authority"]) != (self.fence["policy"], self.fence["authority"]):
                raise RouteUnavailable("authority or policy does not match persistent fence")
            if state["generation"] < self.fence["generation"] or state["issued_at"] < self.fence["issued_at"]:
                raise RouteUnavailable("stale generation or heartbeat")
            if state["generation"] == self.fence["generation"] and digest != self.fence["digest"]:
                raise RouteUnavailable("generation reused for a different route contract")
            if state["issued_at"] == self.fence["issued_at"] and state["expires_at"] != self.fence["expires_at"]:
                raise RouteUnavailable("lease renewal requires a new heartbeat timestamp")
        if state["issued_at"] > now:
            raise RouteUnavailable("lease issued in the future")
        fence = {k: state[k] for k in ("policy", "authority", "generation", "issued_at", "expires_at")}
        fence["digest"] = digest
        if fence != self.fence:
            atomic_json(self.fence_path, fence)
            self.fence = fence
        lease = (digest, state["issued_at"], state["expires_at"])
        if lease != self.lease:
            # A wall-clock jump backwards cannot extend an already observed lease.
            self.lease_deadline = monotonic + max(0, state["expires_at"] - now)
            self.lease = lease
        if state["issued_at"] <= self.restart_heartbeat:
            raise RouteUnavailable("gateway restart requires a fresh authority heartbeat")
        if now >= state["expires_at"] or monotonic >= self.lease_deadline:
            raise RouteUnavailable("route lease expired")
        return state

    def status(self):
        with self.lock:
            try:
                state = self._read()
                result = {k: v for k, v in state.items() if k not in ("route", "version")}
                result["fresh"] = True
            except (OSError, ValueError, TypeError, KeyError) as exc:
                result = {"alias": "local-auto", "policy": None, "authority": None, "generation": None,
                          "mode": "unavailable", "accepting": False, "fresh": False,
                          "issued_at": None, "expires_at": None, "reason": "route unavailable or stale"}
                if isinstance(exc, RouteUnavailable):
                    result["reason"] = str(exc)
                if self.fence:
                    result.update({k: self.fence[k] for k in ("policy", "authority", "generation", "issued_at", "expires_at")})
            result["highest_generation"] = self.fence["generation"] if self.fence else None
            result["active_requests"] = self.active
            return result

    def acquire(self, route=None):
        with self.lock:
            try:
                state = self._read()
                if not state["accepting"]:
                    raise RouteUnavailable("policy admission is closed")
                if route is not None and route != state["route"]:
                    raise RouteUnavailable("strict route does not match the active policy route")
            except (OSError, ValueError, TypeError, KeyError) as exc:
                raise RouteUnavailable("policy admission unavailable; no fallback or replay") from exc
            self.active += 1
            return state

    def check(self, admitted):
        with self.lock:
            try:
                state = self._read()
                if not state["accepting"] or contract_digest(state) != contract_digest(admitted):
                    raise RouteUnavailable("admitted generation is stale")
            except (OSError, ValueError, TypeError, KeyError) as exc:
                raise RouteUnavailable("admitted generation is no longer dispatchable") from exc

    def release(self):
        with self.lock:
            self.active -= 1
