"""Conservative aggregate generation-stall and liveness supervision for vLLM.

Generation monitoring arms only after observed decode progress with known
active work. It cannot identify individual stalled requests, distinguish a
new prefill from a decode freeze without a sampled idle boundary, or detect
a stall before any decode progress. Missing telemetry disarms that monitor;
liveness failures are supervised independently.
"""
from __future__ import annotations

import math
import re
import time
import urllib.request


class StallDetector:
    """Observe health, a cumulative generation counter, and known active load.

    Initial samples and counter decreases establish a baseline, not progress.
    Idle, unavailable telemetry, and liveness failures clear generation-stall
    arming. Only an increase during continuously observed active work arms it.
    """

    def __init__(self, stall_seconds=180, health_grace=30, now=None):
        if not all(math.isfinite(value) and value > 0
                   for value in (stall_seconds, health_grace)):
            raise ValueError("thresholds must be finite and positive")
        self.stall_seconds = stall_seconds
        self.health_grace = health_grace
        self._now = now  # injectable clock for tests
        self.reset()

    def reset(self):
        """Forget observations from the previous engine or recovery attempt."""
        self.last_count = None
        self.last_advance = None
        self._unhealthy_since = None

    def _clock(self):
        return self._now() if self._now else time.monotonic()

    def observe(self, health_ok, generation, active):
        """Return ``(action, detail)`` where action is watch|restart|unhealthy."""
        now = self._clock()
        if not health_ok:
            self.last_count = self.last_advance = None
            if self._unhealthy_since is None:
                self._unhealthy_since = now
            if now - self._unhealthy_since >= self.health_grace:
                return ("unhealthy", "liveness failed for %ds"
                        % (now - self._unhealthy_since))
            return ("watch", "awaiting liveness")
        self._unhealthy_since = None
        if active is not True or generation is None:
            self.last_count = self.last_advance = None
            return ("watch", "idle" if active is False else "telemetry unavailable")
        previous = self.last_count
        self.last_count = generation
        if previous is None or generation < previous:
            self.last_advance = None
            return ("watch", "awaiting generation progress")
        if generation > previous:
            self.last_advance = now
            return ("watch", "progress")
        if self.last_advance is not None and now - self.last_advance >= self.stall_seconds:
            return ("restart", "aggregate generation frozen %ds with active work"
                    % (now - self.last_advance))
        return ("watch", "awaiting generation progress")


_METRIC_NAME = re.compile(r"^\s*([a-zA-Z_:][a-zA-Z0-9_:]*)")
_METRIC_SAMPLE = re.compile(
    r'^\s*[a-zA-Z_:][a-zA-Z0-9_:]*'
    r'(?:\{(?:[^"{}]|"(?:\\.|[^"\\])*")*\})?'
    r'[ \t]+([^ \t]+)(?:[ \t]+[+-]?\d+)?(?:[ \t]+#.*)?[ \t]*$'
)


def metric_totals(text, names):
    """Sum requested nonnegative metric families across all labeled samples.

    Accept native ``vllm:`` names and their ``vllm_`` aliases. Keys retain the
    requested spelling; absent families or families containing any malformed,
    negative, or nonfinite sample return ``None``, never a partial total.
    """
    requested = tuple(names)
    totals = {name.replace("vllm:", "vllm_", 1): None for name in requested}
    invalid = set()
    for line in text.splitlines():
        match = _METRIC_NAME.match(line)
        if match is None:
            continue
        name = match.group(1).replace("vllm:", "vllm_", 1)
        if name not in totals or name in invalid:
            continue
        sample = _METRIC_SAMPLE.fullmatch(line)
        try:
            number = float(sample.group(1)) if sample else float("nan")
        except ValueError:
            number = float("nan")
        total = (totals[name] or 0.0) + number
        if not math.isfinite(number) or number < 0 or not math.isfinite(total):
            invalid.add(name)
            totals[name] = None
        else:
            totals[name] = total
    return {name: totals[name.replace("vllm:", "vllm_", 1)] for name in requested}


def parse_metrics(text):
    """Return generation totals and load, leaving incomplete telemetry unknown."""
    names = ("vllm:generation_tokens_total", "vllm:num_requests_running",
             "vllm:num_requests_waiting")
    totals = metric_totals(text, names)
    generation, running, waiting = (totals[name] for name in names)
    active = None
    if running is not None and waiting is not None:
        active = running > 0 or waiting > 0
    return {"generation_tokens": generation, "active": active}


class HttpProbe:
    """Stdlib HTTP client over a deployment's coordinator endpoint."""

    def __init__(self, base_url, timeout=5):
        self.root = base_url.rstrip("/")
        if self.root.endswith("/v1"):
            self.root = self.root[: -len("/v1")]
        self.timeout = timeout
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def _get(self, path):
        with self._opener.open(self.root + path, timeout=self.timeout) as f:
            return f.read().decode("utf-8", "replace")

    def health(self):
        try:
            self._get("/health")
            return True
        except Exception:
            return False

    def metrics(self):
        try:
            return parse_metrics(self._get("/metrics"))
        except Exception:
            return {"generation_tokens": None, "active": None}


def run_watchdog(detector, probe, restart, poll_interval=5.0, max_restarts=3,
                 report=print, stop_after=None, recovery_timeout=600):
    """Restart on detected failure, allowing bounded time for liveness to return.

    The restart callback only requests a restart; it is not recovery evidence.
    While awaiting liveness, normal health grace is suspended. Attempt counts
    reset only after observed generation progress, not merely restored health.
    Exhausting consecutive attempts raises ``RuntimeError`` without another
    restart. Returns total restart attempts when ``stop_after`` expires.
    """
    if not all(math.isfinite(value) and value > 0
               for value in (poll_interval, recovery_timeout)):
        raise ValueError("poll_interval and recovery_timeout must be finite and positive")
    if not isinstance(max_restarts, int) or isinstance(max_restarts, bool) or max_restarts <= 0:
        raise ValueError("max_restarts must be a positive integer")
    if stop_after is not None and (not math.isfinite(stop_after) or stop_after <= 0):
        raise ValueError("stop_after must be finite and positive")
    start = detector._clock()
    recovery_deadline = None
    consecutive = total = 0
    while True:
        health = probe.health()
        metrics = probe.metrics()
        now = detector._clock()
        if recovery_deadline is not None:
            if health:
                recovery_deadline = None
                detector.reset()
                report("watchdog: liveness restored; awaiting generation progress")
                action, detail = detector.observe(
                    health, metrics["generation_tokens"], metrics["active"])
            elif now >= recovery_deadline:
                action, detail = "unhealthy", "recovery liveness deadline expired"
            else:
                action, detail = "watch", "awaiting recovery liveness"
        else:
            action, detail = detector.observe(
                health, metrics["generation_tokens"], metrics["active"])
        if detail == "progress":
            if consecutive:
                report("watchdog: generation resumed")
            consecutive = 0
        if action in ("restart", "unhealthy"):
            report("watchdog: %s (%s)" % (action, detail))
            if consecutive >= max_restarts:
                raise RuntimeError("watchdog exceeded %d consecutive restart attempts"
                                   % max_restarts)
            consecutive += 1
            total += 1
            detector.reset()
            recovery_deadline = detector._clock() + recovery_timeout
            restart()
        if stop_after is not None and detector._clock() - start >= stop_after:
            return total
        time.sleep(poll_interval)
