"""Engine-stall watchdog for a served vLLM deployment.

The DeepSeek MIA engine can become *healthy but stalled*: the V1 scheduler is
blocked (e.g. inside a cold TRITON/TileLang kernel JIT on the DSpark draft
path) while ``/health`` keeps returning 200 and the periodic-metric loop goes
silent. A Compose ``healthcheck`` never trips, so without a supervisor the
operator must kill the service by hand (see the MIA reliability incidents of
2026-10-29: ``stuck-2036.log`` ~3m10s silent, ``serve-e8f1.log`` ~70s then a
manual kill mid-request).

:class:`StallDetector` is a pure, dependency-free state machine over three
input signals; :func:`run_watchdog` drives it in a loop and calls a
``restart`` callback when a stall is declared. Load/down detection uses the
engine's own Prometheus metrics (``/metrics``) so an *idle-but-healthy* engine
is never restarted — only one that was producing tokens and then froze with
work in flight.
"""
from __future__ import annotations

import time

import urllib.request


class StallDetector:
    """Decide whether a served engine is stalled.

    Signals per ``observe`` tick:

    * ``health_ok`` — ``/health`` returned 200.
    * ``generation`` — cumulative generated-token counter (int) or ``None``.
    * ``active`` — ``True``/``False`` when work is queued/running, ``None`` when
      the metrics source exposes no load signal.

    A restart is raised exactly when the engine *was* making generation
    progress and then froze for ``>= stall_seconds`` while work was queued or
    running (or the load signal is unavailable after prior progress), or when
    it fails liveness for ``>= health_grace``. Idle engines are never touched.
    """

    def __init__(self, stall_seconds=180, health_grace=30, now=None):
        if stall_seconds <= 0 or health_grace <= 0:
            raise ValueError("thresholds must be positive")
        self.stall_seconds = stall_seconds
        self.health_grace = health_grace
        self._now = now  # injectable clock for tests
        self.last_count = None
        self.last_advance = None
        self.last_healthy = None
        self.restarts = 0

    def _clock(self):
        return self._now() if self._now else time.monotonic()

    def observe(self, health_ok, generation, active):
        """Return ``(action, detail)`` where action in watch|restart|unhealthy."""
        now = self._clock()
        if not health_ok:
            if self.last_healthy is None:
                self.last_healthy = now
            if now - self.last_healthy >= self.health_grace:
                return ("unhealthy", "liveness failed for %ds" % (now - self.last_healthy))
            return ("watch", "recovering")
        self.last_healthy = now
        if generation is not None:
            if self.last_count is None or generation > self.last_count:
                self.last_count = generation
                self.last_advance = now
                return ("watch", "progress")
            # Counter frozen: only a stall if there is (or was) work to do.
            if self.last_advance is not None and active is not False \
                    and now - self.last_advance >= self.stall_seconds:
                return ("restart", "generation frozen %ds with work in flight"
                        % (now - self.last_advance))
        return ("watch", "idle")


def parse_metrics(text):
    """Pull the load/progress gauges vLLM exposes at ``/metrics``.

    Returns ``{"generation_tokens": int|None, "active": bool|None}``.
    ``active`` is ``None`` when neither the running nor waiting gauge is found.
    """
    generation = None
    running = waiting = None
    for line in text.splitlines():
        if "{" in line:
            name, _, tail = line.partition("{")
            rest = tail.split("}")[-1].strip()
        else:
            name, _, rest = line.rpartition(" ")
            rest = rest.strip()
        try:
            number = float(rest)
        except ValueError:
            continue  # HELP/TYPE or unparsable lines
        if name == "vllm_generation_tokens_total":
            generation = int(number)
        elif name == "vllm_num_requests_running":
            running = int(number)
        elif name == "vllm_num_requests_waiting":
            waiting = int(number)
    active = None
    if running is not None and waiting is not None:
        active = (running + waiting) > 0
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
                 report=print, stop_after=None):
    """Supervise one engine; call ``restart()`` on stall and retry until done.

    ``max_restarts`` bounds consecutive recoveries; beyond it a ``RuntimeError``
    is raised. Returns the number of restarts performed. ``stop_after`` (seconds,
    injectable) lets a caller run a bounded smoke instead of forever.
    """
    start = detector._clock()
    while True:
        health = probe.health()
        metrics = probe.metrics()
        action, detail = detector.observe(
            health, metrics["generation_tokens"], metrics["active"])
        if action in ("restart", "unhealthy"):
            report("watchdog: %s (%s)" % (action, detail))
            detector.restarts += 1
            if detector.restarts > max_restarts:
                raise RuntimeError("watchdog exceeded %d consecutive restarts" % max_restarts)
            restart()
        if stop_after is not None and detector._clock() - start >= stop_after:
            return detector.restarts
        time.sleep(poll_interval)
