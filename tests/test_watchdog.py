"""Watchdog decision logic and metrics parsing, no SSH or GPU dependencies."""
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster import watchdog


class Clock:
    def __init__(self, start=0.0, step=0.0):
        self.t = float(start)
        self.step = float(step)

    def __call__(self):
        return self.t

    def tick(self, amount=None):
        self.t += self.step if amount is None else amount


def detector(stall=10, health=5, clock=None):
    return watchdog.StallDetector(stall_seconds=stall, health_grace=health, now=clock or Clock())


def test_progressing_engine_never_restarts():
    clock = Clock()
    d = detector(clock=clock)
    assert d.observe(True, 0, True) == ("watch", "progress")
    clock.tick(2)
    assert d.observe(True, 10, True) == ("watch", "progress")
    clock.tick(2)
    assert d.observe(True, 20, True) == ("watch", "progress")
    assert d.restarts == 0


def test_engine_frozen_with_work_in_flight_restarts():
    clock = Clock()
    d = detector(stall=10, clock=clock)
    assert d.observe(True, 0, True) == ("watch", "progress")  # last_advance = 0
    clock.tick(5)
    assert d.observe(True, 0, True) == ("watch", "idle")  # only 5s frozen
    clock.tick(10)
    action, detail = d.observe(True, 0, True)
    assert action == "restart"
    assert "frozen" in detail
    assert d.restarts == 0  # observe reports; runner counts restarts


def test_idle_engine_without_progress_is_not_restarted():
    clock = Clock()
    d = detector(stall=10, clock=clock)
    assert d.observe(True, None, False) == ("watch", "idle")
    clock.tick(100)
    assert d.observe(True, None, False) == ("watch", "idle")
    assert d.observe(True, 5, False) == ("watch", "progress")
    # frozen but idle (active=False) is not a stall
    clock.tick(50)
    assert d.observe(True, 5, False) == ("watch", "idle")


def test_active_unknown_after_progress_still_restarts():
    # When the metrics source exposes no load gauge, an engine that previously
    # advanced and then froze for the stall window is treated as stalled.
    clock = Clock()
    d = detector(stall=10, clock=clock)
    assert d.observe(True, 0, True) == ("watch", "progress")
    clock.tick(12)
    action, _ = d.observe(True, 0, None)
    assert action == "restart"


def test_liveness_failure_triggers_unhealthy_after_grace():
    clock = Clock()
    d = detector(health=5, clock=clock)
    assert d.observe(True, 0, True) == ("watch", "progress")
    clock.tick(3)
    assert d.observe(False, None, None) == ("watch", "recovering")
    clock.tick(5)
    action, _ = d.observe(False, None, None)
    assert action == "unhealthy"


def test_metrics_parse_generation_and_load():
    text = (
        "# HELP vllm_generation_tokens_total Number of generation tokens.\n"
        "# TYPE vllm_generation_tokens_total counter\n"
        "vllm_generation_tokens_total 123\n"
        "vllm_num_requests_running 0\n"
        "vllm_num_requests_waiting 2\n"
    )
    assert watchdog.parse_metrics(text) == {"generation_tokens": 123, "active": True}


def test_metrics_parse_idle_when_no_queue():
    text = "vllm_num_requests_running 0\nvllm_num_requests_waiting 0\n"
    assert watchdog.parse_metrics(text) == {"generation_tokens": None, "active": False}


def test_metrics_active_unknown_without_load_gauges():
    text = "vllm_generation_tokens_total 7\n"
    assert watchdog.parse_metrics(text) == {"generation_tokens": 7, "active": None}


def test_run_watchdog_restarts_then_escalates():
    clock = Clock(step=5.0)

    class StuckProbe:
        def health(self):
            return True

        def metrics(self):
            clock.tick()
            return {"generation_tokens": 10, "active": True}

    d = watchdog.StallDetector(stall_seconds=10, health_grace=5, now=clock)
    restarts = []
    assert d.observe(True, 10, True)[0] == "watch"
    with pytest.raises(RuntimeError, match="exceeded 3"):
        watchdog.run_watchdog(d, StuckProbe(), lambda: restarts.append(1),
                              poll_interval=0.001, max_restarts=3,
                              report=lambda *a: None, stop_after=100_000)
    assert len(restarts) == 3  # callback ran max_restarts times; 4th escalates


def test_run_watchdog_bounded_stop_after():
    clock = Clock(step=2.0)

    class Healthy:
        def health(self):
            return True

        def metrics(self):
            clock.tick()
            return {"generation_tokens": None, "active": False}

    d = watchdog.StallDetector(stall_seconds=100, health_grace=100, now=clock)
    restarts = watchdog.run_watchdog(d, Healthy(), lambda: None,
                                     poll_interval=0.001, report=lambda *a: None,
                                     stop_after=3)
    assert restarts == 0
