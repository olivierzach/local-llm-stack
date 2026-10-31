"""Watchdog decision logic and metrics parsing, no SSH or GPU dependencies."""
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster import watchdog


class Clock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def tick(self, amount):
        self.t += amount


def detector(stall=10, health=5, clock=None):
    return watchdog.StallDetector(stall_seconds=stall, health_grace=health, now=clock or Clock())


def test_native_labeled_metrics_sum_all_engines_and_accept_aliases():
    text = '''# HELP vllm:generation_tokens_total Number of generation tokens.
# TYPE vllm:generation_tokens_total counter
vllm:generation_tokens_total{engine="0",model_name="local-deepseek-v4-flash-mia"} 1.2e2
vllm:generation_tokens_total{engine="1",model_name="model } with spaces"} 3 1793318400000
vllm:num_requests_running{engine="0"} 1
vllm:num_requests_running{engine="1"} 0
vllm:num_requests_waiting{engine="0"} 0
vllm:num_requests_waiting{engine="1"} 0
vllm:spec_decode_num_draft_tokens_total{engine="0",model_name="local-deepseek-v4-flash-mia"} 64
vllm:spec_decode_num_draft_tokens_total{engine="1"} 32
'''
    assert watchdog.parse_metrics(text) == {"generation_tokens": 123, "active": True}
    assert watchdog.parse_metrics(text.replace("vllm:", "vllm_")) == {
        "generation_tokens": 123, "active": True}
    name = "vllm:spec_decode_num_draft_tokens_total"
    assert watchdog.metric_totals(text, (name,)) == {name: 96}
    assert watchdog.metric_totals(text.replace("vllm:", "vllm_"), (name,)) == {name: 96}


def test_invalid_metric_samples_never_produce_partial_totals():
    text = '''vllm:generation_tokens_total{engine="0"} 10
vllm:generation_tokens_total{engine="1"} NaN
vllm:generation_tokens_total{engine="2"} 20
vllm:num_requests_running{engine="0"} +Inf
vllm:num_requests_waiting{engine="0"} -1
vllm:spec_decode_num_draft_tokens_total{engine="0"} broken
vllm:overflow{engine="0"} 1e308
vllm:overflow{engine="1"} 1e308
'''
    names = ("vllm:generation_tokens_total", "vllm:num_requests_running",
             "vllm:num_requests_waiting", "vllm:spec_decode_num_draft_tokens_total",
             "vllm:overflow", "vllm:missing")
    assert watchdog.metric_totals(text, names) == dict.fromkeys(names)
    assert watchdog.parse_metrics(text) == {"generation_tokens": None, "active": None}


def test_load_is_idle_only_when_both_gauges_are_known_zero():
    assert watchdog.parse_metrics("vllm:num_requests_running 0\n") == {
        "generation_tokens": None, "active": None}
    assert watchdog.parse_metrics(
        "vllm_num_requests_running 0\nvllm_num_requests_waiting 0\n") == {
        "generation_tokens": None, "active": False}
    assert watchdog.parse_metrics("vllm:generation_tokens_total 7\n") == {
        "generation_tokens": 7, "active": None}


def test_initial_prefill_never_arms_generation_stall_timer():
    clock = Clock()
    d = detector(clock=clock)
    assert d.observe(True, 0, True)[0] == "watch"
    clock.tick(1000)
    assert d.observe(True, 0, True)[0] == "watch"
    # A nonzero initial cumulative count also says nothing about current progress.
    d = detector(clock=clock)
    assert d.observe(True, 500, True)[0] == "watch"
    clock.tick(1000)
    assert d.observe(True, 500, True)[0] == "watch"


def test_genuine_decode_freeze_restarts_at_stall_boundary():
    clock = Clock()
    d = detector(clock=clock)
    d.observe(True, 0, True)
    clock.tick(1)
    assert d.observe(True, 1, True)[0] == "watch"
    clock.tick(9)
    assert d.observe(True, 1, True)[0] == "watch"
    clock.tick(1)
    assert d.observe(True, 1, True)[0] == "restart"


def test_progress_refreshes_the_generation_stall_timer():
    clock = Clock()
    d = detector(clock=clock)
    d.observe(True, 10, True)
    for count in range(11, 20):
        clock.tick(9)
        assert d.observe(True, count, True)[0] == "watch"
    clock.tick(9)
    assert d.observe(True, 19, True)[0] == "watch"
    clock.tick(1)
    assert d.observe(True, 19, True)[0] == "restart"


def test_idle_to_active_requires_new_observed_decode_progress():
    clock = Clock()
    d = detector(clock=clock)
    d.observe(True, 10, True)
    clock.tick(1)
    d.observe(True, 11, True)
    clock.tick(100)
    assert d.observe(True, 11, False)[0] == "watch"
    clock.tick(100)
    # Tokens produced between idle and active samples cannot arm this new work.
    assert d.observe(True, 20, True)[0] == "watch"
    clock.tick(100)
    assert d.observe(True, 20, True)[0] == "watch"
    d.observe(True, 21, True)
    clock.tick(10)
    assert d.observe(True, 21, True)[0] == "restart"


@pytest.mark.parametrize("generation,active", [(11, None), (None, True)])
def test_missing_telemetry_disarms_until_new_active_progress(generation, active):
    clock = Clock()
    d = detector(clock=clock)
    d.observe(True, 10, True)
    clock.tick(1)
    d.observe(True, 11, True)
    clock.tick(100)
    assert d.observe(True, generation, active)[0] == "watch"
    assert d.observe(True, 11, True)[0] == "watch"
    clock.tick(100)
    assert d.observe(True, 11, True)[0] == "watch"
    d.observe(True, 12, True)
    clock.tick(10)
    assert d.observe(True, 12, True)[0] == "restart"


def test_counter_reset_rebaselines_without_restarting_new_engine():
    clock = Clock()
    d = detector(clock=clock)
    d.observe(True, 100, True)
    clock.tick(1)
    d.observe(True, 101, True)
    clock.tick(100)
    assert d.observe(True, 0, True)[0] == "watch"
    clock.tick(100)
    assert d.observe(True, 0, True)[0] == "watch"
    d.observe(True, 1, True)
    clock.tick(10)
    assert d.observe(True, 1, True)[0] == "restart"


def test_liveness_failure_has_its_own_grace_and_clears_on_health():
    clock = Clock()
    d = detector(clock=clock)
    d.observe(True, None, None)
    clock.tick(100)
    assert d.observe(False, None, None)[0] == "watch"
    clock.tick(4)
    assert d.observe(False, None, None)[0] == "watch"
    d.observe(True, None, None)
    clock.tick(1)
    assert d.observe(False, None, None)[0] == "watch"
    clock.tick(5)
    assert d.observe(False, None, None)[0] == "unhealthy"


class TimelineProbe:
    def __init__(self, clock, samples):
        self.clock = clock
        self.samples = samples

    def sample(self):
        return next(sample for sample in reversed(self.samples) if sample[0] <= self.clock())

    def health(self):
        return self.sample()[1]

    def metrics(self):
        _, _, generation, active = self.sample()
        return {"generation_tokens": generation, "active": active}


def test_recovery_waits_beyond_health_grace_and_rebaselines(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(watchdog.time, "sleep", clock.tick)
    probe = TimelineProbe(clock, [
        (0, True, 100, True), (1, True, 101, True),
        (5, False, None, None), (11, True, 0, True),
        (12, True, 1, True), (14, True, 2, True), (16, True, 3, True),
    ])
    restarts = []
    total = watchdog.run_watchdog(
        detector(stall=3, health=2, clock=clock), probe,
        lambda: restarts.append(clock()), poll_interval=1,
        recovery_timeout=10, max_restarts=1, stop_after=16, report=lambda _: None)
    assert restarts == [4]
    assert total == 1


def test_unsuccessful_recovery_exhausts_consecutive_attempts(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(watchdog.time, "sleep", clock.tick)
    probe = TimelineProbe(clock, [(0, False, None, None)])
    restarts = []
    with pytest.raises(RuntimeError):
        watchdog.run_watchdog(
            detector(health=2, clock=clock), probe, lambda: restarts.append(clock()),
            poll_interval=1, recovery_timeout=8, max_restarts=2,
            stop_after=30, report=lambda _: None)
    assert restarts == [2, 10]
    assert clock() == 18


def test_restored_health_without_generation_does_not_reset_attempt_budget(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(watchdog.time, "sleep", clock.tick)
    probe = TimelineProbe(clock, [
        (0, False, None, None), (3, True, 50, True), (4, False, None, None),
    ])
    restarts = []
    with pytest.raises(RuntimeError):
        watchdog.run_watchdog(
            detector(health=2, clock=clock), probe, lambda: restarts.append(clock()),
            poll_interval=1, recovery_timeout=8, max_restarts=1,
            stop_after=20, report=lambda _: None)
    assert restarts == [2]
    assert clock() == 6


def test_resumed_generation_allows_later_independent_recovery(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(watchdog.time, "sleep", clock.tick)
    probe = TimelineProbe(clock, [
        (0, True, 100, True), (1, True, 101, True),
        (5, False, None, None), (7, True, 0, True), (8, True, 1, True),
        (12, False, None, None), (14, True, 0, True), (15, True, 1, True),
    ])
    restarts = []
    total = watchdog.run_watchdog(
        detector(stall=3, health=2, clock=clock), probe,
        lambda: restarts.append(clock()), poll_interval=1,
        recovery_timeout=8, max_restarts=1, stop_after=15, report=lambda _: None)
    assert restarts == [4, 11]
    assert total == 2


def test_failed_restart_stops_supervision_instead_of_disarming_a_live_stall(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(watchdog.time, "sleep", clock.tick)
    probe = TimelineProbe(clock, [(0, True, 100, True), (1, True, 101, True)])
    refused = RuntimeError("restart ownership check refused")
    attempts = []

    def restart():
        attempts.append(clock())
        raise refused

    with pytest.raises(RuntimeError) as error:
        watchdog.run_watchdog(
            detector(stall=3, clock=clock), probe, restart, poll_interval=1,
            stop_after=30, report=lambda _: None)
    assert error.value is refused
    assert attempts == [4]


@pytest.mark.parametrize("options", [
    {"poll_interval": 0}, {"recovery_timeout": -1},
    {"poll_interval": float("nan")}, {"recovery_timeout": float("inf")},
    {"max_restarts": 0}, {"max_restarts": 1.5},
])
def test_invalid_polling_and_recovery_limits_reject_before_probing(options):
    class NoProbe:
        def health(self):
            pytest.fail("invalid supervision settings must not contact the service")

    with pytest.raises(ValueError):
        watchdog.run_watchdog(detector(), NoProbe(), lambda: None, **options)
