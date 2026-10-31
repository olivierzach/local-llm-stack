"""Warmup completion and speculation verification without real endpoints."""
import io
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster import warmup


class Clock:
    def __init__(self):
        self.now = 0.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds


class Endpoint:
    def __init__(self, clock, metrics=(), completion=None, decode_seconds=0):
        self.clock = clock
        self.metrics = iter(metrics)
        self.last_metric = ""
        self.completion = completion
        self.decode_seconds = decode_seconds
        self.inferences = 0
        self.calls = []

    def open(self, request, timeout):
        self.calls.append((request.full_url, timeout))
        if request.full_url.endswith("/metrics"):
            self.last_metric = next(self.metrics, self.last_metric)
            return io.BytesIO(self.last_metric.encode())
        self.inferences += 1
        self.clock.sleep(self.decode_seconds)
        payload = json.loads(request.data)
        # Model early EOS unless the request actually requires continued decode.
        tokens = payload["max_tokens"] if payload.get("ignore_eos") else 1
        tokens = min(tokens, payload.get("min_tokens", 1))
        result = self.completion if self.completion is not None else completion(tokens)
        return io.BytesIO(json.dumps(result).encode())


def completion(tokens):
    return {"model": "m", "choices": [{"message": {"content": "1 2 3"},
            "finish_reason": "length"}], "usage": {"completion_tokens": tokens}}


def metric(tokens):
    return f'vllm:spec_decode_num_draft_tokens_total{{engine="0",model_name="m"}} {tokens}\n'


def endpoint(monkeypatch, **kwargs):
    clock = Clock()
    server = Endpoint(clock, **kwargs)
    monkeypatch.setattr(warmup.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(warmup.time, "sleep", clock.sleep)
    monkeypatch.setattr(warmup.urllib.request, "build_opener", lambda *args: server)
    return clock, server


def payload():
    return warmup.warmup_speculative_payload("m", {"num_speculative_tokens": 2})


def test_non_speculative_decode_requires_and_observes_minimum(monkeypatch):
    _, server = endpoint(monkeypatch)
    report = warmup.run_warmup("http://test/v1", payload())
    assert report["ready"]
    assert report["usage"]["completion_tokens"] >= 16
    assert server.inferences == 1


@pytest.mark.parametrize("result", [
    completion(1),
    {"choices": [{"message": {"content": ""}, "finish_reason": "length"}],
     "usage": {"completion_tokens": 16}},
    {"choices": [{"message": {"content": "text"}, "finish_reason": None}],
     "usage": {"completion_tokens": 16}},
    {"choices": [{"message": {"content": "text"}, "finish_reason": "content_filter"}],
     "usage": {"completion_tokens": 16}},
    {"choices": [{"message": {"content": "text"}, "finish_reason": "length"}]},
    {"error": {"message": "engine failed"}},
    [],
])
def test_invalid_or_insufficient_completion_is_rejected(monkeypatch, result):
    _, server = endpoint(monkeypatch, completion=result)
    with pytest.raises(RuntimeError):
        warmup.run_warmup("http://test/v1", payload())
    assert server.inferences == 1


def test_speculative_decode_waits_for_published_draft_progress(monkeypatch):
    clock, server = endpoint(monkeypatch, metrics=[metric(10), metric(10), "", metric(14)],
                             decode_seconds=1)
    report = warmup.run_warmup("http://test/v1", payload(), timeout=2,
                               require_speculation=True)
    assert report["ready"]
    assert 1 < clock.now < 2
    assert server.inferences == 1


def test_missing_baseline_prevents_unverifiable_inference(monkeypatch):
    _, server = endpoint(monkeypatch, metrics=["unrelated_metric 1\n"])
    with pytest.raises(RuntimeError):
        warmup.run_warmup("http://test/v1", payload(), require_speculation=True)
    assert server.inferences == 0


@pytest.mark.parametrize("after", [metric(10), ""])
def test_no_observed_draft_progress_expires_shared_deadline(monkeypatch, after):
    clock, server = endpoint(monkeypatch, metrics=[metric(10), after], decode_seconds=0.75)
    with pytest.raises(TimeoutError):
        warmup.run_warmup("http://test/v1", payload(), timeout=1,
                          require_speculation=True)
    assert clock.now == 1
    assert server.inferences == 1


def test_counter_reset_does_not_count_as_progress(monkeypatch):
    _, server = endpoint(monkeypatch, metrics=[metric(10), metric(2), metric(12)])
    with pytest.raises(RuntimeError):
        warmup.run_warmup("http://test/v1", payload(), require_speculation=True)
    assert server.inferences == 1


def test_late_completion_cannot_report_ready(monkeypatch):
    _, server = endpoint(monkeypatch, decode_seconds=2)
    with pytest.raises(TimeoutError):
        warmup.run_warmup("http://test/v1", payload(), timeout=1)
    assert server.inferences == 1


def test_expired_deadline_makes_no_request(monkeypatch):
    _, server = endpoint(monkeypatch)
    with pytest.raises(TimeoutError):
        warmup.run_warmup("http://test/v1", payload(), timeout=0)
    assert server.calls == []
