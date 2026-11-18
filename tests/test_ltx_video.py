"""CPU-only gateway regressions; run in the existing LTX dependency environment.

No real Torch/Diffusers import, CUDA access, model loading, or network is needed.
The instrumented pipelines test orchestration, not model generation quality.
"""
import builtins
from concurrent.futures import ThreadPoolExecutor
import importlib.util
from pathlib import Path
import sys
import threading
from types import SimpleNamespace

from PIL import Image
import pytest
from pydantic import ValidationError


ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def gateway(monkeypatch):
    monkeypatch.delenv("LTX_MODEL", raising=False)
    monkeypatch.delenv("LTX_MODEL_REVISION", raising=False)
    original_import = builtins.__import__

    def without_models(name, *args, **kwargs):
        if name.split(".")[0] in {"torch", "diffusers", "huggingface_hub"}:
            raise AssertionError(f"Model dependency imported during startup: {name}")
        return original_import(name, *args, **kwargs)

    spec = importlib.util.spec_from_file_location("ltx_gateway", ROOT / "ltx-video/serve.py")
    module = importlib.util.module_from_spec(spec)
    with monkeypatch.context() as startup:
        startup.setattr(builtins, "__import__", without_models)
        spec.loader.exec_module(module)
        assert "/video" in module.app.openapi()["paths"]
        assert module._pipe is None

    class Generator:
        def __init__(self, device):
            assert device == "cuda"

        def manual_seed(self, seed):
            self.seed = seed
            return self

    monkeypatch.setitem(sys.modules, "torch", SimpleNamespace(
        Generator=Generator, bfloat16=object()))
    return module


def frames():
    return SimpleNamespace(frames=[[
        Image.new("RGB", (32, 32), "red"),
        Image.new("RGB", (32, 32), "blue"),
    ]])



@pytest.mark.parametrize("failure", ["cache", "cuda"])
def test_failed_load_does_not_publish_pipeline(gateway, monkeypatch, failure):
    def cuda_move(device):
        raise RuntimeError("cuda unavailable")

    def from_pretrained(model, **kwargs):
        assert kwargs["local_files_only"] is True
        if failure == "cache":
            raise OSError("pinned snapshot unavailable")
        return SimpleNamespace(to=cuda_move)

    monkeypatch.setitem(sys.modules, "diffusers", SimpleNamespace(
        LTXConditionPipeline=SimpleNamespace(from_pretrained=from_pretrained)))
    with pytest.raises((OSError, RuntimeError)):
        gateway._load()
    assert gateway._pipe is None


def test_pipeline_calls_are_serialized(gateway, monkeypatch):
    first_entered = threading.Event()
    second_blocked = threading.Event()
    release_first = threading.Event()
    state_lock = threading.Lock()
    active = 0
    peak = 0
    calls = 0
    inference_lock = gateway._infer_lock

    class ObservedLock:
        """Observe contention on the real production lock without timing sleeps."""
        def __enter__(self):
            if first_entered.is_set():
                # The first pipeline is held until the main thread sees this
                # failed acquisition. This proves actual lock contention.
                acquired = inference_lock.acquire(blocking=False)
                if acquired:
                    inference_lock.release()
                    raise AssertionError("second request acquired an unheld inference lock")
                second_blocked.set()
            inference_lock.acquire()
            return self

        def __exit__(self, *args):
            inference_lock.release()

    def pipeline(**kwargs):
        nonlocal active, peak, calls
        with state_lock:
            calls += 1
            number = calls
            active += 1
            peak = max(peak, active)
        try:
            if number == 1:
                first_entered.set()
                assert release_first.wait(10), "first request was never released"
            return frames()
        finally:
            with state_lock:
                active -= 1

    monkeypatch.setattr(gateway, "_infer_lock", ObservedLock())
    monkeypatch.setattr(gateway, "_pipe", pipeline)
    req = gateway.Req(prompt="CPU concurrency regression", num_frames=9, height=32, width=32)
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="ltx") as pool:
        first = pool.submit(gateway.generate, req)
        try:
            assert first_entered.wait(5), "first request did not enter the pipeline"
            second = pool.submit(gateway.generate, req)
            assert second_blocked.wait(5), "second request bypassed the inference lock"
            with state_lock:
                assert calls == 1
                assert active == 1
        finally:
            release_first.set()
        for future in (first, second):
            assert future.result(timeout=5).media_type == "image/gif"
    assert calls == 2
    assert peak == 1



def test_generation_failure_releases_inference_lock(gateway, monkeypatch):
    def failed(**kwargs):
        raise RuntimeError("instrumented failure")

    monkeypatch.setattr(gateway, "_pipe", failed)
    with pytest.raises(gateway.HTTPException) as error:
        gateway.generate(gateway.Req(prompt="test"))
    assert error.value.status_code == 500
    assert gateway._infer_lock.acquire(blocking=False)
    gateway._infer_lock.release()
    monkeypatch.setattr(gateway, "_pipe", lambda **kwargs: frames())
    assert gateway.generate(gateway.Req(prompt="test")).media_type == "image/gif"


@pytest.mark.parametrize("field,value", [
    ("height", 0), ("height", -32), ("width", 0), ("width", -32),
    ("seed", -9223372036854775809), ("seed", 18446744073709551616),
])
def test_unsafe_scalar_inputs_are_rejected(gateway, field, value):
    with pytest.raises(ValidationError):
        gateway.Req(prompt="test", **{field: value})
    assert gateway._pipe is None


@pytest.mark.parametrize("seed", [-9223372036854775808, 18446744073709551615])
def test_seed_boundaries_remain_accepted(gateway, seed):
    assert gateway.Req(prompt="test", seed=seed).seed == seed


@pytest.mark.parametrize("payload", [
    {"height": 33}, {"width": 33}, {"start_image": "not an image"},
])
def test_invalid_request_fails_before_loading(gateway, monkeypatch, payload):
    monkeypatch.setattr(gateway, "_load", lambda: pytest.fail("invalid input loaded model"))
    with pytest.raises(gateway.HTTPException) as error:
        gateway.generate(gateway.Req(prompt="test", **payload))
    assert error.value.status_code == 400
