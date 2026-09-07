import importlib.util
import json
from pathlib import Path
import subprocess
import os

import pytest

ROOT = Path(__file__).resolve().parents[1]


def verifier():
    spec = importlib.util.spec_from_file_location("qwen_verify", ROOT / "scripts/qwen38-verify.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Response:
    headers = {}

    def __init__(self, events):
        self.events = events

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def raise_for_status(self):
        pass

    def iter_lines(self, **kwargs):
        for event in self.events:
            yield b"data: " + (event.encode() if isinstance(event, str) else json.dumps(event).encode())


def fragment(arguments, *, first=False, finish=None):
    call = {"index": 0, "function": {"arguments": arguments}}
    if first:
        call.update(id="test-call", type="function")
        call["function"]["name"] = "get_test_value"
    return {"choices": [{"delta": {"tool_calls": [call]}, "finish_reason": finish}]}


def test_complete_stream_assembles_tool_arguments(monkeypatch):
    module = verifier()
    events = [fragment('{"key":', first=True), fragment('"alpha"}'),
              {"choices": [{"delta": {}, "finish_reason": "tool_calls"}]}, "[DONE]"]
    monkeypatch.setattr(module.requests, "post", lambda *a, **kw: Response(events))
    record, message = module.chat("http://unused/v1", {"stream": True}, "")
    assert record["finish_reason"] == "tool_calls"
    call = message["tool_calls"][0]
    assert call["id"] == "test-call"
    assert json.loads(call["function"]["arguments"]) == {"key": "alpha"}


@pytest.mark.parametrize("events", [
    [fragment('{"key":', first=True)],
    [fragment('{"key":', first=True), "[DONE]"],
    [fragment('{"key":', first=True), {"choices": [{"delta": {}, "finish_reason": "tool_calls"}]}, "[DONE]"],
    [{"error": {"message": "backend restart"}}],
])
def test_partial_or_malformed_tool_stream_is_rejected(monkeypatch, events):
    module = verifier()
    monkeypatch.setattr(module.requests, "post", lambda *a, **kw: Response(events))
    with pytest.raises((RuntimeError, ValueError)):
        module.chat("http://unused/v1", {"stream": True}, "")


@pytest.mark.parametrize("name,value,expected", [
    ("QWEN38_MAX_MODEL_LEN", "1048576", "native rope"),
    ("QWEN38_HOST_RESERVE_GIB", "8", "at least 26"),
    ("QWEN38_PORT", "99999", "invalid QWEN38_PORT"),
])
def test_invalid_config_fails_before_starting_any_service(name, value, expected):
    env = {**os.environ, "QWEN38_BIND_HOST": "127.0.0.1", name: value}
    result = subprocess.run(["bash", str(ROOT / "scripts/qwen38-flash-next.sh"), "probe"],
                            env=env, capture_output=True, text=True, timeout=10)
    assert result.returncode != 0
    assert expected in result.stderr


def test_full_verification_includes_soak_after_benchmark():
    makefile = (ROOT / "Makefile").read_text()
    recipe = makefile.split("qwen38-verify:\n", 1)[1].split("\n\n", 1)[0]
    assert "$(MAKE) qwen38-test" in recipe
    assert "$(MAKE) qwen38-recovery" in recipe
    assert "$(MAKE) qwen38-bench" in recipe
    assert "$(MAKE) qwen38-soak" in recipe
    assert recipe.index("qwen38-bench") < recipe.index("qwen38-soak")
    assert "qwen38-soak:\n" in makefile
