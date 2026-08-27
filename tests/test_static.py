from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import py_compile
import re
import shutil
import subprocess

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]


def run(command: list[str], *, cwd: Path = ROOT, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True, check=False)


def test_shell_scripts_parse() -> None:
    scripts = sorted(str(path) for path in (ROOT / "scripts").glob("*.sh"))
    result = run(["bash", "-n", *scripts, str(ROOT / "tests" / "test.sh")])
    assert result.returncode == 0, result.stderr


def test_python_scripts_parse() -> None:
    paths = list((ROOT / "scripts").glob("*.py")) + list((ROOT / "training").glob("*.py"))
    assert paths
    for path in paths:
        py_compile.compile(str(path), doraise=True)


def test_git_diff_has_no_whitespace_errors() -> None:
    result = run(["git", "diff", "--check"])
    assert result.returncode == 0, result.stdout + result.stderr


def test_requirements_files_cover_tools_and_training() -> None:
    root_requirements = (ROOT / "requirements.txt").read_text()
    tool_requirements = (ROOT / "tools/requirements.txt").read_text()
    training_requirements = (ROOT / "training/requirements.txt").read_text()
    dockerfile = (ROOT / "training/Dockerfile").read_text()

    assert "-r tools/requirements.txt" in root_requirements
    assert "-r training/requirements.txt" in root_requirements
    assert "pytest" in tool_requirements
    for package in ["torch", "transformers", "accelerate", "peft", "trl", "datasets"]:
        assert package in training_requirements
    assert "training-requirements.txt" in dockerfile


def test_advertised_docs_pages_have_expected_titles() -> None:
    assert "# vLLM Deep Dive" in (ROOT / "docs/vllm-deep-dive.md").read_text()
    assert "# Debugging Checklist" in (ROOT / "docs/debugging-checklist.md").read_text()
    assert "# Troubleshooting Notes" not in (ROOT / "docs/vllm-deep-dive.md").read_text()
    assert "# Troubleshooting Notes" not in (ROOT / "docs/debugging-checklist.md").read_text()


def test_docs_index_markdown_links_exist() -> None:
    text = (ROOT / "docs/README.md").read_text()
    links = re.findall(r"\[[^]]+\]\(([^)]+\.md)\)", text)
    assert links
    missing = [target for target in links if not (ROOT / "docs" / target).is_file()]
    assert not missing


def test_spark_setup_runbook_is_complete() -> None:
    runbook = (ROOT / "docs/spark-setup-runbook.md").read_text()
    for required in [
        "make init",
        "python3 -m venv .venv",
        "python -m pip install -r tools/requirements.txt",
        "make check",
        "make gpu-check",
        "docker compose --profile training build training",
        "make download-model MODEL=Qwen/Qwen3-4B-Instruct-2507",
        "make download-model MODEL=Qwen/Qwen3-14B",
        "make download-vision",
        "make up",
        "make smoke",
        "make throughput-eval",
        "make vision-up",
        "make vision-eval",
        "make lora-train",
        "make lora-serve",
        "make lora-eval",
        "models/adapters/qwen3-4b-smoke-lora/current",
    ]:
        assert required in runbook


def test_vllm_avoids_unsupported_thinking_flag() -> None:
    assert "--default-chat-template-kwargs" not in (ROOT / "docker-compose.yml").read_text()


def test_optional_model_services_and_aliases_are_configured() -> None:
    compose = (ROOT / "docker-compose.yml").read_text()
    litellm = yaml.safe_load((ROOT / "config/litellm.yaml").read_text())
    aliases = {entry["model_name"] for entry in litellm["model_list"]}

    for service in ["vllm-qwen30a3b", "vllm-deepseek32b", "vllm-mistral24b", "vllm-gptoss120b", "model-cache"]:
        assert f"  {service}:" in compose

    for alias in ["local-qwen30-a3b", "local-deepseek-r1-qwen32b", "local-mistral-small", "local-gpt-oss-120b", "local-deepseek-v4-flash"]:
        assert alias in aliases


def test_deepseek_install_applies_native_tokenizer_patch() -> None:
    install_script = (ROOT / "scripts/deepseek-v4.sh").read_text()
    engine_patch = (ROOT / "patches/ds4-tokenize-endpoint.patch").read_text()

    assert "patches/ds4-tokenize-endpoint.patch" in install_script
    assert 'git -C "$ENGINE_DIR" apply "$ENGINE_PATCH"' in install_script
    assert 'strcmp(hr.path, "/tokenize")' in engine_patch
    assert 'strcmp(hr.path, "/v1/tokenize")' in engine_patch
    assert "parse_chat_request" in engine_patch


def load_context_guard_module():
    import importlib.util
    import sys

    module_path = ROOT / "scripts/context-guard-proxy.py"
    spec = importlib.util.spec_from_file_location("context_guard_proxy", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_context_guard_caps_output_to_remaining_budget() -> None:
    from types import SimpleNamespace

    module = load_context_guard_module()
    config = module.ProxyConfig(
        upstream_base_url="http://localhost:4000/v1",
        timeout_s=1,
        allow_no_auth=True,
        default_context_tokens=100,
        model_contexts={"tiny": 100},
        fallback_model_contexts={},
        headroom_tokens=8,
        default_output_tokens=32,
        min_output_tokens=16,
        keep_last_messages=2,
        summary_tokens=32,
        compact_source_chars=1000,
        chars_per_token=1.0,
        compact_model=None,
        discover_model_context=False,
        verbose=False,
    )
    handler = object.__new__(module.ContextGuardHandler)
    handler.server = SimpleNamespace(config=config)
    payload = {
        "model": "tiny",
        "messages": [{"role": "user", "content": "x" * 76}],
        "max_tokens": -57,
    }

    sanitized = handler.sanitize_max_tokens(payload)
    estimate = module.estimate_payload_tokens(sanitized, config.chars_per_token)
    budget = max(1, config.context_limit_for("tiny") - estimate - config.headroom_tokens)

    assert sanitized["max_tokens"] <= budget
    assert sanitized["max_tokens"] >= 1


@pytest.mark.parametrize(
    "body",
    [
        b"Prompt has 32889 tokens, but the configured context size is 32768 tokens",
        json.dumps(
            {
                "error": {
                    "message": (
                        "litellm.BadRequestError: Prompt has 32889 tokens, but the "
                        "configured context size is 32768 tokens"
                    )
                }
            }
        ).encode(),
        b"{\"error\":{\"message\":\"request too large\",\"code\":\"context_length_exceeded\"}}",
        b"Prompt has 32,889 tokens, but the configured context size is 32,768 tokens",
    ],
)
def test_context_guard_recognizes_context_overflow_errors(body: bytes) -> None:
    module = load_context_guard_module()

    assert module.is_context_error(400, body)


def test_context_guard_does_not_retry_unrelated_bad_requests() -> None:
    module = load_context_guard_module()
    body = b"{\"error\":{\"message\":\"temperature must be between 0 and 2\",\"code\":\"bad_request\"}}"

    assert not module.is_context_error(400, body)


def test_context_guard_compacts_near_deepseek_boundary() -> None:
    from types import SimpleNamespace

    module = load_context_guard_module()
    config = module.ProxyConfig(
        upstream_base_url="http://localhost:4000/v1",
        timeout_s=1,
        allow_no_auth=True,
        default_context_tokens=32768,
        model_contexts={"local-deepseek-v4-flash": 32768},
        fallback_model_contexts={},
        headroom_tokens=2048,
        default_output_tokens=1024,
        min_output_tokens=64,
        keep_last_messages=1,
        summary_tokens=32,
        compact_source_chars=1000,
        chars_per_token=3.0,
        compact_model=None,
        discover_model_context=False,
        verbose=False,
    )
    handler = object.__new__(module.ContextGuardHandler)
    handler.server = SimpleNamespace(config=config)
    payload = {
        "model": "local-deepseek-v4-flash",
        "messages": [
            {"role": "assistant", "content": "x" * 90000},
            {"role": "user", "content": "Continue from the latest task."},
        ],
        "max_tokens": 1024,
    }

    prepared, compacted = handler.prepare_payload(payload, {})

    assert compacted
    assert prepared["messages"] == [payload["messages"][-1]]
    assert handler.payload_fits(prepared, 32768)


def test_context_guard_forces_history_reduction_after_backend_overflow() -> None:
    from types import SimpleNamespace

    module = load_context_guard_module()
    config = module.ProxyConfig(
        upstream_base_url="http://localhost:4000/v1",
        timeout_s=1,
        allow_no_auth=True,
        default_context_tokens=32768,
        model_contexts={"local-deepseek-v4-flash": 32768},
        fallback_model_contexts={},
        headroom_tokens=2048,
        default_output_tokens=1024,
        min_output_tokens=64,
        keep_last_messages=1,
        summary_tokens=32,
        compact_source_chars=1000,
        chars_per_token=100.0,
        compact_model=None,
        discover_model_context=False,
        verbose=False,
    )
    handler = object.__new__(module.ContextGuardHandler)
    handler.server = SimpleNamespace(config=config)
    payload = {
        "model": "local-deepseek-v4-flash",
        "messages": [
            {"role": "assistant", "content": " hello" * 40000},
            {"role": "user", "content": "Continue."},
        ],
        "max_tokens": 64,
    }

    assert handler.payload_fits(payload, 32768)
    prepared, compacted = handler.compact_payload(payload, {}, force=True)

    assert compacted
    assert prepared["messages"] == [payload["messages"][-1]]


def test_context_guard_prefers_native_tokenizer(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    module = load_context_guard_module()
    config = module.ProxyConfig(
        upstream_base_url="http://localhost:4000/v1",
        timeout_s=1,
        allow_no_auth=True,
        default_context_tokens=32768,
        model_contexts={"exact": 32768},
        fallback_model_contexts={},
        headroom_tokens=2048,
        default_output_tokens=1024,
        min_output_tokens=64,
        keep_last_messages=1,
        summary_tokens=32,
        compact_source_chars=1000,
        chars_per_token=3.0,
        compact_model=None,
        discover_model_context=False,
        verbose=False,
        tokenizer_base_urls={"exact": "http://tokenizer"},
    )
    handler = object.__new__(module.ContextGuardHandler)
    handler.server = SimpleNamespace(config=config)

    class TokenResponse:
        status_code = 200

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, int]:
            return {"count": 1234, "max_model_len": 65536}

    calls = []

    def fake_post(url: str, **kwargs):
        calls.append((url, kwargs))
        return TokenResponse()

    monkeypatch.setattr(module.requests, "post", fake_post)
    payload = {
        "model": "exact",
        "messages": [{"role": "user", "content": "x" * 9000}],
        "functions": [{"name": "legacy", "parameters": {"type": "object"}}],
        "function_call": "auto",
        "parallel_tool_calls": True,
        "response_format": {"type": "json_object"},
        "reasoning_effort": "low",
    }

    assert handler.estimate_input_tokens(payload) == 1234
    assert calls[0][0] == "http://tokenizer/tokenize"
    assert calls[0][1]["json"]["messages"] == payload["messages"]
    assert calls[0][1]["json"]["reasoning_effort"] == "low"
    assert calls[0][1]["json"]["functions"] == payload["functions"]
    assert calls[0][1]["json"]["function_call"] == "auto"
    assert calls[0][1]["json"]["parallel_tool_calls"] is True
    assert calls[0][1]["json"]["response_format"] == payload["response_format"]
    assert config.context_cache["exact"] == 65536
    assert handler.estimate_input_tokens(payload) == 1234
    assert len(calls) == 1


def test_context_guard_tokenizer_failure_uses_conservative_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    module = load_context_guard_module()
    config = module.ProxyConfig(
        upstream_base_url="http://localhost:4000/v1",
        timeout_s=1,
        allow_no_auth=True,
        default_context_tokens=32768,
        model_contexts={"fallback": 32768},
        fallback_model_contexts={},
        headroom_tokens=2048,
        default_output_tokens=1024,
        min_output_tokens=64,
        keep_last_messages=1,
        summary_tokens=32,
        compact_source_chars=1000,
        chars_per_token=3.0,
        compact_model=None,
        discover_model_context=False,
        verbose=False,
        tokenizer_base_urls={"fallback": "http://missing"},
    )
    handler = object.__new__(module.ContextGuardHandler)
    handler.server = SimpleNamespace(config=config)
    monkeypatch.setattr(
        module.requests,
        "post",
        lambda *args, **kwargs: (_ for _ in ()).throw(module.requests.ConnectionError()),
    )
    payload = {"model": "fallback", "messages": [{"role": "user", "content": "x" * 9000}]}

    assert handler.estimate_input_tokens(payload) == module.estimate_payload_tokens(payload, 3.0)


def make_reliable_guard_handler(
    module,
    *,
    context_tokens: int = 300,
    chars_per_token: float = 1.0,
    keep_last_messages: int = 8,
):
    from types import SimpleNamespace

    config = module.ProxyConfig(
        upstream_base_url="http://localhost:4000/v1",
        timeout_s=1,
        allow_no_auth=True,
        default_context_tokens=context_tokens,
        model_contexts={"test": context_tokens},
        fallback_model_contexts={},
        headroom_tokens=16,
        default_output_tokens=32,
        min_output_tokens=16,
        keep_last_messages=keep_last_messages,
        summary_tokens=32,
        compact_source_chars=1000,
        chars_per_token=chars_per_token,
        compact_model=None,
        discover_model_context=False,
        verbose=False,
        max_compaction_retries=3,
    )
    handler = object.__new__(module.ContextGuardHandler)
    handler.server = SimpleNamespace(config=config)
    return handler


def test_context_guard_preserves_active_tool_turn() -> None:
    module = load_context_guard_module()
    handler = make_reliable_guard_handler(module, context_tokens=400)
    messages = [
        {"role": "user", "content": "old request " + "x" * 500},
        {"role": "assistant", "content": "old response"},
        {"role": "user", "content": "inspect the build"},
        {
            "role": "assistant",
            "content": "",
            "tool_calls": [
                {
                    "id": "call-1",
                    "type": "function",
                    "function": {"name": "shell", "arguments": "{\"cmd\":\"make test\"}"},
                }
            ],
        },
        {"role": "tool", "tool_call_id": "call-1", "content": "tests passed"},
    ]
    payload = {"model": "test", "messages": messages, "max_tokens": 16}
    original = copy.deepcopy(payload)

    compacted = handler.trim_payload(payload, force=True)

    assert compacted["messages"] == messages[2:]
    assert handler.payload_fits(compacted, 400)
    assert payload == original


def test_context_guard_truncates_oversized_active_message() -> None:
    module = load_context_guard_module()
    handler = make_reliable_guard_handler(module, context_tokens=240)
    payload = {
        "model": "test",
        "messages": [
            {
                "role": "user",
                "content": "BEGIN-" + "x" * 1000 + "-END",
            }
        ],
        "max_tokens": 16,
    }

    compacted = handler.trim_payload(payload)
    content = compacted["messages"][0]["content"]

    assert len(content) < len(payload["messages"][0]["content"])
    assert content.startswith("BEGIN-")
    assert content.endswith("-END")
    assert "content truncated by Context Guard" in content
    assert handler.payload_fits(compacted, 240)


def test_context_guard_compacts_tool_descriptions_before_dropping_tools() -> None:
    module = load_context_guard_module()
    handler = make_reliable_guard_handler(module, context_tokens=300)
    payload = {
        "model": "test",
        "messages": [{"role": "user", "content": "Use the build tool."}],
        "tools": [
            {
                "type": "function",
                "function": {
                    "name": "build",
                    "description": "x" * 1000,
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "target": {"type": "string"},
                            "description": {"type": "string", "description": "details"},
                        },
                    },
                },
            }
        ],
        "max_tokens": 16,
    }

    compacted = handler.trim_payload(payload)

    assert compacted["tools"][0]["function"]["name"] == "build"
    assert "description" not in compacted["tools"][0]["function"]
    properties = compacted["tools"][0]["function"]["parameters"]["properties"]
    assert "description" in properties
    assert "description" not in properties["description"]
    assert handler.payload_fits(compacted, 300)


def test_context_guard_retries_multiple_backend_overflows() -> None:
    module = load_context_guard_module()
    handler = make_reliable_guard_handler(
        module,
        context_tokens=32768,
        chars_per_token=100.0,
        keep_last_messages=20,
    )
    payload = {
        "model": "test",
        "messages": [
            {"role": "user", "content": "old one"},
            {"role": "assistant", "content": "a" * 1000},
            {"role": "user", "content": "old two"},
            {"role": "assistant", "content": "b" * 1000},
            {"role": "user", "content": "old three"},
            {"role": "assistant", "content": "c" * 1000},
            {"role": "user", "content": "current request"},
            {"role": "tool", "content": "current result"},
        ],
        "max_tokens": 16,
    }

    class FakeResponse:
        def __init__(self, status_code: int):
            self.status_code = status_code
            self.ok = status_code == 200
            self.content = (
                b'{"error":{"code":"context_length_exceeded"}}'
                if status_code >= 400
                else b'{"choices":[]}'
            )

        def close(self) -> None:
            return None

    responses = iter([FakeResponse(400), FakeResponse(400), FakeResponse(200)])
    sent_payloads = []

    def fake_post(headers, sent_payload):
        sent_payloads.append(copy.deepcopy(sent_payload))
        return next(responses)

    handler.post_upstream = fake_post
    response, compacted_payload, compacted, retry_count = handler.post_with_context_retries(
        {}, payload, compacted=False
    )

    assert response is not None and response.ok
    assert compacted
    assert len(sent_payloads) == 3
    assert retry_count == 2
    assert len(sent_payloads[1]["messages"]) < len(sent_payloads[0]["messages"])
    assert len(sent_payloads[2]["messages"]) < len(sent_payloads[1]["messages"])
    assert compacted_payload["messages"][-2:] == payload["messages"][-2:]



def test_context_guard_preserves_small_explicit_output_limit() -> None:
    module = load_context_guard_module()
    handler = make_reliable_guard_handler(module, context_tokens=300)
    payload = {
        "model": "test",
        "messages": [{"role": "user", "content": "short"}],
        "max_completion_tokens": 24,
    }

    sanitized = handler.sanitize_max_tokens(payload)

    assert sanitized["max_completion_tokens"] == 24
    assert module.requested_output_tokens(sanitized, 32, 16) == 24


def test_context_guard_supports_max_completion_tokens() -> None:
    module = load_context_guard_module()
    handler = make_reliable_guard_handler(module, context_tokens=300)
    payload = {
        "model": "test",
        "messages": [{"role": "user", "content": "short"}],
        "max_tokens": 999,
        "max_completion_tokens": 32768,
    }

    sanitized = handler.sanitize_max_tokens(payload)

    assert "max_tokens" not in sanitized
    assert sanitized["max_completion_tokens"] < 32768
    assert module.requested_output_tokens(sanitized, 32, 16) == sanitized["max_completion_tokens"]


def test_context_guard_discovers_ds4_context_length(monkeypatch: pytest.MonkeyPatch) -> None:
    module = load_context_guard_module()
    config = make_reliable_guard_handler(module).config
    config.model_contexts = {}
    config.fallback_model_contexts = {"local-deepseek-v4-flash": 32768}
    config.discover_model_context = True
    config.tokenizer_base_urls = {
        "local-deepseek-v4-flash": "http://host.docker.internal:8011"
    }

    class ModelsResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self):
            return {
                "data": [
                    {
                        "id": "deepseek-v4-flash",
                        "context_length": 65536,
                    }
                ]
            }

    calls = []
    monkeypatch.setattr(
        module.requests,
        "get",
        lambda url, **kwargs: calls.append((url, kwargs)) or ModelsResponse(),
    )

    assert config.context_limit_for("local-deepseek-v4-flash") == 65536
    assert calls[0][0] == "http://host.docker.internal:8011/v1/models"


def test_context_guard_refuses_known_oversized_payload_before_send() -> None:
    module = load_context_guard_module()
    handler = make_reliable_guard_handler(module, context_tokens=100)
    payload = {
        "model": "test",
        "messages": [{"role": "user", "content": "x" * 1000}],
        "max_tokens": 16,
    }
    writes = []
    handler.write_json = lambda status, body, **kwargs: writes.append((status, body, kwargs))
    handler.post_upstream = lambda *args, **kwargs: pytest.fail("oversized payload reached upstream")

    response, _, _, retries = handler.post_with_context_retries(
        {}, payload, compacted=False
    )

    assert response is None
    assert retries == 0
    assert writes[0][0] == 422
    assert writes[0][1]["error"]["code"] == "context_guard_unable_to_fit"
    assert writes[0][1]["error"]["recoverable"] is True
    assert writes[0][2]["headers"]["X-Context-Limit"] == "100"


def test_context_guard_returns_recoverable_error_after_retry_exhaustion() -> None:
    module = load_context_guard_module()
    handler = make_reliable_guard_handler(
        module,
        context_tokens=32768,
        chars_per_token=100.0,
        keep_last_messages=20,
    )
    handler.config.max_compaction_retries = 2
    payload = {
        "model": "test",
        "messages": [
            {"role": "user", "content": "old one"},
            {"role": "assistant", "content": "a" * 1000},
            {"role": "user", "content": "old two"},
            {"role": "assistant", "content": "b" * 1000},
            {"role": "user", "content": "current"},
            {"role": "assistant", "content": "still current"},
        ],
        "max_tokens": 16,
    }

    class OverflowResponse:
        status_code = 400
        ok = False
        content = b"{\"error\":{\"code\":\"context_length_exceeded\"}}"

        def close(self) -> None:
            return None

    writes = []
    sent = []
    handler.post_upstream = lambda headers, body: sent.append(copy.deepcopy(body)) or OverflowResponse()
    handler.write_json = lambda status, body, **kwargs: writes.append((status, body, kwargs))

    response, _, compacted, retries = handler.post_with_context_retries(
        {}, payload, compacted=False
    )

    assert response is None
    assert compacted
    assert retries == 2
    assert len(sent) == 3
    assert writes[0][0] == 422
    assert writes[0][1]["error"]["recoverable"] is True
    assert writes[0][2]["headers"]["X-Context-Retry"] == "2"


def test_context_guard_accounting_headers() -> None:
    module = load_context_guard_module()
    handler = make_reliable_guard_handler(module, context_tokens=300)
    payload = {
        "model": "test",
        "messages": [{"role": "user", "content": "short"}],
        "max_tokens": 16,
    }

    headers = handler.context_headers(payload, compacted=True, retry_count=1)

    assert headers["X-Context-Guard"] == "compacted"
    assert headers["X-Context-Input-Tokens"].isdigit()
    assert headers["X-Context-Output-Reserve"] == "16"
    assert headers["X-Context-Limit"] == "300"
    assert headers["X-Context-Retry"] == "1"


def test_context_guard_uses_conservative_budget_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    from types import SimpleNamespace

    monkeypatch.delenv("CONTEXT_GUARD_HEADROOM_TOKENS", raising=False)
    monkeypatch.delenv("CONTEXT_GUARD_CHARS_PER_TOKEN", raising=False)
    module = load_context_guard_module()
    config = module.build_config(
        SimpleNamespace(
            upstream_base_url="http://localhost:4000/v1",
            timeout=1,
            allow_no_auth=False,
            verbose=False,
        )
    )

    assert config.headroom_tokens == 2048
    assert config.chars_per_token == 3.0
    assert config.max_compaction_retries == 3


def test_context_guard_fallback_contexts_include_routed_aliases() -> None:
    from types import SimpleNamespace

    module = load_context_guard_module()
    config = module.build_config(
        SimpleNamespace(
            upstream_base_url="http://localhost:4000/v1",
            timeout=1,
            allow_no_auth=False,
            verbose=False,
        )
    )

    assert config.fallback_model_contexts["local-coder"] == config.fallback_model_contexts["local-balanced"]
    assert config.fallback_model_contexts["local-balanced-smoke-lora"] == module.env_int("LORA_MAX_MODEL_LEN", 32768)
    assert config.fallback_model_contexts["local-deepseek-v4-flash"] == module.env_int("DEEPSEEKV4_MAX_MODEL_LEN", 65536)


def test_eval_prompt_files_exist_and_parse() -> None:
    prompt_dir = ROOT / "evals/prompts"
    for name in ["smoke", "json", "router", "throughput", "vision"]:
        assert (prompt_dir / f"{name}.jsonl").is_file()
        assert (prompt_dir / f"{name}.jsonl").stat().st_size > 0

    for path in prompt_dir.glob("*.jsonl"):
        for line_no, line in enumerate(path.read_text().splitlines(), 1):
            if not line.strip():
                continue
            item = json.loads(line)
            assert "id" in item, f"{path}:{line_no}: missing id"
            assert "prompt" in item or "messages" in item, f"{path}:{line_no}: missing prompt/messages"


def test_lora_workflow_config_is_wired() -> None:
    makefile = (ROOT / "Makefile").read_text()
    compose = (ROOT / "docker-compose.yml").read_text()
    litellm = yaml.safe_load((ROOT / "config/litellm.yaml").read_text())
    aliases = {entry["model_name"] for entry in litellm["model_list"]}
    config = yaml.safe_load((ROOT / "training/configs/qwen3-lora-smoke.yaml").read_text())

    for target in ["lora-train", "lora-serve", "lora-eval"]:
        assert re.search(rf"^{target}:", makefile, re.MULTILINE)

    assert "lora-eval:\n\tset -a; source .env; set +a; python scripts/run-evals.py" in makefile
    assert "local-balanced-smoke-lora" in aliases
    assert "  vllm-lora:" in compose
    assert "--lora-modules" in compose
    assert "qwen3-4b-smoke-lora/current" in compose

    dataset_path = config["dataset_path"]
    host_dataset = ROOT / dataset_path.removeprefix("/workspace/")
    assert host_dataset.is_file()
    assert config["output_dir"].startswith("/workspace/models/adapters/")


def test_vision_and_throughput_workflow_config_is_wired() -> None:
    makefile = (ROOT / "Makefile").read_text()
    compose = (ROOT / "docker-compose.yml").read_text()
    env_example = (ROOT / ".env.example").read_text()
    litellm = yaml.safe_load((ROOT / "config/litellm.yaml").read_text())
    aliases = {entry["model_name"] for entry in litellm["model_list"]}

    for target in ["download-vision", "throughput-eval", "vision-up", "vision-eval"]:
        assert re.search(rf"^{target}:", makefile, re.MULTILINE)

    assert "local-vision" in aliases
    assert "vision-up:\n\t$(DOCKER_COMPOSE) up -d postgres\n\t$(DOCKER_COMPOSE) --profile vision up -d vllm-vision\n\t$(DOCKER_COMPOSE) up -d --no-deps litellm" in makefile
    assert "  vllm-vision:" in compose
    assert "Qwen/Qwen3-VL-4B-Instruct" in env_example
    assert (ROOT / "evals/assets/red-square.png").is_file()
    assert '"image_path":"evals/assets/red-square.png"' in (ROOT / "evals/prompts/vision.jsonl").read_text()


def test_lora_train_dry_run_reports_versioned_paths() -> None:
    result = run([
        str(ROOT / ".venv/bin/python") if (ROOT / ".venv/bin/python").exists() else "python3",
        str(ROOT / "training/train_lora.py"),
        "--config",
        str(ROOT / "training/configs/qwen3-lora-smoke.yaml"),
        "--run-id",
        "pytest-run",
        "--dry-run",
    ])
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    assert payload["output_root"].endswith("models/adapters/qwen3-4b-smoke-lora")
    assert payload["run_dir"].endswith("models/adapters/qwen3-4b-smoke-lora/runs/pytest-run")
    assert payload["staging_dir"].endswith("models/adapters/qwen3-4b-smoke-lora/runs/.pytest-run.tmp")
    assert payload["current_link"].endswith("models/adapters/qwen3-4b-smoke-lora/current")


def test_lora_current_symlink_update_is_atomic(tmp_path: Path) -> None:
    import importlib.util

    module_path = ROOT / "training/train_lora.py"
    spec = importlib.util.spec_from_file_location("train_lora", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    run_dir = tmp_path / "adapter" / "runs" / "run-a"
    run_dir.mkdir(parents=True)
    current = tmp_path / "adapter" / "current"
    module.update_current_symlink(current, run_dir)

    assert current.is_symlink()
    assert os.readlink(current) == "runs/run-a"


def test_init_creates_dirs_when_chown_is_skipped(tmp_path: Path) -> None:
    result = run(["env", "SKIP_CHOWN=1", str(ROOT / "scripts/init-dirs.sh")], cwd=tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    for path in ["data/huggingface", "data/datasets/processed", "evals/runs", "models/adapters"]:
        assert (tmp_path / path).is_dir()


def test_init_chown_failure_reports_remediation(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    chown = fake_bin / "chown"
    chown.write_text("#!/usr/bin/env bash\nexit 1\n")
    chown.chmod(0o755)

    env = {"PATH": f"{fake_bin}:{shutil.which('bash') and '/usr/bin:/bin'}"}
    result = run([str(ROOT / "scripts/init-dirs.sh")], cwd=tmp_path, env=env)

    assert result.returncode != 0
    output = result.stdout + result.stderr
    assert "ERROR: Could not set ownership for data/prometheus." in output
    assert "sudo chown -R 65534:65534 data/prometheus" in output
    assert "sudo chown -R 472:472 data/grafana" in output


def test_init_chown_success_uses_expected_owners(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    log = tmp_path / "chown.log"
    chown = fake_bin / "chown"
    chown.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$*" >>"$CHOWN_LOG"\n')
    chown.chmod(0o755)

    env = {
        "PATH": f"{fake_bin}:/usr/bin:/bin",
        "CHOWN_LOG": str(log),
    }
    result = run([str(ROOT / "scripts/init-dirs.sh")], cwd=tmp_path, env=env)
    assert result.returncode == 0, result.stdout + result.stderr

    lines = log.read_text().splitlines()
    assert "-R 65534:65534 data/prometheus" in lines
    assert "-R 472:472 data/grafana" in lines
