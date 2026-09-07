from types import SimpleNamespace

from test_static import load_context_guard_module, make_reliable_guard_handler


def test_qwen_timeout_does_not_change_other_models():
    module = load_context_guard_module()
    handler = make_reliable_guard_handler(module)
    assert handler.config.request_timeout_for("local-qwen38-flash-next") == 600
    assert handler.config.request_timeout_for("test") == 1


def test_qwen_tokenizer_receives_multimodal_settings(monkeypatch):
    module = load_context_guard_module()
    handler = make_reliable_guard_handler(module)
    handler.config.tokenizer_base_urls = {"local-qwen38-flash-next": "http://unused"}
    seen = []

    def post(url, **kwargs):
        seen.append(kwargs["json"])
        return SimpleNamespace(status_code=200, raise_for_status=lambda: None,
                               json=lambda: {"count": 123, "max_model_len": 262144})

    monkeypatch.setattr(module.requests, "post", post)
    payload = {
        "model": "local-qwen38-flash-next", "messages": [],
        "tools": [{"type": "function"}], "response_format": {"type": "json_object"},
        "mm_processor_kwargs": {"max_pixels": 12345},
        "media_io_kwargs": {"image": {}},
        "chat_template_kwargs": {"enable_thinking": False}, "max_tokens": 256,
    }
    assert handler.estimate_input_tokens(payload) == 123
    assert seen == [{k: v for k, v in payload.items() if k != "max_tokens"}]
    assert handler.config.context_cache["local-qwen38-flash-next"] == 262144


def test_qwen_summary_uses_resident_model_without_thinking(monkeypatch):
    module = load_context_guard_module()
    handler = make_reliable_guard_handler(module)
    seen = []

    def post(url, **kwargs):
        seen.append(kwargs)
        return SimpleNamespace(raise_for_status=lambda: None,
                               json=lambda: {"choices": [{"message": {"content": "summary"}}]})

    monkeypatch.setattr(module.requests, "post", post)
    assert handler.summarize_messages([{"role": "user", "content": "test"}],
                                     model="local-qwen38-flash-next", headers={}) == "summary"
    assert len(seen) == 1
    assert seen[0]["timeout"] == 600
    assert seen[0]["json"]["model"] == "local-qwen38-flash-next"
    assert seen[0]["json"]["chat_template_kwargs"] == {"enable_thinking": False}


def test_impossible_output_budget_preserves_input_and_template():
    module = load_context_guard_module()
    handler = make_reliable_guard_handler(module, context_tokens=1000)
    payload = {
        "model": "test", "messages": [{"role": "user", "content": "keep this"}],
        "chat_template_kwargs": {"enable_thinking": False},
        "max_completion_tokens": 2000,
    }
    prepared, compacted = handler.prepare_payload(payload, {})
    assert not compacted
    assert prepared["messages"] == payload["messages"]
    assert prepared["chat_template_kwargs"] == payload["chat_template_kwargs"]
    assert 1 <= prepared["max_completion_tokens"] < 1000
    assert "max_tokens" not in prepared
