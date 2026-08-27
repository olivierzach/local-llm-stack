#!/usr/bin/env python3
"""OpenAI-compatible proxy that compacts oversized chat requests before retrying."""

from __future__ import annotations

import argparse
import copy
import hashlib
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
import re
import sys
from typing import Any
from urllib.parse import urlparse, urlunparse

import requests


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIRECT_BASE_URLS = {
    "local-fast": "http://localhost:8001/v1",
    "local-balanced": "http://localhost:8002/v1",
    "local-large": "http://localhost:8003/v1",
    "local-qwen30-a3b": "http://localhost:8004/v1",
    "local-deepseek-r1-qwen32b": "http://localhost:8005/v1",
    "local-mistral-small": "http://localhost:8006/v1",
    "local-balanced-smoke-lora": "http://localhost:8007/v1",
    "local-vision": "http://localhost:8008/v1",
    "local-gpt-oss-120b": "http://localhost:8009/v1",
    "local-laguna-s-2.1": "http://localhost:8010/v1",
    "local-deepseek-v4-flash": "http://localhost:8011/v1",
}
DEFAULT_TOKENIZER_BASE_URLS = {
    model: base_url.removesuffix("/v1")
    for model, base_url in DEFAULT_DIRECT_BASE_URLS.items()
}
HOP_BY_HOP_HEADERS = {
    "connection",
    "content-encoding",
    "content-length",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
CONTEXT_ERROR_MARKERS = (
    "contextwindowexceeded",
    "context_length_exceeded",
    "context window",
    "configured context size",
    "maximum context",
    "maximum context length",
    "input tokens",
    "too many tokens",
    "exceeds the context",
    "max_tokens must be at least 1",
)
DS4_CONTEXT_ERROR_RE = re.compile(
    r"prompt\s+has\s+[\d,]+\s+tokens?.*configured\s+context\s+size\s+is\s+[\d,]+\s+tokens?",
    re.DOTALL,
)


def load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


def env_bool(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def parse_model_contexts(value: str | None) -> dict[str, int]:
    contexts: dict[str, int] = {}
    if not value:
        return contexts
    for item in value.split(","):
        if not item.strip() or ":" not in item:
            continue
        name, raw_limit = item.split(":", 1)
        try:
            contexts[name.strip()] = int(raw_limit.strip())
        except ValueError:
            continue
    return contexts


def parse_model_urls(value: str | None, defaults: dict[str, str]) -> dict[str, str]:
    urls = dict(defaults)
    if not value:
        return urls
    for item in value.split(","):
        if "=" not in item:
            continue
        model, url = item.split("=", 1)
        if model.strip() and url.strip():
            urls[model.strip()] = url.strip().rstrip("/")
    return urls


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=str)


def normalize_model_name(model: str) -> str:
    for prefix in ("spark-litellm/", "litellm/"):
        if model.startswith(prefix):
            return model[len(prefix) :]
    return model


def estimate_text_tokens(text: str, chars_per_token: float) -> int:
    if not text:
        return 0
    return max(1, math.ceil(len(text) / chars_per_token))


def message_to_text(message: dict[str, Any]) -> str:
    parts = [f"role={message.get('role', '')}"]
    if message.get("name"):
        parts.append(f"name={message['name']}")

    content = message.get("content")
    if isinstance(content, str):
        parts.append(content)
    elif content is not None:
        parts.append(compact_json(content))

    for key in ("tool_calls", "function_call"):
        if key in message:
            parts.append(compact_json(message[key]))
    return "\n".join(parts)


def estimate_messages_tokens(messages: list[dict[str, Any]], chars_per_token: float) -> int:
    total = 0
    for message in messages:
        total += 4
        total += estimate_text_tokens(message_to_text(message), chars_per_token)
    return total + 3


def estimate_payload_tokens(payload: dict[str, Any], chars_per_token: float) -> int:
    messages = payload.get("messages") or []
    total = estimate_messages_tokens(messages, chars_per_token) if isinstance(messages, list) else 0
    for key in ("tools", "functions", "response_format"):
        if key in payload:
            total += estimate_text_tokens(compact_json(payload[key]), chars_per_token)
    return total


def transcript_from_messages(messages: list[dict[str, Any]], max_chars: int) -> str:
    blocks = []
    for message in messages:
        role = message.get("role", "unknown")
        blocks.append(f"{role.upper()}:\n{message_to_text(message)}")
    transcript = "\n\n".join(blocks)
    if len(transcript) <= max_chars:
        return transcript
    half = max_chars // 2
    return (
        transcript[:half]
        + "\n\n[... middle of transcript omitted for compaction budget ...]\n\n"
        + transcript[-half:]
    )


def extract_response_text(body: dict[str, Any]) -> str:
    choices = body.get("choices")
    if not isinstance(choices, list) or not choices:
        return ""
    message = choices[0].get("message") or {}
    content = message.get("content")
    return content if isinstance(content, str) else ""


def is_context_error(status_code: int, body: bytes) -> bool:
    if status_code not in {400, 413, 422}:
        return False
    text = body.decode("utf-8", errors="ignore").lower()
    if any(marker in text for marker in CONTEXT_ERROR_MARKERS):
        return True
    try:
        error = json.loads(text).get("error", {})
    except (AttributeError, ValueError):
        error = {}
    if isinstance(error, dict):
        code = str(error.get("code") or error.get("type") or "").lower()
        if code == "context_length_exceeded":
            return True
        message = str(error.get("message") or "").lower()
        if any(marker in message for marker in CONTEXT_ERROR_MARKERS):
            return True
        text = text + "\n" + message
    return bool(DS4_CONTEXT_ERROR_RE.search(text))


def output_token_field(payload: dict[str, Any]) -> str:
    if "max_completion_tokens" in payload:
        return "max_completion_tokens"
    return "max_tokens"


def requested_output_tokens(payload: dict[str, Any], default: int, minimum: int) -> int:
    field = output_token_field(payload)
    try:
        requested = int(payload.get(field))
    except (TypeError, ValueError):
        return max(minimum, default)
    if requested < 1:
        return max(minimum, default)
    return requested


def compact_schema(value: Any, *, preserve_keys: bool = False) -> Any:
    if isinstance(value, dict):
        compacted = {}
        for key, item in value.items():
            if key == "description" and not preserve_keys:
                continue
            compacted[key] = compact_schema(item, preserve_keys=key == "properties")
        return compacted
    if isinstance(value, list):
        return [compact_schema(item) for item in value]
    return value


def retain_text_edges(text: str, max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    if len(text) <= max_chars:
        return text
    marker = "\n\n[... content truncated by Context Guard ...]\n\n"
    if max_chars <= len(marker):
        return text[-max_chars:]
    usable = max_chars - len(marker)
    head = usable // 3
    tail = usable - head
    return text[:head] + marker + text[-tail:]


def split_messages(messages: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    leading: list[dict[str, Any]] = []
    index = 0
    while index < len(messages) and messages[index].get("role") in {"system", "developer"}:
        leading.append(messages[index])
        index += 1
    return leading, messages[index:]


@dataclass
class ProxyConfig:
    upstream_base_url: str
    timeout_s: float
    allow_no_auth: bool
    default_context_tokens: int
    model_contexts: dict[str, int]
    fallback_model_contexts: dict[str, int]
    headroom_tokens: int
    default_output_tokens: int
    min_output_tokens: int
    keep_last_messages: int
    summary_tokens: int
    compact_source_chars: int
    chars_per_token: float
    compact_model: str | None
    discover_model_context: bool
    verbose: bool
    tokenizer_base_urls: dict[str, str] = field(default_factory=dict)
    tokenizer_timeout_s: float = 3.0
    max_compaction_retries: int = 3
    context_cache: dict[str, int] = field(default_factory=dict)

    def context_limit_for(self, model: str) -> int:
        alias = normalize_model_name(model)
        if alias in self.model_contexts:
            return self.model_contexts[alias]
        fallback = self.fallback_model_contexts.get(alias, self.default_context_tokens)
        if not self.discover_model_context:
            return fallback
        if alias in self.context_cache:
            return self.context_cache[alias]

        base_url = self.tokenizer_base_urls.get(alias) or DEFAULT_DIRECT_BASE_URLS.get(alias)
        if not base_url:
            return fallback
        try:
            models_url = base_url.rstrip("/")
            if not models_url.endswith("/v1"):
                models_url += "/v1"
            response = requests.get(models_url + "/models", timeout=2)
            response.raise_for_status()
            data = response.json()
        except Exception:
            return fallback

        items = data.get("data", [])
        for item in items:
            item_id = normalize_model_name(str(item.get("id") or ""))
            if item_id not in {alias, alias.removeprefix("local-")} and len(items) != 1:
                continue
            for key in ("max_model_len", "context_length", "max_context_length"):
                if item.get(key):
                    limit = int(item[key])
                    self.context_cache[alias] = limit
                    return limit
            provider = item.get("top_provider")
            if isinstance(provider, dict) and provider.get("context_length"):
                limit = int(provider["context_length"])
                self.context_cache[alias] = limit
                return limit
        return fallback


class ContextGuardServer(ThreadingHTTPServer):
    config: ProxyConfig

    def __init__(
        self,
        server_address: tuple[str, int],
        handler_class: type[BaseHTTPRequestHandler],
        config: ProxyConfig,
    ):
        super().__init__(server_address, handler_class)
        self.config = config


class ContextGuardHandler(BaseHTTPRequestHandler):
    server_version = "context-guard/0.1"
    protocol_version = "HTTP/1.0"

    @property
    def config(self) -> ProxyConfig:
        return self.server.config  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: Any) -> None:
        if self.config.verbose:
            super().log_message(fmt, *args)

    def do_OPTIONS(self) -> None:
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "authorization, content-type")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.end_headers()

    def do_GET(self) -> None:
        if self.parsed_path() in {"/health", "/healthz"}:
            self.write_json(200, {"status": "ok"})
            return
        self.proxy_plain()

    def do_POST(self) -> None:
        if self.parsed_path().endswith("/chat/completions"):
            self.handle_chat_completion()
            return
        self.proxy_plain()

    def parsed_path(self) -> str:
        return urlparse(self.path).path

    def read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or "0")
        return self.rfile.read(length) if length else b""

    def upstream_url(self) -> str:
        upstream = urlparse(self.config.upstream_base_url.rstrip("/"))
        requested = urlparse(self.path)
        path = requested.path
        if upstream.path.rstrip("/").endswith("/v1") and path.startswith("/v1/"):
            path = path[len("/v1") :]
        return urlunparse(
            (
                upstream.scheme,
                upstream.netloc,
                upstream.path.rstrip("/") + path,
                "",
                requested.query,
                "",
            )
        )

    def incoming_headers(self) -> dict[str, str] | None:
        headers: dict[str, str] = {
            "Accept": self.headers.get("Accept", "*/*"),
            "Accept-Encoding": "identity",
        }
        content_type = self.headers.get("Content-Type")
        if content_type:
            headers["Content-Type"] = content_type

        authorization = self.headers.get("Authorization")
        if authorization:
            headers["Authorization"] = authorization
        elif self.config.allow_no_auth:
            key = os.getenv("LITELLM_MASTER_KEY")
            if key:
                headers["Authorization"] = f"Bearer {key}"
        else:
            self.write_json(
                401,
                {
                    "error": {
                        "message": "Missing Authorization header. Use the same LiteLLM API key.",
                        "type": "auth_error",
                    }
                },
            )
            return None
        return headers

    def proxy_plain(self) -> None:
        headers = self.incoming_headers()
        if headers is None:
            return
        body = self.read_body() if self.command in {"POST", "PUT", "PATCH"} else None
        try:
            response = requests.request(
                self.command,
                self.upstream_url(),
                headers=headers,
                data=body,
                timeout=self.config.timeout_s,
                stream=True,
            )
        except requests.RequestException as exc:
            self.write_json(502, {"error": {"message": str(exc), "type": "upstream_error"}})
            return
        self.relay_response(response, stream=True)

    def handle_chat_completion(self) -> None:
        headers = self.incoming_headers()
        if headers is None:
            return

        raw_body = self.read_body()
        try:
            original_payload = json.loads(raw_body.decode("utf-8"))
        except ValueError:
            self.write_json(400, {"error": {"message": "Request body is not valid JSON.", "type": "bad_request"}})
            return
        if not isinstance(original_payload, dict):
            self.write_json(400, {"error": {"message": "Request body must be a JSON object.", "type": "bad_request"}})
            return

        payload, compacted = self.prepare_payload(original_payload, headers)
        response, payload, compacted, retry_count = self.post_with_context_retries(
            headers, payload, compacted=compacted
        )
        if response is None:
            return

        self.relay_response(
            response,
            stream=bool(payload.get("stream")),
            compacted=compacted,
            payload=payload,
            retry_count=retry_count,
        )

    def post_with_context_retries(
        self,
        headers: dict[str, str],
        payload: dict[str, Any],
        *,
        compacted: bool,
    ) -> tuple[requests.Response | None, dict[str, Any], bool, int]:
        if not self.ensure_payload_fits(payload, compacted=compacted, retry_count=0):
            return None, payload, compacted, 0
        response = self.post_upstream(headers, payload)
        attempts = 0
        while response is not None and attempts < self.config.max_compaction_retries:
            body = response.content if response.status_code >= 400 else b""
            if response.ok or not is_context_error(response.status_code, body):
                break

            response.close()
            previous_payload = compact_json(payload)
            payload, _ = self.compact_payload(payload, headers, force=True)
            payload = self.sanitize_max_tokens(payload)
            compacted = True
            attempts += 1
            if compact_json(payload) == previous_payload:
                break
            if not self.ensure_payload_fits(payload, compacted=True, retry_count=attempts):
                return None, payload, compacted, attempts
            response = self.post_upstream(headers, payload)

        if response is not None and not response.ok and is_context_error(
            response.status_code, response.content
        ):
            response.close()
            self.write_guard_error(
                "The request still exceeds the live model context after bounded compaction.",
                payload,
                compacted=True,
                retry_count=attempts,
            )
            return None, payload, True, attempts

        return response, payload, compacted, attempts

    def estimate_input_tokens(self, payload: dict[str, Any]) -> int:
        model = normalize_model_name(str(payload.get("model") or ""))
        base_url = self.config.tokenizer_base_urls.get(model)
        if base_url:
            tokenizer_payload = {
                key: payload[key]
                for key in (
                    "model",
                    "messages",
                    "tools",
                    "tool_choice",
                    "functions",
                    "function_call",
                    "parallel_tool_calls",
                    "response_format",
                    "chat_template",
                    "chat_template_kwargs",
                    "reasoning_effort",
                    "add_generation_prompt",
                    "continue_final_message",
                    "add_special_tokens",
                )
                if key in payload
            }
            tokenizer_payload["model"] = model
            cache_key = hashlib.sha256(compact_json(tokenizer_payload).encode("utf-8")).hexdigest()
            token_cache = getattr(self, "_token_count_cache", {})
            if cache_key in token_cache:
                return token_cache[cache_key]
            for path in ("/tokenize", "/v1/tokenize"):
                try:
                    response = requests.post(
                        base_url.rstrip("/") + path,
                        json=tokenizer_payload,
                        timeout=self.config.tokenizer_timeout_s,
                    )
                    if response.status_code == 404:
                        continue
                    response.raise_for_status()
                    body = response.json()
                    count = body.get("count")
                    if isinstance(count, int) and count >= 0:
                        live_limit = body.get("max_model_len") or body.get("context_length")
                        if isinstance(live_limit, int) and live_limit > 0:
                            self.config.context_cache[model] = live_limit
                        if len(token_cache) >= 16:
                            token_cache.clear()
                        token_cache[cache_key] = count
                        self._token_count_cache = token_cache
                        return count
                except (requests.RequestException, ValueError, AttributeError):
                    break
        return estimate_payload_tokens(payload, self.config.chars_per_token)

    def prepare_payload(self, payload: dict[str, Any], headers: dict[str, str]) -> tuple[dict[str, Any], bool]:
        prepared = dict(payload)
        if isinstance(prepared.get("model"), str):
            prepared["model"] = normalize_model_name(prepared["model"])
        prepared = self.apply_model_defaults(prepared)

        model = str(prepared.get("model") or "")
        limit = self.config.context_limit_for(model)
        estimate = self.estimate_input_tokens(prepared)
        max_tokens = requested_output_tokens(
            prepared,
            self.config.default_output_tokens,
            self.config.min_output_tokens,
        )
        if estimate + max_tokens + self.config.headroom_tokens <= limit:
            return self.sanitize_max_tokens(prepared), False
        return self.compact_payload(prepared, headers)

    def apply_model_defaults(self, payload: dict[str, Any]) -> dict[str, Any]:
        prepared = dict(payload)
        model = str(prepared.get("model") or "")
        if model == "local-laguna-s-2.1" and "chat_template_kwargs" not in prepared:
            prepared["chat_template_kwargs"] = {"enable_thinking": False}
        return prepared

    def sanitize_max_tokens(self, payload: dict[str, Any]) -> dict[str, Any]:
        sanitized = dict(payload)
        model = str(sanitized.get("model") or "")
        limit = self.config.context_limit_for(model)
        estimate = self.estimate_input_tokens(sanitized)
        budget = max(1, limit - estimate - self.config.headroom_tokens)

        field = output_token_field(sanitized)
        raw_max_tokens = sanitized.get(field)
        try:
            max_tokens = int(raw_max_tokens)
        except (TypeError, ValueError):
            max_tokens = min(
                max(self.config.min_output_tokens, self.config.default_output_tokens),
                budget,
            )
        if max_tokens < 1:
            max_tokens = min(
                max(self.config.min_output_tokens, self.config.default_output_tokens),
                budget,
            )
        sanitized[field] = max(1, min(max_tokens, budget))
        if field == "max_completion_tokens":
            sanitized.pop("max_tokens", None)
        return sanitized

    def compact_payload(
        self,
        payload: dict[str, Any],
        headers: dict[str, str],
        *,
        force: bool = False,
    ) -> tuple[dict[str, Any], bool]:
        compacted = copy.deepcopy(payload)
        messages = compacted.get("messages")
        if not isinstance(messages, list) or len(messages) < 3:
            return self.trim_payload(compacted, force=force), True

        leading, conversational = split_messages(messages)
        keep_count = max(1, self.config.keep_last_messages)
        recent_start = max(0, len(conversational) - keep_count)
        while recent_start > 0 and conversational[recent_start].get("role") != "user":
            recent_start -= 1
        older = conversational[:recent_start]
        recent = conversational[recent_start:]
        if not older:
            return self.trim_payload(compacted, force=force), True

        summary = self.summarize_messages(
            older,
            model=str(compacted.get("model") or ""),
            headers=headers,
        )
        summary_message = {
            "role": "system",
            "content": (
                "Earlier conversation summary, generated automatically because the "
                "context window was near its limit:\n\n"
                + summary
            ),
        }
        compacted["messages"] = leading + [summary_message] + recent
        return self.trim_payload(compacted, force=force), True

    def trim_payload(self, payload: dict[str, Any], *, force: bool = False) -> dict[str, Any]:
        trimmed = copy.deepcopy(payload)
        reserved_output = requested_output_tokens(
            payload,
            self.config.default_output_tokens,
            self.config.min_output_tokens,
        )
        model = str(trimmed.get("model") or "")
        limit = self.config.context_limit_for(model)
        messages = trimmed.get("messages")
        if not isinstance(messages, list):
            return self.sanitize_max_tokens(trimmed)

        leading, conversational = split_messages(messages)
        if not conversational:
            return self.emergency_fit_payload(trimmed, limit, reserved_output)

        latest_user_index = next(
            (
                index
                for index in range(len(conversational) - 1, -1, -1)
                if conversational[index].get("role") == "user"
            ),
            len(conversational) - 1,
        )
        older = conversational[:latest_user_index]
        current_turn = conversational[latest_user_index:]
        minimum_removals = math.ceil(len(older) / 2) if force and older else 0
        removed = 0

        while older:
            trimmed["messages"] = leading + older + current_turn
            if removed >= minimum_removals and self.payload_fits(
                trimmed, limit, output_tokens=reserved_output
            ):
                return self.sanitize_max_tokens(trimmed)
            remove_count = self.oldest_turn_size(older)
            older = older[remove_count:]
            removed += remove_count

        trimmed["messages"] = leading + current_turn
        if self.payload_fits(trimmed, limit, output_tokens=reserved_output):
            return self.sanitize_max_tokens(trimmed)

        current_only = copy.deepcopy(trimmed)
        current_only["messages"] = current_turn
        if self.payload_fits(current_only, limit, output_tokens=reserved_output):
            return self.sanitize_max_tokens(current_only)
        return self.emergency_fit_payload(current_only, limit, reserved_output)


    @staticmethod
    def oldest_turn_size(messages: list[dict[str, Any]]) -> int:
        for index in range(1, len(messages)):
            if messages[index].get("role") == "user":
                return index
        return len(messages)

    def truncate_message_contents_to_fit(
        self,
        payload: dict[str, Any],
        limit: int,
        output_tokens: int,
    ) -> dict[str, Any]:
        candidate = copy.deepcopy(payload)
        messages = candidate.get("messages")
        if not isinstance(messages, list):
            return candidate

        role_priority = {"tool": 0, "assistant": 1, "user": 2}
        truncatable = []
        for index, message in enumerate(messages):
            content = message.get("content")
            if isinstance(content, str):
                text = content
            elif content is not None:
                text = compact_json(content)
            else:
                continue
            truncatable.append(
                (role_priority.get(str(message.get("role")), 3), -len(text), index, text)
            )

        for _, _, index, text in sorted(truncatable):
            if self.payload_fits(candidate, limit, output_tokens=output_tokens):
                return candidate
            low = 0
            high = len(text)
            best = None
            while low <= high:
                retained_chars = (low + high) // 2
                trial = copy.deepcopy(candidate)
                trial["messages"][index]["content"] = retain_text_edges(text, retained_chars)
                if self.payload_fits(trial, limit, output_tokens=output_tokens):
                    best = trial
                    low = retained_chars + 1
                else:
                    high = retained_chars - 1
            if best is not None:
                return best
            candidate["messages"][index]["content"] = ""

        return candidate

    def emergency_fit_payload(
        self,
        payload: dict[str, Any],
        limit: int,
        reserved_output: int,
    ) -> dict[str, Any]:
        emergency = copy.deepcopy(payload)
        original_messages = copy.deepcopy(emergency.get("messages") or [])
        if "tools" in emergency:
            emergency["tools"] = compact_schema(emergency["tools"])

        emergency = self.truncate_message_contents_to_fit(
            emergency, limit, reserved_output
        )
        if self.payload_fits(emergency, limit, output_tokens=reserved_output):
            return self.sanitize_max_tokens(emergency)

        for key in (
            "tools",
            "functions",
            "tool_choice",
            "response_format",
            "chat_template",
            "chat_template_kwargs",
        ):
            emergency.pop(key, None)
        field = output_token_field(emergency)
        emergency[field] = self.config.min_output_tokens
        if field == "max_completion_tokens":
            emergency.pop("max_tokens", None)
        minimum_output = self.config.min_output_tokens

        latest = next(
            (
                message
                for message in reversed(original_messages)
                if message.get("role") == "user"
            ),
            original_messages[-1] if original_messages else {"role": "user", "content": ""},
        )
        minimal_message = {
            "role": latest.get("role") or "user",
            "content": latest.get("content") if latest.get("content") is not None else "",
        }
        if latest.get("name"):
            minimal_message["name"] = latest["name"]
        emergency["messages"] = [minimal_message]
        emergency = self.truncate_message_contents_to_fit(
            emergency, limit, minimum_output
        )
        if not self.payload_fits(emergency, limit, output_tokens=minimum_output):
            emergency["messages"] = [{"role": "user", "content": ""}]
        return self.sanitize_max_tokens(emergency)

    def payload_fits(
        self,
        payload: dict[str, Any],
        limit: int,
        *,
        output_tokens: int | None = None,
    ) -> bool:
        estimate = self.estimate_input_tokens(payload)
        if output_tokens is None:
            output_tokens = requested_output_tokens(
                payload,
                self.config.default_output_tokens,
                self.config.min_output_tokens,
            )
        return estimate + output_tokens + self.config.headroom_tokens <= limit

    def context_accounting(self, payload: dict[str, Any]) -> tuple[int, int, int]:
        model = str(payload.get("model") or "")
        return (
            self.estimate_input_tokens(payload),
            requested_output_tokens(
                payload,
                self.config.default_output_tokens,
                self.config.min_output_tokens,
            ),
            self.config.context_limit_for(model),
        )

    def context_headers(
        self,
        payload: dict[str, Any],
        *,
        compacted: bool,
        retry_count: int,
    ) -> dict[str, str]:
        input_tokens, output_tokens, limit = self.context_accounting(payload)
        headers = {
            "X-Context-Input-Tokens": str(input_tokens),
            "X-Context-Output-Reserve": str(output_tokens),
            "X-Context-Limit": str(limit),
            "X-Context-Retry": str(retry_count),
        }
        if compacted:
            headers["X-Context-Guard"] = "compacted"
        return headers

    def ensure_payload_fits(
        self,
        payload: dict[str, Any],
        *,
        compacted: bool,
        retry_count: int,
    ) -> bool:
        input_tokens, output_tokens, limit = self.context_accounting(payload)
        if input_tokens + output_tokens + self.config.headroom_tokens <= limit:
            return True
        self.write_guard_error(
            "Context Guard could not reduce the request below the live model context.",
            payload,
            compacted=compacted,
            retry_count=retry_count,
        )
        return False

    def write_guard_error(
        self,
        message: str,
        payload: dict[str, Any],
        *,
        compacted: bool,
        retry_count: int,
    ) -> None:
        input_tokens, output_tokens, limit = self.context_accounting(payload)
        self.write_json(
            422,
            {
                "error": {
                    "message": message,
                    "type": "context_guard_error",
                    "code": "context_guard_unable_to_fit",
                    "recoverable": True,
                    "input_tokens": input_tokens,
                    "output_reserve": output_tokens,
                    "context_limit": limit,
                }
            },
            headers=self.context_headers(
                payload,
                compacted=compacted,
                retry_count=retry_count,
            ),
        )

    def summarize_messages(self, messages: list[dict[str, Any]], *, model: str, headers: dict[str, str]) -> str:
        request_model = normalize_model_name(model)
        summary_models = [normalize_model_name(self.config.compact_model or "local-fast")]
        if request_model not in summary_models:
            summary_models.append(request_model)
        transcript = transcript_from_messages(messages, self.config.compact_source_chars)

        for compact_model in summary_models:
            summary_payload = {
                "model": compact_model,
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "Compact this chat transcript for continuing the same session. "
                            "Preserve user goals, decisions, constraints, file paths, commands, "
                            "errors, and unresolved tasks. Do not invent facts."
                        ),
                    },
                    {"role": "user", "content": transcript},
                ],
                "temperature": 0,
                "max_tokens": self.config.summary_tokens,
                "stream": False,
            }
            try:
                response = requests.post(
                    self.upstream_base_chat_url(),
                    headers=headers,
                    json=summary_payload,
                    timeout=self.config.timeout_s,
                )
                response.raise_for_status()
                summary = extract_response_text(response.json()).strip()
                if summary:
                    return summary
            except Exception:
                continue
        return "The earlier conversation was compacted. Recent transcript excerpt:\n\n" + transcript[-4000:]

    def post_upstream(self, headers: dict[str, str], payload: dict[str, Any]) -> requests.Response | None:
        try:
            return requests.post(
                self.upstream_base_chat_url(),
                headers=headers,
                json=payload,
                timeout=self.config.timeout_s,
                stream=bool(payload.get("stream")),
            )
        except requests.RequestException as exc:
            self.write_json(502, {"error": {"message": str(exc), "type": "upstream_error"}})
            return None

    def upstream_base_chat_url(self) -> str:
        return self.config.upstream_base_url.rstrip("/") + "/chat/completions"

    def relay_response(
        self,
        response: requests.Response,
        *,
        stream: bool,
        compacted: bool = False,
        payload: dict[str, Any] | None = None,
        retry_count: int = 0,
    ) -> None:
        self.send_response(response.status_code)
        for name, value in response.headers.items():
            if name.lower() not in HOP_BY_HOP_HEADERS:
                self.send_header(name, value)
        if payload is not None:
            for name, value in self.context_headers(
                payload,
                compacted=compacted,
                retry_count=retry_count,
            ).items():
                self.send_header(name, value)
        elif compacted:
            self.send_header("X-Context-Guard", "compacted")
        self.end_headers()

        if stream:
            try:
                for chunk in response.iter_content(chunk_size=None):
                    if chunk:
                        self.wfile.write(chunk)
                        self.wfile.flush()
            finally:
                response.close()
            return

        self.wfile.write(response.content)
        response.close()

    def write_json(
        self,
        status_code: int,
        body: dict[str, Any],
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        content = json.dumps(body).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(content)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(content)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Run a compact-and-retry proxy in front of LiteLLM.")
    p.add_argument("--host", default=os.getenv("CONTEXT_GUARD_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=env_int("CONTEXT_GUARD_PORT", 4010))
    p.add_argument(
        "--upstream-base-url",
        default=os.getenv(
            "CONTEXT_GUARD_UPSTREAM_BASE_URL",
            os.getenv("LITELLM_BASE_URL", "http://localhost:4000/v1"),
        ),
    )
    p.add_argument("--timeout", type=float, default=float(os.getenv("CONTEXT_GUARD_TIMEOUT", "180")))
    p.add_argument("--allow-no-auth", action="store_true", default=env_bool("CONTEXT_GUARD_ALLOW_NO_AUTH", False))
    p.add_argument("--verbose", action="store_true", default=env_bool("CONTEXT_GUARD_VERBOSE", False))
    return p


def build_config(args: argparse.Namespace) -> ProxyConfig:
    default_contexts = {
        "local-fast": env_int("FAST_MAX_MODEL_LEN", 32768),
        "local-balanced": env_int("BALANCED_MAX_MODEL_LEN", 32768),
        "local-coder": env_int("BALANCED_MAX_MODEL_LEN", 32768),
        "local-large": env_int("LARGE_MAX_MODEL_LEN", 16384),
        "local-qwen30-a3b": env_int("QWEN30A3B_MAX_MODEL_LEN", 32768),
        "local-deepseek-r1-qwen32b": env_int("DEEPSEEK32B_MAX_MODEL_LEN", 16384),
        "local-mistral-small": env_int("MISTRAL24B_MAX_MODEL_LEN", 32768),
        "local-balanced-smoke-lora": env_int("LORA_MAX_MODEL_LEN", 32768),
        "local-vision": env_int("VISION_MAX_MODEL_LEN", 32768),
        "local-gpt-oss-120b": env_int("GPTOSS120B_MAX_MODEL_LEN", 8192),
        "local-laguna-s-2.1": env_int("LAGUNAS21_MAX_MODEL_LEN", 262144),
        "local-deepseek-v4-flash": env_int("DEEPSEEKV4_MAX_MODEL_LEN", 65536),
    }
    return ProxyConfig(
        upstream_base_url=args.upstream_base_url,
        timeout_s=args.timeout,
        allow_no_auth=args.allow_no_auth,
        default_context_tokens=env_int("CONTEXT_GUARD_DEFAULT_CONTEXT_TOKENS", 8192),
        model_contexts=parse_model_contexts(os.getenv("CONTEXT_GUARD_MODEL_CONTEXTS")),
        fallback_model_contexts=default_contexts,
        headroom_tokens=env_int("CONTEXT_GUARD_HEADROOM_TOKENS", 2048),
        default_output_tokens=env_int("CONTEXT_GUARD_DEFAULT_OUTPUT_TOKENS", 4096),
        min_output_tokens=env_int("CONTEXT_GUARD_MIN_OUTPUT_TOKENS", 64),
        keep_last_messages=env_int("CONTEXT_GUARD_KEEP_LAST_MESSAGES", 8),
        summary_tokens=env_int("CONTEXT_GUARD_SUMMARY_TOKENS", 512),
        compact_source_chars=env_int("CONTEXT_GUARD_COMPACT_SOURCE_CHARS", 6000),
        chars_per_token=float(os.getenv("CONTEXT_GUARD_CHARS_PER_TOKEN", "3.0")),
        compact_model=os.getenv("CONTEXT_GUARD_COMPACT_MODEL", "local-fast") or None,
        discover_model_context=env_bool("CONTEXT_GUARD_DISCOVER_MODEL_CONTEXT", True),
        verbose=args.verbose,
        tokenizer_base_urls=parse_model_urls(
            os.getenv("CONTEXT_GUARD_TOKENIZER_BASE_URLS"),
            DEFAULT_TOKENIZER_BASE_URLS,
        ),
        tokenizer_timeout_s=float(os.getenv("CONTEXT_GUARD_TOKENIZER_TIMEOUT", "3.0")),
        max_compaction_retries=env_int("CONTEXT_GUARD_MAX_RETRIES", 3),
    )


def main() -> int:
    load_dotenv(REPO_ROOT / ".env")
    args = parser().parse_args()
    config = build_config(args)
    server = ContextGuardServer((args.host, args.port), ContextGuardHandler, config)
    print(
        f"context guard listening on http://{args.host}:{args.port}/v1 -> {config.upstream_base_url}",
        file=sys.stderr,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\ncontext guard stopped", file=sys.stderr)
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
