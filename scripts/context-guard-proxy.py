#!/usr/bin/env python3
"""OpenAI-compatible proxy that compacts oversized chat requests before retrying."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import math
import os
from pathlib import Path
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
    "context window",
    "maximum context",
    "maximum context length",
    "input tokens",
    "too many tokens",
    "exceeds the context",
    "max_tokens must be at least 1",
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
    return any(marker in text for marker in CONTEXT_ERROR_MARKERS)


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

        base_url = DEFAULT_DIRECT_BASE_URLS.get(alias)
        if not base_url:
            return fallback
        try:
            response = requests.get(base_url.rstrip("/") + "/models", timeout=2)
            response.raise_for_status()
            data = response.json()
        except Exception:
            return fallback

        for item in data.get("data", []):
            if item.get("id") == alias and item.get("max_model_len"):
                limit = int(item["max_model_len"])
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
        response = self.post_upstream(headers, payload)
        if response is None:
            return

        body = response.content if response.status_code >= 400 else b""
        if not response.ok and is_context_error(response.status_code, body) and not compacted:
            response.close()
            payload, compacted = self.compact_payload(original_payload, headers)
            payload = self.sanitize_max_tokens(payload)
            response = self.post_upstream(headers, payload)
            if response is None:
                return

        self.relay_response(response, stream=bool(payload.get("stream")), compacted=compacted)

    def prepare_payload(self, payload: dict[str, Any], headers: dict[str, str]) -> tuple[dict[str, Any], bool]:
        prepared = dict(payload)
        if isinstance(prepared.get("model"), str):
            prepared["model"] = normalize_model_name(prepared["model"])
        prepared = self.sanitize_max_tokens(prepared)

        model = str(prepared.get("model") or "")
        limit = self.config.context_limit_for(model)
        estimate = estimate_payload_tokens(prepared, self.config.chars_per_token)
        max_tokens = int(prepared.get("max_tokens") or self.config.default_output_tokens)
        if estimate + max_tokens + self.config.headroom_tokens <= limit:
            return prepared, False
        return self.compact_payload(prepared, headers)

    def sanitize_max_tokens(self, payload: dict[str, Any]) -> dict[str, Any]:
        sanitized = dict(payload)
        model = str(sanitized.get("model") or "")
        limit = self.config.context_limit_for(model)
        estimate = estimate_payload_tokens(sanitized, self.config.chars_per_token)
        budget = max(1, limit - estimate - self.config.headroom_tokens)

        raw_max_tokens = sanitized.get("max_tokens")
        try:
            max_tokens = int(raw_max_tokens)
        except (TypeError, ValueError):
            max_tokens = min(self.config.default_output_tokens, budget)

        if max_tokens < self.config.min_output_tokens:
            max_tokens = min(self.config.default_output_tokens, max(self.config.min_output_tokens, budget))
        sanitized["max_tokens"] = max(1, min(max_tokens, budget))
        return sanitized

    def compact_payload(self, payload: dict[str, Any], headers: dict[str, str]) -> tuple[dict[str, Any], bool]:
        compacted = dict(payload)
        messages = compacted.get("messages")
        if not isinstance(messages, list) or len(messages) < 3:
            return self.trim_payload(compacted), True

        leading, conversational = split_messages(messages)
        keep_count = max(1, self.config.keep_last_messages)
        older = conversational[:-keep_count]
        recent = conversational[-keep_count:]
        if not older:
            return self.trim_payload(compacted), True

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
        return self.trim_payload(compacted), True

    def trim_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        trimmed = self.sanitize_max_tokens(payload)
        model = str(trimmed.get("model") or "")
        limit = self.config.context_limit_for(model)
        messages = trimmed.get("messages")
        if not isinstance(messages, list):
            return trimmed

        while len(messages) > 1:
            estimate = estimate_payload_tokens(trimmed, self.config.chars_per_token)
            max_tokens = int(trimmed.get("max_tokens") or self.config.default_output_tokens)
            if estimate + max_tokens + self.config.headroom_tokens <= limit:
                break
            remove_index = next(
                (i for i, item in enumerate(messages) if item.get("role") not in {"system", "developer"}),
                1,
            )
            messages.pop(remove_index)
            trimmed["messages"] = messages
            trimmed = self.sanitize_max_tokens(trimmed)
        return trimmed

    def summarize_messages(self, messages: list[dict[str, Any]], *, model: str, headers: dict[str, str]) -> str:
        compact_model = normalize_model_name(self.config.compact_model or model)
        transcript = transcript_from_messages(messages, self.config.compact_source_chars)
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
            pass
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

    def relay_response(self, response: requests.Response, *, stream: bool, compacted: bool = False) -> None:
        self.send_response(response.status_code)
        for name, value in response.headers.items():
            if name.lower() not in HOP_BY_HOP_HEADERS:
                self.send_header(name, value)
        if compacted:
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

    def write_json(self, status_code: int, body: dict[str, Any]) -> None:
        content = json.dumps(body).encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(content)))
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
    }
    return ProxyConfig(
        upstream_base_url=args.upstream_base_url,
        timeout_s=args.timeout,
        allow_no_auth=args.allow_no_auth,
        default_context_tokens=env_int("CONTEXT_GUARD_DEFAULT_CONTEXT_TOKENS", 8192),
        model_contexts=parse_model_contexts(os.getenv("CONTEXT_GUARD_MODEL_CONTEXTS")),
        fallback_model_contexts=default_contexts,
        headroom_tokens=env_int("CONTEXT_GUARD_HEADROOM_TOKENS", 128),
        default_output_tokens=env_int("CONTEXT_GUARD_DEFAULT_OUTPUT_TOKENS", 4096),
        min_output_tokens=env_int("CONTEXT_GUARD_MIN_OUTPUT_TOKENS", 64),
        keep_last_messages=env_int("CONTEXT_GUARD_KEEP_LAST_MESSAGES", 8),
        summary_tokens=env_int("CONTEXT_GUARD_SUMMARY_TOKENS", 512),
        compact_source_chars=env_int("CONTEXT_GUARD_COMPACT_SOURCE_CHARS", 12000),
        chars_per_token=float(os.getenv("CONTEXT_GUARD_CHARS_PER_TOKEN", "4.0")),
        compact_model=os.getenv("CONTEXT_GUARD_COMPACT_MODEL") or None,
        discover_model_context=env_bool("CONTEXT_GUARD_DISCOVER_MODEL_CONTEXT", True),
        verbose=args.verbose,
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
