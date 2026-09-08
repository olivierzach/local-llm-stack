"""Optional registry-backed Context Guard. Legacy guard/Compose stay unchanged.

One registry read binds route, tokenizer and limits for the whole request.
An atomic registry replacement affects subsequent requests only. No automatic
model fallback: unavailable or unsupported routes fail explicitly.
"""
from __future__ import annotations

import argparse
from dataclasses import replace
import hmac
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
from urllib.parse import urlparse

import requests

from .config import fields, integer, name, read, require

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("spark_legacy_guard", ROOT / "scripts/context-guard-proxy.py")
guard = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = guard
spec.loader.exec_module(guard)


def validate_registry(registry):
    fields(registry, ("version", "routes"))
    require(registry["version"] == 1, "unsupported registry version")
    require(isinstance(registry["routes"], dict), "routes must be a mapping")
    for alias, r in registry["routes"].items():
        name(alias)
        fields(r, ("base_url", "upstream_model", "context_tokens", "max_output_tokens", "capabilities"),
               ("tokenizer_base_url", "health_url", "upstream_key_env", "deployment_digest"))
        for key in ("base_url", "tokenizer_base_url", "health_url"):
            if not r.get(key): continue
            u = urlparse(r[key])
            require(u.scheme in ("http", "https") and u.hostname and not u.username and
                    not u.password and not u.query and not u.fragment, "invalid registry URL")
        require(r["base_url"].endswith("/v1"), "base_url must end with /v1")
        require(isinstance(r["upstream_model"], str) and r["upstream_model"], "upstream model required")
        integer(r["context_tokens"], 256, 2097152)
        integer(r["max_output_tokens"], 1, r["context_tokens"] - 1)
        fields(r["capabilities"], ("text", "vision", "tools", "streaming"))
        require(all(type(v) is bool for v in r["capabilities"].values()), "capabilities must be booleans")
        if r.get("upstream_key_env"):
            require(re.fullmatch(r"[A-Z][A-Z0-9_]+", r["upstream_key_env"]), "invalid secret environment name")


def from_plans(plans):
    routes = {}
    for p in plans:
        alias, e = p["recipe"]["alias"], p["endpoint"]
        require(alias not in routes, f"duplicate alias {alias}; choose one placement per alias")
        routes[alias] = {"base_url": e["base_url"], "upstream_model": alias,
                         "tokenizer_base_url": e["base_url"].removesuffix("/v1"),
                         "health_url": e["base_url"].removesuffix("/v1") + "/health",
                         "context_tokens": e["context_tokens"], "max_output_tokens": e["max_output_tokens"],
                         "capabilities": e["capabilities"], "deployment_digest": p["digest"]}
    result = {"version": 1, "routes": routes}
    validate_registry(result)
    return result


def live(route):
    if not route.get("health_url"):
        return True  # Remote provider availability is established by its request.
    try:
        return requests.get(route["health_url"], timeout=2).status_code == 200
    except requests.RequestException:
        return False


class GatewayHandler(guard.ContextGuardHandler):
    @property
    def config(self):
        return getattr(self, "_request_config", self.server.config)

    def registry(self):
        r = read(self.server.registry_path)
        validate_registry(r)
        return r

    def incoming_headers(self):
        supplied = self.headers.get("Authorization", "")
        expected = "Bearer " + self.server.api_key
        if not hmac.compare_digest(supplied.encode(), expected.encode()):
            self.write_json(401, {"error": {"type": "auth_error", "message": "Invalid gateway API key"}})
            return None
        result = {"Accept": self.headers.get("Accept", "*/*"), "Accept-Encoding": "identity",
                  "Content-Type": "application/json"}
        route = getattr(self, "_route", {})
        if route.get("upstream_key_env"):
            key = os.getenv(route["upstream_key_env"])
            if not key:
                self.write_json(503, {"error": {"type": "provider_unconfigured", "message": "Upstream credential is not configured"}})
                return None
            result["Authorization"] = "Bearer " + key
        return result

    def read_body(self):
        if not hasattr(self, "_body"):
            self._body = super().read_body()
        return self._body

    def do_GET(self):
        path = self.parsed_path()
        if path == "/health":
            self.write_json(200, {"status": "ok"})
            return
        if self.incoming_headers() is None: return
        if path != "/v1/models":
            self.write_json(404, {"error": {"message": "Unsupported endpoint"}})
            return
        try:
            registry = self.registry()
        except (ValueError, OSError, TypeError, KeyError):
            self.write_json(503, {"error": {"message": "Invalid route registry"}})
            return
        self.write_json(200, {"object": "list", "data": [
            {"id": alias, "object": "model", "owned_by": "spark-cluster",
             "context_length": r["context_tokens"], "max_output_tokens": r["max_output_tokens"],
             "capabilities": r["capabilities"]}
            for alias, r in registry["routes"].items() if live(r)]})

    def do_POST(self):
        if self.incoming_headers() is None: return
        if self.parsed_path() != "/v1/chat/completions":
            self.write_json(404, {"error": {"message": "This gateway currently supports chat completions"}})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if not 0 < length <= 16 * 1024 * 1024:
                self.write_json(413, {"error": {"message": "Request body must be 1 byte to 16 MiB"}})
                return
            payload = json.loads(self.read_body())
            require(isinstance(payload, dict), "request must be a JSON object")
            alias = guard.normalize_model_name(str(payload.get("model", "")))
        except (ValueError, TypeError):
            self.write_json(400, {"error": {"message": "Invalid request body"}})
            return
        try:
            registry = self.registry()
        except (ValueError, OSError, TypeError, KeyError):
            self.write_json(503, {"error": {"message": "Invalid route registry"}})
            return
        route = registry["routes"].get(alias)
        if not route:
            self.write_json(404, {"error": {"type": "model_not_found", "message": "Model alias is not configured"}})
            return
        caps = route["capabilities"]
        if (payload.get("tools") or payload.get("functions")) and not caps["tools"]:
            self.write_json(400, {"error": {"message": "This deployment has not enabled tool calling"}})
            return
        messages = payload.get("messages")
        if not isinstance(messages, list) or not all(isinstance(m, dict) for m in messages):
            self.write_json(400, {"error": {"message": "messages must be a list of objects"}})
            return
        image_input = any(isinstance(m.get("content"), list) and any(
            isinstance(part, dict) and part.get("type") in ("image_url", "input_image") for part in m["content"])
            for m in messages)
        if image_input and not caps["vision"]:
            self.write_json(400, {"error": {"message": "This deployment does not support vision"}})
            return
        if payload.get("stream") and not caps["streaming"]:
            self.write_json(400, {"error": {"message": "This deployment does not support streaming"}})
            return
        if not live(route):
            self.write_json(503, {"error": {"type": "deployment_unavailable", "message": "Selected deployment is unavailable; no fallback was used"}})
            return
        self._route = route
        # A private per-request config prevents a concurrent registry update from
        # routing with a different model's token budget or cached tokenizer.
        self._request_config = replace(self.server.config,
            upstream_base_url=route["base_url"], model_contexts={alias: route["context_tokens"]},
            fallback_model_contexts={}, context_cache={}, discover_model_context=False,
            default_output_tokens=route["max_output_tokens"], compact_model=alias,
            tokenizer_base_urls={alias: route["tokenizer_base_url"]} if route.get("tokenizer_base_url") else {})
        super().do_POST()

    def sanitize_max_tokens(self, payload):
        result = super().sanitize_max_tokens(payload)
        if hasattr(self, "_route"):
            for key in ("max_tokens", "max_completion_tokens"):
                if key in result:
                    result[key] = min(result[key], self._route["max_output_tokens"])
        return result

    def post_upstream(self, headers, payload):
        wire = dict(payload)
        wire["model"] = self._route["upstream_model"]
        return super().post_upstream(headers, wire)

    def summarize_messages(self, messages, *, model, headers):
        transcript = guard.transcript_from_messages(messages, self.config.compact_source_chars)
        payload = {"model": self._route["upstream_model"], "messages": [
            {"role": "system", "content": "Summarize this conversation to continue it. Preserve goals, decisions, constraints, file paths and unresolved tasks. Do not invent facts."},
            {"role": "user", "content": transcript}], "temperature": 0,
            "max_tokens": min(self.config.summary_tokens, self._route["max_output_tokens"]), "stream": False}
        try:
            r = requests.post(self.upstream_base_chat_url(), headers=headers, json=payload, timeout=self.config.timeout_s)
            try:
                r.raise_for_status()
                summary = guard.extract_response_text(r.json()).strip()
                if summary: return summary
            finally:
                r.close()
        except (ValueError, requests.RequestException):
            pass
        return "Earlier transcript excerpt:\n" + transcript[-4000:]


def serve(registry_path, host, port, key):
    require(isinstance(key, str) and len(key) >= 24, "gateway key must contain at least 24 characters")
    validate_registry(read(registry_path))
    args = guard.parser().parse_args([])
    cfg = guard.build_config(args)
    cfg.headroom_tokens = 512
    server = guard.ContextGuardServer((host, port), GatewayHandler, cfg)
    server.registry_path = Path(registry_path)
    server.api_key = key
    return server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4110)
    args = parser.parse_args()
    server = serve(args.registry, args.host, args.port, os.getenv("SPARK_GATEWAY_KEY", ""))
    try: server.serve_forever()
    finally: server.server_close()


if __name__ == "__main__":
    main()
