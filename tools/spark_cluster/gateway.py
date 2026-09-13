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
import socket
import sys
import threading
from urllib.parse import urlparse

import requests

from .config import absolute, fields, integer, name, read, require, validate_saved_plan

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
               ("tokenizer_base_url", "health_url", "upstream_key_env", "deployment_digest", "model_root", "replicas",
                "request_timeout_s", "tokenizer_timeout_s"))
        endpoints = [r]
        if 'replicas' in r:
            require(isinstance(r['replicas'],list) and 2 <= len(r['replicas']) <= 32,'replicas must contain 2..32 endpoints')
            require('model_root' in r and not r.get('upstream_key_env'),'replicas require one pinned local model contract')
            require(len({e['base_url'] for e in r['replicas']}) == len(r['replicas']),'duplicate replica endpoint')
            for e in r['replicas']:
                fields(e,('base_url','tokenizer_base_url','health_url','deployment_digest'))
                require(re.fullmatch(r'[a-f0-9]{64}',e['deployment_digest']),'invalid replica deployment digest')
                require(e['tokenizer_base_url'] == e['base_url'].removesuffix('/v1') and
                        e['health_url'] == e['base_url'].removesuffix('/v1')+'/health',
                        'replica health and tokenizer must belong to its model endpoint')
                endpoints.append(e)
        for endpoint in endpoints:
            for key in ("base_url", "tokenizer_base_url", "health_url"):
                if not endpoint.get(key): continue
                u = urlparse(endpoint[key])
                require(u.scheme in ("http", "https") and u.hostname and not u.username and
                        not u.password and not u.query and not u.fragment, "invalid registry URL")
            require(endpoint["base_url"].endswith("/v1"), "base_url must end with /v1")
        if 'model_root' in r: absolute(r['model_root'])
        require(isinstance(r["upstream_model"], str) and r["upstream_model"], "upstream model required")
        integer(r["context_tokens"], 256, 2097152)
        integer(r["max_output_tokens"], 1, r["context_tokens"] - 1)
        if 'request_timeout_s' in r: integer(r['request_timeout_s'], 1, 3600)
        if 'tokenizer_timeout_s' in r: integer(r['tokenizer_timeout_s'], 1, 180)
        fields(r["capabilities"], ("text", "vision", "tools", "streaming"))
        require(all(type(v) is bool for v in r["capabilities"].values()), "capabilities must be booleans")
        if r.get("upstream_key_env"):
            require(re.fullmatch(r"[A-Z][A-Z0-9_]+", r["upstream_key_env"]) and not r["upstream_key_env"].startswith("SPARK_"), "invalid or reserved secret environment name")


def from_plans(plans, replicas=False):
    routes = {}
    recipes = {}
    for p in plans:
        validate_saved_plan(p)
        alias, e = p["recipe"]["alias"], p["endpoint"]
        endpoint = {"base_url": e["base_url"],
                         "tokenizer_base_url": e["base_url"].removesuffix("/v1"),
                         "health_url": e["base_url"].removesuffix("/v1") + "/health",
                         "deployment_digest":p['digest']}
        if alias in routes:
            require(replicas,f"duplicate alias {alias}; explicitly select --replicas for identical model deployments")
            require(recipes[alias] == p['recipe'],'replicas must use the identical pinned recipe and model contract')
            route = routes[alias]
            if 'replicas' not in route:
                route['replicas'] = [{k:route[k] for k in endpoint}]
            route['replicas'].append(endpoint)
        else:
            recipes[alias] = p['recipe']
            routes[alias] = {**endpoint, "upstream_model": alias,
                "context_tokens": e["context_tokens"], "max_output_tokens": e["max_output_tokens"],
                "capabilities": e["capabilities"],
                "model_root":"/cache/hub/models--"+p['recipe']['model'].replace('/','--')+'/snapshots/'+p['recipe']['revision']}
            if e['context_tokens'] > 65536:
                # Long prefill can exceed the legacy 180-second read timeout.
                # Bind these bounds to the route snapshot, not every model.
                routes[alias].update(request_timeout_s=3600, tokenizer_timeout_s=180)
    result = {"version": 1, "routes": routes}
    validate_registry(result)
    return result


def provider_key(route, registry_path):
    name = route.get("upstream_key_env")
    if not name: return None
    path = Path(registry_path).with_name("credentials.json")
    try:
        stored = read(path) if path.exists() else {}
        require(isinstance(stored, dict), "invalid provider credential store")
        if name in stored:
            entry = stored[name]
            if entry is None: return None
            require(isinstance(entry, dict) and set(entry) == {"base_url", "key"}, "invalid provider credential entry")
            if entry["base_url"] != route["base_url"]: return None
            key = entry["key"]
        else:
            key = os.getenv(name)
        require(key is None or isinstance(key, str) and 1 <= len(key) <= 8192 and
                all(33 <= ord(c) <= 126 for c in key), "invalid provider credential")
        return key
    except (ValueError, TypeError, OSError):
        raise RuntimeError("provider credentials unavailable") from None


def provider_ready(route, registry_path):
    if not route.get("upstream_key_env"): return True
    try: return bool(provider_key(route, registry_path))
    except RuntimeError: return False


def live(route):
    if not route.get("health_url"):
        return True  # Remote provider availability is established by its request.
    try:
        with requests.get(route["health_url"], timeout=2) as response:
            if response.status_code != 200: return False
        if route.get('model_root'):
            with requests.get(route['base_url']+'/models',timeout=2) as response:
                response.raise_for_status()
                models=response.json()['data']
            return isinstance(models,list) and any(isinstance(m,dict) and m.get('id') == route['upstream_model'] and m.get('root') == route['model_root'] and
                       m.get('max_model_len') == route['context_tokens'] for m in models)
        return True
    except (requests.RequestException,ValueError,KeyError,TypeError):
        return False


def members(route):
    common={k:v for k,v in route.items() if k!='replicas'}
    return [{**common,**endpoint} for endpoint in route['replicas']] if 'replicas' in route else [common]


class ReplicaPool:
    """Least in-flight requests, rotating ties; one lease covers the entire stream."""
    def __init__(self):
        self.lock=threading.Lock()
        self.active={}
        self.ticket=0

    def acquire(self,route):
        healthy=[member for member in members(route) if live(member)]
        if not healthy: return None
        with self.lock:
            offset=self.ticket % len(healthy)
            index=min(range(len(healthy)),key=lambda i:(self.active.get(healthy[i]['base_url'],0),(i-offset)%len(healthy)))
            selected=healthy[index]
            key=selected['base_url']
            self.active[key]=self.active.get(key,0)+1
            self.ticket+=1
            return selected

    def release(self,route):
        with self.lock:
            key=route['base_url']
            self.active[key]-=1
            if self.active[key]==0: del self.active[key]


class GatewayHandler(guard.ContextGuardHandler):
    heartbeat_interval_s = 15

    def relay_chat_events(self, response):
        if not getattr(self, '_route', {}).get('request_timeout_s'):
            return super().relay_chat_events(response)
        # Keep socket-read timeouts alive during long prefill. Comments carry no
        # model content and do not manufacture successful completion. The base
        # relay still validates every event and requires the real [DONE].
        original = self.wfile
        lock = threading.Lock()
        stopped = threading.Event()
        # Own a duplicate of this response's socket so a disconnected client
        # can interrupt a blocking upstream read without racing descriptor reuse.
        cancel_socket = None
        try:
            cancel_socket = socket.fromfd(response.raw.fileno(), socket.AF_INET, socket.SOCK_STREAM)
        except (AttributeError, OSError, ValueError):
            pass
        class Writer:
            def write(self, data):
                with lock:
                    if guard.sse_data(data).strip() == b'[DONE]': stopped.set()
                    return original.write(data)
            def flush(self):
                with lock: return original.flush()
        self.wfile = Writer()
        def heartbeat():
            while not stopped.wait(self.heartbeat_interval_s):
                try:
                    with lock:
                        if stopped.is_set(): return
                        original.write(b': context-guard keepalive\n\n')
                        original.flush()
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
                    stopped.set()
                    if cancel_socket is not None:
                        try: cancel_socket.shutdown(socket.SHUT_RDWR)
                        except OSError: pass
                    return
        worker = threading.Thread(target=heartbeat, daemon=True)
        worker.start()
        try:
            return super().relay_chat_events(response)
        finally:
            stopped.set()
            worker.join(timeout=1)
            if cancel_socket is not None: cancel_socket.close()
            self.wfile = original

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
            key = getattr(self, "_upstream_key", None)
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
            for alias, r in registry["routes"].items() if provider_ready(r, self.server.registry_path) and any(live(member) for member in members(r))]})

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
        try:
            self._upstream_key = provider_key(route, self.server.registry_path)
        except RuntimeError:
            self.write_json(503, {"error": {"type": "provider_unconfigured", "message": "Provider credentials unavailable"}})
            return
        if route.get("upstream_key_env") and not self._upstream_key:
            self.write_json(503, {"error": {"type": "provider_unconfigured", "message": "Provider credential is not configured for this upstream"}})
            return
        selected=self.server.replica_pool.acquire(route)
        if selected is None:
            self.write_json(503, {"error": {"type": "deployment_unavailable", "message": "Selected deployment is unavailable; no fallback was used"}})
            return
        self._route = route = selected
        # A private per-request config prevents a concurrent registry update from
        # routing with a different model's token budget or cached tokenizer.
        self._request_config = replace(self.server.config,
            upstream_base_url=route["base_url"], model_contexts={alias: route["context_tokens"]},
            fallback_model_contexts={}, context_cache={}, discover_model_context=False,
            default_output_tokens=route["max_output_tokens"], compact_model=alias,
            model_timeouts={**self.server.config.model_timeouts,
                guard.normalize_model_name(route['upstream_model']): route.get('request_timeout_s',
                    self.server.config.request_timeout_for(route['upstream_model']))},
            tokenizer_timeout_s=route.get('tokenizer_timeout_s', self.server.config.tokenizer_timeout_s),
            tokenizer_models={alias: route["upstream_model"]},
            tokenizer_base_urls={alias: route["tokenizer_base_url"]} if route.get("tokenizer_base_url") else {})
        try:
            super().do_POST()
        finally:
            self.server.replica_pool.release(selected)

    def context_headers(self,payload,**kwargs):
        headers=super().context_headers(payload,**kwargs)
        if self._route.get('deployment_digest'):
            headers['X-Spark-Deployment']=self._route['deployment_digest']
        return headers

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
    required = {'tokenizer_models', 'tokenizer_base_urls', 'model_timeouts'}
    require(required <= set(cfg.__dataclass_fields__),
            'Gateway requires a matching context-guard-proxy.py dependency; refresh both files')
    cfg.headroom_tokens = 512
    server = guard.ContextGuardServer((host, port), GatewayHandler, cfg)
    server.registry_path = Path(registry_path)
    server.api_key = key
    server.replica_pool = ReplicaPool()
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
