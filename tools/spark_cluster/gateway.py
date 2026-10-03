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
import time
from urllib.parse import urlparse

import requests

from .config import absolute, fields, integer, name, read, require, validate_saved_plan
from .recovery_routes import RecoveryRoutes, RouteUnavailable

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
        require(isinstance(r["upstream_model"], str) and 1 <= len(r["upstream_model"]) <= 256 and
                all(33 <= ord(c) <= 126 for c in r["upstream_model"]), "invalid upstream model identifier")
        if "deployment_digest" in r:
            require(isinstance(r["deployment_digest"], str) and re.fullmatch(r"[a-f0-9]{64}", r["deployment_digest"]),
                    "invalid deployment digest")
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


class GatewayMetrics:
    """Bounded, payload-free completed-chat counters and monotonic timings."""
    def __init__(self):
        self.lock = threading.RLock()
        self.series = {}

    def begin(self, labels):
        with self.lock:
            if labels not in self.series and len(self.series) >= 128:
                labels = ("other", "other", "other")
            values = self.series.setdefault(labels, [0, 0, 0, 0.0, 0, 0.0])
            values[2] += 1
            return labels

    def select(self, previous, labels):
        with self.lock:
            selected = self.begin(labels)
            self.series[previous][2] -= 1
            return selected

    def finish(self, labels, duration, error, ttft):
        with self.lock:
            values = self.series[labels]
            values[0] += 1
            values[1] += int(error)
            values[2] -= 1
            values[3] += duration
            if ttft is not None:
                values[4] += 1
                values[5] += ttft

    def render(self):
        definitions = (
            ("requests_total", "counter", "Completed chat HTTP requests.", 0),
            ("errors_total", "counter", "HTTP errors, interrupted SSE and client disconnects.", 1),
            ("inflight", "gauge", "Chat handlers including full streams.", 2),
            ("request_duration_seconds_count", "counter", "Completed full-handler duration samples.", 0),
            ("request_duration_seconds_sum", "counter", "Full-handler seconds including stream duration.", 3),
            ("time_to_first_token_seconds_count", "counter", "SSE first generated delta samples; excludes role and heartbeats.", 4),
            ("time_to_first_token_seconds_sum", "counter", "Request entry to first observed content/reasoning/tool-argument SSE delta.", 5),
        )
        lines = []
        with self.lock:
            for suffix, kind, help_text, index in definitions:
                metric = "spark_gateway_" + suffix
                lines.extend((f"# HELP {metric} {help_text}", f"# TYPE {metric} {kind}"))
                for labels, values in sorted(self.series.items()):
                    if index in (4, 5) and not values[4]:
                        continue  # Nonstreaming and output-free requests have no observable TTFT.
                    label = ",".join(k + "=" + json.dumps(v) for k, v in zip(("alias", "backend", "mode"), labels))
                    lines.append(f"{metric}{{{label}}} {values[index]}")
        return "\n".join(lines) + "\n"


class GatewayServer(guard.ContextGuardServer):
    def __init__(self, server_address, handler_class, config, *, registry_path, key, recovery=None):
        self.registry_path = Path(registry_path)
        self.api_key = key
        self.replica_pool = ReplicaPool()
        self.metrics = GatewayMetrics()
        self.recovery = recovery
        # Only recovery ingress must drain handlers before releasing its singleton
        # lock. Ordinary/legacy servers retain ThreadingHTTPServer's daemon lifecycle.
        self.daemon_threads = recovery is None
        self._inbound_lock = threading.Lock()
        self._inbound = {}
        self._closing = False
        super().__init__(server_address, handler_class, config)

    def process_request(self, request, client_address):
        if self.recovery is None:
            return super().process_request(request, client_address)
        # Register before starting the handler so close cannot miss a socket
        # accepted just before the listener stops.
        with self._inbound_lock:
            if self._closing:
                self.shutdown_request(request)
                return
            self._inbound[request] = False
            try:
                super().process_request(request, client_address)
            except BaseException:
                self._inbound.pop(request, None)
                raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            if self.recovery is not None:
                with self._inbound_lock:
                    self._inbound.pop(request, None)

    def request_body_complete(self, request):
        if self.recovery is None:
            return
        # Input completion and shutdown share one fence: a cancelled reader
        # cannot subsequently acquire a route lease. Once input is complete,
        # preserve the entire response (including provider streams).
        with self._inbound_lock:
            if self._closing:
                raise ConnectionAbortedError("gateway is shutting down")
            self._inbound[request] = True

    def request_finished(self, request):
        if self.recovery is None:
            return False
        with self._inbound_lock:
            self._inbound[request] = False
            return self._closing

    def server_close(self):
        if self.recovery is not None:
            with self._inbound_lock:
                self._closing = True
                for request, complete in self._inbound.items():
                    if not complete:
                        # close() alone does not wake a handler's buffered read.
                        # Never shutdown an admitted stream's socket: its relay
                        # also watches that socket for client cancellation.
                        try:
                            request.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
        try:
            super().server_close()
        finally:
            if self.recovery is not None:
                self.recovery.close()


class GatewayHandler(guard.ContextGuardHandler):
    heartbeat_interval_s = 15

    def handle_one_request(self):
        try:
            return super().handle_one_request()
        finally:
            # The base handler has flushed the response. Do not let a completed
            # keep-alive request become another unbounded read during shutdown.
            if self.server.request_finished(self.connection):
                self.close_connection = True

    def send_response(self, code, message=None):
        self._response_code = code
        return super().send_response(code, message)

    def policy_unavailable(self):
        self.write_json(503, {"error": {"type": "policy_unavailable",
            "message": "Policy admission is closed or stale; no fallback or replay was used"}},
            headers={"Retry-After": "5"})

    def relay_response(self, response, **kwargs):
        original = self.wfile
        handler = self
        class TrackedWriter:
            def write(self, data):
                try:
                    return original.write(data)
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    handler._request_error = True
                    raise
            def flush(self):
                try:
                    return original.flush()
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    handler._request_error = True
                    raise
        self.wfile = TrackedWriter()
        try:
            return super().relay_response(response, **kwargs)
        finally:
            self.wfile = original

    def relay_chat_events(self, response):
        # Observe validated SSE frames written by the existing relay, not TCP bytes
        # or keepalive comments. A role-only delta is not a generated token.
        original = self.wfile
        handler = self
        class ObservedWriter:
            def write(self, data):
                try:
                    raw = guard.sse_data(data)
                    body = json.loads(raw) if raw and raw.strip() != b"[DONE]" else {}
                    if isinstance(body, dict):
                        if body.get("error"):
                            handler._request_error = True
                        for choice in body.get("choices", []):
                            delta = choice.get("delta", {})
                            generated = any(isinstance(delta.get(k), str) and delta[k] for k in ("content", "reasoning_content", "reasoning"))
                            generated = generated or any(
                                isinstance(t, dict) and isinstance(t.get("function"), dict) and t["function"].get("arguments")
                                for t in delta.get("tool_calls", []))
                            if generated and handler._ttft is None:
                                handler._ttft = time.monotonic() - handler._started
                except (ValueError, TypeError, AttributeError):
                    pass
                try:
                    return original.write(data)
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    handler._request_error = True
                    raise
            def flush(self):
                try:
                    return original.flush()
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    handler._request_error = True
                    raise
        self.wfile = ObservedWriter()
        try:
            return self._relay_with_heartbeat(response)
        finally:
            self.wfile = original

    def _relay_with_heartbeat(self, response):
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
            self.server.request_body_complete(self.connection)
        return self._body

    def do_GET(self):
        path = self.parsed_path()
        if path == "/health":
            self.write_json(200, {"status": "ok"})
            return
        if self.incoming_headers() is None: return
        if path == "/_spark/recovery":
            if self.server.recovery is None:
                self.write_json(404, {"error": {"message": "Recovery policy is not configured"}})
            else:
                self.write_json(200, self.server.recovery.status())
            return
        if path == "/metrics":
            body = self.server.metrics.render().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if path != "/v1/models":
            self.write_json(404, {"error": {"message": "Unsupported endpoint"}})
            return
        try:
            registry = self.registry()
        except (ValueError, OSError, TypeError, KeyError):
            self.write_json(503, {"error": {"message": "Invalid route registry"}})
            return
        routes = dict(registry["routes"])
        state = None
        if self.server.recovery is not None:
            routes.pop("local-auto", None)
            try:
                with self.server.recovery.lock:
                    candidate = self.server.recovery._read()
                    if candidate["accepting"]:
                        state = candidate
                        routes["local-auto"] = state["route"]
            except (ValueError, OSError, TypeError, KeyError):
                pass
            routes = {alias: route for alias, route in routes.items()
                      if route.get("upstream_key_env") or state is not None and route == state["route"]}
        data = []
        for alias, route in routes.items():
            if not provider_ready(route, self.server.registry_path) or not any(live(member) for member in members(route)):
                continue
            model = {"id": alias, "object": "model", "owned_by": "spark-cluster",
                "context_length": route["context_tokens"], "max_output_tokens": route["max_output_tokens"],
                "capabilities": route["capabilities"], "backend_alias": route["upstream_model"],
                "model_root": route.get("model_root"), "deployment_digest": route.get("deployment_digest")}
            if alias == "local-auto" and state is not None:
                model.update(policy=state["policy"], authority=state["authority"],
                             generation=state["generation"], mode=state["mode"])
            data.append(model)
        if self.server.recovery is not None and state is not None:
            # Health discovery may block across a close or generation change.
            try:
                self.server.recovery.check(state)
            except RouteUnavailable:
                data = [model for model in data if routes[model["id"]].get("upstream_key_env")]
        self.write_json(200, {"object": "list", "data": data})

    def do_POST(self):
        for attribute in ("_body", "_route", "_upstream_key", "_request_config", "_token_count_cache"):
            self.__dict__.pop(attribute, None)
        self._started = time.monotonic()
        self._ttft = None
        self._request_error = False
        self._response_code = 500
        self._admitted = None
        self._metric_labels = self.server.metrics.begin(("unselected", "unselected", "unavailable"))
        try:
            self._dispatch_chat()
        except RouteUnavailable:
            self.policy_unavailable()
        except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
            self._request_error = True
            self.close_connection = True
        finally:
            self.server.metrics.finish(self._metric_labels, time.monotonic() - self._started,
                self._request_error or self._response_code >= 400,
                self._ttft)
            if self._admitted is not None:
                self.server.recovery.release()

    def _dispatch_chat(self):
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
        if alias == "local-auto" and self.server.recovery is not None:
            try:
                self._admitted = self.server.recovery.acquire()
            except RouteUnavailable:
                self.policy_unavailable()
                return
            route = self._admitted["route"]
        else:
            try:
                registry = self.registry()
            except (ValueError, OSError, TypeError, KeyError):
                self.write_json(503, {"error": {"message": "Invalid route registry"}})
                return
            route = registry["routes"].get(alias)
        if not route:
            self.write_json(404, {"error": {"type": "model_not_found", "message": "Model alias is not configured"}})
            return
        if self.server.recovery is not None and self._admitted is None and not route.get("upstream_key_env"):
            try:
                self._admitted = self.server.recovery.acquire(route)
            except RouteUnavailable:
                self.policy_unavailable()
                return
        self._metric_labels = self.server.metrics.select(self._metric_labels, (alias, route["upstream_model"],
            self._admitted["mode"] if self._admitted else "strict"))
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
            if alias == "local-auto" and self._admitted is not None:
                self._request_config = replace(self._request_config, min_output_tokens=1, headroom_tokens=0)
                self.policy_completion(payload)
            else:
                super().do_POST()
        finally:
            self.server.replica_pool.release(selected)

    def context_headers(self,payload,**kwargs):
        headers=super().context_headers(payload,**kwargs)
        if self._route.get('deployment_digest'):
            headers['X-Spark-Deployment']=self._route['deployment_digest']
        headers["X-Spark-Backend"] = self._route["upstream_model"]
        headers["X-Spark-Max-Output-Tokens"] = str(self._route["max_output_tokens"])
        headers["X-Spark-Capabilities"] = ",".join(k for k, enabled in self._route["capabilities"].items() if enabled)
        if self._admitted:
            headers["X-Spark-Generation"] = str(self._admitted["generation"])
            headers["X-Spark-Authority"] = self._admitted["authority"]
            headers["X-Spark-Policy-Mode"] = self._admitted["mode"]
        return headers

    def sanitize_max_tokens(self, payload):
        result = super().sanitize_max_tokens(payload)
        if hasattr(self, "_route"):
            for key in ("max_tokens", "max_completion_tokens"):
                if key in result:
                    result[key] = min(result[key], self._route["max_output_tokens"])
        return result

    def post_upstream(self, headers, payload):
        if self._admitted is not None:
            try:
                self.server.recovery.check(self._admitted)
                if not live(self._route):
                    self.write_json(503, {"error": {"type": "backend_identity_changed",
                        "message": "Selected backend is unavailable or no longer matches the admitted plan"}})
                    return None
                self.server.recovery.check(self._admitted)
            except RouteUnavailable:
                self.policy_unavailable()
                return None
        wire = dict(payload)
        wire["model"] = self._route["upstream_model"]
        return super().post_upstream(headers, wire)

    def policy_completion(self, original):
        """Pinned accounting with no compaction, output clamping or request replay."""
        payload = dict(original)
        payload["model"] = "local-auto"
        caps = self._route["capabilities"]
        allowed = {"model", "messages", "tools", "functions", "tool_choice", "function_call", "parallel_tool_calls",
                   "response_format", "max_tokens", "max_completion_tokens", "stream", "stream_options",
                   "temperature", "top_p", "top_k", "min_p", "presence_penalty", "frequency_penalty",
                   "repetition_penalty", "seed", "stop", "n", "user", "logprobs", "top_logprobs",
                   "continue_final_message", "reasoning_effort", "thinking", "think", "reasoning"}
        invalid = set(payload) - allowed or not payload["messages"]
        invalid = invalid or "stream" in payload and type(payload["stream"]) is not bool
        invalid = invalid or "n" in payload and (type(payload["n"]) is not int or payload["n"] != 1)
        for message in payload["messages"]:
            if message.get("role") not in ("system", "developer", "user", "assistant", "tool", "function"):
                invalid = True
            content = message.get("content")
            if content is not None and not isinstance(content, (str, list)):
                invalid = True
            if isinstance(content, list):
                for part in content:
                    if not isinstance(part, dict) or part.get("type") not in ("text", "image_url", "input_image"):
                        invalid = True
        if invalid:
            self.write_json(400, {"error": {"type": "unsupported_request", "message": "Unsupported policy request fields, content, or generation options"}})
            return
        tools_used = any(k in payload for k in ("tools", "functions", "tool_choice", "function_call", "parallel_tool_calls"))
        tools_used = tools_used or any(m.get("role") in ("tool", "function") or "tool_calls" in m or "function_call" in m for m in payload["messages"])
        unsupported_format = payload.get("response_format", {"type": "text"}) != {"type": "text"}
        if not caps["text"] or tools_used and not caps["tools"] or unsupported_format:
            self.write_json(400, {"error": {"type": "unsupported_capability", "message": "Selected backend does not qualify this request capability or format"}})
            return
        outputs = [payload[k] for k in ("max_tokens", "max_completion_tokens") if k in payload]
        if len(outputs) > 1 or any(type(v) is not int or not 1 <= v <= self._route["max_output_tokens"] for v in outputs):
            self.write_json(400, {"error": {"type": "output_limit", "message": "Requested output exceeds selected backend limits or is invalid"}})
            return
        if not outputs:
            payload["max_tokens"] = self._route["max_output_tokens"]
        count = self.estimate_input_tokens(payload)
        # The legacy estimator falls back to a heuristic on tokenizer failure.
        # Only a successful live tokenizer result populates its request cache.
        if type(count) is not int or not getattr(self, "_token_count_cache", None):
            self.write_json(503, {"error": {"type": "tokenizer_unavailable", "message": "Pinned backend token accounting is unavailable"}})
            return
        live_limit = self.config.context_cache.get("local-auto")
        if live_limit is not None and live_limit != self._route["context_tokens"]:
            self.write_json(503, {"error": {"type": "backend_identity_changed", "message": "Backend context no longer matches the admitted plan"}})
            return
        output = payload.get("max_completion_tokens", payload.get("max_tokens"))
        if count + output > self._route["context_tokens"]:
            self.write_json(400, {"error": {"type": "context_length_exceeded",
                "message": "History plus requested output exceeds the selected backend context; no history was truncated"}})
            return
        headers = self.incoming_headers()
        if headers is None:
            return
        response = self.post_upstream(headers, payload)
        if response is not None:
            self.relay_response(response, stream=bool(payload.get("stream")), payload=payload)

    def summarize_messages(self, messages, *, model, headers):
        transcript = guard.transcript_from_messages(messages, self.config.compact_source_chars)
        payload = {"model": self._route["upstream_model"], "messages": [
            {"role": "system", "content": "Summarize this conversation to continue it. Preserve goals, decisions, constraints, file paths and unresolved tasks. Do not invent facts."},
            {"role": "user", "content": transcript}], "temperature": 0,
            "max_tokens": min(self.config.summary_tokens, self._route["max_output_tokens"]), "stream": False}
        # Strict aliases retain ordinary compaction, but its summary is also
        # GPU inference under the request's lease, not an alternate ingress.
        if self._admitted is not None:
            self.server.recovery.check(self._admitted)
            if not live(self._route):
                raise RouteUnavailable("admitted backend identity changed")
            self.server.recovery.check(self._admitted)
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


def serve(registry_path, host, port, key, recovery_state=None):
    require(isinstance(key, str) and len(key) >= 24, "gateway key must contain at least 24 characters")
    validate_registry(read(registry_path))
    args = guard.parser().parse_args([])
    cfg = guard.build_config(args)
    required = {'tokenizer_models', 'tokenizer_base_urls', 'model_timeouts'}
    require(required <= set(cfg.__dataclass_fields__),
            'Gateway requires a matching context-guard-proxy.py dependency; refresh both files')
    cfg.headroom_tokens = 512
    recovery = RecoveryRoutes(recovery_state, validate_registry) if recovery_state is not None else None
    try:
        server = GatewayServer((host, port), GatewayHandler, cfg,
                               registry_path=registry_path, key=key, recovery=recovery)
    except Exception:
        if recovery is not None:
            recovery.close()
        raise
    return server


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, required=True)
    parser.add_argument("--recovery-state", type=Path, help="Private leased local-auto route file; persisted fences live alongside it")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4110)
    args = parser.parse_args()
    server = serve(args.registry, args.host, args.port, os.getenv("SPARK_GATEWAY_KEY", ""), recovery_state=args.recovery_state)
    try: server.serve_forever()
    finally: server.server_close()


if __name__ == "__main__":
    main()
