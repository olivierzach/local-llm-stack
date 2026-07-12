# Context Guard Proxy

The OpenAI chat API is stateless: each request carries the full message history.
vLLM keeps a KV cache while a request is running, but it does not own or reset a
durable chat session. Automatic compaction therefore belongs in the client or in
a proxy that can rewrite the request before it reaches LiteLLM.

`scripts/context-guard-proxy.py` is a small OpenAI-compatible proxy for that job.
Point local tools at it instead of LiteLLM when you want oversized requests to be
compacted and retried automatically.

```bash
cd /path/to/local-llm-stack
make context-guard-up
```

For a foreground debug run instead:

```bash
make context-guard
```

Use the same LiteLLM key you already use:

```text
Base URL: http://<spark-host>:4010/v1
API key:  LITELLM_MASTER_KEY
Model:    local-gpt-oss-120b
```

For OpenClaw, the provider that currently points at Spark LiteLLM on `:4000`
must point at `:4010` instead. If the status line still says `tokens ?/120k`
for this 4k GPT-OSS launch, the client metadata is still wrong; set that model
to `4096` context tokens or route through this proxy with
`CONTEXT_GUARD_MODEL_CONTEXTS=local-gpt-oss-120b:4096`.

The proxy does three things for `/v1/chat/completions`:

1. Clamps invalid `max_tokens` values, including negative values from a bad
   client-side budget calculation.
2. Estimates the request size against the model's context window.
3. Summarizes older messages, keeps the recent tail, and retries the same model.

It preserves leading `system` and `developer` messages, inserts a generated
summary as a new `system` message, then keeps the most recent messages.

## Useful Settings

The proxy auto-loads `.env` and also accepts environment overrides.

```bash
CONTEXT_GUARD_PORT=4010
CONTEXT_GUARD_UPSTREAM_BASE_URL=http://localhost:4000/v1
CONTEXT_GUARD_DEFAULT_OUTPUT_TOKENS=4096
CONTEXT_GUARD_MIN_OUTPUT_TOKENS=64
CONTEXT_GUARD_HEADROOM_TOKENS=128
CONTEXT_GUARD_KEEP_LAST_MESSAGES=8
CONTEXT_GUARD_SUMMARY_TOKENS=512
CONTEXT_GUARD_COMPACT_MODEL=local-fast
CONTEXT_GUARD_COMPACT_SOURCE_CHARS=6000
```

By default, the Compose service passes each model's `*_MAX_MODEL_LEN` setting to
the guard and uses `local-fast` for summarizing older history before falling
back to the requested model. The compact source is intentionally small enough
for a low-memory `local-fast` helper. If compaction still cannot fit, it resets the
request to the newest user prompt. If a client advertises the wrong context
size, pin it explicitly:

```bash
CONTEXT_GUARD_MODEL_CONTEXTS=local-gpt-oss-120b:16384 make context-guard
```

For a larger GPT-OSS launch, match the live vLLM context limit:

```bash
CONTEXT_GUARD_MODEL_CONTEXTS=local-gpt-oss-120b:32768 make context-guard
```

By default, the proxy requires an incoming `Authorization: Bearer ...` header and
passes it through to LiteLLM. For a private one-off local test only, you can allow
missing auth and have the proxy use `LITELLM_MASTER_KEY` from `.env`:

```bash
CONTEXT_GUARD_ALLOW_NO_AUTH=true make context-guard
```

## Limits

This is a practical `/compact`-style rewrite, not a memory reset inside vLLM. If
the client keeps sending an oversized full history, the proxy will compact that
history on each request. For best results, configure the client to use the real
model context window reported by vLLM:

```bash
curl -sS http://localhost:8009/v1/models
```


## Run As A Service

Start or restart the persistent proxy:

```bash
make context-guard-up
docker compose logs -f context-guard
```

On this Spark host, if Docker group membership is not active in the shell, use:

```bash
sg docker -c 'docker compose up -d context-guard'
sg docker -c 'docker compose logs -f context-guard'
```
