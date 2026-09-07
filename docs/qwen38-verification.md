# Qwen3.8 Verification Results

Date: 2026-09-07. Host: single DGX Spark, NVIDIA GB10, 121 GiB reported RAM,
driver 580.159.03. Reproduce with the [operator runbook](qwen38-flash-next.md).
Backend versions are pinned in `config/qwen38-pins.env`.

## Verified

| Check | Result |
| --- | --- |
| Pinned artifacts | ARM64 image, recipe commit, checkpoint revision; 35 indexed shards, 105839538520 weight bytes |
| Startup and generation readiness | Passed, about 12 minutes on first model startup |
| Live context | 262144 tokens reported by the backend |
| Direct, LiteLLM, Guard | Successful responses; small Guard request unchanged |
| Native token accounting | Small input 17 tokens; streaming tool request 281; continuation 326; image request 87 |
| Streaming tools | Complete tool-call arguments followed by a successful tool-result continuation |
| Vision | Correctly identified the red-square fixture |
| Proactive overflow | Reduced oversized input to 260032 tokens; successful response and compacted header, streaming off and on |
| Reactive overflow | Isolated fault-injection Guard observed HTTP 400 then 200, retry header 1; streaming off and on |
| Impossible completion reservation | Clamped to 260079 with 17 input tokens and 2048 headroom; preserved the prompt/template, no unnecessary compaction |
| AIChat and OpenCode | Real CLI requests through Guard both passed |
| Automated tests | 53 passed at live handoff; 61 passed after offline networking and verification-sequence regression coverage |
| Configuration checks | Compose validation, wrapper shell syntax and git whitespace checks passed |
| GPU release | Qwen and watchdog stopped; DS4 inactive; no LLM compute processes remained |

The final uncached proactive-overflow request took 145.37 seconds end to end;
the repeated streaming request took 7.63 seconds and benefited from prefix
caching. These are synthetic recovery tests, not general throughput or recall
benchmarks. Tiny buffered completions must not be interpreted as decode-speed
measurements. Available memory after the final live checks was about 13.3 GiB;
after shutdown it was about 115 GiB. Swap allocation decreased during testing;
remaining allocated swap alone does not establish active swap thrashing.

Raw local results are under `evals/runs/qwen38`:

- `20260907T051712-acceptance.jsonl`: final acceptance suite after the budget fix.
- `20260907T051633-clients.jsonl`: real terminal-client checks.
- `*-recovery.jsonl`: real backend overflow with isolated test-proxy undercounting.
- `*-probe.jsonl` and `*-smoke.jsonl`: startup and routing checks.

Archived backend and memory-watch logs are under
`logs/qwen38-flash-next/archive`. Fixtures and recorded results contain no
credentials or user conversation history.

## Deferred Or Not Proven

After the training handoff, an offline check found that the standalone host Guard
command still assumed localhost for Qwen's private backend. The Make target now
discovers the Docker bridge, with explicit endpoint overrides supported. Compose
routes also honor a custom Qwen bind address consistently. Seven regression
cases cover URL normalization, override precedence, host-command resolution and
Compose configuration. No services were restarted for this change; custom-bind
live verification remains pending the next permitted GPU test window.

The dedicated uncached 32K/128K/near-boundary concurrency benchmark and 45-minute
soak were deferred to release the GPU for training. The user confirmed that the
soak remains part of the full verification sequence, after the benchmark.
Neither script has passed a full run yet. Long-context recall quality is
not established by these synthetic tests. Neither Mac OMP/OpenClaw nor the
authenticated Open WebUI browser workflow was exercised in this run.
Open WebUI retains its existing Guard route; external clients need the model
entry described in the runbook.

DS4's rollback command is documented and its configuration was preserved, but
a complete Qwen-to-DS4-to-Qwen restart cycle was not performed in this run.
Interrupted-stream unit tests exercise the test client's rejection behavior;
they do not prove every external agent's tool-execution policy or guarantee
that every session can recover without user intervention.

## Training Handoff

Qwen is intentionally offline, not failed. LiteLLM, Guard and the UI remain
running, but the Qwen/DS4 aliases cannot generate while their GPU backends are
stopped. No model files were deleted. The root `.env` was not changed.

After training releases the GPU:

```bash
make qwen38-up
make qwen38-smoke
```

To perform the deferred work with exclusive GPU access:

```bash
make qwen38-bench
make qwen38-soak
```

To release the GPU again, use `make qwen38-down`. To restore DS4 instead of
Qwen, use `make deepseekv4-up`; do not run that command during training.
