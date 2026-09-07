# Qwen3.8 Flash Next On One DGX Spark

This optional backend runs the MiaAI-Lab single-Spark recipe using its own
container. Model ID: `local-qwen38-flash-next`. Client API:
`http://<spark-host>:4010/v1`, authenticated with the existing LiteLLM key.

See [verified results and remaining test gaps](qwen38-verification.md) before
assuming the long-context benchmark or extended soak has passed.

## Install

Run from the repository root on the Spark. Requirements: working Docker GPU
access, Git, curl, Python 3 with venv support, and at least 150 GiB free disk
plus room for the image. The existing stack must already have its local `.env`.

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[test]'
make qwen38-install
make qwen38-check
```

Install pins the recipe commit, ARM64 image digest, and Hugging Face checkpoint
revision listed in `config/qwen38-pins.env`. Downloads resume when the install
command is rerun. The shared cache stays in `data/huggingface`; the recipe and
compiled/PLE caches live in `data/qwen38-flash-next`. Install does not stop DS4.
Do not run two install commands concurrently.

## Switch And Verify

```bash
make qwen38-up
make qwen38-status
make qwen38-smoke
make qwen38-test
make qwen38-recovery
make qwen38-clients
make qwen38-bench
make qwen38-soak
```

`qwen38-up` verifies the pinned artifacts before stopping DS4 and the Compose
model servers. This is a planned inference interruption: those model aliases
are unavailable while Qwen occupies the GPU. It starts Qwen, validates a small
generation, and refreshes LiteLLM and Context Guard. Allow up to 30 minutes for
first startup (normally about 10-12 minutes upstream). Failure reports the
rollback command. Do not launch embedding/training or other GPU jobs alongside it.

`qwen38-clients` runs real AIChat and OpenCode CLI requests through the Guard.
Mac OMP/OpenClaw configuration still requires the client-side steps below.

The smoke command checks direct inference, LiteLLM, streaming through Context
Guard, and agreement between native token counts and the Guard headers. The
acceptance command checks streaming tool calls, continuation after a tool result,
image input, overflow compaction, and completion-budget clamping. Benchmarking
exercises uncached 32K, 128K, near-boundary input and concurrency 1/2/4.
Decode estimates are reported only for sufficiently long completions, not tiny
responses where buffered chunks can produce misleading tokens-per-second figures.
Any failed assertion exits nonzero.

The recovery command deliberately undercounts requests in an isolated test proxy
to exercise real backend overflow and retry, with streaming both off and on.
It does not weaken the production Guard on port 4010. Run GPU test commands
sequentially, with other inference clients idle, for comparable measurements.

Results are JSONL under `evals/runs/qwen38`, containing timings, usage, memory,
and Guard headers, without credentials or user conversation content.

For the entire sequential verification sequence after startup, run
`make qwen38-verify`. It stops on the first failed command and includes the
45-minute soak after the benchmark. Reserve about an hour of exclusive inference
access. It does not install artifacts or switch models, so it will not silently
stop DS4.

The soak can also be run separately with `make qwen38-soak`. It repeats synthetic
roughly 90K-token prompts for 45 minutes to check serving stability, not model
quality. It remains part of the full verification path; it was deferred for the
training handoff, not removed or waived.

## Clients

```bash
make aichat MODEL=local-qwen38-flash-next
make opencode MODEL=local-qwen38-flash-next
```

Select `local-qwen38-flash-next` in Open WebUI after refreshing its model list.
Existing client defaults stay unchanged; choose the Qwen entry explicitly.

For Mac OMP, add a model to the existing `spark-context-guard` provider:

```json
{
  "id": "local-qwen38-flash-next",
  "name": "Qwen3.8 Flash Next",
  "contextWindow": 262144,
  "maxTokens": 32768,
  "reasoning": true,
  "input": ["text", "image"]
}
```

Keep the provider's existing API key, `api: openai-completions` and base URL
`http://spark-66f1:4010/v1`. Select
`spark-context-guard/local-qwen38-flash-next`. Use the same model ID and limits
for an OpenClaw OpenAI-compatible provider. Mac configurations are external to
this repository; Spark-side tests alone do not prove those clients' behavior.

For Datasette LLM, add this entry to `extra-openai-models.yaml` in its user
directory, beside the existing Spark models:

```yaml
- model_id: spark-qwen38-flash-next
  model_name: local-qwen38-flash-next
  api_base: http://spark-66f1:4010/v1
  api_key_name: spark
  can_stream: true
  supports_tools: true
  aliases: [local-qwen38-flash-next]
```

Then run `llm chat -m local-qwen38-flash-next`. Use the existing `spark` key.
For reproducible API tests, the checked-in verifier supplies complete requests;
no shell heredoc or manual API-key insertion is required.

## Configuration

Add overrides to the ignored root `.env`. Omitted values use these defaults:

| Variable | Default |
| --- | --- |
| QWEN38_MAX_MODEL_LEN | 262144 |
| QWEN38_MTP_TOKENS | 3 |
| QWEN38_KV_TARGET_GIB | 20 |
| QWEN38_HOST_RESERVE_GIB | 26 |
| QWEN38_KV_CACHE_DTYPE | fp8 |
| QWEN38_SSM_DTYPE | bfloat16 |
| QWEN38_MAX_NUM_SEQS | 4 |
| QWEN38_BATCHED_TOKENS | 2048 |
| QWEN38_PORT | 8012 |
| QWEN38_STARTUP_TIMEOUT | 1800 |

The KV target is a wish capped by the host reserve. The launcher refuses a
reserve below 26 GiB or a context above the native 262144. Keep PLE offload
enabled. The upstream memory watchdog remains active. Kernel sysctl values are
not changed. FP8 KV and BF16 recurrent state need workload-specific quality
evaluation; memory capacity alone is not evidence of useful recall.

`QWEN38_BIND_HOST` defaults to Docker's host bridge; the backend is not exposed
on every host interface. The public client path stays the authenticated Guard.
When overriding it, choose an IPv4 address reachable from the Compose containers,
not a loopback address. Both LiteLLM and the Guard tokenizer route use this
override. The host-side `make context-guard` command discovers the same bridge
and exports `QWEN38_API_BASE`; an explicit `CONTEXT_GUARD_TOKENIZER_BASE_URLS`
entry still takes precedence. When invoking the Python proxy directly, supply
`QWEN38_API_BASE` yourself. Stop the Compose Guard before using the same host port.
An optional `QWEN38_DRAFT_VOCAB` names an existing reduced-vocabulary artifact.
It is empty by default because upstream does not distribute the vocabulary used
for its headline measurements. MTP still works with the full vocabulary.
We do not claim those headline speeds for this reproducible configuration.

Changing launch settings requires `make qwen38-down` then `make qwen38-up`.
Update external client context metadata if the context changes. The Guard reads
the live limit and retains a configured fallback. The Qwen route permits up to
600 seconds per upstream read to accommodate long prefills.

## Monitor And Roll Back

```bash
make qwen38-logs
free -h
make qwen38-down
make deepseekv4-up
make deepseekv4-status
```

Logs and memory-watch timelines live under `logs/qwen38-flash-next`. Stop archives
the logs and reports leftover shared-memory segments without deleting segments
belonging to another process. Restoring DS4 preserves its configured context and
DSpark setting; DSpark remains enabled by default. Cached Qwen files stay on disk.

`make qwen38-dry-run` prepares patches/PLE cache and prints the launch command
without loading the model. It can still allocate CPU memory while building the
PLE cache, so run it with the GPU model stopped. It refuses to rewrite mounted
patches while Qwen is running.

## Provenance

Upstream: https://github.com/MiaAI-Lab/Qwen3.8-Flash-Next-Single-DGX-Spark

The upstream recipe is AGPL-3.0-or-later. Its license remains in the checkout.
`patches/qwen38-runtime.patch` is a modification of that recipe and is supplied
under AGPL-3.0-or-later with the same upstream copyright; it adds private binding,
explicit cache paths and deterministic checkpoint selection. Upstream patch
generators modify the pinned vLLM image's source at runtime. Preserve all upstream
notices and provide the corresponding recipe sources and patch when required.
The independently written local wrapper does not change the repository license.
Checkpoint metadata currently declares Apache-2.0; retain its model card and
review upstream model terms for your deployment.
