# Qwen TP2 + MTP tool calling

The additive recipe `qwen3-next-80b-256k-mtp2-tools-nccl2307` enables the
Hermes parser and automatic tool choice, retaining the accepted BF16 TP2/MTP2
runtime. It supports text and tool requests, not images. OMP or another client
executes tools; the inference server and Context Guard do not execute them.

The old text-only recipe remains available for rollback. Both coordinator
placements have manifests named `large-tp2-mtp2-tools-nccl2307-{66f1,e8f1}.json`.
Never start this recipe alongside the existing deployment: stop only its exact
saved plan first. Keep that plan and its acceptance receipts.

From an installed controller (replace RUN with a fresh output directory):

```bash
scripts/sparkctl up --deployment cluster/deployments/large-tp2-mtp2-tools-nccl2307-66f1.json --output RUN --timeout 3600
.venv/bin/python scripts/probe-spark-tool-calling.py --saved-plan RUN/plan.json --output RUN/tools.json
.venv/bin/python scripts/accept-spark-serving.py --saved-plan RUN/plan.json --output RUN/serving
```

The tool probe tests automatic, named and required choice in both streaming and
non-streaming modes, real returned values, and speculative acceptance counters.
It executes synthetic lookups only. It saves failures and never changes routes.

After direct acceptance, publish the same client alias on each node:

```bash
.venv/bin/python scripts/configure-context-routes.py --root ~/projects/local-llm-stack --plan RUN/plan.json --merge --alias local-qwen3-next-80b
scripts/spark-gateway routes --node NODE --plan RUN/plan.json --merge --alias local-qwen3-next-80b
```

Run `probe-context-route.py --tools` from the production checkout (where the guard
key is configured), `probe-spark-gateway.py` against the managed gateway, and
`probe-spark-omp.py --node NODE --model local-qwen3-next-80b` from the controller.
Each requires a fresh `--output` filename. Preserve unrelated routes.

Once acceptance passes, the existing Mac provider can use:

```bash
omp --model spark-context-guard/local-qwen3-next-80b
```

Implementation reference: [vLLM's Qwen3-Next recipe](https://github.com/vllm-project/recipes/blob/main/Qwen/Qwen3-Next.md).
Verified deployment results are recorded below.

To verify the existing Mac provider with a real, bounded read-tool round trip:

```bash
.venv/bin/python scripts/probe-spark-omp.py --provider spark-context-guard --model local-qwen3-next-80b --output RUN/omp-mac.json
```

This creates a temporary file with an unpredictable value, requires OMP to read
that exact file, and verifies the final answer uses the tool result. It does not
change the provider, save a chat session, or load extensions.

The pinned vLLM runtime deliberately returns `finish_reason: stop` for explicitly
named tool choices, while automatic/required tool calls use `tool_calls`.
Acceptance checks the complete function name, JSON arguments, call ID and result
continuation in either case. The first candidate run passed automatic calls but
triggered rollback because the initial probe incorrectly required `tool_calls`
for named choices. Its failure receipt is retained under `qwen-tools-01`.

## Direct tool acceptance

The corrected `qwen-tools-02` run passed 36 direct requests on the 66f1
coordinator: three rounds of automatic, named and required tool choices, each
with streamed/non-streamed calls and tool-result continuations. Native MTP
accepted 756 draft tokens during these synthetic short conversations. This is
protocol evidence, not a general acceptance-rate or tool-use quality benchmark.
The recipe and deployment digest remained unchanged when correcting the probe:
`a83ebf27f1b5f0ecbc92288191c206660ac5b13b3815ed411dea90e1383e3d20`.

Receipts live in `data/cluster/serving-20260911/qwen-tools-02/` (shared installed
controller state uses the same suffix). The text-only baseline and failed first
probe receipts remain available. The e8f1-coordinator tool manifest is provided
and statically validated; this tool-enabled live acceptance uses 66f1 as the
coordinator and both Sparks as workers.

## Accepted service and clients

The `qwen-tools-02` deployment is left running with 66f1 coordinating both
Sparks, at `http://10.10.20.1:8121/v1`, using internal model `local-large`.
The source used for serving and gateway acceptance was
`f0c12dbc15ad9991ba4ab9451cebcac26c4777c3`. The runtime image, NCCL library,
BF16 weights, direct RoCE cable, 262144 context limit and MTP=2 remain the same
as the accepted baseline. No new model weights were downloaded for tool calling.

The complete serving suite passed in 839 seconds after startup:

- Three 1024-token answers: 49.92, 53.60 and 47.43 decode tokens/s.
- 260031-token retrieval: all codes recovered; first-token latency 137.30s fresh,
  1.20s repeated, with 259280 prefix-cache hits.
- Eighteen sequential 1024-token answers/continuations: median 47.98 tokens/s;
  MTP accepted 10840 of 15190 drafted tokens (71.4%).
- Three 4096-token answers: 48.99, 54.63 and 49.68 tokens/s.
- Both workers remained healthy without a restart during this acceptance run.

Both existing 4010 Context Guards and independent 4110 gateways passed text,
streaming, tool calls and tool-result continuations. The Context Guard probes
also verified exact token accounting and rejected an invalid key. Unrelated
routes were preserved. Mac gateway attachments were refreshed, and generated
client profiles advertise tools on either gateway.

Real OMP read-tool round trips passed on both Sparks and through the existing
Mac provider. On Mac OMP **18.1.18**, an additional isolated coding test read two
files, edited a faulty function, ran its Python test with the bash tool, and
returned the actual success marker. These are bounded protocol and workflow
checks, not broad code-quality or arbitrary-extension certification. Fam Chat's
authenticated UI was not separately exercised.

Your existing session can retry, or start a new session from the Mac:

```bash
omp --model spark-context-guard/local-qwen3-next-80b
```

There is no new provider, endpoint or model name to configure. Tool execution
occurs on the machine running OMP, while inference runs across the two Sparks.
No `--no-tools` or `--no-extensions` flag is required by this serving recipe.

From either installed controller, inspect the current deployment:

```bash
cd ~/projects/local-llm-stack-cluster/current
qwen_plan="$HOME/projects/local-llm-stack-cluster/state/serving-20260911/qwen-tools-02/plan.json"
scripts/sparkctl status --saved-plan "$qwen_plan"
# Stop only when intentionally releasing both GPUs:
# scripts/sparkctl down --saved-plan "$qwen_plan"
```

For rollback, stop this exact plan, start the original
`large-tp2-mtp2-nccl2307-66f1.json` manifest with a fresh output directory, and
republish its plan with the same alias and `--merge` on both gateways. That
restores text-only capability without changing unrelated aliases.
