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
Live deployment results are recorded below after verification.

To verify the existing Mac provider with a real, bounded read-tool round trip:

```bash
.venv/bin/python scripts/probe-spark-omp.py --provider spark-context-guard --model local-qwen3-next-80b --output RUN/omp-mac.json
```

This creates a temporary file with an unpredictable value, requires OMP to read
that exact file, and verifies the final answer uses the tool result. It does not
change the provider, save a chat session, or load extensions.
