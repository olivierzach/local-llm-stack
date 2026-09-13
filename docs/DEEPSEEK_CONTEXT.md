# Qualifying larger DeepSeek TP2 contexts

The 65,536-token launch limit was an initial qualification boundary. It was
not the two-Spark memory limit. The pinned `DeepSeek-V4-Flash-0731` checkpoint
declares `max_position_embeddings: 1048576` and its own YaRN scaling configuration.
Use that native ceiling; do not invent a larger RoPE override from free RAM.

## What the memory buys

Both Sparks load their tensor-parallel portions of the official mixed FP4/FP8
checkpoint. The accepted correctness fix uses BF16 expert **activations**, not
BF16 copies of all weights. Target plus DSpark draft loading reports about
80.59 GiB per GPU. The OS, non-Torch allocations, activation workspaces, graphs,
and KV caches also share each Spark's 121.69 GiB of usable unified memory.

The target has 43 layers: two sliding-window-only layers, 21 layers compressing
history by four, and 20 compressing by 128. The sliding window is 128 tokens.
For an input length `L`, the compressed attention state count grows approximately
as `21 × ceil(L/4) + 20 × ceil(L/128)`, plus sliding-window state. This is not a
byte-capacity formula: the indexer, scales, padding, draft state, cache groups,
prefix retention and allocator fragmentation also cost memory.

The installed attention implementation uses one KV head per rank and replicated
indexer projections. **Do not add the two nodes' KV capacities together.** TP2
primarily makes room by splitting weights; each rank still needs its cache state.
The relevant implementation is `vllm/models/deepseek_v4/attention.py` in the
pinned runtime, particularly `get_kv_cache_spec` and `DeepseekV4Indexer`.

At the 1M launch limit and the original 0.80 memory fraction, the measured vLLM
allocator reports 14.12 GiB of available KV space per GPU and 2,959,465 cache
tokens, or an estimated 2.82 full-length requests. That is an allocator estimate,
not acceptance of concurrent requests. The recipe schedules one request at a
time. The earlier 83% experiment was unnecessary for capacity and left less OS
headroom; the qualification uses 80%.

Changing the configured maximum length changes hybrid-cache allocation ratios.
Consequently, extrapolating the earlier 64K allocator report linearly also gives
the wrong answer. Measure the allocator and host pressure for the exact recipe.

## Reproduce the qualification

Run from `~/projects/local-llm-stack-cluster/current` on either Spark. Release the
current deployment with its exact saved plan before starting another GPU owner.
The extended variants use port 8123 and rendezvous port 29543; they still require
both GPUs exclusively. A separate port does not create extra GPU memory.

```bash
make deepseek-tp-up COORDINATOR=66f1 DEEPSEEK_TP_CONTEXT=1m \
  DEEPSEEK_TP_SPEC=dspark2 DEEPSEEK_TP_EXECUTION=graphs \
  OUTPUT=data/cluster/deepseek-1m-run

make deepseek-tp-context-accept \
  PLAN=data/cluster/deepseek-1m-run/plan.json \
  OUTPUT=data/cluster/deepseek-1m-qualification SWEEP=1
```

`DEEPSEEK_TP_CONTEXT` also accepts `256k`, `512k`, and `768k`. The existing `64k`
default and manifests remain available. Extended contexts require the qualified
BF16 expert activation, DSpark2, CUDA graph combination. A manifest's existence
does not qualify that context size or a changed runtime.

The optional sweep tests 64K, 128K, 256K, 512K, 768K and 1M on the same running
deployment. It sizes a varied synthetic archive with the model's real tokenizer,
retrieves three random codes, repeats the prefix, and measures a 1,024-token
answer. Full acceptance additionally tests near the actual launch limit,
100 repeated answers before and after load, tool protocols, thinking, sequential
soak, and answers with a 4,096-token cap. Host memory, paging, memory-pressure
samples, runtime logs and direct-fabric counters are retained with the receipts.
Publication of an extended context additionally requires both nodes' samples to
cover the serving suite, at least 4 GiB minimum available host memory, less than
5% full memory pressure over any sampled 10-second average, and less than 256 MiB
of swap-out during the campaign. These are explicit operational margins, not
claims that lower memory availability necessarily causes an OOM. Already occupied
swap alone does not indicate active paging.

After qualification, release that exact plan and repeat with `COORDINATOR=e8f1`.
Both coordinator roles must pass the identical recipe before publication. Use
`scripts/publish-deepseek-tp.py` with both saved plans and acceptance directories
on each gateway host. It preserves unrelated aliases and checks the live
deployment digest. Neither node has a permanent master role.

## Keep the client path usable

The alias stays `local-deepseek-v4-flash`. The registry publishes its new context
and output limits together with the selected backend. Generated OMP/OpenClaw/
AIChat profiles inherit these limits. Existing OMP model overrides must also
have `contextWindow` updated, followed by `omp models refresh spark-context-guard`;
no second provider URL or model alias is required.
`scripts/update-omp-context-limit.py --saved-plan PLAN --output BACKUP_DIR --apply`
changes only that existing override and keeps a private backup. Run it after
publishing the accepted plan, then refresh the OMP catalog.

Registry routes generated for contexts above 64K carry a 3,600-second upstream
read timeout and a 180-second tokenizer timeout. These are bounds for long
prefill/tokenization, not a latency promise. Routes without those fields retain
their existing defaults. `scripts/refresh-spark-gateway-policy.py` updates both
CPU gateways with a source-hash precondition and rollback, retaining keys and
routes; it does not restart model workers.

Verify a large prompt through the actual published gateway, rather than only
the raw model server:

```bash
.venv/bin/python scripts/probe-spark-long-context.py \
  --saved-plan data/cluster/deepseek-1m-run/plan.json \
  --input-tokens 1040384 --corpus varied --measure-decode \
  --gateway-url http://127.0.0.1:4110/v1 \
  --key-file ~/.local/state/local-llm-cluster/gateway/api-key \
  --output data/cluster/deepseek-1m-gateway.json
```

This checks the deployment digest, advertised context, exact input accounting,
absence of compaction, retrieval and prefix reuse. The ordinary 2,048-token guard
margin still applies, and input plus requested output must fit the total context.

Long fresh inputs take minutes to read. Cached continuations can be much quicker,
but cache eviction or a changed prefix requires that work again. Increasing the
maximum permits larger requests; it does not make a short prompt 16 times slower
or improve the model's answer quality by itself. Synthetic retrieval is a useful
regression test, not proof of equal reasoning quality at every input length.
