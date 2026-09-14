# GLM53 TP2 hardware qualification

**In progress. These partial measurements do not authorize publication.**

The candidate and reproduction commands are in [GLM53_TP.md](GLM53_TP.md).
Measurements below come from this pair of Sparks, not upstream benchmarks.

## Placement and artifacts

- Coordinator: `e8f1`; worker: `66f1`; TP2 over the direct RoCE cable.
- Saved-plan digest: `27ab7d3afe6843e19356d14e359a247d7f6fd428ca4105a6f99590b4fc79ae78`.
- Plan on 66f1: `~/projects/local-llm-stack-cluster/state/glm53-20260913/e8f1-03/plan.json`.
- Current campaign: the adjacent `qualification-02/` directory; per-check receipts
  are in `serving/`, with both-node memory samples and before/after runtime snapshots.
- Both pinned model snapshots and the runtime image were verified on both nodes.
  Peer copies used the verified `10.10.20.1` to `10.10.20.2` cable route.
- The original image failed to load the first dense MLP. The hash-pinned GLM5Next
  mapping override fixes BF16 ignore matching without changing checkpoint weights.
  The CPU preflight reproduces the original bug and checks 132 dense/shared/expert
  projections with the corrected runtime on each node.

## Completed primary-placement checks

| Check | Observation |
| --- | --- |
| Basic features | Text/SSE, reasoning off/on, two reversed image-color fixtures, 20 repeated exact arithmetic answers passed |
| Tools | 24 call/result checks passed: auto/named/required, streaming and non-streaming, two rounds |
| Speculation during tools | 444 target-accepted tokens out of 540 proposed draft tokens |
| 1,024-token prose/code/planning | 25.87 / 33.62 / 26.78 visible decode tokens/s |
| Cold varied-context retrieval | 259,992 input tokens; 214.26 s to first token; all three planted values correct |
| Repeated-prefix retrieval | 2.28 s to first token; 258,048 prefix tokens reused; all values correct |
| Long-history continuation | 260,020 input tokens, 1,024 output tokens; 32.83 decode tokens/s |
| Sequential soak | 18 requests, 18,101 generated tokens; median decode 24.94 tokens/s; no failed requests |
| 4,096-token prose/code/planning | 27.62 / 40.20 / 23.20 visible decode tokens/s |
| Post-load features | All text, reasoning, image and 20 repeated-answer checks passed again |

The cold long-context timing includes first-use kernel compilation. Repeated
prefix timing benefits from both warmed kernels and the measured prefix hits.

### Concurrency screen

Each level uses four measured requests, 256 output tokens per request, a separate
warmup and unique prompt prefixes. Aggregate rates include prefill and decode.

| Input target | Active requests | Aggregate output tokens/s | Median per-request decode tokens/s | Median first token, seconds |
| --- | ---: | ---: | ---: | ---: |
| 1K | 1 | 22.09 | 24.12 | 0.97 |
| 1K | 2 | 29.56 | 16.63 | 1.20 |
| 1K | 4 | 40.36 | 11.73 | 3.31 |
| 8K | 1 | 14.96 | 23.35 | 6.32 |
| 8K | 2 | 18.20 | 14.49 | 6.83 |
| 8K | 4 | 22.51 | 10.08 | 18.23 |

Four scheduler slots improve multi-request throughput but do not accelerate one
answer. These results do not establish four simultaneous full-window requests.

### Memory and cable

The complete 34-minute campaign retained at least 6.10 GiB available on 66f1 and
4.06 GiB on e8f1. Neither node swapped new pages out. Existing swapped pages were
paged in; this is not a claim that swap was empty. Maximum full-memory PSI
`avg10` was 4.1% on 66f1 and 0% on e8f1, below the 5% gate.

The two logical RoCE interfaces sent approximately 277.6 and 276.5 GiB from 66f1,
with matching receive counters on e8f1 and similar traffic in the opposite
direction. Link-error and discard counters did not increase. NCCL selected
`NET/IB`; its logs report `GDR 0`, so this evidence establishes RoCE transport,
not a separate claim of GPU-direct zero-copy. Both interfaces share the single
physical 200-GbE cable; their link labels are not additive capacity.

One earlier streaming reasoning response returned the correct `49` answer in
Markdown with an explanation despite an integer-only instruction. The capability
probe now requires the explicit correct answer line and separately records exact
format compliance. It does not accept a correct number merely appearing in prose.
The subsequent full feature check obeyed the requested format in both modes.

These synthetic tests establish basic capabilities and serving behavior. They are
not an OCR/document benchmark or a general reasoning/coding accuracy evaluation.
The long-history continuation is timed, not scored as a factual analysis of its
input. Short answers and predictable output can show unusually high token rates.

## Still required

The primary full-profile gate passed. Repeat the role-check suite with `66f1`
coordinating, restore the fully qualified placement, publish
through both Context Guards, and verify actual clients before marking this ready.
