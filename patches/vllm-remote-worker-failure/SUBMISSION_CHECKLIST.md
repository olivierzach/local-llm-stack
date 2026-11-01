# Remote-worker disconnect fix

[PR #61558](https://github.com/vllm-project/vllm/pull/61558) is ready for review. Head: [`3d76823`](https://github.com/olivierzach/vllm/commit/3d76823ad86d92dd3a11da5e534f5a050d931248). Depends on #53597; merge that first.

## Change

- Monitor remote-worker and coordinator disconnects through ZMQ.
- Cancel blocked reads without dropping buffered replies.
- Close cancellation sockets during finalization; retain queues until their monitor sockets close.

The author confirmed code review, personal checks and DCO coverage. AI assistance is disclosed in the PR and commit trailers.

## Validation

| Check | Result |
| --- | --- |
| Executor and queue CPU tests | 88 passed, 2 skipped |
| Ruff 0.14.0 | Check and format check passed for six files |
| Two-host Qwen3-4B | TP2/PP2 forward failure, immediate exit, synchronous scheduling and coordinator loss passed |
| Cleanup after those faults | Both hosts cleared within 3.63–13.90 s |
| Matched prerequisite control | Request timed out at 45.12 s; coordinator/GPU context remained alive |
| Normal requests | Single/concurrent generation and cancellation recovery passed |

The missing-LoRA check returned 404 and subsequent inference worked, but the next utility RPC consumed a stale error. That existing issue, the original CUDA fault and silent partitions are not fixed here. Startup failures and throughput were not tested. The vendor cuSPARSELt wheel's SBSA-tag warning is recorded in the runtime evidence.

Agent-run results are separate from the author's personal test confirmation. Exact commands and model configuration are in [the PR description](PR_DRAFT.md); hashes and receipts are in [verification.json](direct-exit/verification.json) and [provenance.json](provenance.json).

## Reproduce the retained CPU run

From this repository root:

```bash
bash logs/incidents/2026-10-01-vllm-worker-failure/integrated-pr-20261014-b678678d/review-integration.sh
```

This uses the retained immutable image on `spark-e8f1-wired`: no GPU devices or container network, two CPUs, 3 GiB RAM, bounded execution. Do not replace its image with unrelated native binaries. Further GPU fault tests require operator approval and isolated test servers; do not alter normal serving.

## History and evidence

The message-only cleanup reworded our two commits: `c396235` → `c52a711` and `35d0948` → `3d76823`. Both source trees are unchanged; the final tree remains `18aa0a99031aa0977b3effca9938cdc78a17ede9`. The user authorized the rewrite with an explicit expected-head lease. Nanfang Li's three prerequisite commits were not changed.

- Qualification: `logs/incidents/2026-10-01-vllm-worker-failure/integrated-pr-20261014-b678678d/` — 471 sealed evidence files.
- Initial integrated publication: adjacent `integrated-pr-20261014-b678678d-publication/` directory.
- Wording pass and previous checklist: adjacent `pr-wording-20261014-742a6668/` directory.
- `direct-exit/follow-up.patch` and `direct-exit/source/` retain the original c396235 inputs, not the integrated source. Earlier results remain in the verification records.

All test/build containers were removed. Qualified images and reproduction inputs remain; normal workloads were unchanged. The watcher remains stopped.

## Remaining gates

Prerequisite merge, maintainer approval and required CI. The last observed pre-run-check failed contributor/label eligibility, not a code test; pre-commit was skipped. Agents must not request qualifying labels or bypass that gate.
