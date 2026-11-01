<!-- markdownlint-disable -->

## Overview

Propagate remote-worker and coordinator disconnects through the multiprocessing executor's shutdown path. Local process sentinels cannot detect a worker exiting on another host.

Depends on #53597; merge that first.

## Claims

- Fail pending requests and stop peer workers when a remote worker disconnects.
- Exit a worker on coordinator loss, even when its main thread is blocked in a collective.
- Cancel empty socket reads without dropping buffered replies. Keep normal shutdown and receive timeouts nonfatal.

## Validation

**88 passed, 2 skipped** on Linux ARM64. Ruff 0.14.0 checks and formatting passed for all six changed files.

```bash
/opt/final-env/bin/python -m pytest --noconftest -c /dev/null \
  -p pytest_timeout -p queue_log_fixtures -p no:cacheprovider \
  /opt/final-source/tests/v1/executor/test_multiproc_executor.py \
  /opt/final-source/tests/v1/executor/test_multiproc_executor_fail_fast.py \
  /opt/final-source/tests/distributed/test_shm_broadcast.py -q
```

The isolated runner uses the repository's queue logging fixtures. Tests exercise real processes, sockets and SHM queues with synthetic CPU model methods. Both new lifetime regressions failed with their respective fix removed.

Two GB10 hosts, **Qwen/Qwen3-4B-Instruct-2507** at `cdbee75f17c01a7cc42f958dc650907174af0554`: bf16, eager, 2,048-token context, two sequences, 512 MiB KV cache, greedy sampling, seed 42. Normal/concurrent generation and cancellation recovery passed before each fault.

| Fault | Request result | Both-host cleanup |
| --- | --- | --- |
| Prerequisite-only TP2 control | Timeout at 45.12 s; coordinator/GPU context still alive | Manual cleanup required |
| TP2 forward exception, async | HTTP 500 at 9.18 s | Within 12.69 s |
| PP2 forward exception, async | HTTP 500 at 9.21 s | Within 13.90 s |
| TP2 immediate worker exit | HTTP 500 at 9.17 s | Within 12.83 s |
| TP2 forward exception, synchronous | HTTP 500 at 0.16 s | Within 3.63 s |
| Coordinator killed during a TP2 stream | RemoteProtocolError | Within 4.15 s |

One run per case. Cleanup bounds include probe-launch overhead; cleanup was observed before forced teardown. These are observations, not latency guarantees. The control used the same base, native binaries and dependencies.

Runtime: full Python source on `a78d050`, native artifacts from `2521446` (unchanged native/build inputs), Torch 2.13.0+cu130, NCCL 2.29.7. `pip check` rejects the vendor cuSPARSELt wheel's `sbsa` tag; dependency versions, AArch64 loading and GPU serving passed separate checks.

## Details

Reuses ZMQ disconnect monitors and queue cancellation, without a new heartbeat or timeout. Cancellation sockets close in the existing finalizer; queues remain alive until their monitor sockets close.

#58738 covers the same failure with a separate heartbeat protocol. Its author [supports this approach](https://github.com/vllm-project/vllm/pull/53597#issuecomment-6056648555) and [is not continuing that implementation](https://github.com/vllm-project/vllm/pull/58738#issuecomment-6056380548).

Does not fix the original CUDA fault, silent partitions or stale utility replies. Missing-LoRA loading returned 404 and subsequent inference worked, but the next utility RPC consumed a stale error. Startup-before-READY and throughput were not tested.

AI-assisted implementation and validation. The author confirmed code review, personal checks and DCO coverage. Numerical results above are from agent-run checks.

---

<details>
<summary> Pull Request Checklist </summary>

- [x] I used vLLM's `/pr-checklist` skill. (Mandatory for agents, optional for humans).
- [x] AI assistance was used during the creation of this PR.
- [x] **Design Fit:** Reuses existing failure handling.
- [x] **Testing and Validation:** CPU regressions and two-host serving checks, including a matched control.
- [x] **Code Quality and Style:** Ruff checks passed; no new public API or configuration.
- [x] **Pull Request Contents:** Dependency, reproduction command, results and limitations included.
</details>

**BEFORE SUBMITTING, PLEASE READ <https://docs.vllm.ai/en/latest/contributing>** (anything written below this line will be removed by GitHub Actions)
