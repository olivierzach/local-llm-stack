# GLM CUDA fault and remote-worker hang investigation

Status updated 2026-10-12; dated historical results remain distinct. This is the central investigation/runbook. Source, changes, drafts and sanitized results live in this repository; private evidence lives in its **Git-ignored `logs/incidents/` directory**. No GPU workload, restart or fault injection is authorized merely by following a link here.

## Two distinct problems

| Problem | Finding | Status |
| --- | --- | --- |
| **#1: originating CUDA failure** | A remote GLM worker reported `torch.AcceleratorError: CUDA error: unknown error`. | **Unresolved.** No reproduction or root-cause fix. |
| **#2: coordinator hangs after remote-worker failure** | Remote disconnects and blocked response reads did not reliably fail the coordinator. | A follow-up patch was tested and published on a vLLM fork. **Not installed in normal serving.** |

The current logging deployment improves evidence for #1; it does not prevent CUDA faults, deploy #2, or add automatic worker recovery. A failed worker may still leave serving stuck. Do not confuse a synthetic exception used to test #2 with reproducing #1.

## Repository map and private evidence

- This document: findings, method, limits, current operating state and next experiments.
- [`scripts/probe-glm53-cuda.py`](../scripts/probe-glm53-cuda.py): synthetic direct-vLLM smoke, stress and cancellation/churn client.
- [`patches/vllm-remote-worker-failure/`](../patches/vllm-remote-worker-failure/): exact follow-up patch, historical provenance, source snapshots, reproduction helpers and upstream license. This material is not an automatically applied runtime overlay.
- [Current direct-exit adaptation](../patches/vllm-remote-worker-failure/direct-exit/follow-up.patch) and [verification](../patches/vllm-remote-worker-failure/direct-exit/verification.json): applies to revised #53597 head `462261a`; **55 CPU tests passed, one CUDA-only test skipped**. Review reproduced and corrected flushed-reply loss. Matched TP2/PP2 parent controls timed out; candidate worker-failure cases returned 500 and cleaned both hosts. October 11 coordinator loss also passed, with cleanup observed by **3.69s**. The unchanged candidate is now [published as `c396235`](https://github.com/olivierzach/vllm/commit/c3962352b23d8d4db61364d46c66f67ca00c66dd), with human review/check confirmation and authorized DCO sign-off. Original serving remains restored; no new upstream PR or normal-serving patch deployment.
- [Historical candidate recheck, 2026-10-04](../patches/vllm-remote-worker-failure/verification-20261004.json): unchanged `b919a57` patch application/hashes, **17 focused + 2 distributed tests passed**, and three expected failures without queue cancellation. This is not verification of the revised direct-exit parent.
- [`cluster/recipes/glm53-w4a16-256k-dflash2-cuda-log.json`](../cluster/recipes/glm53-w4a16-256k-dflash2-cuda-log.json): source-controlled, opt-in error-logging recipe; corresponding placement files exist for both coordinator roles.
- Private archive within this checkout:
  `logs/incidents/2026-10-01-vllm-worker-failure/`.
  Directories are mode `0700`, files `0600`; `logs/` is Git-ignored. The archive was moved here from the workstation's external incident directory and all 1,542 existing checksums verified. `manifest.json` maps the original local evidence; `SHA256SUMS` covers the original archival snapshot. `remote-state/` preserves live plans and rollback helpers from both Sparks. `source-worktrees/` preserves investigation HEAD metadata and outstanding source diffs. Subsequent verification runs have their own records below this archive.

The private archive contains the original incident logs, kernel/system/sysstat records, full diagnostic clients and outputs, private container inspection, #2 hardware evidence and handoff archive, source snapshots, logging rollout evidence and exact recovery state. Original temporary copies were not deleted. The archive is a point-in-time snapshot, not a continuous log collector.

**Never copy raw logs, request bodies, `.env`, credentials, container-inspect dumps, or the private archive into Git.** Repository documentation records sanitized conclusions and artifact names, not raw evidence. Live Docker logs also remain outside the checkout.

## Original incident and working hypotheses

At `2026-10-01T03:59:01.646168022Z`, the follower's `Worker_TP1` reported the CUDA unknown error at:

```text
vllm/model_executor/layers/fused_moe/runner/moe_runner.py:765
result = shared_output + fused_output
```

CUDA execution is asynchronous. This is the **detection site**, not proof that the addition or shared-expert stream caused the fault. The coordinator accepted a request about 133 ms before the follower exception according to the recorded clocks; cross-host clock alignment was not independently established. The preceding sample showed one running request and 11.8% KV utilization. The workers had approximately 23 hours of uptime. The actual triggering request body and failing batch shape are unavailable.

Pinned incident runtime:

- vLLM `0.1.dev20051+g487ecf187`, upstream commit `487ecf187d3dfe74d2cf6119a92881dba403c219`.
- Torch `2.13.0+cu130`, Triton `3.7.1`, FlashInfer Python `0.6.18.dev20260819`.
- Two GB10 GPUs, TP2, eager execution, async scheduling, prefix caching, DFlash2 with seven speculative tokens.
- W4A16 experts through `CompressedTensorsWNA16MoEMethod` / Marlin; FP8 KV cache; context 262,144; four scheduler slots; utilization 0.82.
- `PYTORCH_CUDA_ALLOC_CONF=expandable_segments:True`; CUDA graphs already disabled.
- Driver versions differed: `580.173.02` and `580.159.03`. That difference is not a diagnosis. Both meet the CUDA 13 family baseline; see [CUDA minor-version compatibility](https://docs.nvidia.com/deploy/cuda-compatibility/minor-version-compatibility.html) and [Spark release notes](https://docs.nvidia.com/dgx/dgx-spark/release-notes.html).

We checked the deployed source and stream ordering, exact image/flags/overlays, driver/package versions, kernel and service journals, and historical memory samples. No matching OOM, Xid or driver-reset event appeared at the failure time. `NV_ERR_NO_MEMORY` messages occurred at startup/recovery, not at this incident. Ten-minute sysstat samples showed roughly 8 GiB available memory around the incident; that does not exclude brief pressure between samples. Source inspection did not establish a missing synchronization operation in the shared-expert path.

**Hypotheses, not findings:** a state-dependent kernel/race/buffer-lifetime issue, a driver/runtime problem, or a rare input/batch-specific kernel failure. The evidence does not rank these reliably. A configuration that runs for hours and survives short tests is not thereby proven correct.

## How #1 was investigated on hardware

The approved experiment used separate two-node containers and copied runtime-cache volumes. Original workers were retained, stopped temporarily, then restored. The diagnostic runtime used the same image, model, kernel and serving flags, but separate endpoint/name, error logging and a 60-second RPC timeout instead of 3,600 seconds. No failure was injected and no proposed #2 patch was loaded.

| Workload | Completed requests | Generated tokens in completed requests | Cancelled streams |
| --- | ---: | ---: | ---: |
| 900-record synthetic workload, serial and 2/4-concurrent requests | 37 | 4,610 | 1 |
| 3,900-record workload, long cached prefixes and varying tails | 29 | 3,586 | 1 |
| Long-context stream cancellation overlapping new requests | 21 | 1,101 | 20 |
| **Total** | **87** | **9,297** | **22** |

The largest prompt was **120,833 tokens**. DFlash drafts/accepted tokens and prefix-cache hits increased during the runs. All three workload clients exited successfully; the original CUDA fault did not recur. Exact historical client versions, command arrays and outputs are in the private archive's `vllm-cuda-fault/` directory (`client-baseline.py`, `client-long.py`, `client.py`, `executed-workloads.json`, `reproduction-results.json`, and the three client logs).

Limits:

- Synthetic requests, not a replay of the lost input/batch.
- Fresh workers, not the original 23-hour process history.
- Mostly 128-token completions, not a long continuous decode campaign.
- Diagnostic KV capacity was 804,247 tokens versus the original 949,317 despite matching utilization settings; live memory state was not identical.
- Cancellation tests are not exhaustive race coverage. Passing runs establish **no root-cause fix**.
- A controlled 24–48-hour soak was not run. Earlier deployment qualification included separate long-output checks; those also do not reproduce the original long-lived state.

Restoration was verified by actual generation, cancellation/overlap, DFlash counters and GPU ownership—not `/health` alone. All isolated test containers, copied caches and staging directories were removed. The follower diagnostic container's exit 137 occurred during explicit shutdown after the workload clients completed, with `OOMKilled=false`; it is not a reproduced inference failure.

## Current diagnostic operating state

As of the logging rollout on 2026-10-03, normal serving runs with exactly these additional environment settings:

```text
CUDA_LOG_FILE=stderr
TORCH_SHOW_CPP_STACKTRACES=1
```

`CUDA_LAUNCH_BLOCKING` is not enabled. No model/controller source, driver, inference argument, cache mapping, endpoint, alias or prompt-body logging was changed by that rollout. Container IDs and ownership changed because the workers were recreated as an explicitly named diagnostic deployment.

Both nodes hold this exact live recovery plan:

```text
~/projects/local-llm-stack-cluster/state/glm53-cuda-logging-20261003/plan.json
```

Its sibling files include `original-plan.json`, `activate.py`, `restore.py` and `deployment-receipt.json`. The existing pinned controller release is `696af2ae90eede000164ac9ee992281917db25b4`. This operational plan predates the source-controlled logging recipe introduced during centralization; **the two plan identities are different**. Do not substitute a newly rendered plan when stopping or recovering the current workers.

Rollout verification passed direct inference, three cancellation/overlap cycles, increasing DFlash counters, and authenticated generation through both 4010 and 4110 gateways on both nodes. Both workers emitted CUDA diagnostics. A CPU-only invalid-tensor-shape probe verified native C++ traces without initializing CUDA. An attempted `/proc/PID/environ` check was inconclusive: `setproctitle` overwrites that memory while `os.environ` remains intact; this was reproduced in a separate CPU process. No GPU fault was injected. The independent rollback timer never fired and was disarmed after verification.

Docker log rotation remains three 20 MB files per worker. Logs survive an ordinary restart but must be collected before removal/recreation. Startup API-lookup/context messages can appear while inference succeeds; do not label every `[CUDA][E]` line as a recurrence of the original unknown error.

### Inspect or restore the current live deployment

Run on either Spark; status is read-only. Restoration interrupts inference and needs a maintenance window. Collect logs first.

```bash
LIVE="$HOME/projects/local-llm-stack-cluster/state/glm53-cuda-logging-20261003"
CONTROLLER="$HOME/projects/local-llm-stack-cluster/releases/696af2ae90eede000164ac9ee992281917db25b4"
python3 "$CONTROLLER/scripts/sparkctl" status --saved-plan "$LIVE/plan.json"

# Only after approving an interruption:
python3 "$LIVE/restore.py"
```

The restore helper checks GPU reservation ownership, removes only the diagnostic deployment, and starts the exact original plan with real text/SSE readiness checks. It does not install #2. Do not use the old original plan to stop the currently owned diagnostic workers.

## Collect the next recurrence before changing anything

1. Note the UTC timestamp, request/client error, stream cancellation or concurrent-request activity, and worker uptime. A prompt hash, token counts and sampling parameters may help; capturing actual prompt bodies requires separate consent.
2. Collect **both** workers' complete timestamped logs before removing containers. Keep full output private; a final 500-line tail can omit the first failure.
3. Capture per-node kernel/system journals around the event, GPU process state, host available memory/swap and runtime version/container identity. A failed privileged journal command does not establish absence; unprivileged journal access may work.
4. Preserve the exact saved plan and its ownership/IDs. Distinguish the first CUDA error from later NCCL disconnect/shutdown errors.
5. Only then restore service through its exact recovery plan. Do not merely retry a request against a failed CUDA worker.

Example **on each Spark**, collecting privately under that controller checkout's ignored `logs/` directory. Copy the resulting per-host evidence into the central workstation archive above before discarding temporary remote files:

```bash
umask 077
OUT="$HOME/projects/local-llm-stack-cluster/logs/incidents/$(date -u +%Y%m%dT%H%M%SZ)-cuda"
mkdir -p "$OUT"
LIVE="$HOME/projects/local-llm-stack-cluster/state/glm53-cuda-logging-20261003"
CONTAINER=$(python3 -c 'import json,sys; p=json.load(open(sys.argv[1])); print(next(iter(p["compose"].values()))["services"]["worker"]["container_name"])' "$LIVE/plan.json")
docker logs --timestamps "$CONTAINER" >"$OUT/worker.log" 2>&1
docker inspect "$CONTAINER" >"$OUT/container-private.json"
nvidia-smi >"$OUT/gpu.txt"
free -m >"$OUT/memory.txt"
journalctl -k --since '30 minutes ago' --utc >"$OUT/kernel.log" 2>&1
journalctl --since '30 minutes ago' --utc >"$OUT/system.log" 2>&1
cp "$LIVE/plan.json" "$OUT/plan-private.json"
```

Check command exit codes and the actual journal time range before concluding that an event is absent. Never attach these files wholesale to a public issue.

## Reproduce bounded synthetic diagnostics

The consolidated client requires `httpx`; `--help` describes accepted arguments. Use a dedicated Python environment under the checkout's ignored `logs/` directory if the client dependency is not installed. No credentials or real prompt bodies are built into the tool. It targets the **direct vLLM root URL**, not a gateway's `/v1` base, because stress/churn also use `/metrics` and `/tokenize`.

The following are representative commands using the consolidated client, not a claim to replay the original incident or every historical client byte. Obtain GPU admission and a bounded maintenance/load-testing window first. Use an isolated diagnostic endpoint for stress/churn; these can consume substantial context and compute.

```bash
umask 077
PRIVATE="$PWD/logs/incidents/diagnostic-run"
mkdir -p "$PRIVATE"
chmod 700 "$PRIVATE"
python3 -m venv "$PRIVATE/client-venv"
"$PRIVATE/client-venv/bin/python" -m pip install 'httpx>=0.28,<1'
PYTHON="$PRIVATE/client-venv/bin/python"
# Set ENDPOINT to the approved direct diagnostic root URL, without /v1.
# Set MODEL to that deployment's served model name.
"$PYTHON" scripts/probe-glm53-cuda.py --url "$ENDPOINT" --model "$MODEL" --mode smoke
"$PYTHON" scripts/probe-glm53-cuda.py --url "$ENDPOINT" --model "$MODEL" --mode stress --rounds 24 --records 900
"$PYTHON" scripts/probe-glm53-cuda.py --url "$ENDPOINT" --model "$MODEL" --mode stress --rounds 16 --records 3900 --request-timeout 240
"$PYTHON" scripts/probe-glm53-cuda.py --url "$ENDPOINT" --model "$MODEL" --mode churn --rounds 20 --records 3900 --request-timeout 240
```

Preserve exit status and output under `PRIVATE`; successful-looking partial output is not a pass. Compare exact image, versions, flags, source overlays, endpoint, elapsed uptime and cache capacity before interpreting results.

## Source-controlled logging recipe for future deployments

`cuda_error_logging` is an optional strict boolean in the recipe schema. True emits exactly the two diagnostic environment values above; missing/false does not enable them. Diagnostic logging is excluded from the compute-cache fingerprint, so corresponding rank caches are reused. Deployment ownership still differs. Existing default recipes remain unchanged.

These CPU-only commands validate/render; they neither stop nor start serving:

```bash
umask 077
PRIVATE="$PWD/logs/incidents/render-cuda-logging"
python3 scripts/sparkctl validate --deployment cluster/deployments/glm53-tp2-256k-dflash2-cuda-log-e8f1.json
python3 scripts/sparkctl render --deployment cluster/deployments/glm53-tp2-256k-dflash2-cuda-log-e8f1.json --output "$PRIVATE/e8f1"
python3 scripts/sparkctl validate --deployment cluster/deployments/glm53-tp2-256k-dflash2-cuda-log-66f1.json
python3 scripts/sparkctl render --deployment cluster/deployments/glm53-tp2-256k-dflash2-cuda-log-66f1.json --output "$PRIVATE/66f1"
```

The old pinned live controller does not accept this new field. Deploy a reviewed, committed complete controller revision through the normal source-release process before using these new profiles on a Spark; do not copy one Python module into an older release. This centralization does **not** perform that deployment or change the existing live plan. Both diagnostic placements use the existing model alias/ports and both GPUs: they are alternative placements, not concurrent replicas.

## #2 patch provenance and reproduction boundaries

### Current direct-exit adaptation — 2026-10-07

The approved [coordination reply](https://github.com/vllm-project/vllm/pull/53597#issuecomment-6044628835) was posted. Nanfang offered a remote follow-up after the local fix; on October 8, [UNIDY supported the ZMQ/cancellable-read direction and offered two-host validation](https://github.com/vllm-project/vllm/pull/53597#issuecomment-6056648555), [discontinuing the standalone heartbeat design](https://github.com/vllm-project/vllm/pull/58738#issuecomment-6056380548). Author direction agreement is recorded, not maintainer approval of the exact revised source or final route. Both PR heads remained unchanged/open in the October 11 16:40 UTC watcher observation. Local implementation proceeded without opening or pushing a competing PR.

The [October 11 coordination update](https://github.com/vllm-project/vllm/pull/53597#issuecomment-6111627839) recorded the then-unpublished candidate and outstanding coordinator-loss test. The [October 12 source/results update](https://github.com/vllm-project/vllm/pull/53597#issuecomment-6115410855) linked the exact review commit and successful coordinator-loss qualification, with CPU reproduction instructions. Its body/ID/URL were verified by a separate GET after one POST. The user reported an hour of code review and explicitly confirmed personal relevant checks and DCO. October 12 authorization covered only fork publication and that update; the later explicit authorization for the dependent draft is recorded below. No serving deployment is authorized.

The current [four-file patch](../patches/vllm-remote-worker-failure/direct-exit/follow-up.patch), SHA-256 `c7398de1095cea84d07a48108f0d97711d1458f887fa46645d78cb1420f678de`, applies to `462261a4cef04dd6735279d4c283ab0e0d6d657f`. It monitors remote ZMQ disconnects, directly exits a follower on coordinator loss, and cancels empty socket-backed reads while preserving flushed replies. It uses no removed error pipe, new heartbeat, or new timeout.

Review caught cancellation masking buffered healthy/fatal replies: two real-RPC regressions failed before correction. Final native CPU verification: **55 passed, 1 CUDA-only skipped**, 79.35s, with 14 Torch deprecation warnings. Clean patch reconstruction, source hashes, changed-file hooks and mypy 3.12 passed. Earlier 9/8-failure controls belong to the initial iteration and include an obsolete queued-data expectation.

Fresh two-host Qwen TP2 and PP2 parent controls timed out at about 45 seconds without automatic cleanup by about 51 seconds. Candidate exceptions returned HTTP 500 in **9.08s / 9.05s**, with both-host cleanup observed by **11.11s / 12.44s**. Synchronous scheduling and immediate worker exit also passed. Both workers reported the exact source hashes; ordinary/concurrent generation and cancellation/recovery passed before faults. These are observed timings, not a latency or throughput guarantee.

Two October 7 coordinator-loss attempts timed out at the first-content gate before SIGKILL. October 11 analysis found that those requests omitted sampling parameters and both hosts started cold FlashInfer sampling compilation; successful normal/cancel probes instead used `temperature=0, seed=42`. This is an evidenced explanation, not proof compilation was the sole blocker. Historical failed attempts remain preserved.

**October 11 coordinator-loss qualification passed on the same exact candidate.** An independent, bounded CPU observer used explicit greedy sampling, received actual content, and remained live when only the isolated coordinator container was killed. The client reported `RemoteProtocolError` after 46 content fragments, not a timeout. Both test containers and GPU contexts were observed cleared **3.69s after injection**, before forced cleanup. This was one candidate case, not a new matched coordinator-loss parent control or a latency guarantee.

The newly approved window restored today's exact original container IDs, images, configuration, mounts, admission bytes and saved plans. Health, GPU ownership, original-model normal/concurrent generation and cancellation/recovery passed **478.59s from window start** (455.96s from stop completion). Both restoration guards exited, maintenance cleared, and owned test containers, cache volumes and remote staging were removed. New private evidence is under `coordinator-gpu-20261011/`; probe diagnosis and the 12-scenario loopback smoke are under `coordinator-qualification-20261011/`. Nine CPU-only guard scenarios passed before arming. These do not validate a later rebase.

Invalid LoRA preserved subsequent generation, but a later all-rank utility RPC consumed a stale rank reply from the parent's unchanged fail-fast response loop. That inherited issue remains separate and unresolved, as does the original CUDA fault.

The approved window used independent host restoration guards, the existing admission mutex, unchanged absolute/monotonic deadlines and exact original container identities. Original IDs/images/admission were preserved; start times changed as expected. Both original workers are healthy with their GPU processes, and original-model normal/concurrent generation plus cancellation/recovery verified restoration within the 60-minute bound. Test resources and owned remote staging were removed; normal serving does not run this candidate.

Exact sources, hashes, commands and limits are in [direct-exit/verification.json](../patches/vllm-remote-worker-failure/direct-exit/verification.json). Sealed private CPU/GPU evidence remains under the dated `direct-exit-*` and `coordinator-*` directories in `logs/incidents/2026-10-01-vllm-worker-failure/`. The original working checkout is unchanged; isolated `data/vllm-worker-failure/submission-direct-exit/` produced signed review commit `c396235` on `olivierzach/vllm:fix/mp-remote-worker-disconnect-direct-exit`. Its four committed files match the reviewed/tested hashes, and the old `b919a57` branch is unchanged. Authorization and publication receipts are under `upstream-watch/`. No tests or GPU operations were rerun merely to publish, and no upstream PR was opened.

At the user's request, an independent October 12 check of the published commit passed the manual DCO/sign-off/license consistency audit and reran the complete CPU suite: **55 passed, 1 skipped, 14 warnings in 71.14s**. An initial stricter no-swap attempt hit a Docker OOM kill; it is retained alongside the successful recorded-limits run, with unchanged source/tests. The DCO audit is not a legal rights determination or cryptographic signature check. All 19 private evidence files under `cpu-dco-20261012-d3ecc914/` were sealed and verified. Test containers/staging were removed and serving identities/configuration/start times were unchanged. No new GPU test, public post, push or watcher restart occurred.

### Dependent upstream draft — 2026-10-14

The user explicitly requested compatibility checking and a dependent PR instead of waiting indefinitely. [vLLM #61558](https://github.com/vllm-project/vllm/pull/61558) is now open as a **draft against main**, depending on #53597 and retaining unchanged human-reviewed head `c396235`. One creation was independently verified by GET, including the exact body, head/base, draft flag, three prerequisite commits plus our commit, and six aggregate files. The description links the four-file follow-up-only range and preserves Nanfang's attribution.

Fresh exact-source CPU validation passed **55 tests, 1 skipped**, 109.39s. A separate isolated merge with pinned main `2521446` had no textual conflicts but exposed two #60472 lifecycle interactions: cancellation sockets must be included in finalization, and remote queues must outlive the executor monitor sockets. Reconciled source passed **88 tests, 2 skipped**, 134.43s; each lifecycle regression failed with its respective correction removed. The first compatibility run retained 85 passes/3 failures/2 skips because 512 MiB SHM could not fit the new tensor arena; the full suite passed with 3 GiB SHM, without changing tests or RAM/swap limits. This is focused executor/queue/arena CPU-overlay evidence, not full-main or GPU qualification.

The scratch integration remains private at `data/vllm-worker-failure/dependent-compatibility/`, not silently published under the earlier human approval. New source needs human review and affected validation before the draft becomes ready. The submitted head's DCO check passed; pre-commit is blocked by the contributor/label eligibility gate, not a reported test failure. No CI trigger, source push, force-push, GPU run, serving operation or watcher restart occurred. Every owned test container and remote staging directory was removed. Later controls verified all four actually running service identities/configurations/start times unchanged; the early historical label filter had matched none.

Private exact inputs, full outputs, negative controls, current-main reconciliation patch and publication receipt are retained under `logs/incidents/2026-10-01-vllm-worker-failure/dependent-pr-20261014-15596992/`. [Verification](../patches/vllm-remote-worker-failure/direct-exit/verification.json) and [submission checklist](../patches/vllm-remote-worker-failure/SUBMISSION_CHECKLIST.md) record the remaining readiness gates.

For review in a separate clean vLLM checkout:

```bash
git fetch origin 462261a4cef04dd6735279d4c283ab0e0d6d657f
git switch -c review-remote-direct-exit 462261a4cef04dd6735279d4c283ab0e0d6d657f
git apply /path/to/local-llm-stack/patches/vllm-remote-worker-failure/direct-exit/follow-up.patch
```

### Full-source integrated qualification — 2026-10-14

The follow-up is now integrated with main `a78d050` as tree `18aa0a99031aa0977b3effca9938cdc78a17ede9`, preserving the original commits. The additional correction is four files, +80/-26. A complete source installation with matching native/build inputs passed **88 CPU tests, 2 skipped**, and six-file Ruff checks.

With explicit shared-GPU permission, fresh two-host Qwen3-4B checks passed TP2/PP2 forward exceptions, immediate worker exit, synchronous scheduling and coordinator loss during streaming. Natural cleanup was verified within **3.63–13.90s**. The matched prerequisite-only control timed out at **45.12s** and retained the coordinator/GPU context until explicit owned cleanup. Normal/concurrent answers and cancellation recovery passed. Protected GPU processes and all 12 running services remained unchanged.

The missing-LoRA request returned 404 and subsequent generation succeeded, but the following utility RPC consumed an inherited stale reply; that failed combined experiment remains recorded. The vendor cuSPARSELt wheel's SBSA-tag warning also remains explicit. Neither the original CUDA fault nor those unrelated issues was patched.

All test containers and 12 stopped builders were removed. Qualified images and reproduction inputs are retained under `logs/incidents/2026-10-01-vllm-worker-failure/integrated-pr-20261014-b678678d/`; 471 evidence files were sealed and verified. The user then explicitly confirmed review of the correction, personal successful checks and DCO. Existing PR #61558 was fast-forwarded to [`35d0948`](https://github.com/olivierzach/vllm/commit/35d0948c11c30e26022db1d5777cef1bc305908c) and marked ready for review. Independent GET verified the exact qualified tree, all six changed-file blobs and the public body. DCO passed; pre-commit is skipped behind the contributor/label eligibility gate, not a code-test failure. No qualifying labels were requested. Prerequisite #53597, maintainer review and required CI remain external. Publication receipts are in the adjacent `integrated-pr-20261014-b678678d-publication/` directory. No normal-serving deployment or watcher restart.

On October 15, an authorized message-only rewrite changed the PR head to [`3d76823`](https://github.com/olivierzach/vllm/commit/3d76823ad86d92dd3a11da5e534f5a050d931248). Both source trees and Nanfang's commits are unchanged. The PR description and submission notes were shortened; the test evidence still applies.

### Historical published candidate

The historical tested follow-up remains [`follow-up-53597.patch`](../patches/vllm-remote-worker-failure/follow-up-53597.patch), SHA-256 `667abe736673ce4ee944d43ef6b955e72223beae5ad4ad6a60c10d7e96c74c3b`.

- Published commit: [`b919a571ef2f92e94c20ed6e2e890a011c485237`](https://github.com/olivierzach/vllm/commit/b919a571ef2f92e94c20ed6e2e890a011c485237), fork branch `fix/mp-remote-worker-failure`.
- Parent / patch application base: Nanfang Li's [PR #53597](https://github.com/vllm-project/vllm/pull/53597) head `89a48785ee496f0cb646f2d7e9d089e98b30bb40`; its upstream base was `d2125ea681784c17251c47939f5ed6d1c67384f8`.
- [Author handoff reply](https://github.com/vllm-project/vllm/pull/53597#issuecomment-5963547766). No competing PR or human/DCO approval is claimed. Recorded upstream status is historical, not a fresh assertion that the PR remains open.
- [Follow-up ownership offer, 2026-10-03](https://github.com/vllm-project/vllm/pull/53597#issuecomment-5974024241): offered to take the remote-worker case through a follow-up PR if Nanfang-Li has not started it. This updates the earlier preference for inclusion in his PR. No separate PR was opened; author coordination is pending as of the offer.
- Local [PR body draft](../patches/vllm-remote-worker-failure/PR_DRAFT.md) and [submission gates / human review walkthrough](../patches/vllm-remote-worker-failure/SUBMISSION_CHECKLIST.md) now describe the current direct-exit adaptation. This historical published implementation, its original snapshots, and its verification record remain unchanged.
- The preparation check found direct problem overlap with [PR #58738](https://github.com/vllm-project/vllm/pull/58738), which uses a dedicated TCP heartbeat channel. Our proposal uses existing ZMQ disconnect events and cancellable response reads. Coordinate with both authors/maintainers before proposing another PR; Nanfang's agreement alone would not resolve this overlap. The published branch also included two prerequisite commits and was 89 commits behind then-current upstream main.

The patch adds remote-disconnect detection and cancellation of blocked response-queue reads. It is not a CUDA kernel fix. In the recorded matched two-host Qwen TP2 experiment, base and updated-PR clients timed out at 60 seconds; two follow-up runs returned HTTP 500 around 18 seconds and both hosts cleaned up by roughly 25 seconds. A PP2 remote-output failure required the queue-cancellation part. A **separate source-compatible GLM backport**, not the upstream wheel, returned HTTP 500 around 18 seconds and cleaned up both workers by roughly 35 seconds. CPU regression and normal inference/cancellation/LoRA checks are recorded in the private handoff.

For source review in a separate vLLM checkout:

```bash
git fetch origin 89a48785ee496f0cb646f2d7e9d089e98b30bb40
git switch -c review-remote-failure 89a48785ee496f0cb646f2d7e9d089e98b30bb40
git apply /path/to/local-llm-stack/patches/vllm-remote-worker-failure/follow-up-53597.patch
```

Review `provenance.json`, the bundled source snapshots and helper CLI help before any hardware run. Native runtime/model compatibility matters: the tested upstream wheel could not run the deployed GLM/DFlash path. Never drop upstream Python files over an unrelated binary runtime. `reproduce/runtime-build.sh` now pins the exact official wheel and both FlashInfer packages and checks core versions in a new isolated verification container; it still requires a compatible base image and is not an OS lockfile. Full build records and launch manifests remain private under `logs/incidents/`. The launch renderer requires an explicit manifest rather than embedding private host topology in this repo.

**Fault injection requires a separate approved window, isolated development-RPC endpoint and tested rollback. Never aim the `failure` client mode at normal serving.** Original private launch manifests/handoff instructions are in `logs/incidents/2026-10-01-vllm-worker-failure/`. Repeating #2 tests is unnecessary to establish that #1 is still unresolved.

## Hourly upstream coordination watch

[`scripts/watch-vllm-upstream.py`](../scripts/watch-vllm-upstream.py) checks [#53597](https://github.com/vllm-project/vllm/pull/53597) and [#58738](https://github.com/vllm-project/vllm/pull/58738) approximately hourly. It uses authenticated `gh api` **GET requests only** and follows pagination for comments, reviews, and inline review comments. It also detects PR-head commits, title/body edits, target-branch changes, draft state, and open/closed/merged transitions. CI runs are not monitored.

**Stopped at the user's request on October 11.** The managed process `vllm-upstream-watch` exited cleanly with code 0; no further hourly polling or alerts are running from it. Do not restart it without a new request. Its previous launch used a 3,600-second interval, detached/persistent lifetime and restart-on-failure; it was not a boot-time LaunchAgent. Historical evidence is retained. Upstream checks are now manual; stopping the watcher does not change GPU serving.

When running, it requests macOS desktop notifications for updates, the first failed check, and recovery, and sends update alerts through the explicitly approved existing OpenClaw Telegram route. No new bot or credentials were created. Historical routing acceptance is not proof of phone receipt. Desktop visibility depends on macOS notification/Focus settings. The local event log is authoritative.

All state is private and Git-ignored under:

```text
logs/incidents/2026-10-01-vllm-worker-failure/upstream-watch/
  status.json             latest attempt and last successful check
  snapshot.json           last complete successful GitHub snapshot
  events.jsonl            observed updates, polling failures, notification results
  triage-request.json     latest bounded-session triage request
  transition-intent.json  temporary durable event identity until snapshot commit
  telegram-delivery.json  pending/accepted deliveries for the approved route
  maintenance.json        Main-owned GPU-window interlock
  verification.json      original watcher smoke-check evidence
  wake-verification.json extension lifecycle and actual OMP command evidence
```

The first successful poll establishes a baseline without notifying about the entire existing discussion. Subsequent changes are recorded and queued for consumers before advancing the snapshot. An interrupted snapshot commit reuses the same event identity; a later return to a previously observed head produces a new alert. Failed API reads preserve the previous snapshot. A lock serializes overlapping manual/loop polls. Pending Telegram deliveries retry on later polls even without another upstream change, and multiple pending events survive. Accepted deliveries are not resent; a crash/timeout after remote acceptance but before persistence can still duplicate delivery. Acceptance is not proof of human receipt.

From the repository root, perform one read-only check or run a foreground watcher:

```bash
uv run --no-project scripts/watch-vllm-upstream.py --once
uv run --no-project scripts/watch-vllm-upstream.py --interval-seconds 3600
```

Do not start a second foreground loop while the managed loop is active. `--no-notify` disables desktop **and Telegram** delivery. To retain Telegram after reboot, restore the approved private route with `--telegram-account`, `--telegram-target`, and optionally `--openclaw-command`; the examples above alone enable desktop notifications only. Never place bot tokens or private chat identifiers in tracked files. Ctrl+C stops a foreground loop. For the managed process, use the harness `hub` tool with `name: "vllm-upstream-watch"`: `describe`, `logs`, `stop`, or `restart`.

The focused watcher suite passes **12 tests**, including failed-send retry, timeout/invalid acceptance, route mismatch, multiple pending events, consumer/snapshot write failures, and head cycles. The head-cycle regression failed before correction: only two alerts were delivered for three real transitions. Existing smoke evidence also covers real GitHub reads, authentication failure/recovery, loop shutdown, and macOS notification requests. Synthetic events were not inserted into the live watcher state.

### What happens when upstream moves

1. The hourly detector records the change and queues a metadata-only Telegram alert. Raw PR bodies/comments and private launch details are not sent.
2. An **open, explicitly subscribed OMP session** can consume the latest request through [`.omp/extensions/vllm-upstream.ts`](../.omp/extensions/vllm-upstream.ts). It waits for an idle session with no pending messages and no active Main-owned maintenance interlock, then requests one read-only triage. Older unacknowledged events remain in the event log and are included in the comparison.
3. Triage compares heads/discussion with the exact current patch and evidence, reports coordination/overlap, and identifies which testing is invalidated. Discussion alone does not invalidate a binary; a changed parent or affected implementation requires review and revalidation. Old GPU timings never certify a new source hash.
4. Nothing automatically edits code, starts GPU tests, posts to GitHub, pushes, signs, or opens a PR. Publication still requires overlap agreement, final-source review/validation, human DCO, and explicit authorization.

The extension is installed through a user-extension symlink to this checkout. **For a process that was already running when it was installed, restart OMP once and resume the existing conversation.** OMP 18.2.1 reports “Plugins reloaded” for `/reload-plugins` but does not discover this newly added native extension in that process; this was reproduced with before/after command inventories. Fresh startup discovers the same installation automatically. After restarting/resuming, run:

```text
/vllm-watch on
/vllm-watch status
```

Subscription is per session; a fork does not inherit permission. Polling uses managed 15-second timers. Triage is limited to 12 evidence reads and 120 seconds, with read/grep/glob restrictions and an exact-event acknowledgment. Failed, timed-out, or unacknowledged attempts latch without automatic retries; inspect status and use `/vllm-watch retry EVENT_ID` explicitly. A dequeued manual user prompt takes ownership and removes the automatic triage deadline/restrictions. Session state restores the acknowledgment and failure latch. `/vllm-watch off` prevents new wakes, but does not interrupt an already-running triage.

Local evidence access accepts one existing literal path, optionally with simple
line/raw selectors. Multi-target lists, wildcard patterns, encoded paths, missing
paths, and symlinks resolving outside the evidence allowlist are rejected. Use a
literal evidence directory for `glob`; do not pass a compound search expression.
The extension regression suite covers these boundaries before publication.

Main-owned GPU windows write `maintenance.json` before arming restoration guards and clear it only after original serving is verified. Active or malformed interlocks block wakes; a deadline never authorizes automatic resumption. Use `/vllm-watch off` before other GPU/background work because those jobs do not necessarily publish this interlock.

Lifecycle behavior was exercised with the actual extension in an isolated Bun driver. Real OMP RPC smoke checks verified explicit loading and fresh-start automatic discovery and executed commands without invoking a model. The live subscribed session subsequently delivered triage, but its guard incorrectly rejected the acknowledgment device's `read`/`write` transport. The October 11 fix exempts only exact `xd://vllm_upstream_ack` schema reads/invocations; other writes/devices remain blocked. Run `node --test tests/test_vllm_upstream_extension.mjs` with Node 24: four regressions pass, two fail with the original guard. Read/time bounds and exact-event/no-replay behavior remain covered. Fresh actual OMP startup loads the repaired source; already-running sessions must restart/resume to load it. The October 8 event was explicitly acknowledged after user takeover; later events remain separate. No independent unattended model session was installed.

## Next experiments and stop conditions

1. **Observe real use with diagnostics enabled.** Capture a recurrence before changing drivers or inference settings. This is the current approach; no artificial background soak is running.
2. **Controlled soak, if approved:** keep the same workers alive for 24–48 hours with bounded representative request rates, long prompts and continuous long decodes, concurrency and cancellations. A soak is about duration/state accumulation, not just maximum instantaneous load. Record actual completed-token counts, uptime, latency, available memory/swap, cache utilization and errors. Short-client loops alone still miss long-decode boundaries. Predetermine resource limits, a hard stop, rollback and operator ownership; both GPUs are already occupied by normal serving.
3. **Once reproducible:** hold the failing input/sequence constant and change one variable at a time—DFlash, async scheduling, or a relevant backend setting. A run without a rare fault is not proof of causation.
4. **Targeted kernel diagnostics:** launch blocking or Compute Sanitizer only in an isolated reproduction. They can be very slow and alter timing, potentially hiding a race.
5. **Driver alignment:** a controlled experiment to remove a variable, not an evidence-backed fix. Do not upgrade both drivers, disable several features and then attribute success to one change.

Stop experiments on the first original fault or resource-limit breach, capture evidence, then restore and verify actual inference plus existing gateway routes. A reproduction must fail before a proposed fix and pass after under matched conditions before calling #1 fixed.

## Centralization verification (2026-10-03)

- Both source-controlled diagnostic placements passed CLI validation and rendering without host contact.
- The published patch applied on its declared PR head and produced the exact three recorded published source hashes; all ten bundled historical source snapshots matched provenance.
- The consolidated client passed help, argument-boundary and import-safety checks in an isolated environment with its documented `httpx` dependency. Its actual smoke request returned `42` against normal serving.
- Both live containers retained their IDs, image IDs and start times; diagnostics remained enabled. No restart or deployment was performed during centralization.
- The repository CPU suite reported **449 passed, 4 skipped, 2 failed** on macOS. Both failures were in untouched directory-initialization tests because GNU `stat -c` is unsupported by macOS `stat`; this is not a clean full-suite pass. The logging-profile regressions passed.
- The private archive was content-hash verified against its local sources and both nodes' recovery-state files. Generated verification output is also kept outside Git.

The raw CUDA fault remains unresolved. Centralizing source and evidence does not add a new hardware soak, deploy the #2 patch, or turn historical results into current upstream approval.
