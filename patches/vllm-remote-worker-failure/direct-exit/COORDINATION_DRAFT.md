# Remote-worker follow-up — posted coordination reply

**Posted October 11, 2026, at 17:19:40 UTC.** [Public comment](https://github.com/vllm-project/vllm/pull/53597#issuecomment-6111627839), ID `6111627839`. Posted once; returned public body, ID and URL independently verified through the GitHub API. Exact body follows. Evidence was checked against `verification.json`, watcher status/events through October 11 16:40 UTC, and live public discussion/current parent immediately before posting. Results below are recorded October 7 results, not new runs. No source publication, commit, push, PR or DCO signing was performed. Private receipt: `logs/incidents/2026-10-01-vllm-worker-failure/upstream-watch/coordination-post-20261011.json`.

## Exact posted public reply

@Nanfang-Li @UNIDY2002 Thanks for coordinating. I agree with the ZMQ disconnect notification + cancellable response-read direction, reusing the existing control plane rather than adding a standalone heartbeat protocol. Thanks for the [follow-up suggestion and review of the historical candidate](https://github.com/vllm-project/vllm/pull/53597#issuecomment-5998289437) and the [offer to help validate the adaptation](https://github.com/vllm-project/vllm/pull/53597#issuecomment-6056648555).

The direct-exit adaptation is now **local and unpublished**:
- Parent: `462261a4cef04dd6735279d4c283ab0e0d6d657f` (the current head of this PR).
- Four-file patch SHA-256: `c7398de1095cea84d07a48108f0d97711d1458f887fa46645d78cb1420f678de` (not a commit ID).

The revised source has not been shared; the previously published `b919a57` is not this candidate, and its review does not cover this adaptation.

**Recorded October 7 local validation**, using a matching Linux ARM64 native runtime based on `d2125ea681784c17251c47939f5ed6d1c67384f8`:
- Executor/fail-fast/queue CPU suite: **55 passed, 1 CUDA-only skipped**. Two real-RPC regressions for preserving already-flushed replies failed before correction and passed in the final suite. Exact inputs/command and observed completion are recorded; full final CPU stdout was not separately retained.
- Matched two-host Qwen3-4B eager worker-exception cases: parent TP2/PP2 requests timed out at **45.05s / 45.06s**, with no automatic cleanup by approximately 51s. The candidate returned HTTP 500 in **9.08s / 9.05s**, with both hosts observed clean by **11.11s / 12.44s**.
- Candidate TP2 synchronous scheduling and immediate worker exit returned HTTP 500 in **0.033s / 9.05s**, with both hosts observed clean by **3.39s / 11.10s**. Loaded source hashes matched in each case. Timings are bounded observations from probe start, not latency guarantees.
- **Coordinator-loss GPU qualification is still incomplete:** both attempts timed out at the first-content-token gate, before SIGKILL was injected. CPU coordinator-disconnect coverage passes but cannot substitute for that GPU case. A new isolated local run is planned; it is not a result.

**Separate inherited utility limitation:** invalid LoRA returned 404 and subsequent generation succeeded, but a later all-rank utility RPC consumed an older rank's LoRA failure. The parent and candidate `collective_rpc` implementations are unchanged; cross-utility reply draining is not fixed here. Fault qualification used fresh servers, not retries that concealed this failure.

Would this collaboration split work?
1. Nanfang continues the local-worker prerequisite; I prepare the remote-disconnect/cancellable-read delta and reproducible CPU checks for human review and source sharing, preserving already-flushed replies and normal shutdown behavior.
2. Once the exact revised source is shared and the base is agreed, UNIDY could independently validate matched parent/candidate TP2 and remote-output PP2 failures on two hosts, then coordinator loss after a healthy stream is established. Record client outcomes and both hosts' process/GPU cleanup before forced cleanup, including remaining gaps.
3. Could the maintainers confirm the preferred route: a remote-worker follow-up after #53597, or contribution to an existing PR? Directional agreement is not approval of the revised source or integration route; any final rebase needs renewed validation.

These are agent-run local checks, not upstream CI or personal human validation. No silent-network-partition guarantee, throughput result, full fault-tolerant serving qualification, or fix for the original CUDA fault is claimed. AI assistance was used for implementation, validation and this reply. Human line-by-line review, personal reruns and DCO sign-off remain pending before an upstream source submission.
