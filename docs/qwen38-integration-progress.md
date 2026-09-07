# Qwen3.8 Integration Progress

Goal: serve MiaAI-Lab's single-Spark Qwen3.8 recipe through every configured
client and Context Guard, with a verified DS4 rollback.

## Plan, Act, Verify

- [x] Inspect live services and select upstream commit ef1af5fa2e1e93d4bd96136568460cda843ccc29.
- [x] Pin the image and checkpoint; download and validate artifacts.
- [x] Implement lifecycle, private bind, memory checks, and mutual exclusion.
- [x] Integrate LiteLLM, Context Guard, client metadata, and memory-watch logs.
- [x] Test direct inference, streaming tools, vision, guarded recovery, AIChat and OpenCode.
- [ ] Deferred for training: dedicated long-context/concurrency benchmark and 45-minute soak.
  Both remain part of the full verification sequence, as confirmed by the user.
- [x] Record results and reproducible operating commands; stop Qwen and verify GPU release.
- [ ] Deferred: complete Qwen-to-DS4-to-Qwen rollback cycle.

Current state: Qwen deliberately stopped for training; DS4 remains inactive.
See [verification results and limitations](qwen38-verification.md).

Baseline: DS4 active at PID 2899322 on 2026-09-07, approximately 2.4 GiB
MemAvailable and 5.5 GiB swap used. Source tree clean before implementation.
Runtime measurements and logs belong under ignored evals/runs and logs paths.
