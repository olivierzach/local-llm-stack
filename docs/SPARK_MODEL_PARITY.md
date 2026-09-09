# Single-node model parity

Working parity requires matching checkpoints, their engines, launch configuration,
and actual inference. An identical Makefile is not sufficient. The explicit
catalog is `cluster/single-node-models.lock.json`: ten Hugging Face snapshots plus
the DeepSeek V4 base and DSpark GGUFs. The separately managed BF16 80B distributed
candidate is excluded. LoRA adapters are training outputs; no adapter checkpoint
was present in either stack's `models/adapters` during this inventory.

## Copy over the direct Spark cable

Run on whichever Spark already has the catalog. Either node can be the source.
The established peer aliases use `10.10.20.1` and `10.10.20.2`, on
`enp1s0f0np0`. SSH is the transport over this physical fabric, not Wi-Fi and not a
Mac relay. Check `ssh -G PEER` and `ip route get PEER_IP` before a new transfer.

```bash
cd ~/projects/local-llm-stack
make model-parity-check
make model-parity-copy PEER=spark-e8f1-wired
# In the opposite direction, run on e8f1 with PEER=spark-66f1-wired.
```

For unattended copying, use an engine-neutral service name. The research
supervisor recognizes engine names in service identities and may pause training
even if a Qwen-named service only copies files. This happened during the initial
image transfer; the supervisor resumed training automatically after it finished.
Do not change or disable that supervisor to perform parity work.

```bash
systemd-run --user --unit=spark-artifact-copy \
  --property=RuntimeMaxSec=8h --property=TimeoutStopSec=60 \
  --property=MemoryMax=2G --property=CPUQuota=200% --property=Nice=10 \
  /usr/bin/flock -n "$HOME/projects/local-llm-stack-cluster/state/model-parity.lock" \
  /usr/bin/ionice -c 2 -n 7 /usr/bin/make \
  -C "$HOME/projects/local-llm-stack" model-parity-copy PEER=spark-e8f1-wired
journalctl --user -u spark-artifact-copy -f
```

Create the lock's parent directory first on a fresh installation. No agent is
needed to keep this job running. After a timeout/disconnect, the same copy command
can be run again. It verifies source and destination SHA-256 hashes, preserves
snapshot symlinks, and refuses conflicting cache `refs/main`. Unrelated caches
are not deleted; credentials, `.env`, sessions and databases are not copied.
Individual failures are recorded and the remaining catalog is attempted. A
successful final `parity.json` is required before claiming artifact parity.

The initial transfer report is under
`~/projects/local-llm-stack-cluster/state/model-parity-20260908/` on 66f1. It was
started with a frozen script/catalog in `~/scratch/spark-parity-20260908/` so later
source edits cannot alter the active job. This receipt does not certify inference.

That initial job completed successfully in 6,610.9 seconds, including repeated
source/destination hashing and verification of models already present. The
verified catalog totals 802,055,924,532 bytes; this is catalog size, not all newly
transmitted bytes or a link throughput benchmark. All eleven catalog entries
(ten snapshots and the two-file DeepSeek group) passed without failures.
The GPT-OSS auxiliary `o200k_base.tiktoken` file was subsequently copied over the
same link and SHA-256 verified separately (3,613,922 bytes). It is now included in
the catalog so future parity copies/checks also cover it.

## Native engines and imported images

DeepSeek uses DS4 commit `4ad370b4a338efe9723a386673c0e04f6e214108`, our native
tokenizer patch, and a local CUDA build for GB10. Qwen3.8 uses the pinned recipe in
`config/qwen38-pins.env` plus its integration patch. Prepare either node with:

```bash
make model-runtime-prepare
```

For an offline peer transfer, create `ds4.bundle` and `qwen38-recipe.bundle` with
`git bundle create FILE HEAD` from the corresponding pinned source checkouts,
copy those two files over the fabric, and use:

```bash
make model-runtime-prepare BUNDLE_DIR="$HOME/scratch/spark-parity-runtime"
```

The preparer refuses different source revisions or extra tracked changes. It
does not load models or download weights. Source bundles exclude local Git
configuration, credentials and ignored runtime artifacts. Docker images are
copied separately using `docker image save IMMUTABLE_ID` piped to peer
`docker image load`. Such a copy loses the registry RepoDigest association.
Qwen's launcher therefore accepts the explicitly pinned ARM64 image ID as an
offline fallback, and rejects a different identity or architecture.

Vision and Laguna have explicit newer image defaults; the legacy NGC image does
not implement Qwen3-VL. Other model services retain their existing defaults.
Mistral's service uses the native Tekken tokenizer and Mistral config/load/tool
formats required by its [model card](https://huggingface.co/mistralai/Mistral-Small-3.2-24B-Instruct-2506).
`make model-parity-check` reports presence/structure separately from inference
acceptance; it does not promise every quantization/kernel/context configuration
fits merely because its files exist.

## Sequential inference acceptance

The installed controller can test all nine Compose model definitions on the
current node, using their configured model/context/quantization arguments in
isolated workers. It never contacts the peer or changes existing model services.
The test changes only the API bind/port, pins the model revision and image ID,
and runs offline without forwarding credentials. It uses the shared GPU lease,
requires an idle node, and verifies a completed stream before exact-ID cleanup.

```bash
cd ~/projects/local-llm-stack-cluster/current
runtime/bin/python scripts/probe-stack-models.py \
  --run-id models-001 \
  --output "$HOME/projects/local-llm-stack-cluster/state/model-tests/models-001"
```

Use `--alias local-mistral-small` (repeatable) for selected models, and a fresh
output directory for each attempt. This probes the serving arguments, not the
legacy Make switching behavior or every tool/vision capability. Native DS4 and
Qwen3.8 recipe acceptance is separate. Failed launches retain diagnostic logs
under the controller's ownership state. To recover after an interrupted probe:

```bash
runtime/bin/python scripts/probe-stack-models.py \
  --cleanup-request /absolute/path/to/run/local-mistral-small/request.json
```

Do not run GPU acceptance on a node carrying the research workload. A failed or
partial report is not all-model acceptance; `finished` and `all_passed` must both
be true. The test is suitable for a bounded systemd user service; refreshed
Docker group membership may require launching it through `sg docker -c`.

## Drafting and placement

For DeepSeek, the old `DEEPSEEKV4_DSPARK_ENABLED` setting remains supported.
Explicit choices take precedence and require a server restart:

```bash
make deepseekv4-down
make deepseekv4-up DRAFT_MODE=off
# Or DRAFT_MODE=local for the matching DSpark drafter on the target Spark.
```

`auto` preserves the existing boolean setting. `remote` is rejected before the
Make switch stops any services, because pinned DS4 has no network drafter
transport. Its weight server uses local Unix sockets/CUDA IPC.

Remote speculative decoding is an extension requiring a compatible engine and
target/draft pairing. A [llama.cpp experimental patch](https://github.com/ggml-org/llama.cpp/discussions/25718)
offers `draft-remote` and a draft URL, while
[llama.cpp RPC](https://github.com/ggml-org/llama.cpp/blob/master/tools/rpc/README.md)
exposes remote compute devices and
[draft device selection](https://github.com/ggml-org/llama.cpp/blob/master/tools/server/README.md)
can choose a draft backend. These are candidates, not validated DS4 support.
The [vLLM disaggregated-drafting RFC](https://github.com/vllm-project/vllm/issues/42109)
also remains an upstream proposal. Network proposals are not ordinary independent
chat completions: verification, token vocabulary, state rollback, cancellation
and the sampling distribution must remain correct. Acceptance must compare
drafting off/local/remote, both node assignments, and failure behavior before
enabling remote drafting in the supported catalog.

There is no permanent compute master. A deployment chooses its target,
coordinator and, when supported, draft node. Client aliases and Context Guard
placement are independent of these roles. Cross-node routing for all legacy
aliases and participation of legacy Make launches in shared GPU admission remain
separate integration work; copying artifacts alone does not establish either.
