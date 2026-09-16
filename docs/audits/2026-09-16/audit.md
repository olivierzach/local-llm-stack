Audit completed on September 16, 2026. This checkout is an operational copy of selected cluster-branch changes layered onto an older `main`. **No substantive source change in the initial dirty working tree is unique to this machine or absent from the fetched feature branch's history.** The running system nevertheless depends on these local files and on untracked runtime assets.

This report reconciles GitHub history, all initially modified/untracked source files, both Spark baseline checkouts, the installed controller releases, local running-container metadata, selected acceptance receipts, and CPU tests. It is an audit and reconciliation plan, not a source rollout or merge. No model was loaded, stopped, restarted, or prompted; no credentials, conversations, or model weights were copied into this report.

**Git and deployment reconciliation**

| Location | Revision / state |
| --- | --- |
| 66f1 baseline checkout | `main`, `500d05023e5a120e453c0a0c6a47d5cd20166a42`, with copied changes |
| Freshly fetched `origin/main` | Same revision; zero commits ahead or behind |
| GitHub `feat/interchangeable-spark-nodes` | `696af2ae90eede000164ac9ee992281917db25b4`; 129 commits ahead of main |
| e8f1 baseline checkout | Detached at `500d050`; same 41 changed/new source files byte for byte |
| Installed controller on both Sparks | `current` selects `696af2a`, matching the feature-branch tip |
| Local installed controller source | No tracked modifications; `data/cluster` is untracked runtime state |
| GitHub pull requests | No open or closed PRs returned by the repository PR query |

The feature branch changes 299 files relative to main (23,922 insertions, 65 deletions). It is not merged into main. There are 252 branch-added files absent from the baseline checkout, including 38 test files. The two-path arrangement is documented in the feature branch: production baseline services and versioned optional controller releases are deliberately separate. The problem is that baseline integrations have been applied by copying a partial, evolving set of files without a reproducible source revision for that set.

Of the initial 55 modified/untracked files, 41 are source/configuration/documentation and 14 are runtime artifacts. The source consists of 10 modified tracked files and 31 untracked files:

- 29 match the current feature tip exactly.
- 10 match earlier versions in feature-branch history.
- Two (`.env.example`, `config/litellm.yaml`) differ from the tip only by an extra trailing blank line.
- No substantive source needs to be recovered from an otherwise unpushed local implementation.

Fetching added the feature-branch tracking reference; it did not change main or the working tree. The report and its evidence directory are the only deliberate additions to this checkout during the audit.

**What the local work implements**

| Workstream | Local implementation | Origin and purpose |
| --- | --- | --- |
| Model parity | `cluster/single-node-models.lock.json`, audit/sync/runtime preparation scripts, image identity fallback | September 8–9 feature work: copy and verify the ten Hugging Face snapshots plus native DeepSeek artifacts; prepare both machines without conflating file presence with inference |
| Python parity | Hashed ARM64/Python 3.12 lock, bootstrap/check script, owned CUDA probe | September 9: reproduce 167 host packages and validate a replacement environment before activation |
| Existing-port routing | Registry-backed Context Guard, native fabric settings, route configuration and probes | September 9 onward: move model placement between nodes while preserving client aliases and the existing 4010 address |
| GPU admission | Make wrapper, reservation/ownership helpers, transaction labels and recovery | September 9: coordinate legacy model/training commands with controller and research workloads |
| Stream correctness | SSE framing, explicit interrupted-stream errors, cancellation, replica release, keepalives | September 9–12: preserve partial-output failure semantics and support long prefill without silently replaying a generation |
| Model compatibility | R1 reasoning parser, Mistral formats/parser, newer Vision/Laguna images, DeepSeek drafting controls | Corrections and controls developed during actual model acceptance |
| Runtime payloads | Route registry, private pre-change environment backup, receipts, NCCL library and model overlays | Machine-local operational state used by the installed controller and running services; not source changes to commit |

The complete branch subsequently added qualified Qwen3-Next TP2/MTP, DeepSeek TP2/1M context, and GLM-5.3 TP2/DFlash2, plus client, recovery, and qualification tooling. Those later workloads are operated from the installed controller, not from the baseline Makefile.

**Findings requiring reconciliation**

1. **High: runtime and credential-bearing files are eligible for accidental staging.** `.gitignore` does not cover the new runtime directories. `data/context-guard-routes/before-native-fabric.env` is an untracked, mode-0600 backup of the pre-change local environment; its content was not printed. The untracked set also contains the live registry, test receipts, a 253,766,464-byte NCCL library, and five generated model-overlay files. Four GLM overlays are currently bind-mounted by the active worker. The feature branch adds only `data/cluster/` to its ignore rules and does not resolve this gap. On e8f1, the active `.venv` symlink and `.venv.before-parity-20260909/` also appear untracked. Add appropriate artifact/backup ignore rules before staging source. Preserve operational artifacts; they are not disposable clutter.

2. **High: the baseline source is an inconsistent deployment bundle.** Current gateway and proxy files are mixed with older `config.py`, `node.py`, `gateway_node.py`, route setup/probe scripts, and tests. A side-effect-free validation of the actual saved GLM plans using local `tools/spark_cluster/config.py` fails with unknown recipe fields `glm53`, `async_scheduling`, and `speculative_config`. Therefore the documented baseline `configure-context-routes.py --plan ...` path cannot handle the current deployment. Its older implementation also lacks the later `--merge` and `--alias` options: existing behavior replaces the complete registry. Use the current controller for modern plan-based publication until a coherent baseline bundle is deployed. This does not invalidate existing routes, which are already materialized registry entries.

3. **Medium: 66f1 LiteLLM has not received the current Compose configuration.** Its container started September 7. It has no `DEEPSEEKV4_API_BASE`, its `QWEN38_API_BASE` differs from the rendered desired value, and it lacks the healthcheck now declared in Compose. The 4010 Context Guard's inspected route-related environment and healthcheck match Compose. The active GLM override goes directly through the registry gateway, so these findings do not establish an outage of that route. Reconcile LiteLLM in a controlled CPU-service rollout and verify both legacy and migrated paths; a plain container restart does not apply changed container environment/healthcheck configuration.

4. **Medium: baseline regression coverage was left behind.** The local suite has 89 passing and two failing tests. Both failures are old Makefile-text assertions that do not recognize the collective GPU wrapper targets. They are already corrected on the feature branch by querying Make's expanded database. Local newer streaming behavior also lacks the branch's keepalive/cancellation regression additions. Restore tests with their source dependency set rather than weakening assertions or treating these failures as proof that model launches are broken.

5. **Medium: the declared system package baseline is still incomplete.** Fresh package-database checks find seven missing package records on 66f1: `ninja-build`, `git-lfs`, `ripgrep`, `sox`, `iperf3`, `openmpi-bin`, `libopenmpi-dev`. e8f1 lacks the last five. This is the declared Debian package baseline; it does not mean every corresponding executable is absent from every alternative installation. Nothing was installed during the audit.

6. **Medium: several status documents are historical but read as current.** Baseline parity docs omit later successful receipts. The feature branch's September 12 `CLUSTER_IMPLEMENTATION.md` says DeepSeek currently owns both GPUs, while its newer GLM runbook and current containers show GLM with e8f1 coordinating. Update the top-level status to reference the newest deployment receipt and distinguish accepted-but-stopped recipes from the active service. `AGENTS.md` also still says there is no test framework or commit history, despite both being present.

No additional functional regression was demonstrated in the complete branch's CPU suite. Passing that suite is not a claim of GPU fault tolerance or full application/browser qualification.

**Current operational state observed without generation**

Both hosts have a healthy GLM worker for saved deployment digest `b1cf7901f81c861758f5f7c14ba7cea5605519713c4fc4816f1e06af41eb3bb2`; e8f1 coordinates. The local GPU reservation names that deployment and is in the `started` phase. The selected recipe is GLM TP2/DFlash2 with 262,144 context tokens and an 8,192-token published output cap. Both baseline and managed gateways are running. The local Open WebUI, database, monitoring services and LiteLLM are also running.

The baseline registry retains three aliases: `local-glm53-flash`, `local-deepseek-v4-flash` (1,048,576-token policy), and `local-qwen3-next-80b` (262,144-token policy). A retained route is not evidence that its backend is running. The GLM runbook identifies DeepSeek TP2 as paused. Qwen3.8 Flash Next is a separate single-node recipe from Qwen3-Next BF16 TP2/MTP; their verification results must not be substituted for each other.

The running baseline guard mounts this checkout's `scripts/` and `tools/spark_cluster/`, plus its runtime registry directory. Resetting/cleaning the working tree would affect the source needed on a future guard start. Runtime overlay paths are mounted into the live GLM worker. Do not use a blind `git reset --hard`/`git clean` as a reconciliation strategy.

**Completed work and evidence boundaries**

| Area | Reconciled evidence | Remaining limit |
| --- | --- | --- |
| Model artifacts | Initial transfer receipt: all catalog entries verified, no failures. Fresh local audit: all 11 catalog groups pass structure/runtime-presence checks; auxiliary file present | No fresh full rehash of approximately 802 GB of catalog contents; presence checks are not new inference tests |
| e8f1 Compose inference | Saved receipts cover nine Compose aliases; subsequent R1 and GPT-OSS reruns explicitly pass version-2 complete-answer checks | Earlier receipts use the older acceptance format. There is no one final nine-model version-2 sweep receipt in the inspected set; do not describe this as all features of all models qualified |
| e8f1 native acceptance | `model-tests/native-20260909-006/acceptance.json` passes; earlier failed attempt retained | Does not establish the separate deferred 45-minute Qwen3.8 sequence |
| Python | Local 167-package lock and dependency check pass. Peer active venv resolves to the locked environment; peer CUDA/optimizer acceptance and cleanup pass | 66f1's recorded CUDA attempt was refused by admission; a successful idle-window host CUDA probe remains outstanding |
| Existing-port routing | Saved local text/stream/tool/token-policy and backend-unavailable receipts pass; newer GLM publication receipt passes | Migrated virtual-key policy/accounting, gateway-host failover and full browser workflow are not established |
| GLM qualification | Selected 0.82 recipe has completed campaigns for both coordinator placements. Local publication and FamChat provider receipt pass. Current workers are healthy | No fresh generation in this audit; full-window concurrency and general model quality are not implied |
| Other distributed recipes | Feature docs and retained receipts record accepted Qwen TP2/MTP and DeepSeek TP2/1M recipes | Acceptance is recipe-specific and does not imply they are active or qualify the older Qwen3.8 recipe |

This corrects the initial status answer: peer Python GPU acceptance and substantial single-node inference acceptance were already completed. They looked outstanding only because the baseline docs and local-only receipt locations were incomplete.

**Validation performed**

| Check | Result |
| --- | --- |
| Local pytest suite | 89 passed, 2 failed in 24.96 seconds; both stale Make target assertions |
| Complete feature snapshot | 449 passed; one Git-only test initially failed because an archive has no `.git`. After creating temporary Git metadata, that exact test passed. All 450 test cases have passing results; the entire suite was not redundantly rerun |
| Compose, all profiles | `docker compose --profile '*' config --quiet` passed |
| Git whitespace | Working-tree diff and main-to-feature committed diff both passed |
| Shell syntax | All 8 local and 12 feature `scripts/*.sh` files checked individually; passed |
| Python syntax | 29 local and 90 feature Python script/tool files parsed; passed |
| Environment | 167 locked local packages match, no broken requirements, only pip extra |
| Model inventory | All 11 catalog groups pass structure and runtime-presence checks |
| Local/peer source identity | All 41 initially changed/new source files match between the two baseline checkouts |
| GLM overlay hashes | All five local generated overlay files match their content-addressed directory names |
| Live plan through local validator | Reproduced rejection of both saved GLM coordinator plans; source-version mismatch confirmed |

Test XML, normalized source differences, file inventory, and filtered runtime/peer summaries are retained with this report. The clean remote archive was tested under `/tmp/spark-audit-20260916/remote-snapshot` using the existing development interpreter; no dependencies were installed. No broad runtime logs or environment contents were saved.

**Recommended reconciliation order**

1. Protect the private backup and runtime trees from accidental staging. Retain the existing live registry, runtime assets and exact saved deployment plans. Include e8f1's environment symlink and rollback environment in the ignore review.
2. Review the existing feature branch as the source of truth. There is no need to invent a new commit containing this copied subset as though it were new implementation. Main can advance along the existing history, but rollout of source used by live services must be deliberate.
3. Keep the documented distinction between baseline services and immutable controller releases, and make the baseline integration reproducible. Either update the baseline checkout to the reviewed complete revision or maintain a committed, explicit baseline integration bundle with matching dependencies/tests. Avoid continued ad hoc copying of individual files. The installed controller already matches the current feature tip on both nodes.
4. Bring baseline helpers, tests, ignore rules and status docs into agreement. Verify current saved-plan validation and preservation of unrelated aliases before any route publication.
5. Apply the outstanding 66f1 LiteLLM Compose configuration during a suitable CPU-service window, then check readiness, model discovery/authentication, legacy routing and the existing overrides. No model restart is inherently required for that CPU configuration work.
6. Install/check the missing declared system packages when authorized, then schedule the remaining hardware tests separately from active GLM use.

No merge, commit, PR, cleanup, source replacement, package installation or rollout was performed as part of this audit.

**Work still outstanding after reconciliation**

- 66f1 full single-node catalog acceptance and host CUDA smoke; broader per-model tool/vision acceptance, Vector and Loop managed GPU acceptance as listed in the branch checklist.
- The original Qwen3.8 uncached long-context/concurrency benchmark, 45-minute soak, full Qwen→DS4→Qwen rollback cycle and remaining custom-bind/client checks. No later inspected evidence supersedes the dedicated checklist.
- Physical failure/fencing/recovery campaigns, a stable front door across gateway-host loss, and availability of application state.
- Migrated-route virtual-key restrictions/accounting integration and authenticated FamChat/Open WebUI browser acceptance.
- DeepSeek agent-concurrency/context tradeoffs and compaction-quality/latency experiments; automatic node enrollment and topology generalization. These are explicitly deferred in the branch plans, so this audit did not start them.
- Remote drafting remains an unimplemented/unqualified extension for the pinned native DS4 engine. Kubernetes migration remains deferred. GLM-specific full-window concurrency and additional AIChat/OpenClaw end-to-end checks are separate from the already completed GLM campaign.

The following inventory captures the initial working tree before this report was added.

**File-by-file source inventory**

| File | Relationship to feature branch | Latest matching historical commit |
| --- | --- | --- |
| [.env.example](../../../.env.example) | Only trailing blank line differs |  |
| [Makefile](../../../Makefile) | Older committed version | ec0fc3f |
| [cluster/inventory.json](../../../cluster/inventory.json) | Identical to tip |  |
| [cluster/python/spark-host-cp312-aarch64.lock](../../../cluster/python/spark-host-cp312-aarch64.lock) | Identical to tip |  |
| [cluster/single-node-models.lock.json](../../../cluster/single-node-models.lock.json) | Identical to tip |  |
| [config/litellm.yaml](../../../config/litellm.yaml) | Only trailing blank line differs |  |
| [config/qwen38-pins.env](../../../config/qwen38-pins.env) | Identical to tip |  |
| [docker-compose.yml](../../../docker-compose.yml) | Identical to tip |  |
| [docs/CONTEXT_GUARD_PLACEMENT.md](../../../docs/CONTEXT_GUARD_PLACEMENT.md) | Identical to tip |  |
| [docs/ROUTING_FAILURES.md](../../../docs/ROUTING_FAILURES.md) | Identical to tip |  |
| [docs/SPARK_GPU_ADMISSION.md](../../../docs/SPARK_GPU_ADMISSION.md) | Identical to tip |  |
| [docs/SPARK_MODEL_PARITY.md](../../../docs/SPARK_MODEL_PARITY.md) | Older committed version | a496bda |
| [docs/SPARK_PYTHON_PARITY.md](../../../docs/SPARK_PYTHON_PARITY.md) | Older committed version | ce781d2 |
| [scripts/audit-stack-models.py](../../../scripts/audit-stack-models.py) | Identical to tip |  |
| [scripts/bootstrap-spark-python.py](../../../scripts/bootstrap-spark-python.py) | Identical to tip |  |
| [scripts/configure-context-routes.py](../../../scripts/configure-context-routes.py) | Older committed version | 9cb40c6 |
| [scripts/configure-native-fabric.py](../../../scripts/configure-native-fabric.py) | Identical to tip |  |
| [scripts/context-guard-proxy.py](../../../scripts/context-guard-proxy.py) | Identical to tip |  |
| [scripts/context-guard-router.py](../../../scripts/context-guard-router.py) | Identical to tip |  |
| [scripts/deepseek-v4.sh](../../../scripts/deepseek-v4.sh) | Identical to tip |  |
| [scripts/init-dirs.sh](../../../scripts/init-dirs.sh) | Identical to tip |  |
| [scripts/prepare-spark-model-runtimes.py](../../../scripts/prepare-spark-model-runtimes.py) | Identical to tip |  |
| [scripts/probe-context-route.py](../../../scripts/probe-context-route.py) | Older committed version | 737c462 |
| [scripts/probe-spark-python.py](../../../scripts/probe-spark-python.py) | Identical to tip |  |
| [scripts/probe-stack-models.py](../../../scripts/probe-stack-models.py) | Identical to tip |  |
| [scripts/qwen38-artifacts.py](../../../scripts/qwen38-artifacts.py) | Identical to tip |  |
| [scripts/qwen38-flash-next.sh](../../../scripts/qwen38-flash-next.sh) | Identical to tip |  |
| [scripts/resolve-spark-runtime-image.py](../../../scripts/resolve-spark-runtime-image.py) | Identical to tip |  |
| [scripts/spark-legacy-run.py](../../../scripts/spark-legacy-run.py) | Identical to tip |  |
| [scripts/spark_transfer.py](../../../scripts/spark_transfer.py) | Identical to tip |  |
| [scripts/sync-spark-model-parity.py](../../../scripts/sync-spark-model-parity.py) | Identical to tip |  |
| [scripts/sync-spark-models.py](../../../scripts/sync-spark-models.py) | Identical to tip |  |
| [tests/test_model_runtime_parity.py](../../../tests/test_model_runtime_parity.py) | Older committed version | a496bda |
| [tests/test_stream_failures.py](../../../tests/test_stream_failures.py) | Older committed version | ec0fc3f |
| [tools/spark_cluster/__init__.py](../../../tools/spark_cluster/__init__.py) | Identical to tip |  |
| [tools/spark_cluster/config.py](../../../tools/spark_cluster/config.py) | Older committed version | 6e24c5a |
| [tools/spark_cluster/gateway.py](../../../tools/spark_cluster/gateway.py) | Identical to tip |  |
| [tools/spark_cluster/gateway_node.py](../../../tools/spark_cluster/gateway_node.py) | Older committed version | 8983feb |
| [tools/spark_cluster/legacy.py](../../../tools/spark_cluster/legacy.py) | Identical to tip |  |
| [tools/spark_cluster/legacy_gateway.py](../../../tools/spark_cluster/legacy_gateway.py) | Identical to tip |  |
| [tools/spark_cluster/node.py](../../../tools/spark_cluster/node.py) | Older committed version | 8bd8be0 |

**Untracked runtime inventory**

| Path | Bytes | Treatment |
| --- | ---: | --- |
| `data/context-guard-routes/before-native-fabric.env` | 2,808 | Private configuration backup; never stage |
| `data/context-guard-routes/lock` | 0 | Local receipt/lock; exclude from source |
| `data/context-guard-routes/registry.json` | 2,125 | Active route configuration; retain privately |
| `data/context-route-tests/66f1-placement-20260909.json` | 1,837 | Local receipt/lock; exclude from source |
| `data/context-route-tests/backend-unavailable-20260909.json` | 252 | Local receipt/lock; exclude from source |
| `data/context-route-tests/stream-recovery-20260909.json` | 1,816 | Local receipt/lock; exclude from source |
| `data/python-parity/research-refusal-20260909/acceptance.json` | 154 | Local receipt/lock; exclude from source |
| `data/python-parity/research-refusal-20260909/request.json` | 656 | Local receipt/lock; exclude from source |
| `data/runtime-libraries/nccl/fc7ea66334edbc934aa25959b9907dbb2b91a1d2485beff18839afc45cbc08d0/libnccl.so.2` | 253,766,464 | Retain runtime asset; exclude from source |
| `data/runtime-overlays/175f5ebd900cccd4972d4ca500100354a4ed986d0bc0d9492b4df38f093ea785/vllm/models/deepseek_v4/nvidia/model.py` | 81,148 | Retain runtime asset; exclude from source |
| `data/runtime-overlays/1f4f11011bff2a4aab1f0deaaa1c77878c354e939e19441064bd56933c50919d/vllm/v1/core/kv_cache_coordinator.py` | 40,664 | Retain runtime asset; exclude from source |
| `data/runtime-overlays/874396a0971a7091540d8122db3ace3b038989f6917d8350e1155845bcb82610/vllm/models/glm5next/nvidia/model.py` | 50,510 | Retain runtime asset; exclude from source |
| `data/runtime-overlays/918b0d8cd4392b7ad81958005aec900eac788d0322a106e3792ad5ef7772f525/chat_template_mm.jinja` | 11,213 | Retain runtime asset; exclude from source |
| `data/runtime-overlays/e4305933dec1f2f19e050494f9ad89ad97d94e80a33b477ca3a0739b1ced72c2/vllm/model_executor/layers/sparse_attn_indexer_kpool.py` | 46,180 | Retain runtime asset; exclude from source |

**Evidence files**

- [reconciliation.json](reconciliation.json)
- [local-to-remote.diff.gz](local-to-remote.diff.gz)
- [runtime-summary.json](runtime-summary.json)
- [peer-summary.json](peer-summary.json)
- [checks.json](checks.json)
- [local-tests.xml](local-tests.xml)
- [remote-tests.xml](remote-tests.xml)
- [remote-git-check.xml](remote-git-check.xml)

The initial remote test XML retains the archive/Git setup failure; `remote-git-check.xml` records its successful rerun. GitHub source: [feature branch](https://github.com/olivierzach/local-llm-stack/tree/696af2ae90eede000164ac9ee992281917db25b4).

The exact source diff is compressed to preserve its whitespace; XML receipts have trailing line whitespace normalized. Original receipts are also retained in the private source rollback archive.
