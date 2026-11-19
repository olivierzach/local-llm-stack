# Flux/LTX qualification and production recovery

## Accepted scope — 2026-11-19 UTC

Both standalone services completed real, offline GPU inference on **each** DGX
Spark during an owner-approved exclusive maintenance window. Six Flux PNGs and
eight LTX GIFs passed decoding and request-contract checks. The experiment
containers were then stopped and removed, and the exact DeepSeek TP2 deployment
and Standard speech were restored on both hosts.

This is bounded hardware qualification, not always-on deployment or Fam Chat
generation integration. Flux and LTX remain opt-in services and must not be
started alongside the current DeepSeek TP2 owner. Generic `make up` is not the
qualified launch or recovery procedure.

Source and operational runbooks live in this repository:

- [Flux branch and runbook](https://github.com/olivierzach/local-llm-stack/blob/feat/flux-dev/docs/FLUX_DEV.md)
- [LTX branch and runbook](https://github.com/olivierzach/local-llm-stack/blob/feat/ltx-video/docs/LTX_VIDEO.md)
- [DeepSeek recovery contract](https://github.com/olivierzach/local-llm-stack/blob/feat/flux-dev/docs/FLUX_DEV.md#future-approved-serving-window-not-preparation-commands)

The service branches are separate from `main`; publication is not a merge,
controller migration, route change or authorization for another GPU window.

## Reproducible model and image identities

| Service | Model | Pinned revision |
|---|---|---|
| Flux | `black-forest-labs/FLUX.1-dev` | `3de623fc3c33e44ffbe2bad470d0f45bccf2eb21` |
| LTX | `Lightricks/LTX-Video-0.9.8-13B-distilled` | `7c64400e1861cc0d7b98d570a1926d5408ec60cd` |

Exact Docker image IDs, present and exercised on both hosts:

```text
Flux local-llm-stack-flux-dev:prepare-20261117
sha256:64025b7296145b0d74b4c014bca22edb75a162240de6720c32b762005fd89330

LTX local-llm-stack-ltx-video:prepare-20261117-r2
sha256:0f6e3d1b7609644a00033109c3c0a0157028e1c04419b08807d1ab9a36eca4b0
```

These are local image identities, not promises that a container registry serves
those images. Per-service image locks and Dockerfiles are in their source
branches. Existing caches were reused; generation ran with offline flags and
`local_files_only=True`. No model downloads, image pulls or cloud inference
were part of the window.

Before the window, both retained caches passed content-address and safetensors
range/shard checks. Nine Flux and fifteen LTX focused CPU regressions passed in
the prepared runtimes. Those preparation checks are distinct from the actual
GPU results below. Cache identities are not independent upstream signatures.

The LTX candidate pins SentencePiece 0.2.3 with Protobuf 7.36.1: the original
runtime lacked SentencePiece, and its inherited Protobuf 6.33.5 was incompatible
with the added tokenizer's generated code. The tested repair did not add a
`tiktoken` fallback or suppress version checks.

Open weights do not imply unrestricted licensing. Flux's default model license
is non-commercial; LTX has its own open-weight license conditions. Preserve the
applicable model terms when using or redistributing outputs or weights.

## Real generation acceptance

All requests used seed zero and ran sequentially, with one generation service
admitted at a time. Each host was checked independently; mirroring was not
substituted for executing the second host.

Flux used a synthetic red-fox-in-snow prompt:

1. Bounded first request: 512×512, four steps, including lazy model loading.
2. Quality request: 1024×1024, 30 steps, guidance 3.5.
3. Same-seed repeat of the quality request.

LTX used a synthetic red-ball-on-a-wooden-floor prompt and the accepted Flux
image as conditioning input:

1. Text-to-video: 384×384, 25 frames, 30 steps, including lazy loading.
2. Same-seed text repeat.
3. Image-to-video: 384×384, 25 frames, 30 steps, using a real base64 PNG.
4. Default-size text request: 672×384, 121 frames, 30 steps.

| Observed request duration (seconds) | Spark 66f1 | Spark e8f1 |
|---|---:|---:|
| Flux first request, including loading | 233.539 | 247.545 |
| Flux warm quality request | 50.753 | 50.807 |
| Flux warm repeat | 50.961 | 51.105 |
| LTX first text request, including loading | 255.547 | 228.024 |
| LTX warm text repeat | 39.114 | 38.797 |
| LTX image-conditioned request | 40.861 | 40.373 |
| LTX default 121-frame request | 143.267 | 142.600 |

HTTP status/content type, decoded dimensions, timing headers and output hashes
were retained. Every LTX GIF had the requested frame count, distinct decoded
frames and 40 ms per frame (25 fps). Same-seed repeats were byte-identical on
each host; all corresponding outputs also matched across hosts.

Representative SHA-256 values:

```text
Flux quality/repeat PNG
f924371b9de115d2b5b95cf642e5fc66301b0fb4f1a12701005e885f15da3ea5

LTX text/repeat GIF
f08c2b3c8333fe6a77042ec98c867a9b86ae9444a10232086ca0f6d03d299d9d

LTX image-conditioned GIF
9bf5bda30ca27624a433b66144216503a43af715c47f68904bda00398d6d9862

LTX default-size GIF
31cb36e0f611566686da3cab4081fff969d906ae2c87a4c638ad0af6bd739ba4
```

The fox PNG matched the prompt visually. The ball video showed movement across
inspected frames. The one-second image-conditioned fox clip preserved source
identity but showed limited motion: strong walking-prompt adherence was not
established. The actual PNG/GIF examples were opened in the owner's browser.

| Sampled memory observation | Spark 66f1 | Spark e8f1 |
|---|---:|---:|
| Flux minimum host available memory (GiB) | 78.52 | 78.78 |
| Flux maximum GPU-process memory (MiB) | 37193 | 37193 |
| LTX minimum host available memory (GiB) | 72.21 | 72.32 |
| LTX maximum GPU-process memory (MiB) | 43033 | 43033 |

Sampling interval was two seconds; these are observed samples, not exact
allocation peaks or proof of co-residency capacity.

## Exact production restoration

The canonical controller's rendered owner, digest, nodes, deployment, recipe,
Compose and endpoint inputs matched the preserved production plan before
shutdown. Backend running/waiting request counters were zero at the final
drain check. Both original speech units were recorded and paused, and only
the exact DeepSeek owner was released.

Each named experiment service was stopped and removed after use. Both GPUs
were idle and both reservations absent before DeepSeek preflight. The
unchanged 102400 MiB-per-host admission requirement passed; no memory guard
was reduced.

Restored identity:

```text
owner: deepseek-tp2-mia-512k-e8f1-d0d964379529
digest: d0d9643795291234c6ca9f45825b3646b8b771728f017a2c27531381dad97327
alias: local-deepseek-v4-flash-mia
context: 524288
maximum output: 8192
```

Both peers became healthy and built-in inference returned `ready`. Following
speech recovery, each legacy Context Guard returned the exact `STACK_READY`
marker with the promoted deployment digest. No Flux/LTX containers remained.
All four proxy registry hashes and both LiteLLM configuration hashes matched
their pre-window values: no route republication was required.

### Downstream acceptance

- **Both legacy Context Guards and both managed gateways:** real text, SSE,
  streamed tool call/result roundtrip, exact backend-tokenizer accounting,
  context/output limits, and expected authentication/input/vision rejection.
  Nine checks per endpoint passed. This was not a full-window context or
  streaming-latency benchmark.
- **OMP:** the existing configured provider executed a real restricted `read`
  tool and returned the exact synthetic fixture marker.
- **Fam Chat:** its actual composer submitted one synthetic message; the
  rendered AI reply matched. The message remains in application history.
- **Open WebUI:** its existing configured provider URL and credential returned
  `ready` with the Mia alias/digest. Authenticated browser chat was explicitly
  owner-deferred at the normal sign-in page; provider success is not UI proof.
- **OpenClaw:** its preexisting policy rejected Mia. After explicit owner
  approval, only the optional Mia provider-model entry, display alias and
  model-policy allowance were added. Schema validation and real local gateway
  inference passed with exact provider/model provenance. The cloud default
  and all existing models remained unchanged. Tools and delivery were disabled
  for that synthetic request; no Telegram message was sent.
- **Speech:** both original units were restored active/running, with boot
  enablement still disabled. Fresh Standard/Kokoro synthesis through Fam Chat
  selected each exact requested host/model and a cache miss. Both PCM WAVs
  decoded and completed browser playback; physical audibility was not claimed.

PersonalTube and Vector Bucket were already unavailable before this window.
They were not started, and unrelated schedulers were not enabled.

### Speech recovery observation

Wait for HTTP startup, not merely systemd `active`. The first speech probe
preceded listener readiness and was retained as a failed attempt.

Separately, e8f1's cold speech worker failed allocating its CUDA context even
though `MemAvailable` was about 15 GiB; `MemFree` was about 2 GiB. Targeted
`POSIX_FADV_DONTNEED` hints on clean checkpoint files released about 11.3 GiB of
redundant disk cache. The next cold load and exact-host synthesis passed,
without restarting DeepSeek, changing its recipe, lowering guards, deleting
files, or changing global VM settings. The earlier hint on retired generation
cache files had no material effect and was not treated as a fix.

This is an observed recovery, not proof that `MemAvailable` guarantees CUDA
context-allocation headroom. Retain failed-load, kernel, memory and successful
recovery receipts when investigating a recurrence; do not replace diagnosis
with global cache dropping or automatic load retries.

## Evidence custody and remaining limits

Private receipts remain outside Git in the operator's canonical checkout:

```text
data/cluster/generation-window-Leqfzilp/
  closure.json
  restored/{plan,acceptance,endpoint}.json
  final-status.json
  spark-<host>-wired-{flux,ltx}/
  guard-protocol-after/
  omp-after/
  app-acceptance-result.json
  openclaw-enabled-acceptance/
  speech-*.json
  generation-examples-and-verification.tar.gz
```

The current accepted recovery plan is `restored/plan.json` in that window,
not an older readiness advertisement. The original saved plan and source
inputs were preserved before teardown. Host-local generation receipts and a
closure copy also remain on each Spark. Private configuration backups, tokens,
route archives, model caches, generated media and raw logs must not be added
to this public repository. The examples bundle deliberately excludes private
configuration and route backups.

Not qualified: maximum image/video sizes, arbitrary prompt quality, sustained
traffic/concurrency, DeepSeek/generation co-residency, full-window context
compaction, failover, Large/F5 speech, ASR, physical audibility, or reboot and
unattended recovery. No generic Compose startup, default-model change, public
service exposure, or new production generation route is implied by publication.
