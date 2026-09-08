# Preparing a model that needs both Sparks

The candidate `Qwen/Qwen3-Next-80B-A3B-Instruct` BF16 model has 162,659,161,528
bytes of weight shards (151.49 GiB), exceeding either Spark's physical memory.
It is intended for combined-node acceptance. Its pinned runtime recognizes the
architecture, but full loading, memory fit, GPU kernels, generation, streaming
and performance are **not yet validated**. Existing single-node recipes remain
the supported starting point; this candidate adds no gateway route automatically.

The [upstream model](https://huggingface.co/Qwen/Qwen3-Next-80B-A3B-Instruct/blob/9c7f2fbe84465e40164a94cc16cd30b6999b0cc7/README.md)
is public, Apache-2.0 licensed, and uses a non-thinking Instruct template.
The repository's download manifest pins revision
`9c7f2fbe84465e40164a94cc16cd30b6999b0cc7`, all 51 file sizes and SHA-256s,
the runtime image digest and `huggingface_hub` version. Total download size is
162,682,272,937 bytes. Allow that space plus at least 10 GiB on each node;
existing models and runtime caches consume additional space.

## Download once on a Spark

Use an installed, immutable controller release. On e8f1, run the following once.
This CPU-only Docker job survives SSH disconnection, uses two download workers,
has a six-hour watchdog and preserves partially downloaded cache files. It uses
no Hugging Face credential. Docker must already contain the pinned runtime image
or be able to pull it; the explicit download-byte budget covers model files only.

```bash
cd "$(readlink -f ~/projects/local-llm-stack-cluster/current)"
model_source="$PWD"
model_cache="$HOME/projects/local-llm-stack/data/huggingface"
model_manifest="$model_source/cluster/model-downloads/qwen3-next-80b-instruct.json"
model_image=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["image"])' "$model_manifest")
mkdir -p "$model_cache"
docker run -d --name spark-model-fetch-9c7f2fbe8446 \
  --runtime runc --restart no --read-only \
  --user "$(id -u):$(id -g)" --cpus 4 --memory 8g --memory-swap 8g \
  --pids-limit 256 --tmpfs /tmp:rw,size=1g,mode=1777 --workdir /tmp \
  --label io.spark.model-download=9c7f2fbe84465e40164a94cc16cd30b6999b0cc7 \
  --mount "type=bind,src=$model_cache,dst=/cache" \
  --mount "type=bind,src=$model_source/scripts/fetch-pinned-spark-model.py,dst=/fetch.py,readonly" \
  --mount "type=bind,src=$model_manifest,dst=/manifest.json,readonly" \
  -e HF_HOME=/cache -e HF_HUB_DISABLE_IMPLICIT_TOKEN=1 \
  -e HF_HUB_DISABLE_PROGRESS_BARS=1 -e HF_XET_NUM_CONCURRENT_RANGE_GETS=4 \
  --entrypoint timeout "$model_image" --kill-after=30s 6h \
  python3 -u /fetch.py fetch --manifest /manifest.json --cache /cache \
  --lock-output /cache/locks/qwen3-next-80b-9c7f2fbe8446.json \
  --max-download-bytes 162682272937 --workers 2
```

Keep that release while the job exists: Docker mounts its exact script and
manifest. Record the returned container ID. A second launch with the same name
fails instead of creating a second writer. Inspect that job directly:

```bash
docker inspect --format '{{.Id}} {{.State.Status}} {{.State.ExitCode}}' spark-model-fetch-9c7f2fbe8446
docker logs --tail 10 spark-model-fetch-9c7f2fbe8446
```

Exit code 0 plus the final `verified: true` log receipt proves every downloaded
file matched its upstream size and SHA-256. A running container's displayed exit
code is not a success receipt. The copy lock is written only after verification.
Code 124 means the watchdog expired. Other failures retain logs and cached files.
After inspecting a **terminal failed** job and correcting its cause, resume its
same command/mounts with `docker start spark-model-fetch-9c7f2fbe8446`. Never
restart solely because an observation command timed out. Do not change mounted
source files during a download. To change the manifest or script, use a separate
reviewed job after the previous writer has stopped.

## Copy the verified cache over the fabric

Run on the download source, after successful verification:

```bash
cd "$(readlink -f ~/projects/local-llm-stack-cluster/current)"
python3 scripts/sync-spark-models.py sync \
  --cache ~/projects/local-llm-stack/data/huggingface \
  --lock ~/projects/local-llm-stack/data/huggingface/locks/qwen3-next-80b-9c7f2fbe8446.json \
  --peer spark-66f1-wired \
  --peer-cache /home/statsparrot/projects/local-llm-stack/data/huggingface
```

The source and destination hashes are checked, snapshot symlinks are preserved,
and unrelated cache entries remain intact. The command reports bytes and elapsed
copy-plus-destination-verification time. This includes SSH encryption and disk
I/O, so it is not a measurement of RDMA or NCCL bandwidth. e8f1 was selected as
download source because its measured RDMA sender direction is faster; actual
SSH model-copy throughput must still be measured independently.

The sync command is a foreground operation. For an unattended copy, run it in a
user service with `systemd-run --user --collect --unit=spark-large-model-copy`,
using absolute script, cache and lock paths. Inspect that exact service and its
journal before retrying. Do not run simultaneous copies into the same snapshot.
A failed copy can be rerun: destination hashes must pass before success is reported.

## Select the coordinator and parallelism

| Deployment | Coordinator | Tensor parallel | Pipeline parallel |
| --- | --- | ---: | ---: |
| `large-tp2-66f1.json` | 66f1 | 2 | 1 |
| `large-tp2-e8f1.json` | e8f1 | 2 | 1 |
| `large-pp2-66f1.json` | 66f1 | 1 | 2 |
| `large-pp2-e8f1.json` | e8f1 | 1 | 2 |

All four use the same model contract: `local-large`, 16,384 context tokens,
4,096 maximum output tokens and text/streaming capabilities. Tools and vision
are not advertised. The hybrid model uses prefix caching with the pinned
runtime's `mamba_cache_mode: align`; compiled execution remains disabled for
initial acceptance. TP/PP consume both GPUs, and only one such deployment can
reserve the nodes at a time.

```bash
scripts/sparkctl validate --deployment cluster/deployments/large-tp2-66f1.json
scripts/sparkctl render --deployment cluster/deployments/large-tp2-66f1.json
# Only after both cache checks pass and both GPUs are available:
scripts/sparkctl up --deployment cluster/deployments/large-tp2-66f1.json
scripts/sparkctl status --deployment cluster/deployments/large-tp2-66f1.json
scripts/sparkctl down --deployment cluster/deployments/large-tp2-66f1.json
```

Admission rejects incomplete numbered shards and index references before taking
a GPU lease. It also refuses busy GPUs or insufficient available shared memory.
This structural cache check is not a substitute for the upstream SHA-256 check.
Successful rendering does not prove hardware support; `up` must complete real
inference before readiness is published. Retain its saved plan for recovery.
After direct generation and streaming acceptance, explicitly add the deployment
to the chosen gateways using the [normal gateway workflow](CLUSTER.md). Clients
then use the same gateway contract regardless of coordinator placement.

Initial offline acceptance loaded `Qwen3NextConfig` and the tokenizer inside the
pinned image with networking and GPU access absent. Attention heads 16/2 and
linear heads 16/32 divide by two; 48 layers divide into two 24-layer stages.
The non-thinking chat template rendered correctly. These metadata checks do
not establish distributed kernel correctness or sufficient runtime memory.
