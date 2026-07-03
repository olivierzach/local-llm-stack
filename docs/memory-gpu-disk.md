# Memory, GPU, And Disk

There are three resources to watch:

```text
disk storage
system RAM / unified memory
GPU compute activity
```

## Disk

Disk is shown with:

```bash
df -h /
```

Your main disk line looks like:

```text
/dev/nvme0n1p2  3.7T  105G  3.4T  3% /
```

That means the Spark has 3.7 TB total, 105 GB used, and 3.4 TB available.

Docker images and downloaded model files use disk.

Check Docker disk usage:

```bash
sudo docker system df
```

Check project cache/data:

```bash
du -sh ~/projects/local-llm-stack/data ~/projects/local-llm-stack/models
```

## RAM

RAM is shown with:

```bash
free -h
```

The useful number is usually `available`.

Running containers, loaded model weights, and KV cache use memory.

## GPU

GPU status is shown with:

```bash
nvidia-smi
```

On this GB10 Spark, `nvidia-smi` may not show a normal GPU memory number. That makes `free -h` especially important because the platform uses unified memory behavior.

## One Watch Command

```bash
watch -n 2 'printf "== RAM ==
"; free -h; printf "
== DISK ==
"; df -h /; printf "
== GPU ==
"; nvidia-smi; printf "
== DOCKER ==
"; sudo docker system df; printf "
== PROJECT CACHE ==
"; du -sh ~/projects/local-llm-stack/data ~/projects/local-llm-stack/models'
```

Use `Ctrl+C` to stop.

## Mental Model

```text
pull Docker image      -> disk increases
download model         -> disk increases
start vLLM             -> RAM/GPU memory increases
send long prompt       -> KV cache memory increases
stop vLLM              -> RAM/GPU memory drops
remove image/cache     -> disk drops
```

## vLLM Token And KV Metrics

vLLM exposes Prometheus metrics directly on each backend:

```bash
curl -sS http://localhost:8001/metrics
```

Useful metric names:

```text
vllm:generation_tokens_total    cumulative output tokens
vllm:prompt_tokens_total        cumulative input/prefill tokens
vllm:kv_cache_usage_perc        fraction of allocated KV cache in use
vllm:num_requests_running       active generation requests
vllm:num_requests_waiting       queued requests
vllm:prefix_cache_hits_total    cached prefix tokens reused
vllm:prefix_cache_queries_total prefix-cache lookup tokens
```

Use the helper script for live token/sec:

```bash
./scripts/watch-vllm.sh
```

For the balanced backend:

```bash
VLLM_METRICS_URL=http://localhost:8002/metrics ./scripts/watch-vllm.sh
```
