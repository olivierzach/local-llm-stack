# Models, Downloads, And Cache

Models are identified by Hugging Face repo names, for example:

```text
Qwen/Qwen3-4B-Instruct-2507
```

vLLM sees that model name and uses Hugging Face tooling to download files if they are not already cached.

## Do You Need A Hugging Face Token?

Not always.

No token is needed for public models.

A token is needed for:

```text
private models
gated models
models requiring license acceptance
higher authenticated rate limits
```

The token goes in `.env`:

```text
HF_TOKEN=...
```

## Where Downloads Go

The Hugging Face cache is mounted here:

```text
/home/statsparrot/projects/local-llm-stack/data/huggingface
```

Inside the vLLM container, that same folder appears as:

```text
/home/vllm/.cache/huggingface
```

That mount is important. It means downloaded model files survive container restarts.

## How To Know Download/Cache Is Happening

Watch cache size:

```bash
watch -n 2 'du -sh ~/projects/local-llm-stack/data/huggingface; df -h /'
```

List recent cache files:

```bash
find ~/projects/local-llm-stack/data/huggingface -type f -printf '%TY-%Tm-%Td %TH:%TM %s %p
' | sort | tail -20
```

Watch logs:

```bash
sudo docker compose logs -f vllm-fast
```

## Disk vs Memory

Downloaded weights use disk.

Loaded weights use RAM/GPU memory.

Stopping containers frees runtime memory, but cached model files remain on disk.
