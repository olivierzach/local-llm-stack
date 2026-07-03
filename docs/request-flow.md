# Request Flow

There are three useful test paths.

## Path 1: vLLM Direct

```text
curl -> Spark port 8001 -> vLLM fast container -> model
```

Test:

```bash
curl -sS http://localhost:8001/v1/models
```

If this fails, fix vLLM first.

## Path 2: LiteLLM To vLLM

```text
curl -> Spark port 4000 -> LiteLLM -> vLLM fast -> model
```

Test:

```bash
make smoke
```

If `/v1/models` works but chat fails, LiteLLM is up but vLLM is unreachable or not ready.

## Path 3: Browser UI

```text
Mac Mini browser -> Spark port 3000 -> Open WebUI -> LiteLLM -> vLLM -> model
```

Open:

```text
http://192.168.1.31:3000
```

Debug from the bottom up:

```text
1. vLLM direct
2. LiteLLM smoke
3. Open WebUI browser
```

That avoids guessing whether the UI, router, or model server is the real failure.
