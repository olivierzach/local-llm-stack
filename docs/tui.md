# Terminal UI

AIChat is the default terminal chat client for this stack. It is configured as
an explicit Compose profile so it does not start with the default stack and does
not load any model by itself.

Build the client image once:

```bash
make aichat-build
```

Start an interactive chat:

```bash
make aichat
```

The service points at Context Guard inside the Compose network:

```text
http://context-guard:4010/v1
```

It uses the same `LITELLM_MASTER_KEY` from `.env`, passed to AIChat as
`SPARK_API_KEY`. The configured model is:

```text
local-gpt-oss-120b
```

This is a client-only integration. Keep vLLM backends under explicit control;
for example, start GPT-OSS deliberately with:

```bash
make gptoss120-up
```

AIChat session state is stored under `data/aichat/`.

## opencode

opencode is available as a coding-agent TUI through the same Compose profile. It
uses Context Guard as an OpenAI-compatible provider:

```text
http://context-guard:4010/v1
```

Start it in the current repository:

```bash
make opencode
```

The default model is:

```text
spark/local-gpt-oss-120b
```

The small model is:

```text
spark/local-fast
```

opencode session state is stored under `data/opencode/`. The repository is
mounted read-write at `/workspace` because opencode is a coding agent. Runtime
artifacts such as `data/`, local virtualenvs, logs, and the real `.env` are
masked inside the container so searches stay focused on source files and local
secrets are not exposed to the agent.
