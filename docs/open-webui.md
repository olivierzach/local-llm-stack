# Open WebUI

Open WebUI is the browser app.

It gives you:

```text
chat UI
conversation history
model picker
settings
user-facing workflow
```

It does not run the model. It sends requests through Context Guard, which forwards to LiteLLM.

In this stack:

```text
Open WebUI container port: 8080
Spark exposed port:       3000
Browser URL:              http://<spark-lan-ip>:3000
```

Open WebUI is configured with:

```text
OPENAI_API_BASE_URL=http://context-guard:4010/v1
OPENAI_API_KEY=<LiteLLM key from .env>
```

Inside Docker, `context-guard` is a service name. Docker DNS resolves it to the Context Guard container.

## Request Path From UI

```text
Browser
  -> Open WebUI :3000
  -> Context Guard :4010
  -> LiteLLM :4000
  -> vLLM backend
```

If Open WebUI loads but chat fails, test Context Guard, LiteLLM, and vLLM directly before debugging the browser.
