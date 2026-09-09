# Move a model without changing its client alias

Context Guard can keep its existing address (including port 4010) while selected
aliases use a different Spark or a combined deployment. For example, OMP keeps
`spark-context-guard/local-deepseek-v4-flash`. Change the server-side registry to
move that model; do not create another OMP provider or conversation.

This is opt-in. With `CONTEXT_GUARD_ROUTE_REGISTRY` empty, the original guard runs.
With a registry enabled, aliases absent from it retain the original LiteLLM
path. An explicitly migrated alias never falls back to the old model if its new
backend fails. Each request captures its endpoint, tokenizer and token limits
together; atomic registry updates affect subsequent requests.

Migrated aliases authenticate with the guard host's existing **LiteLLM master
key**. They do not forward that key to a model worker. Existing LiteLLM virtual
keys continue to work for unmigrated aliases; overrides currently reject them
rather than bypass their LiteLLM model restrictions, budgets and accounting.
Do not migrate a virtual-key-only workflow until that authorization integration
is implemented. Provider credentials, when needed, use the registry gateway's
separate origin-bound credential store.

## Native DeepSeek example

Configure either node's native listeners from its inventory without restarting
services:

```bash
cd ~/projects/local-llm-stack
make native-fabric-plan
make native-fabric-config
```

This sets `DEEPSEEKV4_BIND_HOST` and `QWEN38_BIND_HOST` to that host's first private
fabric address after checking it is assigned to the expected interface. It
preserves other `.env` settings and retains the original in
`data/context-guard-routes/before-native-fabric.env`. Keep that backup private;
it contains the original local credentials. Repeating the command is safe.

Both local LiteLLM and native tokenizer configuration follow these settings.
They take effect on the next model/router start. Finish other GPU work first,
then use the usual `make deepseekv4-up` after any existing DeepSeek instance has
been stopped. On September 9, both nodes were configured for their own fabric
addresses without restarting e8f1 DeepSeek or 66f1 research. Binding to the fabric
makes the worker reachable by the peer; do not expose an unauthenticated worker
on a public interface.

On the Spark running the client-facing Context Guard, save this as
`deepseek-routes.json` (adjust node and actual configured context as needed):

```json
{
  "version": 1,
  "routes": {
    "local-deepseek-v4-flash": {
      "base_url": "http://10.10.20.2:8011/v1",
      "upstream_model": "deepseek-v4-flash",
      "health_url": "http://10.10.20.2:8011/v1/models",
      "tokenizer_base_url": "http://10.10.20.2:8011",
      "context_tokens": 65536,
      "max_output_tokens": 4096,
      "capabilities": {
        "text": true, "vision": false, "tools": true, "streaming": true
      }
    }
  }
}
```

The registry describes a running backend; it does not start one or certify its
capabilities. Validate the backend and its configured context before publishing.

```bash
cd ~/projects/local-llm-stack
make context-routes ROUTES=deepseek-routes.json
```

This validates and atomically replaces the complete override registry, preserves
the other `.env` settings, enables overrides and applies only the CPU guard's
Compose configuration. First activation recreates the guard container; plan for
active requests to finish first. Later route replacements require no process
restart when its Compose configuration is unchanged. Omitted aliases return to
their legacy route. To disable all overrides and restore the original guard:

```bash
make context-routes-disable
```

With the target running and the registry active, verify the normal guard URL:

```bash
make context-route-test MODEL=local-deepseek-v4-flash RUN_ID=placement-001 TOOLS=1 THINKING_DISABLED=1
```

This checks model discovery, text and SSE completion, exact tokenizer counts and
context policy, invalid-key rejection, and a synthetic streamed tool call/result.
It executes no external tool. The test uses the current host's guard key and
starts no model. `GATEWAY_URL` can select another reachable guard using that same
credential. Use a fresh run ID for every receipt. Replica and authenticated cloud
backends have their separate probes; this command expects a single local backend
or TP/PP coordinator with its exact tokenizer endpoint.

## Single-node, TP/PP and replicas

Use saved controller plans to derive the exact model identity, context,
capabilities, endpoint and tokenizer contract instead of writing them manually:

```bash
.venv/bin/python scripts/configure-context-routes.py --plan /path/to/plan.json
docker compose up -d --no-deps context-guard
```

Repeat `--plan` for different aliases. Add `--replicas` for identical recipes
running as independent replicas. A TP/PP plan exposes its chosen coordinator's
endpoint; that coordinator can be either Spark. Changing the registry does not
start, stop or migrate model weights and does not interrupt research jobs.

Compute roles are interchangeable. The current client URL still depends on the
machine hosting that URL; this feature does not create a floating IP or make a
powered-off gateway available. Either Spark can host the same guard configuration
and routing commands. Automated front-door failover is separate work.
