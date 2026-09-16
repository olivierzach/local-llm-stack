# Routing failures and streaming recovery

Run the deterministic CPU-only socket tests on either configured Spark:

```bash
cd ~/projects/local-llm-stack
make routing-failure-test
```

The command uses the stack's development `.venv` with pytest. It starts synthetic
HTTP backends and both guard types on ephemeral loopback ports. It does not load
models, contact live applications, acquire a GPU, or read family conversations.
An isolated controller checkout can also run the tests using the baseline
development interpreter; its lightweight controller `.venv` does not include
pytest.

## Failure behavior

For chat-completion SSE responses, Context Guard forwards complete events as they
arrive. Successful streams retain their bytes, token-policy headers and selected
deployment identity. It buffers only an unfinished event, with a 16 MiB per-event
limit, and handles LF, CRLF and CR delimiters across network read boundaries.

If the model closes early, stalls beyond the configured upstream read timeout,
sends malformed JSON, or cuts off a tool-call event, the guard emits:

```json
{"error":{"type":"upstream_stream_interrupted","message":"The model stream did not complete. Partial output is incomplete; retry explicitly."}}
```

This arrives as an SSE `data:` event. HTTP headers may already say 200, so clients
must inspect stream errors and require the normal completion marker. The guard
does not add `[DONE]` to a failed stream. A fragment of an unfinished event is
discarded instead of being concatenated with the error. Existing upstream error
events are preserved and do not receive a synthetic success marker.

A disconnect before response headers produces HTTP 502. An unavailable backend
detected during placement produces HTTP 503. The selected replica's in-flight
count is released when the request finishes or fails. Client cancellation also
closes the upstream response once the relay observes the closed client socket;
while the upstream is silent, this can take the configured read timeout.

The gateway never silently replays a failed generation on another replica or the
old legacy route. After a failure, a new explicit request can select another
healthy replica of the same pinned model. Partial tool arguments must not be
executed; the caller must require a completed tool-call response. A retry is a
new generation, not resumption of the old one.

## What these tests establish

The suite exercises real local sockets, including dropped connections, partial
events, read timeouts and TCP client resets, through both standalone and existing
Context Guard routing. It checks cleanup, preserved normal streaming, rejection
of corrupted streams, no automatic replay, and explicit retry to a healthy peer.
It does not establish GPU worker recovery, a physical cable partition, or failure
of the gateway host itself. Fam Chat's current OpenWebUI/guard address still
depends on 66f1 being reachable.

Changing a registry can move subsequent requests to another live model backend.
Updating this Python relay code requires restarting the CPU guard to activate
it. It does not require stopping or restarting any model or research workload.
