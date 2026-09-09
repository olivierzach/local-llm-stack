# GPU admission for existing Make commands

On an inventoried Spark, the existing GPU Make targets now use the same local
reservation file and lock as `sparkctl`, `spark-loop`, `spark-vector` and the
model/Python acceptance tools. The commands and model aliases stay the same.
CPU-only operations such as `make init`, status and configuration checks remain
independent. Bare `make` still means `make init`.

Check a proposed operation without starting or stopping anything:

```bash
cd ~/projects/local-llm-stack
make gpu-admission-check TARGET=deepseekv4-up
make deepseekv4-up DRAFT_MODE=local
```

Admission refuses active or unresolved research windows, another controller's
reservation, unrecognized GPU containers/processes and conflicting resident
models. A refusal does not pause research. It must be run as the stack user on
the host identified by `cluster/inventory.json`; the reservation is per Unix
user, not a machine-wide security boundary.

The existing `qwen38-up` and `deepseekv4-up` commands remain explicit switches:
after admission and recipe preflight they may stop this checkout's other model
servers. They do not stop training. Other model start targets require you to stop
the current model first; they do not silently oversubscribe memory. `make up`
starts the default model and refuses nonempty `COMPOSE_PROFILES`; use the named
model target instead. Draft mode `remote` is still unsupported by pinned DS4
and is rejected before a switch.

For example, on a node running DeepSeek:

```bash
make deepseekv4-down
make large-up
```

`make down` intentionally stops this checkout's stack, including its CPU router
services. Prefer the model-specific stop command when preserving the gateway.
Changing the compute host does not automatically change a guard's configured
placement: use the [routing registry](CONTEXT_GUARD_PLACEMENT.md) to move the
stable alias to the new backend.

## Interrupted commands

A launch/switch records the parent and child process identities before allowing
Make to execute. Its durable transaction blocks competing launches while the
operation runs. Successful operations release that transaction; Compose or the
systemd user unit continues supervising the model, whose resident GPU workload
blocks conflicting controller placements.

A failed command that changed GPU workers retains their identities. If the
controller dies before recording a new worker, its transaction label identifies
that worker. Inspect the saved record and existing service logs, then recover:

```bash
cat ~/.local/state/local-llm-cluster/gpu.json
make gpu-recover
```

Recovery refuses a still-running operation, foreign ownership, active research
or replacement workers. It stops the recorded transaction's recognized stack
workers by exact container ID or reverified systemd invocation, then releases
the reservation after GPU cleanup. It does not automatically restart a previous
model or roll back CPU router configuration. An unchanged failed operation does
not retain a reservation. Never delete the reservation to force admission.

Internal `_spark-*` Make targets require ancestry from the admitted child Make;
calling one directly cannot bypass admission. `make -n` does not acquire a lease
or launch services. Direct Docker, systemd and launcher-script commands remain
low-level operations outside this protocol; use the public Make/controller
commands for coordinated operation.

## Rollout

Installing these source files does not restart services. New Compose launches
carry root and transaction labels; the first admitted start of an older
unlabelled Compose model may recreate it to add those labels. Native Qwen must
have the matching root label before admission can manage a resident container.
Native DeepSeek is also checked against its expected executable and systemd
invocation, so an existing service can be recognized without restarting it.
