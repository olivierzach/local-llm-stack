# Networking And Ports

A service is reached by combining a host address and a port.

For physical cabling, Internet/default-route setup, persistent management Ethernet,
trusted SSH and reconnecting after losing access, use the
[connection playbook](SPARK_CONNECTIONS_RUNBOOK.md). For inter-Spark collective
links, use the [multi-Spark playbook](SPARK_MULTI_NODE_RUNBOOK.md). This page explains
application ports; it does not provision those underlying paths.

For this stack:

```text
Spark LAN IP: <spark-lan-ip>
Open WebUI:   http://<spark-lan-ip>:3000
LiteLLM API:  http://<spark-lan-ip>:4000/v1
vLLM fast:    http://<spark-lan-ip>:8001/v1
```

## Host Port vs Container Port

Docker Compose maps a port on the Spark to a port inside the container.

Example:

```yaml
ports:
  - "3000:8080"
```

Read it as:

```text
Spark host port 3000 -> container port 8080
```

From another machine, use the left side:

```text
http://<spark-lan-ip>:3000
```

The browser never directly uses the container port. Docker receives traffic on the Spark host port and forwards it into the container.

## What 0.0.0.0 Means

This stack uses bindings like:

```yaml
"${BIND_HOST:-127.0.0.1}:${OPEN_WEBUI_PORT:-3000}:8080"
```

`127.0.0.1` means listen only on the host. Set `BIND_HOST=0.0.0.0` to allow another device on the LAN to connect.

If `BIND_HOST=0.0.0.0`, Docker listens on all host network interfaces.

## Tailscale

Tailscale gives the Spark another private IP, usually `100.x.y.z`. Approved users on the tailnet can access the same ports over that private network:

```text
http://<spark-tailscale-ip>:3000
```

Tailscale does not run the LLM. It is only the secure network path.
