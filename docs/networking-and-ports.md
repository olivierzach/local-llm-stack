# Networking And Ports

A service is reached by combining a host address and a port.

For this stack:

```text
Spark LAN IP: 192.168.1.31
Open WebUI:   http://192.168.1.31:3000
LiteLLM API:  http://192.168.1.31:4000/v1
vLLM fast:    http://192.168.1.31:8001/v1
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
http://192.168.1.31:3000
```

The browser never directly uses the container port. Docker receives traffic on the Spark host port and forwards it into the container.

## What 0.0.0.0 Means

This stack uses bindings like:

```yaml
"${BIND_HOST:-0.0.0.0}:${OPEN_WEBUI_PORT:-3000}:8080"
```

`0.0.0.0` means listen on all Spark network interfaces. That allows another device on the LAN, like the Mac Mini, to connect.

If `BIND_HOST=127.0.0.1`, only the Spark itself can connect.

## Tailscale

Tailscale gives the Spark another private IP, usually `100.x.y.z`. Approved users on the tailnet can access the same ports over that private network:

```text
http://<spark-tailscale-ip>:3000
```

Tailscale does not run the LLM. It is only the secure network path.
