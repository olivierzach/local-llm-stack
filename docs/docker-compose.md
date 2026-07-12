# Docker Compose

Docker images are packaged software. Containers are running instances of those images.

Compose is the file-driven way to start several containers together.

In this project, `docker-compose.yml` defines:

```text
vllm-fast
vllm-balanced
vllm-large
litellm
open-webui
postgres
prometheus
grafana
training
```

## Why Compose Helps

Instead of remembering many long `docker run` commands, Compose records:

```text
which image to use
which ports to expose
which folders to mount
which environment variables to pass
which services depend on each other
```

Then this starts the default stack. Profiled model services such as `vllm-balanced` and larger backends require explicit startup commands.

```bash
sudo docker compose up -d
```

`-d` means detached mode, so containers keep running in the background.

## Makefile Shortcuts

The Makefile wraps common commands.

```bash
make up
make ps
make logs
make smoke
make down
```

When Docker needs sudo:

```bash
DOCKER_COMPOSE="sudo docker compose" make up
```

That does not change Compose. It only changes which command Make runs.

## Useful Commands

```bash
sudo docker compose ps
sudo docker compose logs -f vllm-fast
sudo docker compose stop vllm-balanced
sudo docker compose down
sudo docker system df
```
