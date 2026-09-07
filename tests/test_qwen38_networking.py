import os
from pathlib import Path
import shutil
import subprocess

import pytest
import yaml

from test_static import load_context_guard_module

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("api_base", ["http://172.99.0.1:8123/v1", "http://172.99.0.1:8123/v1/"])
def test_guard_uses_qwen_private_endpoint(monkeypatch, api_base):
    module = load_context_guard_module()
    monkeypatch.setenv("QWEN38_API_BASE", api_base)
    monkeypatch.delenv("CONTEXT_GUARD_TOKENIZER_BASE_URLS", raising=False)
    config = module.build_config(module.parser().parse_args([]))
    assert config.tokenizer_base_urls["local-qwen38-flash-next"] == "http://172.99.0.1:8123"
    assert config.tokenizer_base_urls["local-fast"] == module.DEFAULT_TOKENIZER_BASE_URLS["local-fast"]
    assert module.DEFAULT_TOKENIZER_BASE_URLS["local-qwen38-flash-next"] == "http://localhost:8012"


def test_explicit_tokenizer_override_still_wins(monkeypatch):
    module = load_context_guard_module()
    monkeypatch.setenv("QWEN38_API_BASE", "http://172.99.0.1:8123/v1")
    monkeypatch.setenv("CONTEXT_GUARD_TOKENIZER_BASE_URLS", "local-qwen38-flash-next=http://custom:9000")
    config = module.build_config(module.parser().parse_args([]))
    assert config.tokenizer_base_urls["local-qwen38-flash-next"] == "http://custom:9000"


@pytest.mark.parametrize("settings,expected", [
    ("", "http://172.99.0.1:8012/v1"),
    ("QWEN38_BIND_HOST=192.0.2.10\nQWEN38_PORT=8123\n", "http://192.0.2.10:8123/v1"),
    ("QWEN38_API_BASE=http://explicit:8124/v1\n", "http://explicit:8124/v1"),
])
def test_host_make_target_resolves_private_endpoint_without_starting_services(tmp_path, settings, expected):
    shutil.copy(ROOT / "Makefile", tmp_path / "Makefile")
    (tmp_path / ".env").write_text(settings)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for name, content in {
        "docker": '#!/bin/sh\n[ "$1 $2 $3" = "network inspect bridge" ] || exit 99\nprintf "%s\\n" 172.99.0.1\n',
        "python": '#!/bin/sh\nprintf "%s\\n" "$QWEN38_API_BASE"\n',
    }.items():
        command = bin_dir / name
        command.write_text(content)
        command.chmod(0o755)
    env = {k: v for k, v in os.environ.items() if not k.startswith("QWEN38_")}
    env["PATH"] = str(bin_dir) + os.pathsep + env["PATH"]
    result = subprocess.run(["make", "--no-print-directory", "context-guard"], cwd=tmp_path,
                            env=env, capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines()[-1] == expected


def test_compose_routes_use_the_same_configurable_private_host():
    services = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
    endpoint = "http://${QWEN38_BIND_HOST:-host.docker.internal}:${QWEN38_PORT:-8012}"
    assert services["litellm"]["environment"]["QWEN38_API_BASE"] == endpoint + "/v1"
    assert "local-qwen38-flash-next=" + endpoint in services["context-guard"]["environment"]["CONTEXT_GUARD_TOKENIZER_BASE_URLS"]
