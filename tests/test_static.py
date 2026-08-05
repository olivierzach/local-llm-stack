from __future__ import annotations

import json
import os
from pathlib import Path
import py_compile
import re
import shutil
import subprocess

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]


def run(command: list[str], *, cwd: Path = ROOT, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, env=env, text=True, capture_output=True, check=False)


def test_shell_scripts_parse() -> None:
    scripts = sorted(str(path) for path in (ROOT / "scripts").glob("*.sh"))
    result = run(["bash", "-n", *scripts, str(ROOT / "tests" / "test.sh")])
    assert result.returncode == 0, result.stderr


def test_python_scripts_parse() -> None:
    paths = list((ROOT / "scripts").glob("*.py")) + list((ROOT / "training").glob("*.py"))
    assert paths
    for path in paths:
        py_compile.compile(str(path), doraise=True)


def test_git_diff_has_no_whitespace_errors() -> None:
    result = run(["git", "diff", "--check"])
    assert result.returncode == 0, result.stdout + result.stderr


def test_requirements_files_cover_tools_and_training() -> None:
    root_requirements = (ROOT / "requirements.txt").read_text()
    tool_requirements = (ROOT / "tools/requirements.txt").read_text()
    training_requirements = (ROOT / "training/requirements.txt").read_text()
    dockerfile = (ROOT / "training/Dockerfile").read_text()

    assert "-r tools/requirements.txt" in root_requirements
    assert "-r training/requirements.txt" in root_requirements
    assert "pytest" in tool_requirements
    for package in ["torch", "transformers", "accelerate", "peft", "trl", "datasets"]:
        assert package in training_requirements
    assert "training-requirements.txt" in dockerfile


def test_advertised_docs_pages_have_expected_titles() -> None:
    assert "# vLLM Deep Dive" in (ROOT / "docs/vllm-deep-dive.md").read_text()
    assert "# Debugging Checklist" in (ROOT / "docs/debugging-checklist.md").read_text()
    assert "# Troubleshooting Notes" not in (ROOT / "docs/vllm-deep-dive.md").read_text()
    assert "# Troubleshooting Notes" not in (ROOT / "docs/debugging-checklist.md").read_text()


def test_docs_index_markdown_links_exist() -> None:
    text = (ROOT / "docs/README.md").read_text()
    links = re.findall(r"\[[^]]+\]\(([^)]+\.md)\)", text)
    assert links
    missing = [target for target in links if not (ROOT / "docs" / target).is_file()]
    assert not missing


def test_spark_setup_runbook_is_complete() -> None:
    runbook = (ROOT / "docs/spark-setup-runbook.md").read_text()
    for required in [
        "make init",
        "python3 -m venv .venv",
        "python -m pip install -r tools/requirements.txt",
        "make check",
        "make gpu-check",
        "docker compose --profile training build training",
        "make download-model MODEL=Qwen/Qwen3-4B-Instruct-2507",
        "make download-model MODEL=Qwen/Qwen3-14B",
        "make download-vision",
        "make up",
        "make smoke",
        "make throughput-eval",
        "make vision-up",
        "make vision-eval",
        "make lora-train",
        "make lora-serve",
        "make lora-eval",
        "models/adapters/qwen3-4b-smoke-lora/current",
    ]:
        assert required in runbook


def test_vllm_avoids_unsupported_thinking_flag() -> None:
    assert "--default-chat-template-kwargs" not in (ROOT / "docker-compose.yml").read_text()


def test_optional_model_services_and_aliases_are_configured() -> None:
    compose = (ROOT / "docker-compose.yml").read_text()
    litellm = yaml.safe_load((ROOT / "config/litellm.yaml").read_text())
    aliases = {entry["model_name"] for entry in litellm["model_list"]}

    for service in ["vllm-qwen30a3b", "vllm-deepseek32b", "vllm-mistral24b", "vllm-gptoss120b", "model-cache"]:
        assert f"  {service}:" in compose

    for alias in ["local-qwen30-a3b", "local-deepseek-r1-qwen32b", "local-mistral-small", "local-gpt-oss-120b", "local-deepseek-v4-flash"]:
        assert alias in aliases


def load_context_guard_module():
    import importlib.util
    import sys

    module_path = ROOT / "scripts/context-guard-proxy.py"
    spec = importlib.util.spec_from_file_location("context_guard_proxy", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_context_guard_caps_output_to_remaining_budget() -> None:
    from types import SimpleNamespace

    module = load_context_guard_module()
    config = module.ProxyConfig(
        upstream_base_url="http://localhost:4000/v1",
        timeout_s=1,
        allow_no_auth=True,
        default_context_tokens=100,
        model_contexts={"tiny": 100},
        fallback_model_contexts={},
        headroom_tokens=8,
        default_output_tokens=32,
        min_output_tokens=16,
        keep_last_messages=2,
        summary_tokens=32,
        compact_source_chars=1000,
        chars_per_token=1.0,
        compact_model=None,
        discover_model_context=False,
        verbose=False,
    )
    handler = object.__new__(module.ContextGuardHandler)
    handler.server = SimpleNamespace(config=config)
    payload = {
        "model": "tiny",
        "messages": [{"role": "user", "content": "x" * 76}],
        "max_tokens": -57,
    }

    sanitized = handler.sanitize_max_tokens(payload)
    estimate = module.estimate_payload_tokens(sanitized, config.chars_per_token)
    budget = max(1, config.context_limit_for("tiny") - estimate - config.headroom_tokens)

    assert sanitized["max_tokens"] <= budget
    assert sanitized["max_tokens"] >= 1


def test_context_guard_fallback_contexts_include_routed_aliases() -> None:
    from types import SimpleNamespace

    module = load_context_guard_module()
    config = module.build_config(
        SimpleNamespace(
            upstream_base_url="http://localhost:4000/v1",
            timeout=1,
            allow_no_auth=False,
            verbose=False,
        )
    )

    assert config.fallback_model_contexts["local-coder"] == config.fallback_model_contexts["local-balanced"]
    assert config.fallback_model_contexts["local-balanced-smoke-lora"] == module.env_int("LORA_MAX_MODEL_LEN", 32768)
    assert config.fallback_model_contexts["local-deepseek-v4-flash"] == module.env_int("DEEPSEEKV4_MAX_MODEL_LEN", 32768)


def test_eval_prompt_files_exist_and_parse() -> None:
    prompt_dir = ROOT / "evals/prompts"
    for name in ["smoke", "json", "router", "throughput", "vision"]:
        assert (prompt_dir / f"{name}.jsonl").is_file()
        assert (prompt_dir / f"{name}.jsonl").stat().st_size > 0

    for path in prompt_dir.glob("*.jsonl"):
        for line_no, line in enumerate(path.read_text().splitlines(), 1):
            if not line.strip():
                continue
            item = json.loads(line)
            assert "id" in item, f"{path}:{line_no}: missing id"
            assert "prompt" in item or "messages" in item, f"{path}:{line_no}: missing prompt/messages"


def test_lora_workflow_config_is_wired() -> None:
    makefile = (ROOT / "Makefile").read_text()
    compose = (ROOT / "docker-compose.yml").read_text()
    litellm = yaml.safe_load((ROOT / "config/litellm.yaml").read_text())
    aliases = {entry["model_name"] for entry in litellm["model_list"]}
    config = yaml.safe_load((ROOT / "training/configs/qwen3-lora-smoke.yaml").read_text())

    for target in ["lora-train", "lora-serve", "lora-eval"]:
        assert re.search(rf"^{target}:", makefile, re.MULTILINE)

    assert "lora-eval:\n\tset -a; source .env; set +a; python scripts/run-evals.py" in makefile
    assert "local-balanced-smoke-lora" in aliases
    assert "  vllm-lora:" in compose
    assert "--lora-modules" in compose
    assert "qwen3-4b-smoke-lora/current" in compose

    dataset_path = config["dataset_path"]
    host_dataset = ROOT / dataset_path.removeprefix("/workspace/")
    assert host_dataset.is_file()
    assert config["output_dir"].startswith("/workspace/models/adapters/")


def test_vision_and_throughput_workflow_config_is_wired() -> None:
    makefile = (ROOT / "Makefile").read_text()
    compose = (ROOT / "docker-compose.yml").read_text()
    env_example = (ROOT / ".env.example").read_text()
    litellm = yaml.safe_load((ROOT / "config/litellm.yaml").read_text())
    aliases = {entry["model_name"] for entry in litellm["model_list"]}

    for target in ["download-vision", "throughput-eval", "vision-up", "vision-eval"]:
        assert re.search(rf"^{target}:", makefile, re.MULTILINE)

    assert "local-vision" in aliases
    assert "vision-up:\n\t$(DOCKER_COMPOSE) up -d postgres\n\t$(DOCKER_COMPOSE) --profile vision up -d vllm-vision\n\t$(DOCKER_COMPOSE) up -d --no-deps litellm" in makefile
    assert "  vllm-vision:" in compose
    assert "Qwen/Qwen3-VL-4B-Instruct" in env_example
    assert (ROOT / "evals/assets/red-square.png").is_file()
    assert '"image_path":"evals/assets/red-square.png"' in (ROOT / "evals/prompts/vision.jsonl").read_text()


def test_lora_train_dry_run_reports_versioned_paths() -> None:
    result = run([
        str(ROOT / ".venv/bin/python") if (ROOT / ".venv/bin/python").exists() else "python3",
        str(ROOT / "training/train_lora.py"),
        "--config",
        str(ROOT / "training/configs/qwen3-lora-smoke.yaml"),
        "--run-id",
        "pytest-run",
        "--dry-run",
    ])
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)
    assert payload["output_root"].endswith("models/adapters/qwen3-4b-smoke-lora")
    assert payload["run_dir"].endswith("models/adapters/qwen3-4b-smoke-lora/runs/pytest-run")
    assert payload["staging_dir"].endswith("models/adapters/qwen3-4b-smoke-lora/runs/.pytest-run.tmp")
    assert payload["current_link"].endswith("models/adapters/qwen3-4b-smoke-lora/current")


def test_lora_current_symlink_update_is_atomic(tmp_path: Path) -> None:
    import importlib.util

    module_path = ROOT / "training/train_lora.py"
    spec = importlib.util.spec_from_file_location("train_lora", module_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    run_dir = tmp_path / "adapter" / "runs" / "run-a"
    run_dir.mkdir(parents=True)
    current = tmp_path / "adapter" / "current"
    module.update_current_symlink(current, run_dir)

    assert current.is_symlink()
    assert os.readlink(current) == "runs/run-a"


def test_init_creates_dirs_when_chown_is_skipped(tmp_path: Path) -> None:
    result = run(["env", "SKIP_CHOWN=1", str(ROOT / "scripts/init-dirs.sh")], cwd=tmp_path)
    assert result.returncode == 0, result.stdout + result.stderr
    for path in ["data/huggingface", "data/datasets/processed", "evals/runs", "models/adapters"]:
        assert (tmp_path / path).is_dir()


def test_init_chown_failure_reports_remediation(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    chown = fake_bin / "chown"
    chown.write_text("#!/usr/bin/env bash\nexit 1\n")
    chown.chmod(0o755)

    env = {"PATH": f"{fake_bin}:{shutil.which('bash') and '/usr/bin:/bin'}"}
    result = run([str(ROOT / "scripts/init-dirs.sh")], cwd=tmp_path, env=env)

    assert result.returncode != 0
    output = result.stdout + result.stderr
    assert "ERROR: Could not set ownership for data/prometheus." in output
    assert "sudo chown -R 65534:65534 data/prometheus" in output
    assert "sudo chown -R 472:472 data/grafana" in output


def test_init_chown_success_uses_expected_owners(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    log = tmp_path / "chown.log"
    chown = fake_bin / "chown"
    chown.write_text('#!/usr/bin/env bash\nprintf "%s\\n" "$*" >>"$CHOWN_LOG"\n')
    chown.chmod(0o755)

    env = {
        "PATH": f"{fake_bin}:/usr/bin:/bin",
        "CHOWN_LOG": str(log),
    }
    result = run([str(ROOT / "scripts/init-dirs.sh")], cwd=tmp_path, env=env)
    assert result.returncode == 0, result.stdout + result.stderr

    lines = log.read_text().splitlines()
    assert "-R 65534:65534 data/prometheus" in lines
    assert "-R 472:472 data/grafana" in lines
