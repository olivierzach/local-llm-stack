#!/usr/bin/env python3
"""Train a small chat-format LoRA adapter from a YAML config."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
ADAPTER_ROOT = REPO_ROOT / "models" / "adapters"


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Train a PEFT LoRA adapter for a chat model.")
    p.add_argument("--config", type=Path, default=Path("training/configs/qwen3-lora-smoke.yaml"))
    p.add_argument("--run-id", help="Optional adapter run id. Defaults to a UTC timestamp.")
    p.add_argument("--dry-run", action="store_true", help="Validate config and dataset without loading the model.")
    return p


def resolve_path(value: str | Path) -> Path:
    path = Path(value)
    if path.is_absolute() and path.exists():
        return path
    if str(path).startswith("/workspace/"):
        return REPO_ROOT / str(path).removeprefix("/workspace/")
    if path.is_absolute():
        return path
    return REPO_ROOT / path


def load_config(path: Path) -> dict[str, Any]:
    config_path = resolve_path(path)
    with config_path.open(encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if not isinstance(config, dict):
        raise ValueError(f"{config_path} must contain a YAML mapping")
    return config


def load_jsonl_dataset(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line_no, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            messages = item.get("messages")
            if not isinstance(messages, list) or not messages:
                raise ValueError(f"{path}:{line_no}: expected non-empty messages list")
            rows.append({"messages": messages})
    if not rows:
        raise ValueError(f"{path} did not contain any training rows")
    return rows


def adapter_paths(output_root: Path, run_id: str | None) -> tuple[Path, Path, Path, str]:
    output_root = output_root.resolve()
    adapter_root = ADAPTER_ROOT.resolve()
    if adapter_root not in output_root.parents and output_root != adapter_root:
        raise ValueError(f"output_dir must be under {adapter_root}; got {output_root}")

    selected_run_id = run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if "/" in selected_run_id or selected_run_id in {"", ".", ".."}:
        raise ValueError("--run-id must be a single path segment")

    run_dir = output_root / "runs" / selected_run_id
    staging_dir = run_dir.with_name(f".{run_dir.name}.tmp")
    current_link = output_root / "current"
    return run_dir, staging_dir, current_link, selected_run_id


def update_current_symlink(current_link: Path, run_dir: Path) -> None:
    current_link.parent.mkdir(parents=True, exist_ok=True)
    relative_target = os.path.relpath(run_dir, start=current_link.parent)
    tmp_link = current_link.parent / f".{current_link.name}.tmp"
    if tmp_link.exists() or tmp_link.is_symlink():
        tmp_link.unlink()
    os.symlink(relative_target, tmp_link)
    os.replace(tmp_link, current_link)


def write_metadata(run_dir: Path, config: dict[str, Any], dataset_path: Path, rows: int, run_id: str) -> None:
    metadata = {
        "run_id": run_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "base_model": config["base_model"],
        "dataset_path": str(dataset_path),
        "rows": rows,
        "adapter_run_dir": str(run_dir),
        "lora": config["lora"],
    }
    (run_dir / "adapter-run.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")


def render_chat(tokenizer: Any, messages: list[dict[str, str]]) -> str:
    if getattr(tokenizer, "chat_template", None):
        return tokenizer.apply_chat_template(messages, tokenize=False, add_generation_prompt=False)
    rendered = []
    for message in messages:
        rendered.append(f"{message['role']}: {message['content']}")
    return "\n".join(rendered) + "\n"


def main() -> int:
    args = parser().parse_args()
    config = load_config(args.config)
    dataset_path = resolve_path(config["dataset_path"])
    output_root = resolve_path(config["output_dir"])
    run_dir, staging_dir, current_link, run_id = adapter_paths(output_root, args.run_id)
    rows = load_jsonl_dataset(dataset_path)

    if args.dry_run:
        print(
            json.dumps(
                {
                    "base_model": config["base_model"],
                    "dataset_path": str(dataset_path),
                    "rows": len(rows),
                    "output_root": str(output_root),
                    "run_dir": str(run_dir),
                    "staging_dir": str(staging_dir),
                    "current_link": str(current_link),
                    "lora": config["lora"],
                },
                indent=2,
            )
        )
        return 0

    if run_dir.exists():
        print(f"Adapter run already exists: {run_dir}. Pick a new --run-id.", file=sys.stderr)
        return 2
    if staging_dir.exists():
        print(f"Staged adapter run already exists: {staging_dir}. Remove it or pick a new --run-id.", file=sys.stderr)
        return 2

    try:
        from datasets import Dataset
        from peft import LoraConfig, get_peft_model
        from transformers import AutoModelForCausalLM, AutoTokenizer, Trainer, TrainingArguments
    except ImportError as exc:
        print(
            "Missing training dependencies. Run this inside the training container "
            "or install the packages from training/Dockerfile.",
            file=sys.stderr,
        )
        print(str(exc), file=sys.stderr)
        return 2

    tokenizer = AutoTokenizer.from_pretrained(config["base_model"], trust_remote_code=True)
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    model = AutoModelForCausalLM.from_pretrained(
        config["base_model"],
        trust_remote_code=True,
        device_map="auto",
    )
    peft_config = LoraConfig(
        r=int(config["lora"]["r"]),
        lora_alpha=int(config["lora"]["alpha"]),
        lora_dropout=float(config["lora"]["dropout"]),
        target_modules=list(config["lora"]["target_modules"]),
        task_type="CAUSAL_LM",
    )
    model = get_peft_model(model, peft_config)

    texts = [render_chat(tokenizer, row["messages"]) for row in rows]

    def tokenize(batch: dict[str, list[str]]) -> dict[str, Any]:
        encoded = tokenizer(
            batch["text"],
            truncation=True,
            max_length=int(config["max_seq_length"]),
            padding="max_length",
        )
        encoded["labels"] = [ids.copy() for ids in encoded["input_ids"]]
        return encoded

    dataset = Dataset.from_dict({"text": texts}).map(tokenize, batched=True, remove_columns=["text"])
    training_args = TrainingArguments(
        output_dir=str(staging_dir),
        num_train_epochs=float(config["num_train_epochs"]),
        per_device_train_batch_size=int(config["per_device_train_batch_size"]),
        gradient_accumulation_steps=int(config["gradient_accumulation_steps"]),
        learning_rate=float(config["learning_rate"]),
        logging_steps=int(config.get("logging_steps", 1)),
        save_strategy="epoch",
        report_to=[],
        remove_unused_columns=False,
    )
    staging_dir.mkdir(parents=True)
    trainer = Trainer(model=model, args=training_args, train_dataset=dataset)
    trainer.train()
    model.save_pretrained(staging_dir)
    tokenizer.save_pretrained(staging_dir)
    write_metadata(staging_dir, config, dataset_path, len(rows), run_id)
    os.replace(staging_dir, run_dir)
    update_current_symlink(current_link, run_dir)
    print(f"Saved LoRA adapter run to {run_dir}")
    print(f"Updated current adapter link: {current_link} -> {os.readlink(current_link)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
