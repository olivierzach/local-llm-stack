# Evals Lab

This repo uses small JSONL prompt sets to compare local model aliases.

## Prompt Files

Prompt sets live under `evals/prompts/`. `throughput.jsonl` is for tok/s comparison and `vision.jsonl` is for multimodal image checks.

Each row needs an `id` and either `prompt` or `messages`:

```json
{"id":"exact-local-stack-ok","prompt":"Reply with exactly: local stack ok","expect_exact":"local stack ok"}
```

Optional fields:

```text
max_tokens      override generation length for this prompt
temperature     override sampling for this prompt
expect_exact    compare stripped response text to this value
expect_json     parse the response as JSON
```

## Run Evals

```bash
set -a; source .env; set +a
python scripts/run-evals.py --models local-fast local-balanced --prompt-file evals/prompts/smoke.jsonl
```

Results are written to `evals/runs/` as JSONL. Each row includes model, prompt
id, latency, HTTP status, finish reason, response text, usage, and validator
results.

## What This Measures

Exact-match prompts catch regressions in routing, formatting, and deterministic
instruction following. They are intentionally brittle.

JSON validity checks catch structured-output failures without pretending the
content is semantically correct.

Latency records show whether a model alias is slower under the same prompt set.
They are not a benchmark unless the server state, concurrency, cache warmth,
and token budgets are controlled.

Qualitative notes still matter. For prompts without validators, read the saved
responses and record why one model is preferable before promoting it for a
workflow.

## Promotion Notes

Before adding or changing a default alias, keep a short note with:

```text
base model and candidate model
prompt file and result path
validator pass/fail summary
observed latency tradeoff
manual judgment
rollback command or config change
```

## Vision Rows

Rows can include `image_path` with a local file. The eval runner converts it to an OpenAI-compatible `image_url` data URL before sending the request.
