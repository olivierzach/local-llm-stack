#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

pass() {
  printf 'ok - %s\n' "$1"
}

fail() {
  printf 'not ok - %s\n' "$1" >&2
  exit 1
}

run_in_tmp() {
  local tmp
  tmp="$(mktemp -d)"
  (
    cd "$tmp"
    "$@"
  )
  rm -rf "$tmp"
}

test_shell_syntax() {
  bash -n "$ROOT"/scripts/*.sh
  pass "shell scripts parse"
}

test_diff_whitespace() {
  git -C "$ROOT" diff --check
  pass "git diff has no whitespace errors"
}

test_docs_pages_restored() {
  grep -qx '# vLLM Deep Dive' "$ROOT/docs/vllm-deep-dive.md"
  grep -qx '# Debugging Checklist' "$ROOT/docs/debugging-checklist.md"
  ! grep -q '# Troubleshooting Notes' "$ROOT/docs/vllm-deep-dive.md" "$ROOT/docs/debugging-checklist.md"
  pass "advertised docs pages have restored titles"
}

test_docs_links_exist() {
  local link target
  while IFS= read -r link; do
    target="${link#*](}"
    target="${target%)}"
    [[ -f "$ROOT/docs/$target" ]] || fail "missing docs target: $target"
  done < <(grep -oE '\[[^]]+\]\([^)]+\.md\)' "$ROOT/docs/README.md")
  pass "docs index markdown links exist"
}

test_vllm_avoids_unsupported_thinking_flag() {
  ! grep -q -- '--default-chat-template-kwargs' "$ROOT/docker-compose.yml" || fail "pinned vLLM image does not support default chat-template kwargs"
  pass "vLLM config avoids unsupported thinking flag"
}

test_model_zoo_config() {
  for service in vllm-qwen30a3b vllm-deepseek32b vllm-mistral24b model-cache; do
    grep -q "^  ${service}:" "$ROOT/docker-compose.yml" || fail "missing compose service: $service"
  done

  for alias in local-qwen30-a3b local-deepseek-r1-qwen32b local-mistral-small; do
    grep -q "model_name: ${alias}" "$ROOT/config/litellm.yaml" || fail "missing LiteLLM alias: $alias"
  done

  pass "optional model services and aliases are configured"
}

test_init_skip_chown_creates_dirs() {
  run_in_tmp env SKIP_CHOWN=1 "$ROOT/scripts/init-dirs.sh"
  pass "init creates directories when chown is skipped"
}

test_init_chown_failure_is_clear() {
  local tmp fake_bin output status
  tmp="$(mktemp -d)"
  fake_bin="$tmp/bin"
  mkdir -p "$fake_bin"
  printf '#!/usr/bin/env bash\nexit 1\n' >"$fake_bin/chown"
  chmod +x "$fake_bin/chown"

  set +e
  output="$(
    cd "$tmp"
    PATH="$fake_bin:$PATH" "$ROOT/scripts/init-dirs.sh" 2>&1
  )"
  status=$?
  set -e

  rm -rf "$tmp"

  [[ "$status" -ne 0 ]] || fail "init should fail when monitoring chown fails"
  grep -q 'ERROR: Could not set ownership for data/prometheus.' <<<"$output" || fail "missing prometheus chown error"
  grep -q 'sudo chown -R 65534:65534 data/prometheus' <<<"$output" || fail "missing prometheus remediation"
  grep -q 'sudo chown -R 472:472 data/grafana' <<<"$output" || fail "missing grafana remediation"
  pass "init chown failure reports remediation"
}

test_init_chown_success_calls_expected_owners() {
  local tmp fake_bin log
  tmp="$(mktemp -d)"
  fake_bin="$tmp/bin"
  log="$tmp/chown.log"
  mkdir -p "$fake_bin"
  printf '#!/usr/bin/env bash\nprintf "%%s\\n" "$*" >>"$CHOWN_LOG"\n' >"$fake_bin/chown"
  chmod +x "$fake_bin/chown"

  (
    cd "$tmp"
    CHOWN_LOG="$log" PATH="$fake_bin:$PATH" "$ROOT/scripts/init-dirs.sh"
  )

  grep -qx -- '-R 65534:65534 data/prometheus' "$log" || fail "prometheus chown args missing"
  grep -qx -- '-R 472:472 data/grafana' "$log" || fail "grafana chown args missing"
  rm -rf "$tmp"
  pass "init chown success uses expected owners"
}

test_shell_syntax
test_diff_whitespace
test_docs_pages_restored
test_docs_links_exist
test_vllm_avoids_unsupported_thinking_flag
test_model_zoo_config
test_init_skip_chown_creates_dirs
test_init_chown_failure_is_clear
test_init_chown_success_calls_expected_owners
