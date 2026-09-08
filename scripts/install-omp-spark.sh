#!/usr/bin/env bash
set -euo pipefail
# Official standalone Linux ARM64 release; same OMP version as the controller Mac.
VERSION=18.1.11
SHA256=20dd6c946dcb61e2f79df83feeea513ae7ee93c71190d1cb8e7f81a82d5e5e13
URL="https://github.com/can1357/oh-my-pi/releases/download/v${VERSION}/omp-linux-arm64"
[[ "$(uname -s)" == Linux && "$(uname -m)" == aarch64 ]] || {
  echo 'This pinned installer supports Linux ARM64 only.' >&2; exit 2;
}
target="$HOME/.local/opt/omp/$VERSION/omp"
mkdir -p "$(dirname "$target")" "$HOME/.local/bin"
if [[ ! -f "$target" ]] || ! printf '%s  %s\n' "$SHA256" "$target" | sha256sum --check --status; then
  temporary="$(mktemp "$(dirname "$target")/.download.XXXXXX")"
  trap 'rm -f "$temporary"' EXIT
  curl --fail --location --silent --show-error --retry 3 "$URL" -o "$temporary"
  printf '%s  %s\n' "$SHA256" "$temporary" | sha256sum --check --status
  chmod 755 "$temporary"
  mv "$temporary" "$target"
fi
[[ "$("$target" --version)" == "omp/$VERSION" ]] || { echo 'OMP version mismatch' >&2; exit 1; }
if [[ -e "$HOME/.local/bin/omp" && ! -L "$HOME/.local/bin/omp" ]]; then
  echo 'An unmanaged ~/.local/bin/omp exists; refusing to overwrite it.' >&2; exit 1
fi
ln -sfn "$target" "$HOME/.local/bin/omp"
"$HOME/.local/bin/omp" --version
