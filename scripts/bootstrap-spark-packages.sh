#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${1:---check}"
[[ "$MODE" == --check || "$MODE" == --install ]] || { echo 'Usage: bootstrap-spark-packages.sh [--check|--install]' >&2; exit 2; }
mapfile -t packages < <(sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' "$ROOT/cluster/system-packages.txt")
missing=()
for package in "${packages[@]}"; do
  [[ "$package" =~ ^[a-z0-9][a-z0-9.+-]*$ ]] || { echo "Invalid package: $package" >&2; exit 2; }
  if [[ "$(dpkg-query -W -f='${db:Status-Status}' "$package" 2>/dev/null || true)" != installed ]]; then
    missing+=("$package")
  fi
done
if (( ${#missing[@]} == 0 )); then
  echo 'System tool baseline complete.'
  exit 0
fi
printf 'Missing package: %s\n' "${missing[@]}"
[[ "$MODE" == --install ]] || exit 1
(( EUID == 0 )) || { echo 'Run sudo bash scripts/bootstrap-spark-packages.sh --install' >&2; exit 1; }
# Prevent iperf3 from being enabled as an unattended listening service.
printf 'iperf3 iperf3/start_daemon boolean false\n' | debconf-set-selections
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${missing[@]}"
bash "$0" --check
