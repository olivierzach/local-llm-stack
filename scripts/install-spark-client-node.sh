#!/usr/bin/env bash
set -euo pipefail
# Isolated OpenClaw runtime. Does not replace system/Homebrew node or npm.
version=22.23.2
case "$(uname -s)-$(uname -m)" in
  Darwin-arm64)
    platform=darwin-arm64
    checksum=61130f394c1630d211dd50aecc4353d379480f36d3ac913cd85dbba1aed585c6 ;;
  Linux-aarch64)
    platform=linux-arm64
    checksum=013b59cfd2819703a6f4a14ab891fc46fc2a4e3f5bcd92de3fb4929b43e35b30 ;;
  *) echo 'Supported platforms: macOS ARM64 and Linux ARM64' >&2; exit 1 ;;
esac
target="$HOME/.local/opt/spark-client-node/$version"
if [[ -x "$target/bin/node" ]]; then
  [[ "$("$target/bin/node" --version)" == "v$version" ]] || exit 1
  echo "Node $version already installed in $target"
  exit 0
fi
temporary="$(mktemp -d)"
trap 'rm -rf "$temporary"' EXIT
archive="node-v$version-$platform.tar.gz"
curl --fail --location --silent --show-error "https://nodejs.org/dist/v$version/$archive" -o "$temporary/$archive"
python3 - "$temporary/$archive" "$checksum" <<'PY'
import hashlib, pathlib, sys
assert hashlib.sha256(pathlib.Path(sys.argv[1]).read_bytes()).hexdigest() == sys.argv[2], 'Node release checksum mismatch'
PY
tar -xzf "$temporary/$archive" -C "$temporary"
mkdir -p "$(dirname "$target")"
[[ ! -e "$target" ]] || { echo 'Incomplete target exists; inspect before retrying' >&2; exit 1; }
mv "$temporary/node-v$version-$platform" "$target"
"$target/bin/node" --version
