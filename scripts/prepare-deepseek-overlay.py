#!/usr/bin/env python3
"""Build a checksum-pinned Python overlay from an installed immutable image, without a GPU."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest', type=Path, required=True)
    parser.add_argument('--data', type=Path, default=Path.home() / 'projects/local-llm-stack/data')
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    relative = Path(manifest['path'])
    if relative.is_absolute() or '..' in relative.parts or relative.suffix != '.py':
        parser.error('overlay target must be a relative Python package path')
    original = subprocess.check_output(['docker', 'run', '--rm', '--network', 'none',
        '--entrypoint', 'cat', manifest['base_image'], '/usr/local/lib/python3.12/dist-packages/' + str(relative)])
    if hashlib.sha256(original).hexdigest() != manifest['base_sha256']:
        raise RuntimeError('base source SHA-256 mismatch; no overlay installed')
    suffix = (args.manifest.parent / manifest['append']).read_bytes()
    patched = original + b'\n\n' + suffix
    ast.parse(patched)
    digest = hashlib.sha256(patched).hexdigest()
    if digest != manifest['sha256']:
        raise RuntimeError('patched source SHA-256 mismatch; no overlay installed')
    destination = args.data / 'runtime-overlays' / digest / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink():
        raise RuntimeError('refusing a symlink overlay destination')
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as f:
        f.write(patched)
        temporary = Path(f.name)
    temporary.chmod(0o644)
    temporary.replace(destination)
    print(json.dumps({'path': str(destination), 'sha256': digest, 'gpu_used': False}))


if __name__ == '__main__':
    main()
