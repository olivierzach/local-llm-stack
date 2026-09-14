#!/usr/bin/env python3
"""Stage pinned GLM runtime assets without starting a GPU process."""
import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def build_patch(original, patch):
    if hashlib.sha256(original).hexdigest() != patch['base_sha256']:
        raise RuntimeError('GLM base source checksum mismatch; no patch applied')
    with tempfile.TemporaryDirectory() as folder:
        source = Path(folder) / 'source.py'
        source.write_bytes(original)
        if patch.get('patch_script') == 'patch_hybrid_prefix_hit.py':
            subprocess.run([sys.executable, ROOT / 'cluster/runtime-overlays/glm53/patch_hybrid_prefix_hit.py'],
                           env={**os.environ, 'GLM53_KV_COORDINATOR_PY': str(source)}, check=True)
        elif 'replace' in patch:
            text = original.decode()
            old, new = patch['replace']['old'], patch['replace']['new']
            if text.count(old) != 1:
                raise RuntimeError('GLM patch anchor mismatch')
            source.write_text(text.replace(old, new, 1))
        else:
            raise RuntimeError('unsupported GLM patch')
        result = source.read_bytes()
    ast.parse(result)
    if hashlib.sha256(result).hexdigest() != patch['sha256']:
        raise RuntimeError('GLM patched source checksum mismatch')
    return result


def stage(source, data, relative, expected):
    content = source.read_bytes()
    if hashlib.sha256(content).hexdigest() != expected:
        raise RuntimeError('GLM asset checksum mismatch')
    destination = data / 'runtime-overlays' / expected / relative
    if destination.is_symlink():
        raise RuntimeError('refusing symlink asset destination')
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        if destination.read_bytes() != content:
            raise RuntimeError('existing immutable asset differs')
        return destination
    with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as stream:
        stream.write(content)
        temporary = Path(stream.name)
    temporary.chmod(0o644)
    temporary.replace(destination)
    return destination


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data', type=Path, default=Path.home() / 'projects/local-llm-stack/data')
    args = parser.parse_args()
    recipe = json.loads((ROOT / 'cluster/recipes/glm53-w4a16-256k-dflash2.json').read_text())
    manifest = json.loads((ROOT / 'cluster/runtime-overlays/glm53/patches.json').read_text())
    if manifest['version'] != 1 or manifest['image'] != recipe['image']:
        raise RuntimeError('GLM patch image differs from recipe')
    patches = {p['path']: p for p in manifest['patches']}
    results = []
    for overlay in recipe['glm53']['source_overlays']:
        if overlay['path'] == 'chat_template_mm.jinja':
            target = stage(ROOT / 'cluster/runtime-overlays/glm53' / overlay['path'],
                           args.data.expanduser(), overlay['path'], overlay['sha256'])
        else:
            patch = patches[overlay['path']]
            if patch['sha256'] != overlay['sha256']:
                raise RuntimeError('GLM patch identity differs from recipe')
            original = subprocess.check_output(['docker', 'run', '--rm', '--network', 'none',
                '--entrypoint', 'cat', recipe['image'], '/usr/local/lib/python3.12/dist-packages/' + overlay['path']])
            content = build_patch(original, patch)
            with tempfile.TemporaryDirectory() as folder:
                source = Path(folder) / 'patched.py'; source.write_bytes(content)
                target = stage(source, args.data.expanduser(), overlay['path'], overlay['sha256'])
        results.append({'path': str(target), 'sha256': overlay['sha256']})
    print(json.dumps({'assets': results, 'gpu_used': False}))


if __name__ == '__main__':
    main()
