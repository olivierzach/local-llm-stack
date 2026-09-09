#!/usr/bin/env python3
"""Prepare pinned native model runtimes on either Spark, without loading models.

Pass --bundle-dir to use Git bundles copied from the peer over the fabric.
Without bundles, only the pinned source repositories are fetched from GitHub.
Weights and Docker images are handled separately by the parity copy tools.
"""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import tempfile

DS4_REF = '4ad370b4a338efe9723a386673c0e04f6e214108'


def command(args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def prepare(root, name, directory, url, revision, patch, changed_file, bundle=None):
    if not (directory / '.git').is_dir():
        if directory.exists():
            raise ValueError('refusing to replace existing non-Git directory: ' + str(directory))
        directory.parent.mkdir(parents=True, exist_ok=True)
        command(['git', 'clone', '--no-checkout', str(bundle) if bundle else url, str(directory)])
        command(['git', '-C', str(directory), 'checkout', '--detach', revision])
    if command(['git', '-C', str(directory), 'rev-parse', 'HEAD']) != revision:
        raise ValueError(name + ': existing source revision differs; no reset performed')
    # Construct the expected patched file independently. Refuse extra tracked
    # edits, including edits in the same file as our integration patch.
    with tempfile.TemporaryDirectory() as temporary:
        expected = Path(temporary) / changed_file
        expected.parent.mkdir(parents=True, exist_ok=True)
        expected.write_bytes(subprocess.check_output(['git', '-C', str(directory), 'show', 'HEAD:' + changed_file]))
        command(['git', 'apply', str(patch)], cwd=temporary)
        modifications = command(['git', '-C', str(directory), 'diff', 'HEAD', '--name-only']).splitlines()
        if modifications and (modifications != [changed_file] or
                              (directory / changed_file).read_bytes() != expected.read_bytes()):
            raise ValueError(name + ': source has extra edits; no files replaced')
        if not modifications:
            command(['git', '-C', str(directory), 'apply', '--check', str(patch)])
            command(['git', '-C', str(directory), 'apply', str(patch)])
    return {'runtime': name, 'revision': revision,
            'patch_sha256': hashlib.sha256(patch.read_bytes()).hexdigest()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--bundle-dir', type=Path)
    parser.add_argument('--build-jobs', type=int, default=4)
    args = parser.parse_args()
    if platform.machine() != 'aarch64':
        parser.error('requires an aarch64 Spark')
    if not 1 <= args.build_jobs <= 8:
        parser.error('build jobs must be 1..8')
    root = args.root.expanduser().resolve()
    pins = dict(line.split('=', 1) for line in (root / 'config/qwen38-pins.env').read_text().splitlines()
                if line and not line.startswith('#'))
    active = subprocess.run(['systemctl', '--user', 'is-active', '--quiet', 'local-deepseek-v4.service'])
    if active.returncode == 0:
        parser.error('stop the owned DeepSeek runtime before rebuilding its engine')
    qwen = subprocess.run(['docker', 'inspect', '-f', '{{.State.Running}}', 'local-qwen38-flash-next'],
                          text=True, capture_output=True)
    if qwen.returncode == 0 and qwen.stdout.strip() == 'true':
        parser.error('stop the Qwen runtime before preparing its mounted recipe')
    if qwen.returncode and 'No such' not in qwen.stderr:
        parser.error('cannot establish Qwen runtime state: ' + qwen.stderr.strip())
    records = []
    for name, relative, url, revision, patch, changed, bundle in [
        ('deepseek-v4', 'data/deepseek-v4/ds4', 'https://github.com/Entrpi/ds4.git', DS4_REF,
         'patches/ds4-tokenize-endpoint.patch', 'ds4_server.c', 'ds4.bundle'),
        ('qwen38', 'data/qwen38-flash-next/recipe', 'https://github.com/MiaAI-Lab/Qwen3.8-Flash-Next-Single-DGX-Spark.git',
         pins['QWEN38_RECIPE_REF'], 'patches/qwen38-runtime.patch', 'start.sh', 'qwen38-recipe.bundle'),
    ]:
        records.append(prepare(root, name, root / relative, url, revision, root / patch, changed,
                               args.bundle_dir / bundle if args.bundle_dir else None))
    engine = root / 'data/deepseek-v4/ds4'
    subprocess.run(['make', '-C', str(engine), 'cuda', '-j' + str(args.build_jobs), 'CUDA_ARCH=sm_121'], check=True)
    dependencies = command(['ldd', str(engine / 'ds4-server')])
    if 'not found' in dependencies:
        raise ValueError('DeepSeek runtime library is missing: ' + dependencies)
    records[0]['binary_sha256'] = hashlib.sha256((engine / 'ds4-server').read_bytes()).hexdigest()
    recipe = root / 'data/qwen38-flash-next/recipe'
    if not (recipe / '.env').exists():
        (recipe / '.env').write_bytes((recipe / '.env.sample').read_bytes())
    for name in ('deepseek-v4', 'qwen38-flash-next'):
        (root / 'logs' / name).mkdir(parents=True, exist_ok=True)
    if not (recipe / 'logs').exists() and not (recipe / 'logs').is_symlink():
        (recipe / 'logs').symlink_to(root / 'logs/qwen38-flash-next')
    output = root / 'data/model-parity'
    output.mkdir(parents=True, exist_ok=True)
    (output / 'runtimes.json').write_text(json.dumps({'runtimes': records, 'inference_verified': False}, indent=2) + '\n')
    print(json.dumps({'runtimes_prepared': records, 'models_loaded': False}))


if __name__ == '__main__':
    main()
