#!/usr/bin/env python3
"""Inventory configured single-node artifacts; does not load models or reserve GPUs.

This checks snapshot structure and engine/image presence. SHA-256 copy receipts
and real inference acceptance are separate evidence, never inferred from this.
"""
import argparse
import json
from pathlib import Path
import subprocess


def snapshot_check(snapshot):
    config = json.loads((snapshot / 'config.json').read_text())
    index = snapshot / 'model.safetensors.index.json'
    if index.exists():
        shards = set(json.loads(index.read_text())['weight_map'].values())
    else:
        shards = {p.name for p in snapshot.glob('*.safetensors')}
    if not shards:
        raise ValueError('no safetensors weights/index')
    for name in shards:
        p = Path(name)
        if p.is_absolute() or '..' in p.parts:
            raise ValueError('unsafe shard path')
        if not (snapshot / p).is_file() or (snapshot / p).stat().st_size == 0:
            raise ValueError('missing/empty weight shard: ' + name)
    for name in ('tokenizer_config.json',):
        if not (snapshot / name).is_file():
            raise ValueError('missing ' + name)
    return {'weight_shards': len(shards), 'architectures': config.get('architectures', []),
            'weight_bytes': sum((snapshot / p).stat().st_size for p in shards)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()
    root = args.root.expanduser().resolve()
    catalog = json.loads((root / 'cluster/single-node-models.lock.json').read_text())
    settings = {}
    # Parse only public model/runtime settings; never source or report .env.
    allowed = {m['setting'] for m in catalog['models']} | {'VLLM_IMAGE', 'VISION_VLLM_IMAGE', 'LAGUNAS21_VLLM_IMAGE'}
    for filename in ('.env.example', '.env', 'config/qwen38-pins.env'):
        path = root / filename
        if path.is_file():
            for line in path.read_text().splitlines():
                key, separator, value = line.partition('=')
                if separator and (key in allowed or key in ('QWEN38_IMAGE', 'QWEN38_IMAGE_ID')):
                    settings[key] = value.strip().strip('\"\'')
    results = []
    for model in catalog['models']:
        errors = []
        row = {**model}
        if settings.get(model['setting'], model['repo']) != model['repo']:
            errors.append('configured model differs from parity catalog')
        snapshot = root / 'data/huggingface/hub' / ('models--' + model['repo'].replace('/', '--')) / 'snapshots' / model['revision']
        try:
            row.update(snapshot_check(snapshot))
            ref = snapshot.parents[1] / 'refs/main'
            if model['setting'] != 'QWEN38_MODEL' and (not ref.is_file() or ref.read_text().strip() != model['revision']):
                errors.append('cache main reference differs or is missing')
        except (OSError, ValueError, KeyError) as error:
            errors.append(str(error))
        image = settings.get('VLLM_IMAGE', 'nvcr.io/nvidia/vllm:25.09-py3')
        if model['setting'] == 'LAGUNAS21_MODEL':
            image = settings.get('LAGUNAS21_VLLM_IMAGE', image)
        elif model['setting'] == 'VISION_MODEL':
            image = settings.get('VISION_VLLM_IMAGE', image)
        elif model['setting'] == 'QWEN38_MODEL':
            image = settings.get('QWEN38_IMAGE_ID', settings['QWEN38_IMAGE'])
            recipe = root / 'data/qwen38-flash-next/recipe'
            if not (recipe / 'start.sh').is_file():
                errors.append('native recipe missing')
        try:
            inspected = json.loads(subprocess.check_output(['docker', 'image', 'inspect', image], text=True, stderr=subprocess.PIPE))[0]
            if inspected['Architecture'] != 'arm64':
                errors.append('runtime image is not arm64')
            row['image_id'] = inspected['Id']
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            errors.append('runtime image unavailable: ' + image)
        row.update(artifact_checks_passed=not errors, errors=errors)
        results.append(row)
    errors = []
    for name in catalog['gguf']:
        path = root / name
        if not path.is_file() or path.stat().st_size == 0:
            errors.append('missing GGUF: ' + name)
    engine = root / 'data/deepseek-v4/ds4/ds4-server'
    if not engine.is_file():
        errors.append('native engine missing')
    results.append({'alias': 'local-deepseek-v4-flash', 'make_target': 'deepseekv4-up',
                    'artifact_checks_passed': not errors, 'errors': errors})
    report = {'models': results, 'artifact_checks_passed': all(r['artifact_checks_passed'] for r in results),
              'inference_verified': False,
              'qualification': 'Presence/structure only. Does not establish image architecture support, GPU fit, checksum integrity, or routed completion. LoRA adapters are generated by training and are not included in this base-model catalog.'}
    print(json.dumps(report, indent=2))
    return int(not report['artifact_checks_passed'])


if __name__ == '__main__':
    raise SystemExit(main())
