#!/usr/bin/env python3
"""Resolve a pinned Qwen image after a registry pull or peer docker-save copy.

Docker load preserves the immutable image ID but not its registry RepoDigest.
Accept only the ARM64 image ID recorded with the registry pin; never a mutable
tag or a different image behind an existing reference.
"""
import json
from pathlib import Path
import subprocess


def resolve(pins):
    for reference in (pins['QWEN38_IMAGE'], pins['QWEN38_IMAGE_ID']):
        result = subprocess.run(['docker', 'image', 'inspect', reference],
                                text=True, capture_output=True)
        if result.returncode:
            if 'no such image' in result.stderr.lower():
                continue
            raise RuntimeError('cannot inspect pinned image: ' + result.stderr.strip())
        image = json.loads(result.stdout)[0]
        if image['Id'] != pins['QWEN38_IMAGE_ID'] or image['Architecture'] != 'arm64':
            raise ValueError('pinned image identity or architecture differs')
        return reference
    raise ValueError('pinned runtime image is absent; copy it from the peer or install it')


if __name__ == '__main__':
    root = Path(__file__).resolve().parents[1]
    pins = dict(line.split('=', 1) for line in (root / 'config/qwen38-pins.env').read_text().splitlines()
                if line and not line.startswith('#'))
    print(resolve(pins))
