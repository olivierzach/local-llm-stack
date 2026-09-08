#!/usr/bin/env python3
"""Container entrypoint for a pinned, offline Vector Bucket embedding job."""
import hashlib
import importlib.metadata
import json
from pathlib import Path
import time

import numpy as np
import torch
from vector_bucket.worker import run_embedding_job


def main():
    root, output = Path('/input'), Path('/output')
    spec = json.loads((root / 'spec.json').read_text())
    if not torch.cuda.is_available(): raise RuntimeError('CUDA is required; CPU fallback is disabled')
    torch.set_num_threads(spec['threads'])
    started = time.monotonic()
    summary = run_embedding_job(root / 'job.json', output / 'vectors.npz', manifest_dir=root / 'data/manifests',
        model=spec['model'], max_seconds=spec['max_seconds'], device='cuda', batch_size=spec['batch_size'],
        windows_per_track=spec['windows_per_track'], window_mode=spec['window_mode'],
        audio_workers=spec['audio_workers'], prefetch_batches=spec['prefetch_batches'],
        checkpoint_every_batches=spec['checkpoint_every_batches'], output_level=spec['output_level'])
    artifact = output / 'vectors.npz'
    with np.load(artifact, allow_pickle=False) as arrays:
        vectors = arrays['clip_vectors' if spec['output_level'] == 'clip' else 'track_vectors']
        if vectors.ndim != 2 or not len(vectors) or not np.isfinite(vectors).all():
            raise RuntimeError('embedding artifact has invalid/empty vectors')
        if not np.allclose(np.linalg.norm(vectors, axis=1), 1, atol=1e-4):
            raise RuntimeError('embedding vectors are not normalized')
        shape = list(vectors.shape)
    report = {'summary': summary, 'vector_shape': shape, 'finite_normalized': True,
        'recipe': spec,
        'model_revisions': {m['repo']:m['revision'] for m in json.loads((root / 'model-lock.json').read_text())['models']},
        'bundle_identity': hashlib.sha256(json.dumps(json.loads((root / '.spark-bundle.json').read_text()),
            sort_keys=True,separators=(',',':')).encode()).hexdigest(),
        'elapsed_s': round(time.monotonic()-started, 3), 'device': torch.cuda.get_device_name(),
        'runtime': {n: importlib.metadata.version(n) for n in ('torch','torchaudio','torchcodec','transformers','numpy')},
        'artifact_sha256': hashlib.sha256(artifact.read_bytes()).hexdigest()}
    (output / 'acceptance.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__': main()
