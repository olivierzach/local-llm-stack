import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('serving_soak', ROOT / 'scripts/soak-spark-serving.py')
soak = importlib.util.module_from_spec(spec)
spec.loader.exec_module(soak)
from spark_cluster.config import load, plan


@pytest.mark.parametrize('failure', ['stream', 'no-acceptance', None])
def test_soak_never_certifies_partial_streams_or_inactive_speculation(tmp_path, monkeypatch, failure):
    saved = tmp_path / 'plan.json'
    saved.write_text(json.dumps(plan(*load(ROOT, ROOT / 'cluster/inventory.json',
        ROOT / 'cluster/deployments/large-tp2-256k-mtp2-e8f1.json'))))
    output = tmp_path / 'result.json'
    monkeypatch.setattr(soak.sys, 'argv', ['soak', '--saved-plan', str(saved),
        '--rounds', '1', '--output', str(output)])
    calls = []
    def measure(*args, **kwargs):
        if failure == 'stream' and calls:
            raise RuntimeError('upstream returned a streaming error')
        calls.append(kwargs)
        return {'text': 'synthetic previous answer', 'completion_tokens': 256,
                'finish_reason': 'length', 'decode_tokens_per_second': 40}
    monkeypatch.setattr(soak.profile.benchmark, 'measure', measure)
    class Response:
        def __init__(self, count): self.text = f'vllm:spec_decode_num_accepted_tokens_total {count}'
        def raise_for_status(self): pass
    class Session:
        def get(self, *args, **kwargs): return Response(0 if failure == 'no-acceptance' else len(calls))
        def close(self): pass
    monkeypatch.setattr(soak.requests, 'Session', Session)
    if failure:
        with pytest.raises(RuntimeError): soak.main()
    else:
        soak.main()
    receipt = json.loads(output.read_text())
    assert receipt['complete'] is (failure is None)
    assert len(receipt['records']) == (1 if failure == 'stream' else 6)
    if failure != 'stream':
        assert calls[1]['messages'][1] == {'role': 'assistant', 'content': 'synthetic previous answer'}
    if failure: assert 'error' in receipt
