import importlib.util
import json
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('accept_serving', ROOT / 'scripts/accept-spark-serving.py')
accept = importlib.util.module_from_spec(spec)
spec.loader.exec_module(accept)


@pytest.mark.parametrize('failure', ['process', 'incomplete', 'wrong-deployment'])
def test_fail_fast_retains_receipts(tmp_path, monkeypatch, failure):
    from spark_cluster import config
    plan = config.plan(*config.load(ROOT, ROOT / 'cluster/inventory.json',
                                   ROOT / 'cluster/deployments/large-tp2-mtp2-nccl2307-e8f1.json'))
    saved = tmp_path / 'plan.json'
    saved.write_text(json.dumps(plan))
    out = tmp_path / 'acceptance'
    monkeypatch.setattr('sys.argv', ['accept', '--saved-plan', str(saved), '--output', str(out)])
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        receipt = Path(argv[argv.index('--output') + 1])
        payload = {'complete': True, 'deployment_digest': plan['digest']}
        if len(calls) == 2:
            if failure == 'process':
                raise subprocess.CalledProcessError(1, argv)
            if failure == 'incomplete':
                payload['complete'] = False
            else:
                payload['deployment_digest'] = 'other'
        receipt.write_text(json.dumps(payload))

    monkeypatch.setattr(accept.subprocess, 'run', run)
    with pytest.raises((RuntimeError, subprocess.CalledProcessError)):
        accept.main()
    report = json.loads((out / 'acceptance.json').read_text())
    assert len(calls) == 2 and report['checks'] == ['decode']
    assert report['phase'] == 'failed' and not report['complete']
    assert json.loads((out / 'decode.json').read_text())['complete']


def test_deepseek_profile_uses_64k_bound_without_changing_workers(tmp_path, monkeypatch):
    from spark_cluster import config
    plan = config.plan(*config.load(ROOT, ROOT / 'cluster/inventory.json',
                                   ROOT / 'cluster/deployments/deepseek-tp2-66f1.json'))
    saved = tmp_path / 'plan.json'
    saved.write_text(json.dumps(plan))
    out = tmp_path / 'acceptance'
    monkeypatch.setattr('sys.argv', ['accept', '--saved-plan', str(saved), '--output', str(out),
                                  '--profile', 'deepseek-64k'])
    calls = []
    def run(argv, **kwargs):
        calls.append(argv)
        Path(argv[argv.index('--output') + 1]).write_text(json.dumps(
            {'complete': True, 'deployment_digest': plan['digest']}))
    monkeypatch.setattr(accept.subprocess, 'run', run)
    accept.main()
    assert len(calls) == 4
    assert calls[1][calls[1].index('--input-tokens') + 1] == '63424'
    assert all('sparkctl' not in argv for argv in calls)
    assert json.loads((out / 'acceptance.json').read_text())['complete']
