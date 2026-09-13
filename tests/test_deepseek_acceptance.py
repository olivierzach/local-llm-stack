import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import config
from spark_cluster.deepseek_acceptance import verify, verify_pair


@pytest.fixture
def accepted(tmp_path):
    pairs = []
    for node in ('66f1', 'e8f1'):
        plan = config.plan(*config.load(ROOT, ROOT / 'cluster/inventory.json',
            ROOT / f'cluster/deployments/deepseek-tp2-a16-dspark2-graphs-{node}.json'))
        folder = tmp_path / node
        (folder / 'serving').mkdir(parents=True)
        common = dict(complete=True, deployment_digest=plan['digest'])
        receipts = {
            'repeatability.json': dict(common, passed=True, records=[dict(passed=True, answer='323') for _ in range(100)]),
            'tools.json': dict(common, checks=[{} for _ in range(36)]),
            'thinking.json': dict(common, checks=[dict(passed=True) for _ in range(4)]),
            'serving/acceptance.json': dict(common, checks=['decode', 'long-context', 'soak', 'decode-4096']),
        }
        for name, value in receipts.items():
            (folder / name).write_text(json.dumps(value))
        pairs.append((plan, folder))
    return pairs


def test_requires_both_coordinator_roles_for_same_recipe(accepted):
    (plan, folder), (other, other_folder) = accepted
    assert len(verify_pair(plan, folder, other, other_folder)) == 2
    with pytest.raises(config.ConfigError, match='both coordinator'):
        verify_pair(plan, folder, plan, folder)


@pytest.mark.parametrize('change', ['wrong-answer', 'few-trials', 'stale-digest', 'incomplete-soak'])
def test_cannot_publish_partial_or_failed_results(accepted, change):
    plan, folder = accepted[0]
    path = folder / ('serving/acceptance.json' if change == 'incomplete-soak' else 'repeatability.json')
    value = json.loads(path.read_text())
    if change == 'wrong-answer':
        value['records'][29]['answer'] = '289'
    elif change == 'few-trials':
        value['records'].pop()
    elif change == 'stale-digest':
        value['deployment_digest'] = '0' * 64
    else:
        value['checks'].remove('soak')
    path.write_text(json.dumps(value))
    with pytest.raises(config.ConfigError):
        verify(plan, folder)


def test_diagnostic_trace_cannot_be_published(accepted):
    _, folder = accepted[0]
    plan = config.plan(*config.load(ROOT, ROOT / 'cluster/inventory.json',
        ROOT / 'cluster/deployments/deepseek-tp2-a16-trace-e8f1.json'))
    with pytest.raises(config.ConfigError, match='untraced'):
        verify(plan, folder)
