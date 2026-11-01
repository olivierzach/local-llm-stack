import json
from pathlib import Path
import runpy
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
            'repeatability-after.json': dict(common, passed=True, records=[dict(passed=True, answer='323') for _ in range(100)]),
            'tools.json': dict(common, checks=[{} for _ in range(36)]),
            'thinking.json': dict(common, checks=[dict(passed=True) for _ in range(4)]),
            'serving/acceptance.json': dict(common, started_at=10, ended_at=100,
                                           checks=['decode', 'long-context', 'soak', 'decode-4096']),
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


def test_extended_context_cannot_reuse_short_context_qualification(accepted):
    _, folder = accepted[0]
    plan = config.plan(*config.load(ROOT, ROOT / 'cluster/inventory.json',
        ROOT / 'cluster/deployments/deepseek-tp2-a16-dspark2-graphs-1m-66f1.json'))
    for p in folder.rglob('*.json'):
        data = json.loads(p.read_text()); data['deployment_digest'] = plan['digest']
        p.write_text(json.dumps(data))
    long = dict(complete=True, deployment_digest=plan['digest'], actual_input_tokens=63424,
                corpus='varied', prefix_reuse_observed=True,
                runs=[dict(retrieval_passed=True, tokenizer_usage_match=True)] * 2,
                decode_run=dict(completion_tokens=1024, tokenizer_usage_match=True, finish_reason='length'))
    target = folder / 'serving/long-context.json'; target.write_text(json.dumps(long))
    with pytest.raises(config.ConfigError, match='near-limit'):
        verify(plan, folder)
    long['actual_input_tokens'] = 1046464; target.write_text(json.dumps(long))
    for node in plan['nodes']:
        (folder.parent / f'memory-{node}.summary.json').write_text(json.dumps(dict(
            started_at=0, ended_at=110, samples=12, min_available_gib=8,
            max_pressure_full_avg10=0, swapout_pages=0, page_size_bytes=4096)))
    assert verify(plan, folder)['coordinator'] == '66f1'
    memory_path = folder.parent / 'memory-e8f1.summary.json'
    good = json.loads(memory_path.read_text())
    for change in ({'min_available_gib': 3.9}, {'max_pressure_full_avg10': 5},
                   {'swapout_pages': 65536}, {'started_at': 11}, {'ended_at': 99}, {'samples': 1}):
        memory_path.write_text(json.dumps({**good, **change}))
        with pytest.raises(config.ConfigError): verify(plan, folder)


@pytest.mark.parametrize('field,counter,reasoning,passes', [
    ('reasoning', None, 'PRIVATE_REASONING_FIXTURE', True),
    ('reasoning_content', None, 'PRIVATE_REASONING_FIXTURE', True),
    ('reasoning', 16, '', False),
    ('reasoning', 0, 'PRIVATE_REASONING_FIXTURE', False),
])
def test_thinking_gate_checks_channel_not_optional_usage(tmp_path, monkeypatch, field, counter, reasoning, passes):
    import requests

    chat = runpy.run_path(str(ROOT / 'scripts/qwen38-verify.py'))['chat']
    main = runpy.run_path(str(ROOT / 'scripts/probe-deepseek-thinking.py'))['main']
    monkeypatch.setitem(chat.__globals__, 'memory', lambda: {})
    monkeypatch.setattr(runpy, 'run_path', lambda path: {'chat': chat})
    modes = iter([(False, False), (False, True), (True, False), (True, True)])

    def respond(*args, **kwargs):
        thinking, stream = next(modes)
        usage = dict(prompt_tokens=36, completion_tokens=18 if thinking else 2,
                     total_tokens=54 if thinking else 38)
        if counter is not None:
            usage['completion_tokens_details'] = {'reasoning_tokens': counter if thinking else 0}
        response = requests.Response()
        response.status_code = 200
        response._content_consumed = True
        if stream:
            events = [
                {'choices': [{'delta': {field: reasoning if thinking else ''}, 'finish_reason': None}]},
                {'choices': [{'delta': {'content': '323'}, 'finish_reason': None}]},
                {'choices': [{'delta': {}, 'finish_reason': 'stop'}]},
                {'choices': [], 'usage': usage},
            ]
            response._content = (''.join('data: ' + json.dumps(event) + '\n\n' for event in events)
                                 + 'data: [DONE]\n\n').encode()
        else:
            response._content = json.dumps({'choices': [{'message': {
                'content': '323', field: reasoning if thinking else ''}, 'finish_reason': 'stop'}],
                'usage': usage}).encode()
        return response

    monkeypatch.setattr(requests, 'post', respond)
    plan = config.plan(*config.load(ROOT, ROOT / 'cluster/inventory.json',
        ROOT / 'cluster/deployments/deepseek-tp2-mia-512k-e8f1.json'))
    saved = tmp_path / 'plan.json'
    saved.write_text(json.dumps(plan))
    output = tmp_path / 'thinking.json'
    monkeypatch.setattr(sys, 'argv', ['probe', '--controller', str(ROOT),
                                     '--saved-plan', str(saved), '--output', str(output)])
    if passes:
        main()
    else:
        with pytest.raises(RuntimeError):
            main()
    receipt = json.loads(output.read_text())
    assert receipt['complete'] is passes
    if passes:
        assert [check['reasoning_present'] for check in receipt['checks']] == [False, False, True, True]
        assert all(check['passed'] for check in receipt['checks'])
    else:
        assert receipt['checks'][-1]['passed'] is False
    assert 'PRIVATE_REASONING_FIXTURE' not in output.read_text()
