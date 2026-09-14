import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import config
from spark_cluster.glm53_acceptance import verify, verify_pair


@pytest.fixture
def accepted(tmp_path):
    pairs = []
    for node in ('66f1', 'e8f1'):
        plan = config.plan(*config.load(ROOT, ROOT / 'cluster/inventory.json',
            ROOT / f'cluster/deployments/glm53-tp2-256k-dflash2-{node}.json'))
        folder = tmp_path / node / 'serving'
        folder.mkdir(parents=True)
        common = dict(complete=True, deployment_digest=plan['digest'])
        spec = {'vllm:spec_decode_num_accepted_tokens_total': 10}
        records = [dict(completion_tokens=256, finish_reason='length')] * 18
        receipts = {
            'features': dict(common, started_at=10, ended_at=100, checks=[dict(test=label, finish_reason='stop', content='answer',
                reasoning_characters=10 if label.startswith('reasoning-') else 0) for label in
                ('text-False', 'text-True', 'reasoning-False', 'reasoning-True', 'vision-reverse-False', 'vision-reverse-True')]),
            'tools': dict(common, checks=[{}] * 24, speculative_counter_deltas=spec),
            'soak': dict(common, records=records, speculative_counter_deltas=spec),
            'acceptance': dict(common, profile='glm53-256k', started_at=10, ended_at=100,
                checks=['features', 'tools', 'decode', 'long-context', 'soak', 'decode-4096', 'concurrency', 'features-after']),
            'long-context': dict(common, actual_input_tokens=260000, corpus='varied', prefix_reuse_observed=True,
                runs=[dict(retrieval_passed=True, tokenizer_usage_match=True)] * 2,
                decode_run=dict(completion_tokens=1024, tokenizer_usage_match=True, finish_reason='length')),
            'concurrency': dict(common, levels=[dict(concurrency=c) for c in (1, 2, 4)]),
            'decode': common, 'decode-4096': common,
        }
        receipts['features']['checks'] += [dict(test=f'repeatability-{i}', finish_reason='stop',
            content='391', reasoning_characters=0) for i in range(20)]
        receipts['features-after'] = receipts['features']
        for name, value in receipts.items():
            (folder / (name + '.json')).write_text(json.dumps(value))
        for memory_node in plan['nodes']:
            (folder.parent / f'memory-{memory_node}.summary.json').write_text(json.dumps(dict(
                samples=12, started_at=0, ended_at=110, min_available_gib=8,
                max_pressure_full_avg10=0, swapout_pages=0, page_size_bytes=4096)))
        for boundary, timestamp, count in [('runtime-before', 0, 100), ('runtime-after', 110, 200)]:
            snapshot = folder.parent / boundary
            snapshot.mkdir()
            fabric = dict(time=timestamp, nodes={})
            status = dict(healthy=True, nodes={})
            for name, settings in plan['nodes'].items():
                fabric['nodes'][name] = dict(rails=[dict(rail,
                    rdma_counters=dict(port_xmit_data=count, port_rcv_data=count, port_rcv_errors=0),
                    net_counters=dict(rx_dropped=0), rdma_hw_counters=dict(req_cqe_error=0))
                    for rail in settings['fabric']])
                status['nodes'][name] = dict(reservation=dict(digest=plan['digest'], container_ids=[name+'-container']))
                (snapshot / f'{name}-0.log').write_text('NCCL INFO Using network IB\nChannel via NET/IB/0\n')
            (snapshot / 'fabric.json').write_text(json.dumps(fabric))
            (snapshot / 'status.json').write_text(json.dumps(status))
        pairs.append((plan, folder))
    return pairs


def test_same_recipe_needs_two_distinct_coordinator_roles(accepted):
    (plan, folder), (other, other_folder) = accepted
    assert len(verify_pair(plan, folder, other, other_folder)) == 2
    with pytest.raises(config.ConfigError, match='both GLM coordinator'):
        verify_pair(plan, folder, plan, folder)


@pytest.mark.parametrize('name,change', [
    ('features', {'complete': False}), ('tools', {'deployment_digest': '0' * 64}),
    ('soak', {'speculative_counter_deltas': {}}), ('long-context', {'actual_input_tokens': 32000}),
    ('long-context', {'prefix_reuse_observed': False}), ('concurrency', {'levels': [{'concurrency': 1}]}),
])
def test_partial_or_wrong_evidence_cannot_publish(accepted, name, change):
    plan, folder = accepted[0]
    path = folder / (name + '.json')
    path.write_text(json.dumps({**json.loads(path.read_text()), **change}))
    with pytest.raises(config.ConfigError):
        verify(plan, folder)


def test_low_memory_or_uncovered_run_cannot_publish(accepted):
    plan, folder = accepted[0]
    path = folder.parent / 'memory-e8f1.summary.json'
    good = json.loads(path.read_text())
    for change in ({'min_available_gib': 3}, {'ended_at': 99}, {'swapout_pages': 65536}):
        path.write_text(json.dumps({**good, **change}))
        with pytest.raises(config.ConfigError):
            verify(plan, folder)


def test_answer_flip_after_load_blocks_publication(accepted):
    plan, folder = accepted[0]
    path = folder / 'features-after.json'
    data = json.loads(path.read_text())
    data['checks'][-1]['content'] = 'wrong'
    path.write_text(json.dumps(data))
    with pytest.raises(config.ConfigError, match='repeated-answer'):
        verify(plan, folder)


def test_alternate_coordinator_also_requires_memory_headroom(accepted):
    plan, folder = accepted[1]
    assert verify(plan, folder, full=False)
    path = folder.parent / 'memory-66f1.summary.json'
    data = json.loads(path.read_text())
    for change in ({'min_available_gib': 3}, {'started_at': 11}, {'ended_at': 99}):
        path.write_text(json.dumps({**data, **change}))
        with pytest.raises(config.ConfigError):
            verify(plan, folder, full=False)


def test_alternate_coordinator_must_serve_the_advertised_context(accepted):
    plan, folder = accepted[1]
    path = folder / 'long-context.json'
    data = json.loads(path.read_text()); data['actual_input_tokens'] = 32000
    path.write_text(json.dumps(data))
    with pytest.raises(config.ConfigError, match='near-limit'):
        verify(plan, folder, full=False)


def test_either_qualified_role_can_be_published_with_one_full_profile(accepted):
    (plan, folder), (other, other_folder) = accepted
    (folder / 'acceptance.json').unlink()
    result = verify_pair(plan, folder, other, other_folder)
    assert [r['full_profile'] for r in result] == [False, True]
    (other_folder / 'acceptance.json').unlink()
    with pytest.raises(config.ConfigError, match='full-profile'):
        verify_pair(plan, folder, other, other_folder)


@pytest.mark.parametrize('failure', ['no-traffic', 'counter-reset', 'errors', 'wrong-interface', 'missing-coverage'])
def test_missing_or_failed_cable_evidence_cannot_publish(accepted, failure):
    plan, folder = accepted[0]
    path = folder.parent / 'runtime-after/fabric.json'
    data = json.loads(path.read_text())
    rail = data['nodes']['e8f1']['rails'][0]
    if failure == 'no-traffic': rail['rdma_counters']['port_xmit_data'] = 100
    if failure == 'counter-reset': rail['rdma_counters']['port_rcv_data'] = 0
    if failure == 'errors': rail['net_counters']['rx_dropped'] = 1
    if failure == 'wrong-interface': rail['ip'] = '192.168.1.18'
    if failure == 'missing-coverage': data['time'] = 99
    path.write_text(json.dumps(data))
    with pytest.raises(config.ConfigError):
        verify(plan, folder)


def test_socket_collectives_or_replaced_workers_cannot_publish(accepted):
    plan, folder = accepted[0]
    log = folder.parent / 'runtime-after/e8f1-0.log'
    good = log.read_text()
    log.write_text(good + 'Channel via NET/Socket/0\n')
    with pytest.raises(config.ConfigError, match='socket fallback'):
        verify(plan, folder)
    log.write_text(good)
    path = folder.parent / 'runtime-after/status.json'
    data = json.loads(path.read_text())
    data['nodes']['e8f1']['reservation']['container_ids'] = ['replacement']
    path.write_text(json.dumps(data))
    with pytest.raises(config.ConfigError, match='different workers'):
        verify(plan, folder)
