"""Gate GLM publication on measured serving and interchangeable-role evidence."""
from pathlib import Path

from .config import read, require, validate_saved_plan


def verify_fabric(plan, campaign, start, end):
    """Require owned workers and observed RoCE traffic throughout this campaign."""
    snapshots = [Path(campaign) / name for name in ('runtime-before', 'runtime-after')]
    before, after = [read(path / 'fabric.json') for path in snapshots]
    require(before['time'] <= start and after['time'] >= end, 'GLM fabric evidence does not cover the run')
    statuses = [read(path / 'status.json') for path in snapshots]
    require(all(s.get('healthy') is True for s in statuses), 'GLM workers were not healthy at both boundaries')
    traffic = {}
    for name, node in plan['nodes'].items():
        reservations = [s['nodes'][name]['reservation'] for s in statuses]
        require(all(r['digest'] == plan['digest'] for r in reservations)
                and reservations[0]['container_ids'] == reservations[1]['container_ids']
                and bool(reservations[0]['container_ids']), 'GLM fabric evidence belongs to different workers')
        for index in range(len(reservations[1]['container_ids'])):
            log = (snapshots[1] / f'{name}-{index}.log').read_text()
            require('Using network IB' in log and 'via NET/IB/' in log
                    and 'Using network Socket' not in log and 'via NET/Socket/' not in log,
                    'GLM requires observed NCCL IB transport without socket fallback')
        rails = [{r['interface']: r for r in snapshot['nodes'][name]['rails']}
                 for snapshot in (before, after)]
        require(all(set(r) == {f['interface'] for f in node['fabric']} for r in rails),
                'GLM fabric interfaces differ from the plan')
        traffic[name] = {}
        for expected in node['fabric']:
            left, right = [r[expected['interface']] for r in rails]
            require(all(all(r[k] == expected[k] for k in ('interface', 'ip', 'rdma'))
                        for r in (left, right)), 'GLM fabric identity changed')
            deltas = {}
            for key in ('port_xmit_data', 'port_rcv_data'):
                delta = right['rdma_counters'][key] - left['rdma_counters'][key]
                require(delta > 0, 'GLM requires increasing send/receive RDMA counters on every rail')
                deltas[key + '_bytes'] = delta * 4  # IB Port*Data counters count 32-bit words.
            for group in ('net_counters', 'rdma_counters', 'rdma_hw_counters'):
                require(set(left[group]) == set(right[group]), 'GLM fabric counter set changed')
                for key, value in left[group].items():
                    if any(part in key for part in ('error', '_err', 'drop', 'discard', 'link_down', 'link_error')):
                        require(right[group][key] == value, 'GLM fabric error/drop counter changed: ' + key)
            traffic[name][expected['interface']] = deltas
    return traffic


def verify(plan, directory, full=True):
    validate_saved_plan(plan)
    require(bool(plan['recipe'].get('glm53')) and plan['deployment']['mode'] == 'tensor'
            and plan['deployment']['tensor_parallel'] == 2, 'requires a GLM53 TP2 plan')
    directory = Path(directory)
    def receipt(name):
        result = read(directory / (name + '.json'))
        require(result.get('complete') is True and result.get('deployment_digest') == plan['digest'],
                'missing, failed or mismatched GLM ' + name + ' receipt')
        return result
    feature_windows = {}
    for name in ('features', 'features-after'):
        features = receipt(name)
        feature_windows[name] = features
        expected = {'text-False', 'text-True', 'reasoning-False', 'reasoning-True',
                    'vision-reverse-False', 'vision-reverse-True'} | {f'repeatability-{i}' for i in range(20)}
        require({r['test'] for r in features.get('checks', [])} == expected, 'incomplete GLM features')
        for check in features['checks']:
            require(check.get('finish_reason') == 'stop' and bool(check.get('content')),
                    'incomplete GLM feature response')
            require((check.get('reasoning_characters', 0) > 0) == check['test'].startswith('reasoning-'),
                    'incorrect GLM reasoning toggle')
            if check['test'].startswith('repeatability-'):
                require(check['content'].strip() == '391', 'GLM repeated-answer regression failed')
    tools = receipt('tools')
    require(len(tools.get('checks', [])) >= 24, 'incomplete GLM tool continuation')
    soak = receipt('soak')
    require(len(soak.get('records', [])) >= 18 and all(
        r.get('finish_reason') in ('stop', 'length') and r.get('completion_tokens', 0) >= 128
        for r in soak['records']), 'incomplete GLM soak')
    for data in (tools, soak):
        require(data.get('speculative_counter_deltas', {}).get('vllm:spec_decode_num_accepted_tokens_total', 0) > 0,
                'GLM speculative decoding must accept tokens')
    concurrency = receipt('concurrency')
    require({r.get('concurrency') for r in concurrency.get('levels', [])} >= {1, 2, plan['recipe']['max_num_seqs']},
            'GLM scheduler concurrency must be exercised')
    if full:
        acceptance = receipt('acceptance')
        require(acceptance.get('profile') == 'glm53-256k' and set(acceptance.get('checks', [])) == {
            'features', 'tools', 'decode', 'long-context', 'soak', 'decode-4096', 'concurrency', 'features-after'},
            'incomplete GLM sustained serving acceptance')
        receipt('decode'); receipt('decode-4096')
    long = receipt('long-context')
    require(long.get('actual_input_tokens', 0) >= plan['recipe']['context_tokens'] - 8192
            and long.get('corpus') == 'varied' and long.get('prefix_reuse_observed') is True,
            'GLM requires near-limit varied input and prefix reuse')
    require(len(long.get('runs', [])) == 2 and all(r.get('retrieval_passed') is True
            and r.get('tokenizer_usage_match') is True for r in long['runs']), 'GLM retrieval failed')
    decode = long.get('decode_run', {})
    require(decode.get('completion_tokens', 0) >= 256 and decode.get('tokenizer_usage_match') is True
            and decode.get('finish_reason') in ('stop', 'length'), 'GLM long-input decode failed')
    start = acceptance['started_at'] if full else feature_windows['features'].get('started_at', float('-inf'))
    end = acceptance['ended_at'] if full else feature_windows['features-after'].get('ended_at', float('inf'))
    for node in plan['nodes']:
        memory = read(directory.parent / f'memory-{node}.summary.json')
        require(memory.get('samples', 0) >= 2 and memory.get('started_at', float('inf')) <= start
                and memory.get('ended_at', 0) >= end, 'GLM requires memory coverage on both nodes')
        require(memory.get('min_available_gib', 0) >= 4 and memory.get('max_pressure_full_avg10', 100) < 5,
                'GLM exceeded memory headroom/pressure limits')
        require(memory.get('swapout_pages', -1) >= 0 and memory.get('page_size_bytes', 0) > 0
                and memory['swapout_pages'] * memory['page_size_bytes'] < 256 * 1024**2,
                'GLM exceeded the paging limit')
    traffic = verify_fabric(plan, directory.parent, start, end)
    return dict(deployment_digest=plan['digest'], coordinator=plan['deployment']['coordinator'],
                acceptance=str(directory), full_profile=full, fabric_bytes=traffic)


def verify_pair(plan, directory, alternate_plan, alternate_directory):
    current_full = (Path(directory) / 'acceptance.json').exists()
    alternate_full = (Path(alternate_directory) / 'acceptance.json').exists()
    require(current_full or alternate_full, 'one GLM coordinator must have full-profile acceptance')
    current = verify(plan, directory, full=current_full)
    alternate = verify(alternate_plan, alternate_directory, full=alternate_full)
    require(plan['recipe'] == alternate_plan['recipe'], 'GLM coordinator recipes differ')
    require(plan['nodes'] == alternate_plan['nodes'] and current['coordinator'] != alternate['coordinator'],
            'both GLM coordinator roles must be accepted')
    return [current, alternate]
