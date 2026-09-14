"""Gate GLM publication on measured serving and interchangeable-role evidence."""
from pathlib import Path

from .config import read, require, validate_saved_plan


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
    return dict(deployment_digest=plan['digest'], coordinator=plan['deployment']['coordinator'],
                acceptance=str(directory), full_profile=full)


def verify_pair(plan, directory, alternate_plan, alternate_directory):
    current = verify(plan, directory)
    alternate = verify(alternate_plan, alternate_directory, full=False)
    require(plan['recipe'] == alternate_plan['recipe'], 'GLM coordinator recipes differ')
    require(plan['nodes'] == alternate_plan['nodes'] and current['coordinator'] != alternate['coordinator'],
            'both GLM coordinator roles must be accepted')
    return [current, alternate]
