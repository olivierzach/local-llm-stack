"""Require exact, untraced DeepSeek serving evidence before publishing an alias."""
from pathlib import Path

from .config import read, require, validate_saved_plan


def verify(plan, directory):
    validate_saved_plan(plan)
    ds = plan['recipe'].get('deepseek_v4', {})
    require(ds.get('moe_force_a16') is True and not ds.get('source_overlays'),
            'publication requires untraced BF16 expert activations')
    require(plan['deployment']['mode'] == 'tensor' and plan['deployment']['tensor_parallel'] == 2,
            'publication requires a DeepSeek TP2 plan')
    directory = Path(directory)
    receipts = {}
    for name, relative in [('repeatability', 'repeatability.json'), ('repeatability-after', 'repeatability-after.json'), ('tools', 'tools.json'),
                           ('thinking', 'thinking.json'), ('serving', 'serving/acceptance.json')]:
        receipt = read(directory / relative)
        require(receipt.get('complete') is True and receipt.get('deployment_digest') == plan['digest'],
                'missing, failed or mismatched ' + name + ' acceptance')
        receipts[name] = receipt
    for name in ('repeatability', 'repeatability-after'):
        repeats = receipts[name]
        require(repeats.get('passed') is True and len(repeats.get('records', [])) >= 100
                and all(r.get('passed') is True and r.get('answer') == '323' for r in repeats['records']),
                'publication requires 100 successful first-token trials before and after serving acceptance')
    require(len(receipts['tools'].get('checks', [])) >= 36, 'incomplete tool acceptance')
    require(len(receipts['thinking'].get('checks', [])) == 4
            and all(r.get('passed') is True for r in receipts['thinking']['checks']),
            'incomplete thinking acceptance')
    require(set(receipts['serving'].get('checks', [])) == {'decode', 'long-context', 'soak', 'decode-4096'},
            'incomplete sustained serving acceptance')
    if plan['recipe']['context_tokens'] > 65536:
        long = read(directory / 'serving/long-context.json')
        require(long.get('complete') is True and long.get('deployment_digest') == plan['digest']
                and long.get('actual_input_tokens', 0) >= plan['recipe']['context_tokens'] - 8192
                and long.get('corpus') == 'varied' and long.get('prefix_reuse_observed') is True,
                'extended context requires near-limit varied-corpus acceptance')
        require(len(long.get('runs', [])) == 2 and all(r.get('retrieval_passed') is True
                and r.get('tokenizer_usage_match') is True for r in long['runs']), 'extended retrieval failed')
        decode = long.get('decode_run', {})
        require(decode.get('completion_tokens', 0) >= 256 and decode.get('tokenizer_usage_match') is True
                and decode.get('finish_reason') in ('stop', 'length'), 'extended decode failed')
        for node in plan['nodes']:
            memory = read(directory.parent / f'memory-{node}.summary.json')
            serving = receipts['serving']
            require(memory.get('samples', 0) >= 2
                    and memory.get('started_at', float('inf')) <= serving['started_at']
                    and memory.get('ended_at', 0) >= serving['ended_at'],
                    'extended context requires memory samples covering serving acceptance on both nodes')
            require(memory.get('min_available_gib', 0) >= 4,
                    'extended context requires at least 4 GiB measured OS headroom per node')
            require(memory.get('max_pressure_full_avg10', 100) < 5,
                    'extended context exceeded the full-memory-pressure acceptance bound')
            require(memory.get('swapout_pages', -1) >= 0
                    and memory.get('page_size_bytes', 0) > 0
                    and memory['swapout_pages'] * memory['page_size_bytes'] < 256 * 1024**2,
                    'extended context exceeded the paging acceptance bound')
    return {'deployment_digest': plan['digest'], 'coordinator': plan['deployment']['coordinator'],
            'acceptance': str(directory)}


def verify_pair(plan, directory, alternate_plan, alternate_directory):
    current = verify(plan, directory)
    alternate = verify(alternate_plan, alternate_directory)
    require(plan['recipe'] == alternate_plan['recipe'], 'coordinator acceptance recipes differ')
    require(plan['nodes'] == alternate_plan['nodes']
            and current['coordinator'] != alternate['coordinator'], 'both coordinator roles must be accepted')
    return [current, alternate]
