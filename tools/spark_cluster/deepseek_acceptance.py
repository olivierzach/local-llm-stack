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
    return {'deployment_digest': plan['digest'], 'coordinator': plan['deployment']['coordinator'],
            'acceptance': str(directory)}


def verify_pair(plan, directory, alternate_plan, alternate_directory):
    current = verify(plan, directory)
    alternate = verify(alternate_plan, alternate_directory)
    require(plan['recipe'] == alternate_plan['recipe'], 'coordinator acceptance recipes differ')
    require(plan['nodes'] == alternate_plan['nodes']
            and current['coordinator'] != alternate['coordinator'], 'both coordinator roles must be accepted')
    return [current, alternate]
