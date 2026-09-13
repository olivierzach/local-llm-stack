#!/usr/bin/env python3
"""Enable the tested DeepSeek V4 thinking toggle in one existing OMP override."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys
import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.config import read, validate_saved_plan
from spark_cluster.gateway_node import write

SETTINGS = {
    'reasoning': True,
    'thinking': {'mode': 'effort', 'efforts': ['high'], 'defaultLevel': 'high', 'requiresEffort': False},
    'compat': {
        'supportsReasoningEffort': False,
        'disableReasoningOnToolChoice': False,
        'reasoningContentField': 'reasoning',
        'maxTokensField': 'max_tokens',
        'extraBody': {'chat_template_kwargs': {'thinking': False}},
        'whenThinking': {
            'requiresReasoningContentForToolCalls': True,
            'extraBody': {'chat_template_kwargs': {'thinking': True, 'reasoning_effort': 'high'}},
        },
    },
}


def merge(target, changes):
    for key, value in changes.items():
        if isinstance(value, dict):
            if not isinstance(target.get(key, {}), dict):
                raise ValueError('expected mapping at ' + key)
            merge(target.setdefault(key, {}), value)
        else:
            target[key] = copy.deepcopy(value)


def update(text, provider, model):
    wanted = copy.deepcopy(yaml.safe_load(text))
    target = wanted['providers'][provider]['modelOverrides'][model]
    merge(target, SETTINGS)
    node = yaml.compose(text)
    for key in ('providers', provider, 'modelOverrides', model):
        matches = [value for field, value in node.value if field.value == key]
        if len(matches) != 1: raise ValueError('expected exactly one existing OMP model override')
        node = matches[0]
    suffix = '' if node.flow_style else '\n' + ' ' * node.end_mark.column
    changed = text[:node.start_mark.index] + json.dumps(target) + suffix + text[node.end_mark.index:]
    if yaml.safe_load(changed) != wanted:
        raise ValueError('update would change unrelated OMP configuration')
    return changed


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--saved-plan', type=Path, required=True)
    p.add_argument('--models-file', type=Path, default=Path.home() / '.omp/agent/models.yml')
    p.add_argument('--provider', default='spark-context-guard')
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--apply', action='store_true')
    args = p.parse_args()
    plan = read(args.saved_plan); validate_saved_plan(plan)
    if not plan['recipe'].get('deepseek_v4') or plan['recipe']['kind'] != 'vllm':
        raise ValueError('this mapping requires the DeepSeek V4 vLLM recipe')
    path = args.models_file.expanduser()
    previous = path.read_text()
    model = plan['recipe']['alias']
    changed = update(previous, args.provider, model)
    report = {'provider': args.provider, 'model': model, 'applied': False,
              'deployment_digest': plan['digest'], 'supported_thinking': ['off', 'high'],
              'scope': 'OMP model metadata and conditional thinking request fields only; output/context limits and other clients unchanged.'}
    if args.apply:
        args.output.mkdir(parents=True, exist_ok=False, mode=0o700)
        write(args.output / 'models.yml.before', previous)
        if path.read_text() != previous: raise RuntimeError('OMP configuration changed during preflight')
        write(path, changed, path.stat().st_mode & 0o777)
        report.update(applied=True, unrelated_configuration_preserved=True,
                      configuration_sha256=hashlib.sha256(changed.encode()).hexdigest())
        write(args.output / 'update.json', json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__': main()
