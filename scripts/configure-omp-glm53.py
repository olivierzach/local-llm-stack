#!/usr/bin/env python3
"""Add the qualified GLM model to one existing OMP provider, preserving defaults."""
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


def update(text, provider, recipe):
    previous = yaml.safe_load(text)
    wanted = copy.deepcopy(previous)
    overrides = wanted['providers'][provider]['modelOverrides']
    alias = recipe['alias']
    overrides[alias] = {
        'contextWindow': recipe['context_tokens'], 'maxTokens': recipe['max_output_tokens'],
        'input': ['text', 'image'], 'supportsTools': True, 'reasoning': True,
        'thinking': {'mode': 'effort', 'efforts': ['high'], 'defaultLevel': 'high', 'requiresEffort': False},
        'compat': {'supportsStore': False, 'supportsDeveloperRole': False, 'supportsReasoningEffort': False,
            'maxTokensField': 'max_tokens', 'streamIdleTimeoutMs': 3600000,
            'reasoningContentField': 'reasoning', 'disableReasoningOnToolChoice': False,
            'extraBody': {'chat_template_kwargs': {'enable_thinking': False}},
            'whenThinking': {'requiresReasoningContentForToolCalls': True,
                'extraBody': {'chat_template_kwargs': {'enable_thinking': True, 'reasoning_effort': 'high'}}}},
    }
    node = yaml.compose(text)
    for key in ('providers', provider, 'modelOverrides'):
        matches = [value for field, value in node.value if field.value == key]
        if len(matches) != 1:
            raise ValueError('requires exactly one existing provider modelOverrides mapping')
        node = matches[0]
    if not isinstance(node, yaml.MappingNode):
        raise ValueError('modelOverrides must be a mapping')
    suffix = '' if node.flow_style else '\n' + ' ' * node.end_mark.column
    changed = text[:node.start_mark.index] + json.dumps(overrides) + suffix + text[node.end_mark.index:]
    if yaml.safe_load(changed) != wanted:
        raise ValueError('update would change unrelated OMP configuration')
    return changed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--saved-plan', type=Path, required=True)
    parser.add_argument('--features', type=Path, required=True)
    parser.add_argument('--models-file', type=Path, default=Path.home() / '.omp/agent/models.yml')
    parser.add_argument('--provider', default='spark-context-guard')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    plan = read(args.saved_plan)
    validate_saved_plan(plan)
    features = read(args.features)
    if not plan['recipe'].get('glm53') or features.get('complete') is not True or features.get('deployment_digest') != plan['digest']:
        parser.error('requires a GLM plan and matching completed feature checks')
    path = args.models_file.expanduser()
    previous = path.read_text()
    changed = update(previous, args.provider, plan['recipe'])
    report = dict(applied=False, provider=args.provider, model=plan['recipe']['alias'],
                  deployment_digest=plan['digest'], supported_thinking=['off', 'high'],
                  scope='Model metadata and conditional request fields only; provider URLs and client defaults unchanged.')
    if args.apply:
        args.output.mkdir(parents=True, exist_ok=False, mode=0o700)
        write(args.output / 'models.yml.before', previous)
        if path.read_text() != previous:
            raise RuntimeError('OMP configuration changed during preparation')
        write(path, changed, path.stat().st_mode & 0o777)
        report.update(applied=True, configuration_sha256=hashlib.sha256(changed.encode()).hexdigest())
        write(args.output / 'update.json', json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__':
    main()
