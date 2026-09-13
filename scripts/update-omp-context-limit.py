#!/usr/bin/env python3
"""Update one existing OMP model override from a saved plan, preserving the rest of the file."""
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


def update(text, provider, model, context):
    before = yaml.safe_load(text)
    wanted = copy.deepcopy(before)
    wanted['providers'][provider]['modelOverrides'][model]['contextWindow'] = context
    node = yaml.compose(text)
    for key in ('providers', provider, 'modelOverrides', model, 'contextWindow'):
        matches = [value for field, value in node.value if field.value == key]
        if len(matches) != 1: raise ValueError('expected exactly one existing OMP contextWindow override')
        node = matches[0]
    if not isinstance(node, yaml.ScalarNode): raise ValueError('contextWindow must be a scalar')
    changed = text[:node.start_mark.index] + str(context) + text[node.end_mark.index:]
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
    path = args.models_file.expanduser()
    previous = path.read_text()
    alias, limit = plan['recipe']['alias'], plan['endpoint']['context_tokens']
    changed = update(previous, args.provider, alias, limit)
    report = {'provider': args.provider, 'model': alias, 'contextWindow': limit,
              'deployment_digest': plan['digest'], 'applied': False,
              'refresh_command': 'omp models refresh ' + args.provider}
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
