#!/usr/bin/env python3
"""Update an existing OpenClaw model's context and provider deadline from a saved plan."""
import argparse
import copy
import hashlib
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.config import read, validate_saved_plan
from spark_cluster.gateway_node import write


def update(config, provider, alias, context):
    changed = copy.deepcopy(config)
    target = changed['models']['providers'][provider]
    matches = [m for m in target['models'] if m['id'] == alias]
    if len(matches) != 1:
        raise ValueError('expected exactly one existing OpenClaw model entry')
    matches[0]['contextWindow'] = context
    if context > 65536:
        # OpenClaw places HTTP/stream deadlines on providers, not individual
        # models. Preserve an already longer deadline and all agent settings.
        target['timeoutSeconds'] = max(target.get('timeoutSeconds', 0), 3600)
    return changed


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--saved-plan', type=Path, required=True)
    p.add_argument('--config', type=Path, default=Path.home() / '.openclaw/openclaw.json')
    p.add_argument('--provider', default='spark-litellm')
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--apply', action='store_true')
    args = p.parse_args()
    plan = read(args.saved_plan); validate_saved_plan(plan)
    path = args.config.expanduser()
    previous = path.read_text()
    alias, limit = plan['recipe']['alias'], plan['endpoint']['context_tokens']
    changed = update(json.loads(previous), args.provider, alias, limit)
    content = json.dumps(changed, indent=2) + '\n'
    report = {'provider': args.provider, 'model': alias, 'contextWindow': limit,
              'provider_timeoutSeconds': changed['models']['providers'][args.provider].get('timeoutSeconds'),
              'deployment_digest': plan['digest'], 'applied': False,
              'scope': 'Existing model context and shared local-provider deadline; URLs, keys, other model limits and agent settings preserved.'}
    if args.apply:
        args.output.mkdir(parents=True, exist_ok=False, mode=0o700)
        write(args.output / 'openclaw.json.before', previous)
        if path.read_text() != previous:
            raise RuntimeError('OpenClaw configuration changed during preflight')
        write(path, content, path.stat().st_mode & 0o777)
        report.update(applied=True, configuration_sha256=hashlib.sha256(content.encode()).hexdigest())
        write(args.output / 'update.json', json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__': main()
