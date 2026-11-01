#!/usr/bin/env python3
"""Render the Mia Context Guard route and downstream catalogs. Starts nothing."""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.clients import profiles
from spark_cluster.config import load, plan
from spark_cluster.gateway import from_plans, read, upsert_routes

ALIAS = 'local-deepseek-v4-flash-mia'


def render(coordinator, registry_path=None):
    deployment = ROOT / 'cluster/deployments' / f'deepseek-tp2-mia-512k-{coordinator}.json'
    saved = plan(*load(ROOT, ROOT / 'cluster/inventory.json', deployment))
    if saved['recipe']['alias'] != ALIAS:
        raise RuntimeError('Mia deployment does not expose its own alias')
    route = from_plans([saved])
    merged = None
    source_unchanged = None
    if registry_path is not None:
        source = Path(registry_path).resolve()
        before = source.read_bytes()
        merged = upsert_routes(read(source), route)
        if source.read_bytes() != before:
            raise RuntimeError('source registry was modified')
        source_unchanged = True
        for alias, existing in read(source)['routes'].items():
            if alias != ALIAS and merged['routes'][alias] != existing:
                raise RuntimeError('unrelated route changed')
    return {
        'plan': saved,
        'route': route,
        'merged': merged,
        'profiles': profiles(merged or route, coordinator, 4110),
        'report': {
            'alias': ALIAS,
            'coordinator': coordinator,
            'deployment_digest': saved['digest'],
            'base_url': saved['endpoint']['base_url'],
            'model_root': route['routes'][ALIAS]['model_root'],
            'published': False,
            'models_started': False,
            'services_restarted': False,
            'gpu_workloads_changed': False,
            'source_registry_unchanged': source_unchanged,
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--coordinator', required=True, choices=('66f1', 'e8f1'))
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--registry', type=Path, help='read-only registry to merge in the preview')
    args = parser.parse_args()
    output = args.output.expanduser().resolve()
    if output.exists():
        parser.error('use a fresh output directory')
    rendered = render(args.coordinator, args.registry)
    output.mkdir(parents=True, exist_ok=False, mode=0o700)
    for name, value in (
            ('plan.json', rendered['plan']),
            ('route.json', rendered['route']),
            ('profiles.json', rendered['profiles']),
            ('render.json', rendered['report'])):
        (output / name).write_text(json.dumps(value, indent=2) + '\n')
    if rendered['merged'] is not None:
        (output / 'merged-registry.json').write_text(json.dumps(rendered['merged'], indent=2) + '\n')
    print(json.dumps(rendered['report']))


if __name__ == '__main__':
    main()
