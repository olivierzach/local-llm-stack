#!/usr/bin/env python3
"""Publish the accepted GLM53 alias through this Spark's existing Context Guards."""
import argparse
import json
from pathlib import Path
import socket
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.cli import inspect, save_json
from spark_cluster.config import read
from spark_cluster.glm53_acceptance import verify_pair
from spark_cluster.gateway import guard
from spark_cluster.gateway_node import write


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--root', type=Path, default=Path.home() / 'projects/local-llm-stack')
    p.add_argument('--plan', type=Path, required=True)
    p.add_argument('--acceptance', type=Path, required=True)
    p.add_argument('--alternate-plan', type=Path, required=True)
    p.add_argument('--alternate-acceptance', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--apply', action='store_true')
    args = p.parse_args()
    plan = read(args.plan)
    evidence = verify_pair(plan, args.acceptance, read(args.alternate_plan), args.alternate_acceptance)
    node = socket.gethostname().removeprefix('spark-')
    if node not in plan['nodes']:
        p.error('run on a Spark included in the accepted plan')
    status = inspect(plan)
    if not status['healthy'] or any(n['reservation']['digest'] != plan['digest'] for n in status['nodes'].values()):
        raise RuntimeError('the exact accepted deployment must be healthy on both nodes')
    alias = plan['recipe']['alias']
    report = dict(node=node, deployment_digest=plan['digest'], alias=alias,
                  endpoint=plan['endpoint']['base_url'], evidence=evidence, applied=False)
    if not args.apply:
        print(json.dumps(report))
        return
    args.output.mkdir(parents=True, exist_ok=False)
    base = args.root.expanduser().resolve()
    guard.load_dotenv(base / '.env')
    registries = [base / 'data/context-guard-routes/registry.json',
                  Path.home() / '.local/state/local-llm-cluster/gateway/config/registry.json']
    backups = [(path, path.read_text(), path.stat().st_mode & 0o777) for path in [*registries, base / '.env']]
    for i, path in enumerate(registries):
        save_json(args.output / f'prior-registry-{i}.json', read(path))

    def run(script, *arguments):
        subprocess.run([str(ROOT / '.venv/bin/python'), str(ROOT / 'scripts' / script),
                        *map(str, arguments)], check=True, timeout=300)

    try:
        run('configure-context-routes.py', '--root', base, '--plan', args.plan, '--merge', '--alias', alias)
        run('spark-gateway', 'routes', '--node', node, '--plan', args.plan, '--merge', '--alias', alias)
        for path, previous, _ in backups[:2]:
            old, new = json.loads(previous)['routes'], read(path)['routes']
            if any(new.get(k) != v for k, v in old.items() if k != alias):
                raise RuntimeError('unrelated route changed')
            if new[alias]['deployment_digest'] != plan['digest']:
                raise RuntimeError('published route does not match accepted deployment')
        run('probe-context-route.py', '--registry', registries[0], '--model', alias, '--tools',
            '--expected-deployment', plan['digest'],
            '--output', args.output / 'context-tools.json')
        run('probe-spark-gateway.py', '--base-url', 'http://127.0.0.1:4110/v1',
            '--key-file', Path.home() / '.local/state/local-llm-cluster/gateway/api-key',
            '--model', alias, '--expected-deployment', plan['digest'],
            '--output', args.output / 'gateway-tools.json')
        report.update(applied=True, passed=True, unrelated_routes_preserved=True)
    except BaseException as exc:
        for path, contents, mode in backups:
            write(path, contents, mode)
        report.update(passed=False, rolled_back=True, error=repr(exc))
        raise
    finally:
        save_json(args.output / 'publication.json', report)
    print(json.dumps(report))


if __name__ == '__main__':
    main()
