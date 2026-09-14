#!/usr/bin/env python3
"""Qualify an existing GLM deployment while sampling both hosts and cable counters.

This never starts/stops workers or changes routes. Full qualification runs once;
--role-check checks the identical recipe after reversing coordinator/worker roles.
"""
import argparse
import json
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.cli import inspect, save_json
from spark_cluster.config import read, validate_saved_plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--saved-plan', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--role-check', action='store_true')
    args = parser.parse_args()
    plan = read(args.saved_plan)
    validate_saved_plan(plan)
    if not plan['recipe'].get('glm53') or socket.gethostname() not in [n['hostname'] for n in plan['nodes'].values()]:
        parser.error('run on a participating Spark with a GLM53 plan')
    state = inspect(plan)
    if not state['healthy'] or any(n['reservation']['digest'] != plan['digest'] for n in state['nodes'].values()):
        raise RuntimeError('the exact selected deployment must be healthy')
    out = args.output.expanduser().resolve()
    out.mkdir(parents=True, exist_ok=False)
    save_json(out / 'plan.json', plan)
    report = dict(complete=False, deployment_digest=plan['digest'], started_at=time.time(),
                  phase='starting-monitors', role_check=args.role_check)
    monitors = []
    def command(node, argv):
        return argv if node['hostname'] == socket.gethostname() else [
            'ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', node['ssh'], shlex.join(argv)]
    def run(script, *flags, timeout=180):
        subprocess.run([sys.executable, str(ROOT / 'scripts' / script), '--saved-plan', str(out / 'plan.json'),
                        *map(str, flags)], check=True, timeout=timeout)
    try:
        save_json(out / 'campaign.json', report)
        run('snapshot-spark-serving.py', '--output', out / 'runtime-before')
        for name, node in plan['nodes'].items():
            sample = out / f'memory-{name}.jsonl'
            log = (out / f'monitor-{name}.log').open('w')
            script = str(Path(node['projects']) / 'local-llm-stack-cluster/current/scripts/sample-spark-memory.py')
            proc = subprocess.Popen(command(node, ['python3', script, '--output', str(sample)]),
                                    stdout=log, stderr=subprocess.STDOUT)
            monitors.append((node, sample, proc, log))
            deadline = time.monotonic() + 20
            while subprocess.run(command(node, ['test', '-s', str(sample)]), timeout=15).returncode:
                if proc.poll() is not None or time.monotonic() >= deadline:
                    raise RuntimeError('memory monitor did not produce its first sample')
                time.sleep(.5)
        report['phase'] = 'role-check' if args.role_check else 'full-acceptance'
        save_json(out / 'campaign.json', report)
        if args.role_check:
            run('probe-glm53-features.py', '--output', out / 'serving/features.json', timeout=3600)
            run('probe-spark-tool-calling.py', '--rounds', '2', '--output', out / 'serving/tools.json', timeout=3600)
            run('soak-spark-serving.py', '--rounds', '3', '--max-tokens', '256',
                '--output', out / 'serving/soak.json', timeout=2700)
            run('profile-spark-serving.py', '--prompt-tokens', '8192', '--concurrency', '1', '2',
                str(plan['recipe']['max_num_seqs']), '--max-tokens', '256',
                '--output', out / 'serving/concurrency.json', timeout=3600)
        else:
            run('accept-spark-serving.py', '--profile', 'glm53-256k', '--output', out / 'serving', timeout=21600)
        report.update(complete=True, phase='passed')
    except BaseException as exc:
        report.update(phase='failed', error=repr(exc))
        raise
    finally:
        errors = []
        for node, sample, proc, log in monitors:
            try:
                subprocess.run(command(node, ['touch', str(sample.with_suffix('.stop'))]), check=True, timeout=20)
                proc.wait(timeout=25)
                if proc.returncode:
                    raise RuntimeError('memory monitor failed')
                if node['hostname'] != socket.gethostname():
                    for path in (sample, sample.with_suffix('.summary.json')):
                        path.write_bytes(subprocess.check_output(command(node, ['cat', str(path)]), timeout=30))
            except Exception as exc:
                errors.append(repr(exc))
            finally:
                log.close()
        try:
            run('snapshot-spark-serving.py', '--output', out / 'runtime-after')
        except Exception as exc:
            errors.append(repr(exc))
        if errors:
            report.update(complete=False, monitoring_errors=errors)
        report['ended_at'] = time.time()
        save_json(out / 'campaign.json', report)
        print(json.dumps(report), flush=True)
        if errors:
            raise RuntimeError('qualification monitoring failed; inspect campaign.json')


if __name__ == '__main__':
    main()
