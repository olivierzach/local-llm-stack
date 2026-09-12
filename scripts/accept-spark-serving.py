#!/usr/bin/env python3
"""Run a bounded serving acceptance sequence against an existing deployment.

Leaves lifecycle and route changes to sparkctl and configure-context-routes.py.
Stops on the first failed check and retains every completed receipt.
"""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.cli import save_json
from spark_cluster.config import read, validate_saved_plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--saved-plan', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True, help='new receipt directory')
    parser.add_argument('--profile', choices=('qwen-256k', 'deepseek-64k'), default='qwen-256k')
    args = parser.parse_args()
    saved = args.saved_plan.resolve()
    plan = read(saved)
    validate_saved_plan(plan)
    minimum = 262144 if args.profile == 'qwen-256k' else 65536
    if plan['recipe']['context_tokens'] < minimum or plan['recipe']['max_output_tokens'] < 4096:
        parser.error('recipe context/output limits are too small for this acceptance profile')
    if args.profile == 'deepseek-64k' and not plan['recipe'].get('deepseek_v4'):
        parser.error('the DeepSeek profile requires a DeepSeek V4 recipe')
    args.output.mkdir(parents=True, exist_ok=False)
    report = dict(complete=False, deployment_digest=plan['digest'], profile=args.profile,
                  started_at=time.time(), checks=[])
    checks = [
        ('decode', 'profile-spark-decode.py', ['--max-tokens', '1024'], 2100),
        ('long-context', 'probe-spark-long-context.py', ['--input-tokens', str(minimum - 2112)], 2100),
        ('soak', 'soak-spark-serving.py', ['--rounds', '3', '--max-tokens', '1024'], 2700),
        ('decode-4096', 'profile-spark-decode.py', ['--max-tokens', '4096'], 2100),
    ]
    try:
        for label, script, flags, timeout in checks:
            report['phase'] = label
            save_json(args.output / 'acceptance.json', report)
            receipt = args.output / (label + '.json')
            subprocess.run([sys.executable, str(ROOT / 'scripts' / script), '--saved-plan', str(saved),
                            '--output', str(receipt), *flags], check=True, timeout=timeout)
            result = read(receipt)
            if not result.get('complete') or result.get('deployment_digest') != plan['digest']:
                raise RuntimeError('incomplete or mismatched acceptance receipt: ' + label)
            report['checks'].append(label)
        report.update(complete=True, phase='passed')
    except BaseException as exc:
        report.update(phase='failed', error=repr(exc))
        raise
    finally:
        report['ended_at'] = time.time()
        save_json(args.output / 'acceptance.json', report)
        print(json.dumps(report), flush=True)


if __name__ == '__main__':
    main()
