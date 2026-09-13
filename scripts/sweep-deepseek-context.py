#!/usr/bin/env python3
"""Compare increasing prompt lengths on one existing DeepSeek deployment, with no lifecycle changes."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.config import read, validate_saved_plan
from spark_cluster.cli import save_json


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--saved-plan', type=Path, required=True)
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    plan = read(args.saved_plan); validate_saved_plan(plan)
    if not plan['recipe'].get('deepseek_v4'): p.error('requires a DeepSeek plan')
    args.output.mkdir(parents=True, exist_ok=False)
    report = {'complete': False, 'deployment_digest': plan['digest'], 'started_at': time.time(), 'results': []}
    try:
        for limit in (65536, 131072, 262144, 524288, 786432, 1048576):
            if limit > plan['recipe']['context_tokens']: break
            receipt = args.output / (str(limit) + '.json')
            print(json.dumps({'phase': 'testing-context', 'context_tokens': limit}), flush=True)
            subprocess.run([sys.executable, str(ROOT / 'scripts/probe-spark-long-context.py'),
                '--saved-plan', str(args.saved_plan.resolve()), '--input-tokens', str(limit - 8192),
                '--corpus', 'varied', '--measure-decode', '--output', str(receipt)], check=True, timeout=10800)
            result = read(receipt)
            if not result.get('complete'): raise RuntimeError('incomplete context probe')
            report['results'].append({'context_tokens': limit, 'actual_input_tokens': result['actual_input_tokens'],
                'fresh_ttft_s': result['runs'][0]['ttft_s'], 'reused_ttft_s': result['runs'][1]['ttft_s'],
                'decode_tokens_per_second': result['decode_run']['decode_tokens_per_second']})
            save_json(args.output / 'sweep.json', report)
        report['complete'] = True
    except BaseException as exc:
        report['error'] = repr(exc)
        raise
    finally:
        report['ended_at'] = time.time()
        save_json(args.output / 'sweep.json', report)


if __name__ == '__main__': main()
