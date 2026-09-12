#!/usr/bin/env python3
"""Regression probe for observed first-token answer flips; not a quality benchmark."""
import argparse
import json
from pathlib import Path
import sys
import time

import requests


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--controller', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--saved-plan', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--trials', type=int, default=20)
    args = parser.parse_args()
    sys.path.insert(0, str(args.controller.resolve() / 'tools'))
    from spark_cluster.cli import save_json
    from spark_cluster.config import read, validate_saved_plan
    plan = read(args.saved_plan)
    validate_saved_plan(plan)
    if not plan['recipe'].get('deepseek_v4') or args.output.exists() or not 10 <= args.trials <= 100:
        parser.error('requires a DeepSeek plan, fresh output file, and 10..100 trials')
    payload = {
        'model': plan['recipe']['alias'], 'temperature': 0, 'seed': 0, 'max_tokens': 1,
        'logprobs': True, 'top_logprobs': 5,
        'messages': [
            {'role': 'system', 'content': 'Your final answer must contain only the integer result. Do not include an equation, label, explanation, or markdown.'},
            {'role': 'user', 'content': 'Calculate 17 multiplied by 19.'}],
        'chat_template_kwargs': {'thinking': False, 'reasoning_effort': 'high'},
    }
    report = dict(complete=False, passed=False, deployment_digest=plan['digest'],
                  started_at=time.time(), request=payload, expected='323', records=[],
                  scope='Identical sequential first-token requests with fixed seed; no speculation-generated continuation. Does not establish general model quality or numerical determinism.')
    save_json(args.output, report)
    session = requests.Session()
    session.trust_env = False
    try:
        for trial in range(args.trials):
            response = session.post(plan['endpoint']['base_url'] + '/chat/completions', json=payload, timeout=120)
            response.raise_for_status()
            result = response.json()
            choice = result['choices'][0]
            answer = choice['message'].get('content')
            usage = result.get('usage') or {}
            token_logs = (choice.get('logprobs') or {}).get('content') or []
            passed = (answer == '323' and usage.get('completion_tokens') == 1
                      and len(token_logs) == 1 and token_logs[0]['token'] == answer)
            record = dict(trial=trial, answer=answer, passed=passed, usage=usage,
                          finish_reason=choice.get('finish_reason'), logprobs=choice.get('logprobs'))
            report['records'].append(record)
            save_json(args.output, report)
            print(json.dumps({'trial': trial, 'answer': answer, 'passed': passed}), flush=True)
        report['passed'] = all(r['passed'] for r in report['records'])
        report['complete'] = report['passed']
        if not report['passed']:
            raise RuntimeError('repeated first-token regression failed; do not publish this deployment')
    except BaseException as exc:
        report['error'] = repr(exc)
        raise
    finally:
        session.close()
        report['ended_at'] = time.time()
        save_json(args.output, report)


if __name__ == '__main__':
    main()
