#!/usr/bin/env python3
"""Check explicit DeepSeek thinking controls without storing reasoning text."""
import argparse
import json
from pathlib import Path
import runpy
import sys
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--controller', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--saved-plan', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    root = args.controller.resolve()
    sys.path.insert(0, str(root / 'tools'))
    from spark_cluster.cli import save_json
    from spark_cluster.config import read, validate_saved_plan
    plan = read(args.saved_plan)
    validate_saved_plan(plan)
    if (not plan['recipe'].get('deepseek_v4') or plan['recipe']['max_output_tokens'] < 1024
            or args.output.exists()):
        parser.error('requires a DeepSeek V4 plan with at least 1024 output tokens and a fresh output file')
    chat = runpy.run_path(str(root / 'scripts/qwen38-verify.py'))['chat']
    report = dict(complete=False, deployment_digest=plan['digest'], checks=[], started_at=time.time(),
                  scope='Thinking toggle, final answer, usage accounting and stream completion; not reasoning quality.')
    save_json(args.output, report)
    try:
        for thinking in (False, True):
            for stream in (False, True):
                payload = {'model': plan['recipe']['alias'], 'temperature': 0, 'max_tokens': 1024,
                           'messages': [
                               {'role': 'system', 'content': 'Your final answer must contain only the integer result. Do not include an equation, label, explanation, or markdown.'},
                               {'role': 'user', 'content': 'Calculate 17 multiplied by 19.'}],
                           'chat_template_kwargs': {'thinking': thinking, 'reasoning_effort': 'high'}, 'stream': stream}
                if stream:
                    payload['stream_options'] = {'include_usage': True}
                record, message = chat(plan['endpoint']['base_url'], payload, None)
                record.pop('memory', None)
                count = (record['usage'].get('completion_tokens_details') or {}).get('reasoning_tokens', 0)
                passed = (record['finish_reason'] == 'stop' and message['content'].strip() == '323'
                          and (count > 0 if thinking else count == 0))
                record.update(thinking=thinking, stream=stream, passed=passed,
                              final_answer=message['content'], reasoning_tokens=count)
                report['checks'].append(record)
                save_json(args.output, report)
                print(json.dumps(record), flush=True)
                if not passed:
                    raise RuntimeError('thinking toggle, final answer or usage check failed')
        report['complete'] = True
    except BaseException as exc:
        report['error'] = repr(exc)
        raise
    finally:
        report['ended_at'] = time.time()
        save_json(args.output, report)


if __name__ == '__main__':
    main()
