#!/usr/bin/env python3
"""Check installed OMP thinking on/off against its existing provider; save counts only."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import time


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--provider', default='spark-context-guard')
    p.add_argument('--model', default='local-deepseek-v4-flash')
    p.add_argument('--output', type=Path, required=True)
    args = p.parse_args()
    if args.output.exists(): p.error('use a fresh output filename')
    report = {'passed': False, 'provider': args.provider, 'model': args.model, 'checks': [],
              'scope': 'Real OMP reasoning toggle, final-answer and stream completion check; reasoning text is not saved.'}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(prefix='spark-omp-thinking-') as folder:
            for level in ('off', 'high'):
                start = time.monotonic()
                # Separate capability fixtures, not a same-prompt accuracy A/B:
                # an easy off request may answer without reasoning in either mode.
                prompt = ('Calculate 17 multiplied by 19. Your final answer must be only the integer result. Do not use tools.'
                    if level == 'off' else
                    'Carefully reason through (347 * 29) - (186 * 17) + 945. Work it out privately before answering. '
                    'Your final answer must be only the integer result. Do not use tools.')
                expected = '323' if level == 'off' else '7846'
                r = subprocess.run(['omp', '--model', args.provider+'/'+args.model, '--thinking', level,
                    '--tools', 'read', '--no-lsp', '--no-extensions', '--no-skills', '--no-rules',
                    '--no-session', '--no-title', '--mode', 'json', '--max-time', '120', '--print-thoughts', '--print',
                    prompt],
                    cwd=folder, capture_output=True, text=True, timeout=150)
                events = []
                for line in r.stdout.splitlines():
                    try: events.append(json.loads(line))
                    except ValueError: pass
                endings = [e for e in events if e.get('type') == 'agent_end']
                messages = endings[-1].get('messages', []) if endings else []
                final = messages[-1] if messages else {}
                content = final.get('content', [])
                answer = ''.join(c.get('text', '') for c in content if c.get('type') == 'text').strip()
                count = sum(len(c.get('thinking', '')) for c in content if c.get('type') == 'thinking')
                passed = (r.returncode == 0 and final.get('role') == 'assistant' and answer == expected
                    and (count > 0) == (level == 'high')
                    and not any(e.get('type') == 'tool_execution_start' for e in events))
                record = {'thinking': level, 'passed': passed, 'thinking_characters': count,
                          'expected_answer': expected, 'final_answer': answer,
                          'elapsed_s': time.monotonic()-start, 'returncode': r.returncode}
                report['checks'].append(record)
                print(json.dumps(record), flush=True)
                if not passed: raise RuntimeError('OMP thinking toggle failed; inspect provider mapping')
        report['passed'] = True
    finally:
        args.output.write_text(json.dumps(report, indent=2)+'\n')


if __name__ == '__main__': main()
