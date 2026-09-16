#!/usr/bin/env python3
"""Exercise OMP read/edit/bash through an existing provider on an isolated fixture."""
import argparse
import json
from pathlib import Path
import subprocess
import tempfile
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--provider', default='spark-context-guard')
    parser.add_argument('--model', default='local-deepseek-v4-flash')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists():
        parser.error('use a fresh output filename')
    output.parent.mkdir(parents=True, exist_ok=True)
    report = dict(passed=False, provider=args.provider, model=args.model,
                  scope='One isolated read/edit/test task; not a coding-quality benchmark.')
    with tempfile.TemporaryDirectory(prefix='spark-omp-coding-') as folder:
        root = Path(folder)
        (root / 'calc.py').write_text('def add(a, b):\n    return a - b\n')
        value = 'TEST_OK_' + uuid.uuid4().hex
        check_source = ('from calc import add\nassert add(7, 5) == 12\n'
                        'assert add(-2, 3) == 1\nprint(' + repr(value) + ')\n')
        (root / 'check.py').write_text(check_source)
        prompt = ('Work only in this temporary directory: ' + folder +
                  '. Read calc.py and check.py. Fix only calc.py so add correctly adds '
                  'its arguments. Use the edit tool for the fix. Run python3 check.py '
                  'with the bash tool to verify. Do not modify check.py. Reply only '
                  'with the verification string printed by that test. Do not use other paths or commands.')
        try:
            result = subprocess.run([
                'omp', '--model', args.provider + '/' + args.model,
                '--tools', 'read,edit,bash', '--no-lsp', '--no-extensions',
                '--no-skills', '--no-rules', '--no-session', '--no-title',
                '--mode', 'json', '--max-time', '180', '--approval-mode', 'yolo',
                '--print', prompt], cwd=root, capture_output=True, text=True, timeout=210)
            output.with_suffix('.jsonl').write_text(result.stdout)
            output.with_suffix('.stderr').write_text(result.stderr)
            events = []
            for line in result.stdout.splitlines():
                try:
                    events.append(json.loads(line))
                except ValueError:
                    continue
            calls = [e for e in events if e.get('type') == 'tool_execution_start']
            endings = [e for e in events if e.get('type') == 'agent_end']
            messages = endings[-1].get('messages', []) if endings else []
            final = messages[-1] if messages else {}
            text = ''.join(c.get('text', '') for c in final.get('content', []) if c.get('type') == 'text')
            check = subprocess.run(['python3', 'check.py'], cwd=root,
                                   capture_output=True, text=True, timeout=15)
            report.update(returncode=result.returncode, tools=[e.get('toolName') for e in calls],
                          verification_passed=check.returncode == 0,
                          final_matches=final.get('role') == 'assistant' and text.strip() == value)
            report['passed'] = ((root / 'check.py').read_text() == check_source
                                and result.returncode == 0 and check.returncode == 0
                                and report['final_matches']
                                and {'read', 'edit', 'bash'} == set(report['tools']))
        except BaseException as exc:
            report['error'] = repr(exc)
            raise
        finally:
            output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))
    if not report['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
