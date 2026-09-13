#!/usr/bin/env python3
"""Require a real read-only OMP call through a Spark profile or existing provider."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import uuid

ROOT=Path(__file__).resolve().parents[1]


def validate(events, fixture, value):
    calls=[e for e in events if e.get('type')=='tool_execution_start']
    if not calls or any(e.get('toolName')!='read' or e.get('args',{}).get('path')!=str(fixture) for e in calls):
        raise RuntimeError('expected only real read-tool execution on the selected fixture')
    ids={e['toolCallId'] for e in calls}
    completed=[e for e in events if e.get('type')=='tool_execution_end' and e.get('toolCallId') in ids]
    if not any(value in json.dumps(e) and not e.get('isError') for e in completed):
        raise RuntimeError('read tool did not return the fixture verification value')
    endings=[e for e in events if e.get('type')=='agent_end']
    messages=endings[-1].get('messages',[]) if endings else []
    final=messages[-1] if messages else {}
    text=''.join(c.get('text','') for c in final.get('content',[]) if c.get('type')=='text')
    if final.get('role')!='assistant' or text.strip()!=value:
        raise RuntimeError('final assistant response did not return the tool result')
    return len(calls)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    target=parser.add_mutually_exclusive_group(required=True)
    target.add_argument('--node')
    target.add_argument('--provider',help='Use an existing local OMP provider without changing its configuration')
    parser.add_argument('--model',default='local-coder')
    parser.add_argument('--thinking',choices=('off','high'),help='Explicit thinking mode for toggle/tool integration checks')
    parser.add_argument('--port',type=int,default=4110)
    parser.add_argument('--key-file',type=Path)
    parser.add_argument('--registry',type=Path)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.provider and (args.key_file or args.registry):
        parser.error('--key-file and --registry require --node')
    args.output=args.output.resolve()
    value='spark-tool-'+uuid.uuid4().hex
    with tempfile.TemporaryDirectory(prefix='spark-omp-probe-') as temporary:
        root=Path(temporary)
        fixture=root/('7846.txt' if args.thinking == 'high' else 'verification.txt')
        fixture.write_text('Verification value: '+value+'\n')
        prompt = 'Use the read tool to read '+str(fixture)+'. Reply only with the verification value from the file. Do not guess.'
        if args.thinking == 'high':
            prompt = ('Carefully reason through (347 * 29) - (186 * 17) + 945 to determine the filename. '
                'The file is in '+str(root)+' and is named <integer result>.txt. '
                'Work out the arithmetic privately, then use the read tool to read that file. '
                'Reply only with the verification value from the file. Do not guess the verification value.')
        if args.provider:
            command=['omp']
            provider=args.provider
        else:
            command=[sys.executable,str(ROOT/'scripts/spark-client'),'run','--node',args.node,
                '--port',str(args.port),'--client','omp','--output',str(root/'profile')]
            for flag,path in [('--key-file',args.key_file),('--registry',args.registry)]:
                if path: command.extend([flag,str(path.resolve())])
            command.append('--')
            provider='spark-'+args.node
        command.extend(['--model',provider+'/'+args.model,'--tools','read',
            '--no-lsp','--no-extensions','--no-session','--mode','json','--max-time','90','--print',
            prompt])
        if args.thinking:
            index = 1 if args.provider else command.index('--') + 1
            command[index:index] = ['--thinking', args.thinking, '--print-thoughts']
        result=subprocess.run(command,cwd=root,text=True,capture_output=True,timeout=120)
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.with_suffix('.jsonl').write_text(result.stdout)
        args.output.with_suffix('.stderr').write_text(result.stderr)
        if result.returncode: raise RuntimeError('OMP exited unsuccessfully; inspect the saved stderr and event log')
        events=[]
        for line in result.stdout.splitlines():
            try: events.append(json.loads(line))
            except ValueError: continue
        calls=validate(events,fixture,value)
        report={'passed':True,'node':args.node,'provider':provider,'model':args.model,'tool':'read','tool_calls':calls,
                'verification_value':value,'events':len(events),'scope':'Read-only synthetic fixture; not a coding-quality evaluation.'}
        if args.thinking:
            report['thinking'] = args.thinking
            report['thinking_characters'] = sum(len(c.get('thinking', '')) for e in events
                if e.get('type') == 'message_end' for c in e.get('message', {}).get('content', [])
                if c.get('type') == 'thinking')
            if (report['thinking_characters'] > 0) != (args.thinking == 'high'):
                raise RuntimeError('OMP thinking blocks did not match the requested mode')
        args.output.write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps(report))


if __name__=='__main__':main()
