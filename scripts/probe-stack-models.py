#!/usr/bin/env python3
"""Test legacy Compose model arguments on this node with isolated owned workers.

Uses the installed controller's GPU lease and exact-ID cleanup. Never starts a
peer worker or changes legacy services/routes. Native DS4/Qwen recipe acceptance
is separate. Each run writes recovery requests before it reserves the GPU.
"""
import argparse
import hashlib
import json
import platform
from pathlib import Path
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import node

SERVICES = {
    'local-fast': 'vllm-fast', 'local-balanced': 'vllm-balanced', 'local-large': 'vllm-large',
    'local-qwen30-a3b': 'vllm-qwen30a3b', 'local-deepseek-r1-qwen32b': 'vllm-deepseek32b',
    'local-mistral-small': 'vllm-mistral24b', 'local-gpt-oss-120b': 'vllm-gptoss120b',
    'local-laguna-s-2.1': 'vllm-lagunas21', 'local-vision': 'vllm-vision',
}
PUBLIC_ENV = {'HF_HOME', 'VLLM_NO_USAGE_STATS', 'TIKTOKEN_ENCODINGS_BASE', 'CUTE_DSL_ARCH', 'MAX_JOBS'}


def completed_text(stream):
    events = [json.loads(line[6:]) for line in stream.splitlines()
              if line.startswith('data: ') and line != 'data: [DONE]']
    choices = [c for event in events for c in event.get('choices', [])]
    text = ''.join(c.get('delta', {}).get('content') or '' for c in choices)
    reasons = [c['finish_reason'] for c in choices if c.get('finish_reason')]
    if (not text.strip() or 'data: [DONE]' not in stream or reasons != ['stop'] or
            any('error' in event for event in events)):
        raise RuntimeError('stream did not finish with a complete answer (finish reasons: ' + repr(reasons) + ')')
    return {'text': text, 'stream_done': True, 'finish_reason': 'stop'}


def load_definition(stack):
    # Compose omits inactive profiles unless explicitly selected. Rendering all
    # profiles starts nothing and is required to inspect optional model services.
    return json.loads(subprocess.check_output(
        ['docker', 'compose', '--profile', '*', 'config', '--format', 'json'], cwd=stack, text=True))


def build_request(stack, definition, model, host, port, run_id):
    service = definition['services'][SERVICES[model['alias']]]
    command = list(service['command'])
    if command[command.index('--model') + 1] != model['repo']:
        raise ValueError('configured model differs from catalog')
    command[command.index('--host') + 1] = '127.0.0.1'
    command[command.index('--port') + 1] = str(port)
    command += ['--revision', model['revision']]
    image = json.loads(subprocess.check_output(['docker', 'image', 'inspect', service['image']], text=True))[0]
    environment = {k: v for k, v in service.get('environment', {}).items() if k in PUBLIC_ENV}
    environment.update(HF_HUB_OFFLINE='1', TRANSFORMERS_OFFLINE='1', HF_HUB_DISABLE_TELEMETRY='1')
    worker = {'image': image['Id'], 'entrypoint': [], 'command': command,
              'environment': environment, 'network_mode': 'host', 'ipc': 'host',
              'gpus': 'all', 'restart': 'no', 'volumes': service['volumes']}
    recipe = {'model': model['repo'], 'revision': model['revision'], 'image': image['Id'],
              'alias': model['alias'], 'min_available_mib': 110000}
    inputs = {'node': host, 'recipe': recipe, 'worker': worker, 'port': port, 'run_id': run_id}
    digest = hashlib.sha256(json.dumps(inputs, sort_keys=True).encode()).hexdigest()
    owner = 'accept-stack-' + digest[:24]
    worker.update(container_name=owner, labels={'io.spark.owner': owner, 'io.spark.digest': digest})
    return {'owner': owner, 'digest': digest, 'node': host, 'recipe': recipe,
            'deployment': {'mode': 'single', 'port': port},
            'compose': {'name': owner, 'services': {'worker': worker}}}


def exercise(request, output, timeout):
    output.mkdir(parents=True, exist_ok=False)
    node.atomic(output / 'request.json', request)
    report = {'alias': request['recipe']['alias'], 'passed': False, 'owner': request['owner']}
    acquired = False
    try:
        node.check_port('127.0.0.1', request['deployment']['port'])
        node.main({**request, 'action': 'reserve'})
        acquired = True
        started = time.monotonic()
        node.main({**request, 'action': 'start'})
        url = 'http://127.0.0.1:' + str(request['deployment']['port']) + '/v1'
        deadline = time.monotonic() + timeout
        while True:
            state = node.main({**request, 'action': 'status'})
            if any(c['state'] in ('exited', 'dead') for c in state['containers']):
                raise RuntimeError('worker exited during startup; see retained logs')
            try:
                with urllib.request.urlopen(url + '/models', timeout=3) as response:
                    models = json.load(response)
                if request['recipe']['alias'] in [m['id'] for m in models['data']]:
                    break
            except (OSError, ValueError, KeyError):
                pass
            if time.monotonic() >= deadline:
                raise TimeoutError('startup deadline exceeded')
            time.sleep(5)
        payload = {'model': request['recipe']['alias'], 'messages': [{'role': 'user', 'content': 'Say hello in one short sentence.'}],
                   'max_tokens': 1024 if request['recipe']['alias'] in (
                       'local-deepseek-r1-qwen32b', 'local-gpt-oss-120b', 'local-laguna-s-2.1') else 128,
                   'temperature': 0, 'stream': True}
        if request['recipe']['model'].startswith('Qwen/Qwen3'):
            payload['chat_template_kwargs'] = {'enable_thinking': False}
        req = urllib.request.Request(url + '/chat/completions', data=json.dumps(payload).encode(),
                                     headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=180) as response:
            stream = response.read().decode()
        answer = completed_text(stream)
        report.update(passed=True, startup_and_completion_s=time.monotonic() - started,
                      **answer, acceptance_version=2, image=request['recipe']['image'], revision=request['recipe']['revision'])
    except Exception as error:
        report['error'] = str(error)
    finally:
        if acquired:
            try:
                report['cleanup'] = node.main({**request, 'action': 'stop'})
                report['cleanup_verified'] = node.reservation() is None
                if not report['cleanup_verified']:
                    report['passed'] = False
            except Exception as error:
                report.update(passed=False, cleanup_error=str(error))
        node.atomic(output / 'acceptance.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stack-root', type=Path, default=Path.home() / 'projects/local-llm-stack')
    parser.add_argument('--inventory', type=Path, default=ROOT / 'cluster/inventory.json')
    parser.add_argument('--alias', action='append', choices=list(SERVICES))
    parser.add_argument('--run-id')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--cleanup-request', type=Path, help='recover only the exact owned request saved by this probe')
    parser.add_argument('--timeout', type=int, default=1200)
    parser.add_argument('--port', type=int, default=8180)
    args = parser.parse_args()
    if args.cleanup_request:
        request = json.loads(args.cleanup_request.read_text())
        if not request['owner'].startswith('accept-stack-'):
            parser.error('cleanup request is outside the acceptance namespace')
        print(json.dumps(node.main({**request, 'action': 'stop'})))
        return 0
    if not args.run_id or not args.output:
        parser.error('--run-id and --output are required for acceptance')
    if not 10 <= args.timeout <= 3600 or not 1024 <= args.port <= 65535:
        parser.error('invalid startup timeout or port')
    host = [n for n in json.loads(args.inventory.read_text())['nodes'].values() if n['hostname'] == platform.node()]
    if len(host) != 1:
        parser.error('current host must match exactly one inventory node; no remote execution')
    stack = args.stack_root.expanduser().resolve()
    definition = load_definition(stack)
    catalog = json.loads((stack / 'cluster/single-node-models.lock.json').read_text())
    args.output.mkdir(parents=True, exist_ok=False)
    reports = []
    aliases = args.alias or list(SERVICES)
    node.atomic(args.output / 'acceptance.json', {'models': [], 'requested': aliases, 'finished': False, 'all_passed': False})
    def interrupted(signum, frame):
        raise SystemExit(128 + signum)
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    for alias in aliases:
        model = next(m for m in catalog['models'] if m['alias'] == alias)
        request = build_request(stack, definition, model, host[0], args.port, args.run_id)
        print(json.dumps({'phase': 'testing', 'alias': alias, 'owner': request['owner']}), flush=True)
        report = exercise(request, args.output / alias, args.timeout)
        reports.append(report)
        node.atomic(args.output / 'acceptance.json', {'models': reports, 'requested': aliases, 'finished': False, 'all_passed': False})
        print(json.dumps(report), flush=True)
        if node.reservation() is not None:
            raise RuntimeError('GPU lease remains; stop acceptance and recover the recorded owner')
    node.atomic(args.output / 'acceptance.json', {'models': reports, 'requested': aliases, 'finished': True, 'all_passed': all(r['passed'] for r in reports)})
    return int(not all(r['passed'] for r in reports))


if __name__ == '__main__':
    raise SystemExit(main())
