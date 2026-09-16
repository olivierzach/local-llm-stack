#!/usr/bin/env python3
"""Require real OpenClaw read-tool execution through a selected Spark gateway.

Tested with OpenClaw 2026.9.1's agent-exec envelope and SQLite transcript schema.
Only a generated fixture is exposed to tools; existing OpenClaw state is untouched.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.clients import client_environment, profiles


def text(message):
    content = message.get('content', [])
    if isinstance(content, str):
        return content
    return ''.join(part.get('text', '') for part in content if part.get('type') == 'text')


def validate(envelope, messages, fixture, value, provider, model):
    if (envelope.get('ok') is not True or envelope.get('status') != 'ok'
            or envelope.get('provider') != provider or envelope.get('model') != model
            or envelope.get('final', '').strip() != value):
        raise RuntimeError('OpenClaw did not finish successfully on the selected model')
    pending, completed, calls = set(), set(), 0
    for message in messages:
        role = message.get('role')
        if role in ('system', 'user') and value in text(message):
            raise RuntimeError('verification value leaked into the prompt')
        if role == 'assistant':
            if message.get('provider') != provider or message.get('model') != model:
                raise RuntimeError('transcript used a different provider/model')
            for part in message.get('content', []):
                if part.get('type') != 'toolCall':
                    continue
                args = part.get('arguments', {})
                selected = args.get('path', args.get('file_path', ''))
                selected = Path(selected)
                if not selected.is_absolute():
                    selected = fixture.parent / selected
                call_id = part.get('id')
                if (part.get('name') != 'read' or selected.resolve() != fixture.resolve()
                        or not call_id or call_id in pending or call_id in completed):
                    raise RuntimeError('expected only read calls on the selected fixture')
                pending.add(call_id)
                calls += 1
        elif role == 'toolResult':
            call_id = message.get('toolCallId')
            if (call_id not in pending or message.get('toolName') != 'read'
                    or message.get('isError') or value not in text(message)):
                raise RuntimeError('missing or mismatched successful read result')
            pending.remove(call_id)
            completed.add(call_id)
    summary = envelope.get('toolSummary', {})
    if (not calls or pending or summary.get('calls') != calls
            or summary.get('tools') != ['read'] or summary.get('failures', 0)
            or not messages or messages[-1].get('role') != 'assistant'
            or text(messages[-1]).strip() != value):
        raise RuntimeError('tool transcript and final answer did not agree')
    return calls


def transcript(state, session_id):
    uuid.UUID(session_id)
    database = state / 'agents/main/agent/openclaw-agent.sqlite'
    with sqlite3.connect(database.resolve().as_uri() + '?mode=ro', uri=True) as connection:
        rows = connection.execute(
            'SELECT event_json FROM transcript_events WHERE session_id = ? ORDER BY seq LIMIT 1001',
            (session_id,)).fetchall()
    if not rows or len(rows) > 1000:
        raise RuntimeError('missing or unexpectedly large probe transcript')
    return [event['message'] for row in rows
            if (event := json.loads(row[0])).get('type') == 'message']


def bounded(command, env, workspace, seconds):
    process = subprocess.Popen(command, env=env, cwd=workspace, text=True,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               start_new_session=True)
    try:
        out, err = process.communicate(timeout=seconds)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            out, err = process.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            out, err = process.communicate()
        return 124, out, err + '\nProbe process-group deadline expired.\n'
    return process.returncode, out, err


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--node', required=True)
    parser.add_argument('--model', default='local-coder')
    parser.add_argument('--port', type=int, default=4110)
    parser.add_argument('--registry', type=Path)
    parser.add_argument('--key-file', type=Path)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    local = socket.gethostname() == 'spark-' + args.node
    gateway = (Path.home() / '.local/state/local-llm-cluster/gateway' if local
               else ROOT / 'data/cluster/gateways' / args.node)
    registry = json.loads((args.registry or gateway / ('config/registry.json' if local else 'registry.json')).read_text())
    generated = profiles(registry, args.node, args.port)
    route = registry['routes'].get(args.model)
    if not route or not route['capabilities']['tools']:
        raise ValueError('probe requires a registered tool-capable model')
    key = (args.key_file or gateway / 'api-key').read_text().strip()
    if len(key) < 24:
        raise ValueError('invalid gateway credential')
    output = args.output.resolve()
    artifacts = output.with_suffix('.artifacts')
    if output.exists() or artifacts.exists():
        raise ValueError('choose new output paths; prior evidence is preserved')
    output.parent.mkdir(parents=True, exist_ok=True)
    artifacts.mkdir(mode=0o700)
    value = 'spark-claw-' + uuid.uuid4().hex
    provider = 'spark-' + args.node
    with tempfile.TemporaryDirectory(prefix='spark-openclaw-probe-') as temporary:
        root = Path(temporary)
        workspace, state = root / 'workspace', root / 'state'
        workspace.mkdir(); state.mkdir()
        fixture = workspace / 'verification.txt'
        fixture.write_text('Verification value: ' + value + '\n')
        config = generated['openclaw/openclaw.json']
        config['agents']['defaults'].update(workspace=str(workspace), skipBootstrap=True, timeoutSeconds=90)
        config['tools'] = {'profile': 'full', 'allow': ['read'], 'fs': {'workspaceOnly': True},
                           'codeMode': {'enabled': False}}
        config['plugins'] = {'enabled': False}
        config['models']['catalogRefresh'] = {'enabled': False}
        config['skills'] = {'allowBundled': [], 'load': {'watch': False}}
        config['env'] = {'shellEnv': {'enabled': False}}
        config_path = root / 'openclaw.json'
        config_path.write_text(json.dumps(config, indent=2) + '\n')
        # Preserve identity/runtime essentials; do not inherit unrelated provider keys.
        env = {k: os.environ[k] for k in ('HOME', 'PATH', 'USER', 'LOGNAME', 'LANG', 'LC_ALL', 'TMPDIR')
               if k in os.environ}
        env.update(client_environment(root, key))
        env['OPENCLAW_CONFIG_PATH'] = str(config_path)
        env['OPENCLAW_STATE_DIR'] = str(state)
        node = Path.home() / '.local/opt/spark-client-node/22.23.2/bin'
        env['PATH'] = str(Path.home() / '.local/bin') + os.pathsep + env.get('PATH', os.defpath)
        if (node / 'node').is_file():
            env['PATH'] = str(node) + os.pathsep + env['PATH']
        executable = shutil.which('openclaw', path=env['PATH'])
        if not executable:
            raise RuntimeError('OpenClaw is not installed on this client machine')
        version = subprocess.check_output([executable, '--version'], env=env, text=True, timeout=30).strip()
        code, out, err = bounded([executable, 'agent', 'exec', '--config', str(config_path),
            '--cwd', str(workspace), '--state-dir', str(state), '--model', provider + '/' + args.model,
            '--code-mode', 'direct', '--thinking', 'off', '--timeout', '90', '--json',
            'Use the read tool to read verification.txt in the workspace. Reply only with the verification value from the file. Do not guess.'],
            env, workspace, 150)
        (artifacts / 'stdout.json').write_text(out.replace(key, '[redacted]'))
        (artifacts / 'stderr.txt').write_text(err.replace(key, '[redacted]'))
        if code:
            raise RuntimeError('OpenClaw failed; inspect the retained probe artifacts (exit ' + str(code) + ')')
        envelope = json.loads(out)
        messages = transcript(state, envelope['sessionId'])
        (artifacts / 'messages.json').write_text(json.dumps(messages, indent=2).replace(key, '[redacted]') + '\n')
        calls = validate(envelope, messages, fixture, value, provider, args.model)
        report = {'passed': True, 'client_version': version, 'gateway_node': args.node,
                  'model': args.model, 'model_root': route['model_root'], 'tool_calls': calls,
                  'assistant_turns': envelope.get('assistantTurns'), 'verification_value': value,
                  'session_id': envelope['sessionId'], 'usage': envelope.get('usage'),
                  'scope': 'Isolated read-only fixture and complete tool transcript; not a coding-quality evaluation.'}
        output.write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(report))


if __name__ == '__main__':
    main()
