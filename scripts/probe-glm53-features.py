#!/usr/bin/env python3
"""Check GLM text, reasoning and image input using synthetic fixtures only.

No worker or route mutations. Tool continuation has its own protocol probe.
These checks establish basic capability, not general model accuracy.
"""
import argparse
import base64
import json
from pathlib import Path
import re
import struct
import sys
import time
import zlib

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster.cli import save_json
from spark_cluster.config import read, validate_saved_plan


def image_url(reverse=False):
    """Lossless 256x128 fixture: equal solid red and blue halves, no labels."""
    colors = [b'\xff\x00\x00', b'\x00\x00\xff']
    if reverse:
        colors.reverse()
    pixels = (b'\x00' + colors[0] * 128 + colors[1] * 128) * 128
    def chunk(kind, data):
        return struct.pack('>I', len(data)) + kind + data + struct.pack('>I', zlib.crc32(kind + data))
    png = (b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('>IIBBBBB', 256, 128, 8, 2, 0, 0, 0))
           + chunk(b'IDAT', zlib.compress(pixels)) + chunk(b'IEND', b''))
    return 'data:image/png;base64,' + base64.b64encode(png).decode()


def collect(response, stream):
    text, reasoning, finish, usage, done = '', '', None, {}, False
    if not stream:
        body = response.json()
        choice = body['choices'][0]
        message = choice['message']
        return dict(content=message.get('content') or '',
                    reasoning=message.get('reasoning_content') or message.get('reasoning') or '',
                    finish_reason=choice['finish_reason'], usage=body.get('usage') or {})
    for line in response.iter_lines():
        if not line.startswith(b'data:'):
            continue
        data = line[5:].strip()
        if data == b'[DONE]':
            done = True
            break
        item = json.loads(data)
        if item.get('error'):
            raise RuntimeError('server returned an SSE error')
        if item.get('usage'):
            usage = item['usage']
        for choice in item.get('choices', []):
            delta = choice.get('delta') or {}
            text += delta.get('content') or ''
            reasoning += delta.get('reasoning_content') or delta.get('reasoning') or ''
            finish = choice.get('finish_reason') or finish
        if len(text) + len(reasoning) > 2_000_000:
            raise RuntimeError('response exceeded the bounded probe size')
    if not done:
        raise RuntimeError('stream ended without DONE')
    return dict(content=text, reasoning=reasoning, finish_reason=finish, usage=usage)


def integer_answer(text, expected):
    """Require an explicit answer line, not a matching number buried in prose."""
    lines = text.strip().splitlines()
    return bool(lines) and lines[0].strip() in (str(expected), f'**{expected}**')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--saved-plan', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--base-url', help='optional gateway endpoint; uses the saved model alias')
    parser.add_argument('--key-file', type=Path)
    args = parser.parse_args()
    plan = read(args.saved_plan)
    validate_saved_plan(plan)
    if not plan['recipe'].get('glm53'):
        parser.error('requires a GLM53 plan')
    if args.output.exists():
        parser.error('use a new output filename')
    base = (args.base_url or plan['endpoint']['base_url']).rstrip('/')
    report = dict(complete=False, deployment_digest=plan['digest'], started_at=time.time(), checks=[],
                  scope='Synthetic capability checks; not a general quality evaluation.')
    session = requests.Session()
    session.trust_env = False
    if args.key_file:
        session.headers['Authorization'] = 'Bearer ' + args.key_file.read_text().strip()

    def exercise(label, content, expected, thinking=False, stream=False):
        body = dict(model=plan['recipe']['alias'], temperature=0, max_tokens=2048 if thinking else 128,
                    messages=[dict(role='user', content=content)], stream=stream,
                    chat_template_kwargs=dict(enable_thinking=thinking, reasoning_effort='high'))
        if stream:
            body['stream_options'] = dict(include_usage=True)
        started = time.monotonic()
        with session.post(base + '/chat/completions', json=body, stream=stream, timeout=(10, 600)) as response:
            response.raise_for_status()
            result = collect(response, stream)
        record = dict(test=label, stream=stream, elapsed_seconds=time.monotonic() - started,
                      content=result['content'], reasoning_characters=len(result['reasoning']),
                      finish_reason=result['finish_reason'], usage=result['usage'])
        if label.startswith('reasoning-'):
            record['followed_integer_only_format'] = result['content'].strip() == '49'
        report['checks'].append(record)
        save_json(args.output, report)
        if result['finish_reason'] != 'stop' or not expected(result['content']):
            raise RuntimeError(label + ': incomplete or incorrect synthetic response')
        if thinking != bool(result['reasoning'].strip()):
            raise RuntimeError(label + ': reasoning channel does not match the requested mode')
        print(json.dumps(record), flush=True)

    try:
        save_json(args.output, report)
        for stream in (False, True):
            exercise(f'text-{stream}', 'Compute 17 times 23. Reply only with the integer.',
                     lambda text: text.strip() == '391', stream=stream)
            exercise(f'reasoning-{stream}',
                     'A box has 17 rows of 23 beads. Remove 48 beads, then divide the rest into 7 equal groups. '
                     'How many beads are in each group? Give your final answer as only an integer.',
                     lambda text: integer_answer(text, 49), thinking=True, stream=stream)
        for reverse in (False, True):
            expected = ['blue', 'red'] if reverse else ['red', 'blue']
            exercise(f'vision-reverse-{reverse}', [
                dict(type='text', text='Name the two solid colors from left to right. Reply only as color,color.'),
                dict(type='image_url', image_url=dict(url=image_url(reverse)))],
                lambda text, expected=expected: re.findall(r'\b(?:red|blue)\b', text.lower()) == expected,
                stream=reverse)
        for trial in range(20):
            exercise(f'repeatability-{trial}', 'Compute 17 times 23. Reply only with the integer.',
                     lambda text: text.strip() == '391')
        report['complete'] = True
    except BaseException as exc:
        report['error'] = repr(exc)
        raise
    finally:
        report['ended_at'] = time.time()
        save_json(args.output, report)
        session.close()


if __name__ == '__main__':
    main()
