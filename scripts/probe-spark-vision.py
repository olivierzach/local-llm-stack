#!/usr/bin/env python3
"""Verify actual image understanding, token accounting and optional Context Guard compaction."""
import argparse
import base64
import hashlib
import json
from pathlib import Path
import re
import struct
import time
import zlib

import requests


COLORS = {'red': (255, 0, 0), 'blue': (0, 0, 255)}


def png(color, size=384):
    def chunk(kind, data):
        return struct.pack('!I', len(data))+kind+data+struct.pack('!I', zlib.crc32(kind+data) & 0xffffffff)
    rows = (b'\x00'+bytes(COLORS[color])*size)*size
    return b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR', struct.pack('!2I5B', size, size, 8, 2, 0, 0, 0))+chunk(b'IDAT', zlib.compress(rows))+chunk(b'IEND', b'')


def payload(model, order, size=384):
    images = [{'type': 'image_url', 'image_url': {'url': 'data:image/png;base64,'+base64.b64encode(png(color, size)).decode()}} for color in order]
    return {'model': model, 'messages': [{'role': 'user', 'content': [
        {'type': 'text', 'text': 'Identify the solid fill color of each image. Reply only as a JSON array of lowercase English color names, in image order.'}, *images]}],
        'max_tokens': 64, 'temperature': 0}


def answer(text, expected):
    match = re.search(r'\[[^\]]*\]', text)
    if not match or json.loads(match.group()) != expected:
        raise RuntimeError('image answer did not match the actual fixture colors: '+text[:300])


def tokenize(base, value):
    request = {key: value[key] for key in ('model', 'messages')}
    response = requests.post(base.rstrip('/')+'/tokenize', json=request, timeout=60)
    response.raise_for_status()
    count = response.json()['count']
    if not isinstance(count, int) or count < 0: raise RuntimeError('invalid tokenizer count')
    return count


def completion(base, headers, value, stream=False):
    value = {**value, 'stream': stream}
    if stream: value['stream_options'] = {'include_usage': True}
    started = time.monotonic()
    first = None
    with requests.post(base.rstrip('/')+'/chat/completions', headers=headers, json=value, stream=stream, timeout=180) as response:
        response.raise_for_status()
        metadata = {k: v for k, v in response.headers.items() if k.lower().startswith(('x-context-', 'x-spark-'))}
        if stream:
            parts, usage, done = [], None, False
            for line in response.iter_lines():
                if not line.startswith(b'data:'): continue
                data = line[5:].strip()
                if data == b'[DONE]':
                    done = True
                    break
                event = json.loads(data)
                if event.get('usage'): usage = event['usage']
                for choice in event.get('choices', []):
                    content = choice.get('delta', {}).get('content')
                    if content:
                        if first is None: first = time.monotonic()-started
                        parts.append(content)
            if not done: raise RuntimeError('incomplete vision stream')
            text = ''.join(parts)
        else:
            body = response.json()
            text = body['choices'][0]['message']['content']
            usage = body.get('usage')
        if not usage: raise RuntimeError('vision response omitted token usage')
        return {'text': text, 'usage': usage, 'headers': metadata,
                'elapsed_seconds': time.monotonic()-started, 'first_delta_seconds': first}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url', required=True, help='Direct or gateway URL ending /v1')
    parser.add_argument('--tokenizer-url', required=True, help='Direct model URL without /v1')
    parser.add_argument('--key-file', type=Path)
    parser.add_argument('--model', default='local-vision')
    parser.add_argument('--context-tokens', type=int, default=8192)
    parser.add_argument('--guard', action='store_true', help='Also require over-context compaction and image preservation')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    headers = {'Authorization': 'Bearer '+args.key_file.read_text().strip()} if args.key_file else {}
    report = {'model': args.model, 'base_url': args.base_url, 'cases': {},
              'fixtures': {color: hashlib.sha256(png(color)).hexdigest() for color in COLORS},
              'scope': 'Synthetic solid-color image recognition and request/context handling; not a general vision-quality benchmark.'}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    # Keep incremental evidence if a later boundary check fails.
    def save(): args.output.write_text(json.dumps(report, indent=2)+'\n')
    for label, order, stream in [('two-images', ['red', 'blue'], False), ('reversed-stream', ['blue', 'red'], True)]:
        request = payload(args.model, order)
        count = tokenize(args.tokenizer_url, request)
        text_only = payload(args.model, [])
        text_count = tokenize(args.tokenizer_url, text_only)
        result = completion(args.base_url, headers, request, stream)
        result.update(tokenizer_count=count, text_only_count=text_count)
        report['cases'][label] = result
        save()
        answer(result['text'], order)
        if count <= text_count or count != result['usage']['prompt_tokens']:
            raise RuntimeError('image token count did not match actual model usage')
    maximum = payload(args.model, ['blue'], size=1024)
    oversized = payload(args.model, ['blue'], size=1536)
    maximum_count = tokenize(args.tokenizer_url, maximum)
    oversized_count = tokenize(args.tokenizer_url, oversized)
    result = completion(args.base_url, headers, oversized)
    result.update(maximum_size_tokenizer_count=maximum_count, oversized_tokenizer_count=oversized_count)
    report['cases']['oversized-image'] = result
    save()
    answer(result['text'], ['blue'])
    if maximum_count != oversized_count or oversized_count != result['usage']['prompt_tokens']:
        raise RuntimeError('one-megapixel image processing limit did not match actual usage')
    too_many = payload(args.model, ['red', 'blue', 'red'])
    with requests.post(args.base_url.rstrip('/')+'/chat/completions', headers=headers, json=too_many, timeout=60) as response:
        report['image_limit_rejected'] = response.status_code == 400
        save()
        if not report['image_limit_rejected']: raise RuntimeError('three-image request did not respect the two-image limit')
    if args.guard:
        request = payload(args.model, ['red', 'blue'])
        history = []
        for index in range(18):
            history += [{'role': 'user', 'content': ('Earlier context record '+str(index)+'. The test uses ordinary filler words. ')*100},
                        {'role': 'assistant', 'content': 'Acknowledged the earlier context.'}]
        request['messages'] = history+request['messages']
        original_count = tokenize(args.tokenizer_url, request)
        if original_count <= args.context_tokens: raise RuntimeError('fixture did not exceed the model context')
        result = completion(args.base_url, headers, request)
        result['original_tokenizer_count'] = original_count
        report['cases']['compacted-images'] = result
        save()
        answer(result['text'], ['red', 'blue'])
        compacted = {k.lower(): v for k, v in result['headers'].items()}.get('x-context-guard')
        if compacted != 'compacted' or result['usage']['prompt_tokens']+64 > args.context_tokens:
            raise RuntimeError('guard did not prove bounded image-preserving compaction')
    if args.guard:
        for result in report['cases'].values():
            evidence = {k.lower(): v for k, v in result['headers'].items()}
            if int(evidence.get('x-context-input-tokens', -1)) != result['usage']['prompt_tokens'] or int(evidence.get('x-context-limit', -1)) != args.context_tokens:
                raise RuntimeError('gateway context accounting differs from actual model usage')
    report['passed'] = True
    save()
    print(json.dumps({'passed': True, 'model': args.model, 'cases': list(report['cases']), 'output': str(args.output)}))


if __name__ == '__main__': main()
