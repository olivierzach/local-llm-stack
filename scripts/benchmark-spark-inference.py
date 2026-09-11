#!/usr/bin/env python3
"""Bounded streaming inference measurements with explicit concurrency and prefix reuse."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import math
from pathlib import Path
import statistics
import time
import uuid

import requests


def measure(base_url, key, model, prompt, max_tokens, request_timeout=180, capture_text=False):
    session = requests.Session()
    session.trust_env = False
    if key: session.headers['Authorization'] = 'Bearer ' + key
    started = time.monotonic()
    first = None
    usage = None
    done = False
    text = ''
    finish_reason = None
    started_at = time.time()
    with session, session.post(base_url + '/chat/completions',json={
        'model':model,'messages':[{'role':'user','content':prompt}], 'temperature':0,
        'max_tokens':max_tokens,'stream':True,'stream_options':{'include_usage':True}},
        stream=True,timeout=(10,request_timeout)) as response:
        response.raise_for_status()
        deployment = response.headers.get('X-Spark-Deployment')
        for line in response.iter_lines(chunk_size=1,decode_unicode=True):
            if not line.startswith('data:'): continue
            body = line[5:].strip()
            if body == '[DONE]':
                done = True
                break
            chunk = json.loads(body)
            if chunk.get('error'):
                raise RuntimeError('upstream returned a streaming error')
            if chunk.get('usage'): usage = chunk['usage']
            for choice in chunk.get('choices',[]):
                if choice.get('finish_reason'): finish_reason = choice['finish_reason']
                delta = choice.get('delta',{})
                if delta.get('reasoning') or delta.get('reasoning_content'):
                    raise RuntimeError('text benchmark requires thinking disabled; reasoning tokens have different timing')
                content = delta.get('content')
                if content:
                    if first is None: first = time.monotonic()
                    text += content
    elapsed = time.monotonic()-started
    if not done or first is None or not usage or usage.get('completion_tokens',0)<2:
        raise RuntimeError('benchmark requires complete text SSE and actual token usage')
    if (usage.get('completion_tokens_details') or {}).get('reasoning_tokens',0):
        raise RuntimeError('text benchmark cannot attribute hidden reasoning tokens to text decoding')
    generated = usage['completion_tokens']
    result = {'ttft_s':first-started,'elapsed_s':elapsed,'prompt_tokens':usage['prompt_tokens'],
        'completion_tokens':generated,'decode_tokens_per_second':(generated-1)/(elapsed-(first-started)),
        'text_chars':len(text), 'deployment_digest':deployment,
        'started_at':started_at, 'finish_reason':finish_reason,
        'cached_prompt_tokens':(usage.get('prompt_tokens_details') or {}).get('cached_tokens')}
    if capture_text: result['text'] = text
    return result


def summarize(records, elapsed):
    ttft = sorted(r['ttft_s'] for r in records)
    return {'requests':len(records),'wall_s':elapsed,
        'output_tokens':sum(r['completion_tokens'] for r in records),
        'aggregate_output_tokens_per_second':sum(r['completion_tokens'] for r in records)/elapsed,
        'ttft_p50_s':statistics.median(ttft),'ttft_p95_s':ttft[math.ceil(.95*len(ttft))-1],
        'median_request_decode_tokens_per_second':statistics.median(r['decode_tokens_per_second'] for r in records)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base-url',required=True)
    parser.add_argument('--key-file',type=Path)
    parser.add_argument('--model',required=True)
    parser.add_argument('--concurrency',type=int,nargs='+',default=[1,2,4])
    parser.add_argument('--requests',type=int,default=4,help='requests per concurrency level, excluding one warmup')
    parser.add_argument('--max-tokens',type=int,default=128)
    parser.add_argument('--prompt-repeats',type=int,default=64)
    parser.add_argument('--prefix-mode',choices=['unique','shared'],default='unique')
    parser.add_argument('--output',type=Path,required=True)
    args = parser.parse_args()
    if not (1<=args.requests<=256 and 2<=args.max_tokens<=4096 and 1<=args.prompt_repeats<=1024 and
            args.concurrency and all(1<=c<=min(32,args.requests) for c in args.concurrency)):
        parser.error('benchmark bounds exceeded')
    key = args.key_file.read_text().strip() if args.key_file else None
    prefix = ('A compute node can run an independent workload or join a distributed deployment.\n'*args.prompt_repeats)
    prompt = prefix+'Explain the operational tradeoffs in detail, with several concrete examples.'
    call = lambda text:measure(args.base_url.rstrip('/'),key,args.model,text,args.max_tokens)
    call('Warmup '+str(uuid.uuid4())+'\n'+prompt)
    result={'model':args.model,'base_url':args.base_url,'prefix_mode':args.prefix_mode,
        'max_tokens':args.max_tokens,'prompt_repeats':args.prompt_repeats,'levels':[],
        'qualification':'Client-observed text SSE latency and server-reported tokens. Unique prefixes avoid intentional prefix-cache reuse; weights and kernels are warmed. Synthetic prompts do not measure answer quality.'}
    for concurrency in args.concurrency:
        prompts=[(str(uuid.uuid4())+'\n' if args.prefix_mode=='unique' else '')+prompt for _ in range(args.requests)]
        started=time.monotonic()
        with ThreadPoolExecutor(max_workers=concurrency) as pool: records=list(pool.map(call,prompts))
        level={'concurrency':concurrency,**summarize(records,time.monotonic()-started),'records':records}
        result['levels'].append(level)
        args.output.parent.mkdir(parents=True,exist_ok=True)
        args.output.write_text(json.dumps(result,indent=2)+'\n')
        print(json.dumps({k:v for k,v in level.items() if k!='records'}),flush=True)


if __name__=='__main__':main()
