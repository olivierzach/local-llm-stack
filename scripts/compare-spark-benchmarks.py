#!/usr/bin/env python3
"""Compare matched workload measurements and retain exact deployment provenance."""
import argparse
import json
import math
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'tools'))
from spark_cluster.config import read, require, validate_saved_plan


def compare(baseline, candidate, baseline_plan, candidate_plan):
    for result, plan in ((baseline, baseline_plan), (candidate, candidate_plan)):
        validate_saved_plan(plan)
        require(result['model'] == plan['recipe']['alias'], 'benchmark alias does not match its saved plan')
    for key in ('model', 'revision', 'dtype', 'context_tokens', 'max_output_tokens'):
        require(baseline_plan['recipe'][key] == candidate_plan['recipe'][key], 'model contracts differ: '+key)
    for key in ('prefix_mode', 'max_tokens', 'prompt_repeats'):
        require(baseline[key] == candidate[key], 'benchmark workloads differ: '+key)
    def levels(value):
        result = {row['concurrency']: row for row in value['levels']}
        require(result and len(result) == len(value['levels']), 'missing or duplicate concurrency levels')
        return result
    left, right = levels(baseline), levels(candidate)
    require(left.keys() == right.keys(), 'concurrency levels differ; incomplete runs cannot be compared')
    gpus = [sum(node['gpus'] for node in p['nodes'].values()) for p in (baseline_plan, candidate_plan)]
    rows = []
    def metric(row, key):
        value = row[key]
        require(type(value) in (int, float) and math.isfinite(value) and value > 0, 'invalid benchmark metric: '+key)
        return value
    for concurrency in sorted(left):
        a, b = left[concurrency], right[concurrency]
        require(a['requests'] == b['requests'] and a['requests'] >= concurrency, 'request counts differ or cannot fill the concurrency level')
        rate_a, rate_b = [metric(row, 'aggregate_output_tokens_per_second') for row in (a, b)]
        ttft_a, ttft_b = [metric(row, 'ttft_p50_s') for row in (a, b)]
        rows.append({'concurrency': concurrency, 'requests_per_variant': a['requests'],
            'baseline_output_tokens_per_second': rate_a, 'candidate_output_tokens_per_second': rate_b,
            'throughput_ratio': rate_b/rate_a, 'throughput_per_gpu_ratio': (rate_b/gpus[1])/(rate_a/gpus[0]),
            'baseline_ttft_p50_s': ttft_a, 'candidate_ttft_p50_s': ttft_b, 'ttft_ratio': ttft_b/ttft_a})
    return {'baseline_digest': baseline_plan['digest'], 'candidate_digest': candidate_plan['digest'],
            'model': baseline_plan['recipe']['model'], 'revision': baseline_plan['recipe']['revision'],
            'baseline_gpus': gpus[0], 'candidate_gpus': gpus[1], 'levels': rows,
            'qualification': 'Matched synthetic workload settings and model contract. Ratios are candidate/baseline; higher throughput and lower TTFT are better. This is not a quality or wall-power comparison.'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for option in ('baseline', 'candidate', 'baseline-plan', 'candidate-plan', 'output'):
        parser.add_argument('--'+option, type=Path, required=True)
    args = parser.parse_args()
    result = compare(read(args.baseline), read(args.candidate), read(args.baseline_plan), read(args.candidate_plan))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2)+'\n')
    print(json.dumps(result))


if __name__ == '__main__': main()
