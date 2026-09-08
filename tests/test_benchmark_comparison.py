import copy
import importlib.util
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'tools'))
from spark_cluster.config import load, plan
spec = importlib.util.spec_from_file_location('benchmark_comparison', ROOT/'scripts/compare-spark-benchmarks.py')
comparison = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparison)


def inputs():
    p = plan(*load(ROOT, ROOT/'cluster/inventory.json', ROOT/'cluster/deployments/balanced-e8f1.json'))
    result = {'model': 'local-balanced', 'prefix_mode': 'unique', 'max_tokens': 128, 'prompt_repeats': 64,
              'levels': [{'concurrency': 1, 'requests': 8, 'aggregate_output_tokens_per_second': 8, 'ttft_p50_s': .5}]}
    return p, result


def test_matching_benchmark_reports_direction_and_provenance():
    p, baseline = inputs()
    candidate = copy.deepcopy(baseline)
    candidate['levels'][0].update(aggregate_output_tokens_per_second=10, ttft_p50_s=.4)
    report = comparison.compare(baseline, candidate, p, p)
    assert report['levels'][0]['throughput_ratio'] == 1.25
    assert report['levels'][0]['ttft_ratio'] == .8
    assert report['baseline_digest'] == p['digest']


def test_mismatched_or_incomplete_workloads_are_not_reported_as_improvements():
    p, baseline = inputs()
    for change in ({'levels': []}, {'max_tokens': 64}, {'model': 'wrong-model'}):
        with pytest.raises(ValueError): comparison.compare(baseline, {**baseline, **change}, p, p)
    candidate = copy.deepcopy(baseline)
    candidate['levels'][0]['requests'] = 4
    with pytest.raises(ValueError): comparison.compare(baseline, candidate, p, p)
    candidate = copy.deepcopy(baseline)
    candidate['levels'][0]['aggregate_output_tokens_per_second'] = float('nan')
    with pytest.raises(ValueError): comparison.compare(baseline, candidate, p, p)
