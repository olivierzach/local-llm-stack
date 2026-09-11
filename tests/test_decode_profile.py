import importlib.util
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    'decode_profile', Path(__file__).resolve().parents[1] / 'scripts/profile-spark-decode.py')
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


def test_speculative_counters_sum_labels_without_counting_histogram_samples():
    metrics = '''# TYPE vllm:spec_decode_num_accepted_tokens_total counter
vllm:spec_decode_num_accepted_tokens_total{engine="0"} 12
vllm:spec_decode_num_accepted_tokens_total{engine="1"} 8
vllm:spec_decode_num_draft_tokens_total{engine="0"} 30
vllm:spec_decode_num_drafts_total 10
vllm:spec_decode_accepted_tokens_per_pos_bucket{le="1"} 99
vllm:prefix_cache_hits_total 50
'''
    assert profile.counters(metrics) == {
        'vllm:spec_decode_num_accepted_tokens_total': 20,
        'vllm:spec_decode_num_draft_tokens_total': 30,
        'vllm:spec_decode_num_drafts_total': 10,
    }
