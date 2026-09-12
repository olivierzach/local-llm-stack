"""DeepSeek's optional runtime profile must not alter existing model plans."""
import copy
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import config


@pytest.fixture
def ds_inputs():
    inv, recipe, deployment = config.load(ROOT, ROOT / 'cluster/inventory.json',
        ROOT / 'cluster/deployments/large-tp2-mtp2-tools-nccl2307-66f1.json')
    recipe = copy.deepcopy(recipe)
    for key in ('mamba_cache_mode', 'speculative_config', 'nccl_library'):
        recipe.pop(key, None)
    recipe.update(model='deepseek-ai/DeepSeek-V4-Flash-0731', tool_call_parser='deepseek_v4',
        deepseek_v4=dict(backend='b12x', kv_cache_dtype='fp8', block_size=256, max_num_batched_tokens=2048),
        default_chat_template_kwargs=dict(thinking=False, reasoning_effort='high'))
    return inv, recipe, deployment


@pytest.mark.parametrize('coordinator', ['66f1', 'e8f1'])
def test_deepseek_uses_both_fabric_ranks_and_its_native_parsers(ds_inputs, coordinator):
    inv, recipe, deployment = ds_inputs
    deployment['coordinator'] = coordinator
    config.validate_recipe(recipe)
    plan = config.plan(inv, recipe, deployment)
    config.validate_saved_plan(plan)
    for node, compose in plan['compose'].items():
        worker = compose['services']['worker']
        argv = worker['command']
        for option, value in [('--tokenizer-mode', 'deepseek_v4'), ('--tool-call-parser', 'deepseek_v4'),
                ('--attention-backend', 'B12X'), ('--kv-cache-dtype', 'fp8'),
                ('--tensor-parallel-size', '2'), ('--block-size', '256')]:
            assert argv[argv.index(option) + 1] == value
        assert argv[argv.index('--master-addr') + 1] == inv['nodes'][coordinator]['fabric'][0]['ip']
        assert ('--headless' in argv) == (node != coordinator)
        assert '--speculative-config' not in argv
        assert worker['environment']['NCCL_IB_DISABLE'] == '0'
        assert worker['environment']['VLLM_USE_B12X_MOE'] == '1'
        assert worker['environment']['HF_HUB_OFFLINE'] == '1'


def test_dspark_is_explicit_and_does_not_change_the_model_alias(ds_inputs):
    inv, recipe, deployment = ds_inputs
    plain = config.plan(inv, recipe, deployment)
    recipe['speculative_config'] = dict(method='dspark', num_speculative_tokens=2,
        draft_sample_method='probabilistic', attention_backend='B12X')
    config.validate_recipe(recipe)
    draft = config.plan(inv, recipe, deployment)
    assert draft['digest'] != plain['digest']
    assert draft['endpoint'] == plain['endpoint']
    argv = draft['compose']['66f1']['services']['worker']['command']
    assert json.loads(argv[argv.index('--speculative-config') + 1]) == recipe['speculative_config']


@pytest.mark.parametrize('change', [
    {'speculative_config': {'method': 'mtp', 'num_speculative_tokens': 2}},
    {'default_chat_template_kwargs': {'thinking': 'false', 'reasoning_effort': 'high'}},
    {'default_chat_template_kwargs': {'thinking': True, 'reasoning_effort': 'max'}},
    {'model': 'deepseek-ai/DeepSeek-V4-Flash'},
    {'tool_call_parser': 'hermes'},
    {'deepseek_v4': {'backend': 'b12x', 'kv_cache_dtype': 'fp8', 'block_size': 256,
                     'max_num_batched_tokens': 2048, 'env': {'NCCL_IB_DISABLE': '1'}}},
])
def test_unsupported_deepseek_combinations_fail_before_launch(ds_inputs, change):
    recipe = ds_inputs[1]
    recipe.update(change)
    with pytest.raises(config.ConfigError):
        config.validate_recipe(recipe)


def test_accepted_qwen_plan_is_unchanged():
    plan = config.plan(*config.load(ROOT, ROOT / 'cluster/inventory.json',
        ROOT / 'cluster/deployments/large-tp2-mtp2-tools-nccl2307-66f1.json'))
    assert plan['digest'] == 'a83ebf27f1b5f0ecbc92288191c206660ac5b13b3815ed411dea90e1383e3d20'
    for compose in plan['compose'].values():
        worker = compose['services']['worker']
        assert '--tokenizer-mode' not in worker['command']
        assert 'VLLM_USE_B12X_MOE' not in worker['environment']


@pytest.mark.parametrize('coordinator', ['66f1', 'e8f1'])
@pytest.mark.parametrize('speculation', ['', '-dspark2'])
def test_graph_variants_are_explicit_and_keep_the_fabric(coordinator, speculation):
    p = config.plan(*config.load(ROOT, ROOT / 'cluster/inventory.json',
        ROOT / f'cluster/deployments/deepseek-tp2{speculation}-graphs-{coordinator}.json'))
    config.validate_saved_plan(p)
    for c in p['compose'].values():
        worker = c['services']['worker']
        argv = worker['command']
        assert '--enforce-eager' not in argv
        assert argv[argv.index('--max-cudagraph-capture-size') + 1] == '8'
        assert json.loads(argv[argv.index('--compilation-config') + 1]) == {
            'cudagraph_mode': 'FULL_AND_PIECEWISE', 'custom_ops': ['all']}
        assert ('--speculative-config' in argv) == bool(speculation)
        assert worker['environment']['NCCL_IB_DISABLE'] == '0'


@pytest.mark.parametrize('size', [0, 129, True, '8'])
def test_graph_capture_bounds(ds_inputs, size):
    r = ds_inputs[1]
    r['extra_args'] = [a for a in r['extra_args'] if a != '--enforce-eager']
    r['deepseek_v4']['cudagraph_capture_size'] = size
    with pytest.raises(config.ConfigError):
        config.validate_recipe(r)


def test_graphs_cannot_silently_override_eager(ds_inputs):
    r = ds_inputs[1]
    r['extra_args'].append('--enforce-eager')
    r['deepseek_v4']['cudagraph_capture_size'] = 8
    with pytest.raises(config.ConfigError, match='conflict'):
        config.validate_recipe(r)


@pytest.mark.parametrize('coordinator', ['66f1', 'e8f1'])
def test_flashinfer_control_changes_attention_but_preserves_math_and_transport(coordinator):
    p = config.plan(*config.load(ROOT, ROOT / 'cluster/inventory.json',
        ROOT / f'cluster/deployments/deepseek-tp2-flashinfer-control-{coordinator}.json'))
    config.validate_saved_plan(p)
    for compose in p['compose'].values():
        worker = compose['services']['worker']
        argv = worker['command']
        assert argv[argv.index('--attention-backend') + 1] == 'FLASHINFER_MLA_SPARSE_DSV4'
        assert argv[argv.index('--moe-backend') + 1] == 'b12x'
        assert argv[argv.index('--linear-backend') + 1] == 'b12x'
        assert '--enforce-eager' in argv and '--speculative-config' not in argv
        assert worker['environment']['NCCL_IB_DISABLE'] == '0'


def test_alternate_attention_cannot_silently_change_drafter_backend(ds_inputs):
    r = ds_inputs[1]
    r['deepseek_v4']['attention_backend'] = 'flashinfer-sm120'
    r['speculative_config'] = dict(method='dspark', num_speculative_tokens=2,
        draft_sample_method='probabilistic', attention_backend='B12X')
    with pytest.raises(config.ConfigError, match='non-speculative diagnostic'):
        config.validate_recipe(r)


@pytest.mark.parametrize('value,expected', [(True, '1'), (False, '0')])
def test_shared_experts_overlap_switch_is_applied_to_both_ranks(ds_inputs, value, expected):
    inv, r, d = ds_inputs
    before = config.plan(inv, r, d)
    r['deepseek_v4']['disable_shared_experts_stream'] = value
    config.validate_recipe(r)
    after = config.plan(inv, r, d)
    for node in d['nodes']:
        worker = after['compose'][node]['services']['worker']
        old = before['compose'][node]['services']['worker']
        assert worker['environment']['VLLM_DISABLE_SHARED_EXPERTS_STREAM'] == expected
        assert 'VLLM_DISABLE_SHARED_EXPERTS_STREAM' not in old['environment']
        assert worker['command'] == old['command']
        assert worker['environment']['NCCL_IB_DISABLE'] == '0'


def test_shared_experts_overlap_switch_rejects_string_boolean(ds_inputs):
    r = ds_inputs[1]
    r['deepseek_v4']['disable_shared_experts_stream'] = 'false'
    with pytest.raises(config.ConfigError, match='must be boolean'):
        config.validate_recipe(r)


def test_cuda_blocking_is_explicit_eager_only_and_applied_to_both_ranks(ds_inputs):
    inv, r, d = ds_inputs
    r['extra_args'] = ['--enforce-eager']
    before = config.plan(inv, r, d)
    r['deepseek_v4']['cuda_launch_blocking'] = True
    config.validate_recipe(r)
    after = config.plan(inv, r, d)
    for node in d['nodes']:
        assert 'CUDA_LAUNCH_BLOCKING' not in before['compose'][node]['services']['worker']['environment']
        assert after['compose'][node]['services']['worker']['environment']['CUDA_LAUNCH_BLOCKING'] == '1'
    r['extra_args'] = []
    with pytest.raises(config.ConfigError, match='requires eager'):
        config.validate_recipe(r)
