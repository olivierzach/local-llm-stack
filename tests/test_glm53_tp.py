"""GLM cannot alter old placements or escape the pinned fabric/cache contract."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import config, node


def inputs(coordinator='e8f1'):
    return config.load(ROOT, ROOT / 'cluster/inventory.json',
        ROOT / f'cluster/deployments/glm53-tp2-256k-dflash2-{coordinator}.json')


@pytest.mark.parametrize('coordinator', ['66f1', 'e8f1'])
def test_offline_drafter_and_both_coordinator_paths(coordinator):
    inv, recipe, deployment = inputs(coordinator)
    p = config.plan(inv, recipe, deployment)
    config.validate_saved_plan(p)
    assert deployment['nodes'][-1] == coordinator  # worker starts first
    for name, compose in p['compose'].items():
        worker = compose['services']['worker']; argv = worker['command']
        spec = json.loads(argv[argv.index('--speculative-config') + 1])
        assert spec['model'].endswith('/snapshots/' + recipe['speculative_config']['revision'])
        assert 'revision' not in spec and spec['model'].startswith('/cache/hub/')
        assert ('--headless' in argv) == (name != coordinator)
        assert argv[argv.index('--master-addr') + 1] == inv['nodes'][coordinator]['fabric'][0]['ip']
        assert worker['environment']['NCCL_NET'] == 'IB'
        assert worker['environment']['HF_HUB_OFFLINE'] == '1'
        assert worker['environment']['NCCL_SOCKET_IFNAME'] == '=' + inv['nodes'][name]['fabric'][0]['interface']
        assert '--trust-remote-code' not in argv


@pytest.mark.parametrize('kind', ['draft_revision', 'draft_path', 'draft_count', 'overlay_escape', 'overlay_hash', 'parser', 'mixed_profile'])
def test_reject_unpinned_or_incompatible_recipe(kind):
    _, recipe, _ = inputs()
    if kind == 'draft_revision': recipe['speculative_config']['revision'] = 'main'
    if kind == 'draft_path': recipe['speculative_config']['model'] = '/tmp/untrusted'
    if kind == 'draft_count': recipe['speculative_config']['num_speculative_tokens'] = 2
    if kind == 'overlay_escape': recipe['glm53']['source_overlays'][0]['path'] = '../../etc/passwd'
    if kind == 'overlay_hash': recipe['glm53']['source_overlays'][0]['sha256'] = 'latest'
    if kind == 'parser': recipe.pop('glm53')
    if kind == 'mixed_profile': recipe['deepseek_v4'] = {}
    with pytest.raises(config.ConfigError): config.validate_recipe(recipe)


def test_tp3_fails_before_launch():
    inv, recipe, deployment = inputs()
    deployment['tensor_parallel'] = 3
    with pytest.raises(config.ConfigError, match='TP2'):
        config.plan(inv, recipe, deployment)


def test_missing_draft_and_corrupt_overlay_fail_admission(tmp_path):
    _, recipe, _ = inputs()
    request = {'recipe': recipe, 'node': {'cache': str(tmp_path / 'cache')}}
    with pytest.raises(RuntimeError, match='not cached'): node.verify_draft_cache(request)
    asset = recipe['glm53']['source_overlays'][0]
    path = tmp_path / 'runtime-overlays' / asset['sha256'] / asset['path']
    path.parent.mkdir(parents=True); path.write_text('corrupt')
    with pytest.raises(RuntimeError, match='SHA-256'): node.verify_source_overlays(request)


def test_asset_staging_hash_gate_and_idempotence(tmp_path):
    spec = importlib.util.spec_from_file_location('glm_assets', ROOT / 'scripts/prepare-glm53-runtime.py')
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    src = tmp_path / 'source'; src.write_text('template')
    digest = hashlib.sha256(src.read_bytes()).hexdigest()
    first = mod.stage(src, tmp_path / 'data', 'chat_template_mm.jinja', digest)
    assert mod.stage(src, tmp_path / 'data', 'chat_template_mm.jinja', digest) == first
    src.write_text('changed')
    with pytest.raises(RuntimeError, match='checksum'): mod.stage(src, tmp_path / 'data', 'chat_template_mm.jinja', digest)
    assert first.read_text() == 'template'


def test_patch_refuses_wrong_base_before_modification():
    spec = importlib.util.spec_from_file_location('glm_patch', ROOT / 'scripts/prepare-glm53-runtime.py')
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    manifest = json.loads((ROOT / 'cluster/runtime-overlays/glm53/patches.json').read_text())
    for patch in manifest['patches']:
        with pytest.raises(RuntimeError, match='base source checksum'):
            mod.build_patch(b'wrong image source', patch)
