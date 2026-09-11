import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys
import zipfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import cli, config, node

spec = importlib.util.spec_from_file_location('install_nccl', ROOT / 'scripts/install-spark-nccl.py')
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


@pytest.fixture
def package(tmp_path):
    wheel = tmp_path / 'fixture.whl'
    payload = b'test library'
    with zipfile.ZipFile(wheel, 'w') as archive:
        archive.writestr('nvidia/nccl/lib/libnccl.so.2', payload)
    manifest = dict(version='2.30.7', member='nvidia/nccl/lib/libnccl.so.2',
                    sha256=hashlib.sha256(payload).hexdigest(), wheel_sha256=installer.digest(wheel))
    return manifest, tmp_path / 'data/huggingface', wheel


def test_verified_install_is_idempotent_and_launch_checks_tampering(package):
    manifest, cache, wheel = package
    path = installer.install(manifest, cache, wheel)
    assert installer.install(manifest, cache, wheel) == path
    req = {'node': {'cache': str(cache)}, 'recipe': {'nccl_library': {k: manifest[k] for k in ('version', 'sha256')}}}
    assert node.verify_nccl_library(req)['override']
    path.chmod(0o644)
    path.write_bytes(b'changed')
    with pytest.raises(RuntimeError, match='SHA-256 mismatch'):
        node.verify_nccl_library(req)
    with pytest.raises(RuntimeError, match='refusing to overwrite'):
        installer.install(manifest, cache, wheel)
    assert path.read_bytes() == b'changed'


@pytest.mark.parametrize('field', ['sha256', 'wheel_sha256'])
def test_wrong_hash_never_publishes_library(package, field):
    manifest, cache, wheel = package
    manifest[field] = '0' * 64
    with pytest.raises(RuntimeError, match='SHA-256 mismatch'):
        installer.install(manifest, cache, wheel)
    assert not list(cache.parent.rglob('libnccl.so.2'))


@pytest.mark.parametrize('coordinator', ['66f1', 'e8f1'])
def test_both_workers_override_the_actual_shared_library(coordinator):
    plan = config.plan(*config.load(ROOT, ROOT / 'cluster/inventory.json',
                                   ROOT / f'cluster/deployments/large-tp2-mtp2-nccl2307-{coordinator}.json'))
    manifest = json.loads((ROOT / 'cluster/runtime-libraries/nccl-2.30.7-aarch64.json').read_text())
    for n in plan['nodes']:
        worker = plan['compose'][n]['services']['worker']
        target = '/usr/local/lib/python3.12/dist-packages/nvidia/nccl/lib/libnccl.so.2'
        mount = next(v for v in worker['volumes'] if v['target'] == target)
        assert mount['read_only'] and manifest['sha256'] in mount['source']
        assert worker['environment']['VLLM_NCCL_SO_PATH'] == target
        assert 'LD_PRELOAD' not in worker['environment']
        assert worker['environment'].get('VLLM_DISABLE_PYNCCL', '0') == '0'


def test_start_checks_library_before_docker_mutations(monkeypatch):
    monkeypatch.setattr(node, 'owned', lambda request: {'phase': 'reserved'})
    req = {'node': {'cache': '/nonexistent/huggingface'},
           'recipe': {'nccl_library': {'version': '2.30.7', 'sha256': '0' * 64}}}
    with pytest.raises(RuntimeError, match='library missing'):
        node.start(req)


@pytest.mark.parametrize('library', [dict(version=23007, sha256='0'*64),
                                    dict(version='2.30.7', sha256='../bad'),
                                    dict(version='2.30.7', sha256='0'*64, path='/arbitrary')])
def test_bad_library_configuration_rejected(library):
    recipe = config.read(ROOT / 'cluster/recipes/qwen3-next-80b-256k-mtp2-nccl2307.json')
    recipe['nccl_library'] = library
    with pytest.raises(config.ConfigError):
        config.validate_recipe(recipe)
