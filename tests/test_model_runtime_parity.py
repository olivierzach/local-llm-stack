import importlib.util
import json
import os
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]


def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'scripts' / filename)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def test_peer_loaded_image_resolves_by_exact_identity(monkeypatch):
    resolver = module('image_resolver', 'resolve-spark-runtime-image.py')
    pins = {'QWEN38_IMAGE': 'repo@sha256:' + 'a'*64, 'QWEN38_IMAGE_ID': 'sha256:' + 'b'*64}
    seen = []
    def inspect(args, **kwargs):
        seen.append(args[-1])
        if args[-1] == pins['QWEN38_IMAGE']:
            return SimpleNamespace(returncode=1, stdout='', stderr='No such image')
        return SimpleNamespace(returncode=0, stderr='', stdout=json.dumps([
            {'Id': pins['QWEN38_IMAGE_ID'], 'Architecture': 'arm64'}]))
    monkeypatch.setattr(resolver.subprocess, 'run', inspect)
    assert resolver.resolve(pins) == pins['QWEN38_IMAGE_ID']
    assert seen == list(pins.values())


@pytest.mark.parametrize('image', [
    {'Id': 'sha256:'+'c'*64, 'Architecture': 'arm64'},
    {'Id': 'sha256:'+'b'*64, 'Architecture': 'amd64'},
])
def test_existing_registry_reference_with_wrong_identity_is_not_bypassed(monkeypatch, image):
    resolver = module('bad_image_resolver', 'resolve-spark-runtime-image.py')
    monkeypatch.setattr(resolver.subprocess, 'run', lambda *a, **kw:
                        SimpleNamespace(returncode=0, stderr='', stdout=json.dumps([image])))
    with pytest.raises(ValueError, match='identity or architecture'):
        resolver.resolve({'QWEN38_IMAGE': 'repo@sha256:'+'a'*64, 'QWEN38_IMAGE_ID': 'sha256:'+'b'*64})


def test_runtime_preparation_preserves_extra_edits(tmp_path):
    runtime = module('runtime_preparation', 'prepare-spark-model-runtimes.py')
    repo = tmp_path / 'source'
    repo.mkdir()
    subprocess.run(['git', 'init', '-q', str(repo)], check=True)
    original = repo / 'runtime.c'
    original.write_text('original\n')
    subprocess.run(['git', '-C', str(repo), 'add', '.'], check=True)
    subprocess.run(['git', '-C', str(repo), '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid',
                    'commit', '-qm', 'fixture'], check=True)
    revision = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', 'HEAD'], text=True).strip()
    original.write_text('patched\n')
    patch = tmp_path / 'patch.diff'
    patch.write_bytes(subprocess.check_output(['git', '-C', str(repo), 'diff']))
    original.write_text('original\n')
    args = (tmp_path, 'fixture', repo, 'unused', revision, patch, 'runtime.c')
    runtime.prepare(*args)
    assert original.read_text() == 'patched\n'
    runtime.prepare(*args)
    original.write_text('patched\nuser work\n')
    with pytest.raises(ValueError, match='extra edits'):
        runtime.prepare(*args)
    assert original.read_text() == 'patched\nuser work\n'
    with pytest.raises(ValueError, match='revision differs'):
        runtime.prepare(tmp_path, 'fixture', repo, 'unused', '0'*40, patch, 'runtime.c')


def test_unsupported_remote_drafting_fails_before_service_commands(tmp_path):
    binary = tmp_path / 'bin'
    binary.mkdir()
    marker = tmp_path / 'called'
    for name in ('docker', 'systemctl', 'systemd-run', 'nvidia-smi'):
        fake = binary / name
        fake.write_text('#!/bin/sh\ntouch "' + str(marker) + '"\nexit 99\n')
        fake.chmod(0o755)
    env = {**os.environ, 'DRAFT_MODE': 'remote', 'PATH': str(binary) + os.pathsep + os.environ['PATH']}
    for action in ('check-draft', 'start'):
        result = subprocess.run(['bash', str(ROOT / 'scripts/deepseek-v4.sh'), action],
                                env=env, text=True, capture_output=True)
        assert result.returncode and 'no remote-drafter transport' in result.stderr
    assert not marker.exists()


def test_copy_receiver_detects_corruption_and_rejects_symlink_targets(tmp_path):
    parity = module('parity_copy', 'sync-spark-model-parity.py')
    p = tmp_path / 'models/deepseek-v4/test.gguf'
    p.parent.mkdir(parents=True)
    p.write_bytes(b'model')
    lock = {str(p.relative_to(tmp_path)): {'size': 5, 'sha256': parity.digest(p)}}
    def verify():
        return subprocess.run(['python3', '-c', parity.GGUF_RECEIVER], text=True, capture_output=True,
                              input=json.dumps({'root': str(tmp_path), 'files': lock, 'action': 'verify'}))
    assert verify().returncode == 0
    p.write_bytes(b'other')
    assert verify().returncode != 0
    p.unlink()
    other = tmp_path / 'other'
    other.write_bytes(b'model')
    p.symlink_to(other)
    assert verify().returncode != 0


def test_mistral_native_tokenizer_is_accepted_but_missing_tokenizer_is_not(tmp_path):
    audit = module('stack_artifacts', 'audit-stack-models.py')
    (tmp_path / 'config.json').write_text(json.dumps({'architectures': ['Mistral3ForConditionalGeneration']}))
    (tmp_path / 'model.safetensors').write_bytes(b'weights')
    with pytest.raises(ValueError, match='tekken'):
        audit.snapshot_check(tmp_path)
    (tmp_path / 'tekken.json').write_text('{}')
    assert audit.snapshot_check(tmp_path)['weight_shards'] == 1
