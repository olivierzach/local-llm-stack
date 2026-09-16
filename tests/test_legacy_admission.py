"""Legacy operations must not bypass research, ownership or crash recovery."""
import json
import os
from pathlib import Path
import platform
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import legacy, node


@pytest.fixture
def root(tmp_path, monkeypatch):
    monkeypatch.setattr(node, 'STATE', tmp_path / 'state')
    project = tmp_path / 'project'
    project.mkdir()
    return project


def saved(root, **kwargs):
    value = {**legacy.identity(root), 'kind': 'legacy-make', 'phase': 'workload',
             'root': str(root), 'transaction': 'owned-nonce', 'before': [], **kwargs}
    node.atomic(node.STATE / 'gpu.json', value)
    return value


def test_foreign_lease_blocks_before_inspecting_or_starting(root, monkeypatch):
    node.atomic(node.STATE / 'gpu.json', {'owner': 'other', 'digest': 'f' * 64})
    monkeypatch.setattr(legacy, 'inspect_stack', lambda _: pytest.fail('must not reach stack inspection'))
    with pytest.raises(RuntimeError, match='ownership mismatch'):
        legacy.check(root, 'deepseekv4-up')
    assert node.reservation()['owner'] == 'other'


@pytest.mark.parametrize('window', ['entered', 'unresolved', 'waiting'])
def test_research_blocks_before_container_inspection(root, monkeypatch, window):
    monkeypatch.setattr(node, 'research_window', lambda: window)
    monkeypatch.setattr(node, 'containers', lambda: pytest.fail('must stop at research admission'))
    with pytest.raises(RuntimeError, match='research window'):
        legacy.check(root, 'qwen38-up')
    assert node.reservation() is None


def test_unknown_gpu_process_blocks_known_compose_worker(root, monkeypatch):
    monkeypatch.setattr(node, 'research_window', lambda: None)
    worker = {'Id': 'owned', 'Name': '/owned', 'State': {'Status': 'running'},
              'HostConfig': {'DeviceRequests': [{}]}, 'Config': {'Labels': {
                  'com.docker.compose.project.working_dir': str(root),
                  'com.docker.compose.service': 'vllm-fast'}}}
    monkeypatch.setattr(node, 'containers', lambda: [worker])
    monkeypatch.setattr(legacy, 'native_unit', lambda _: None)
    monkeypatch.setattr(node, 'run', lambda args, **kw: 'PID\n11' if args[0] == 'docker' else '11\n99')
    with pytest.raises(RuntimeError, match='GPU process belongs'):
        legacy.inspect_stack(root)


def test_unlabelled_native_container_is_not_claimed(root, monkeypatch):
    monkeypatch.setattr(node, 'research_window', lambda: None)
    worker = {'Id': 'foreign', 'Name': '/local-qwen38-flash-next', 'State': {'Status': 'running'},
              'HostConfig': {'DeviceRequests': [{}]}, 'Config': {'Labels': {}}}
    monkeypatch.setattr(node, 'containers', lambda: [worker])
    with pytest.raises(RuntimeError, match='GPU container belongs'):
        legacy.inspect_stack(root)


def test_explicit_native_switch_allowed_but_oversubscription_refused(root):
    resident = [{'service': 'native-deepseek', 'pid': 11}]
    legacy.admit(root, 'deepseekv4-up', resident)
    legacy.admit(root, 'qwen38-up', resident)
    with pytest.raises(RuntimeError, match='another stack model'):
        legacy.admit(root, 'large-up', resident)
    with pytest.raises(RuntimeError, match='training is active'):
        legacy.admit(root, 'qwen38-up', [{'service': 'training'}])


def test_invalid_draft_mode_changes_no_reservation(root, monkeypatch):
    monkeypatch.setattr(legacy, 'inspect_stack', lambda _: [])
    monkeypatch.setenv('DRAFT_MODE', 'remote')
    with pytest.raises(RuntimeError, match='DRAFT_MODE'):
        legacy.execute(root, 'deepseekv4-up')
    assert node.reservation() is None


def test_active_make_cannot_be_recovered(root, monkeypatch):
    saved(root, child={'pid': 11, 'start': 'old'})
    monkeypatch.setattr(legacy, 'process_alive', lambda record: bool(record))
    monkeypatch.setattr(legacy, 'inspect_stack', lambda _: pytest.fail('must not touch active operation'))
    with pytest.raises(RuntimeError, match='still running'):
        legacy.recover(root)
    assert node.reservation() is not None


@pytest.mark.parametrize("nonce", ["other", "owned-nonce"])
def test_recovery_refuses_replacement_worker(root, monkeypatch, nonce):
    saved(root, after=[{'service': 'vllm-fast', 'id': 'original', 'transaction': 'owned-nonce'}])
    monkeypatch.setattr(legacy, 'inspect_stack', lambda _: [{'service': 'vllm-fast', 'id': 'replacement', 'transaction': nonce}])
    monkeypatch.setattr(node, 'run', lambda *a, **k: pytest.fail('must not stop replacement'))
    with pytest.raises(RuntimeError, match='identities changed'):
        legacy.recover(root)
    assert node.reservation() is not None


def test_crash_recovery_recognizes_label_before_parent_recorded_id(root, monkeypatch):
    saved(root)
    current = [{'service': 'vllm-fast', 'id': 'exact-created-id', 'transaction': 'owned-nonce'}]
    monkeypatch.setattr(legacy, 'inspect_stack', lambda _: list(current))
    calls = []
    def run(args, **kwargs):
        calls.append(args)
        if args[:2] == ['docker', 'rm']:
            current.clear()
        return ''
    monkeypatch.setattr(node, 'run', run)
    assert legacy.recover(root)['released']
    assert calls == [['docker', 'stop', '--time', '30', 'exact-created-id'], ['docker', 'rm', 'exact-created-id']]
    assert node.reservation() is None


@pytest.mark.skipif(platform.system() != 'Linux', reason='real process ancestry uses Linux /proc')
def test_child_is_recorded_before_it_can_run_and_nonce_is_reused(root, monkeypatch):
    monkeypatch.setattr(legacy, 'inspect_stack', lambda _: [{'service': 'vllm-fast', 'id': 'same', 'transaction': 'stable'}])
    fake = root / 'make-test'
    fake.write_text('#!' + sys.executable + '\n' +
        'import os,sys,json\nfrom pathlib import Path\n' +
        'sys.path.insert(0,' + repr(str(ROOT / 'tools')) + ')\nfrom spark_cluster import legacy,node\n' +
        'node.STATE=Path(' + repr(str(node.STATE)) + ')\n' +
        'assert node.reservation()["child"]["pid"]==os.getpid()\n' +
        'assert os.environ["SPARK_LEGACY_TRANSACTION"]=="stable"\n' +
        'assert legacy.verify(Path(' + repr(str(root)) + '))["admitted"]\n')
    fake.chmod(0o700)
    assert legacy.execute(root, 'up', str(fake)) == 0
    assert node.reservation() is None


def test_internal_target_has_no_unadmitted_bypass(root):
    with pytest.raises(RuntimeError, match='ownership'):
        legacy.verify(root)


@pytest.mark.parametrize('target,key,value', [
    ('up', 'COMPOSE_PROFILES', 'large'),
    ('deepseekv4-up', 'DEEPSEEKV4_DSPARK_ENABLED', 'invalid'),
])
def test_invalid_launch_settings_are_rejected_in_check_and_run(root, monkeypatch, target, key, value):
    monkeypatch.setattr(legacy, 'inspect_stack', lambda _: [])
    monkeypatch.setenv(key, value)
    for operation in (legacy.check, legacy.execute):
        with pytest.raises(RuntimeError):
            operation(root, target)
        assert node.reservation() is None


def test_make_dry_run_never_enters_admission_or_changes_files(tmp_path):
    import subprocess
    result = subprocess.run(['make', '-n', '-f', str(ROOT / 'Makefile'), 'deepseekv4-up', 'DRAFT_MODE=off'],
                            cwd=tmp_path, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert 'spark-legacy-run.py run' in result.stdout
    assert not list(tmp_path.iterdir())


@pytest.mark.skipif(platform.system() != 'Linux', reason='real process ancestry uses Linux /proc')
@pytest.mark.parametrize('changed', [False, True])
def test_failed_make_retains_only_changed_gpu_transaction(root, monkeypatch, changed):
    calls = 0
    def inspect(_):
        nonlocal calls
        calls += 1
        return [{'service': 'vllm-fast', 'id': 'new'}] if changed and calls > 1 else []
    monkeypatch.setattr(legacy, 'inspect_stack', inspect)
    fake = root / 'fail-make'
    fake.write_text('#!/bin/sh\nexit 7\n')
    fake.chmod(0o700)
    assert legacy.execute(root, 'up', str(fake)) == 7
    if changed:
        record = node.reservation()
        assert record['failed'] and record['exit_code'] == 7
        assert record['after'] == [{'service': 'vllm-fast', 'id': 'new'}]
    else:
        assert node.reservation() is None
