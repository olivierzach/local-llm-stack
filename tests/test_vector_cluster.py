"""Real bundle/cache transports and admission failures without a GPU or SSH."""
import hashlib
import importlib.machinery
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tarfile

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import bundles, node


def script(filename):
    loader = importlib.machinery.SourceFileLoader(filename, str(ROOT / 'scripts' / filename))
    module = importlib.util.module_from_spec(importlib.util.spec_from_loader(filename, loader))
    loader.exec_module(module)
    return module


models = script('sync-spark-models.py')
vector = script('spark-vector')


@pytest.fixture
def local(tmp_path):
    return {'hostname':socket.gethostname(),'projects':str(tmp_path / 'projects'),'ssh':'must-not-use-ssh'}


def test_bundle_round_trip_is_immutable(tmp_path, local):
    source = tmp_path / 'source'
    (source / 'nested').mkdir(parents=True)
    (source / 'nested/file').write_bytes(b'original input')
    receipt = bundles.publish(source, local)
    assert bundles.publish(source, local) == receipt
    destination = Path(receipt['path'])
    assert (destination / 'nested/file').read_bytes() == b'original input'
    (destination / 'nested/file').write_bytes(b'changed input')
    with pytest.raises(RuntimeError, match='bundle'): bundles.publish(source, local)
    assert (destination / 'nested/file').read_bytes() == b'changed input'


@pytest.mark.parametrize('alteration', ['extra', 'symlink', 'manifest'])
def test_existing_bundle_rejects_untracked_code(tmp_path, local, alteration):
    source = tmp_path / 'source'
    source.mkdir()
    (source / 'input').write_text('input')
    destination = Path(bundles.publish(source, local)['path'])
    if alteration == 'extra': (destination / 'injected.py').write_text('raise RuntimeError()')
    if alteration == 'symlink': (destination / 'link').symlink_to(source)
    if alteration == 'manifest': (destination / '.spark-bundle.json').write_text('{}')
    with pytest.raises(RuntimeError, match='bundle'): bundles.publish(source, local)


@pytest.mark.parametrize('filename,kind', [('../escape','file'),('/absolute','file'),('link','symlink')])
def test_archive_rejects_path_escape_and_links(tmp_path, filename, kind):
    archive = io.BytesIO()
    with tarfile.open(fileobj=archive,mode='w') as tar:
        member = tarfile.TarInfo(filename)
        if kind == 'symlink':
            member.type = tarfile.SYMTYPE
            member.linkname = str(tmp_path / 'outside')
            tar.addfile(member)
        else:
            member.size = 1
            tar.addfile(member,io.BytesIO(b'x'))
    result = subprocess.run([sys.executable,'-c',bundles.RECEIVER,str(tmp_path / 'received'),'a'*64],
                            input=archive.getvalue(),capture_output=True)
    assert result.returncode != 0
    assert b'unsafe archive member' in result.stderr
    assert not (tmp_path / 'escape').exists()
    assert list((tmp_path / 'received').iterdir()) == []


@pytest.fixture
def cache(tmp_path):
    root = tmp_path / 'cache'
    base = root / 'hub/models--test--model'
    snapshot = base / 'snapshots' / ('a'*40)
    snapshot.mkdir(parents=True)
    (base / 'blobs').mkdir()
    (base / 'blobs/config').write_text('{}')
    (snapshot / 'config.json').symlink_to('../../blobs/config')
    (root / 'token').write_text('must never be copied')
    return root


def receive(cache, lock, action):
    return subprocess.run([sys.executable,'-c',models.RECEIVER],input=json.dumps(
        {'cache':str(cache),'lock':lock,'action':action}),text=True,capture_output=True)


def test_model_lock_preserves_symlinks_and_excludes_credentials(cache):
    lock = models.capture(cache,['test/model@'+'a'*40])
    assert list(lock['files']) == ['hub/models--test--model/blobs/config']
    assert list(lock['symlinks'].values()) == ['../../blobs/config']
    assert receive(cache,lock,'verify').returncode == 0
    (cache / 'hub/models--test--model/blobs/config').write_text('corrupt')
    assert receive(cache,lock,'verify').returncode != 0


def test_model_lock_rejects_ambiguous_main(cache):
    with pytest.raises(ValueError,match='one revision'):
        models.capture(cache,['test/model@'+'a'*40]*2)
    lock = models.capture(cache,['test/model@'+'a'*40])
    refs = cache / 'hub/models--test--model/refs'
    refs.mkdir()
    (refs / 'main').write_text('b'*40)
    result = receive(cache,lock,'prepare')
    assert result.returncode != 0 and 'reference differs' in result.stderr


def test_model_copy_rejects_destination_parent_symlink(cache, tmp_path):
    lock = models.capture(cache,['test/model@'+'a'*40])
    target = tmp_path / 'target'
    target.mkdir()
    (target / 'hub').symlink_to(cache / 'hub')
    result = receive(target,lock,'prepare')
    assert result.returncode != 0 and 'parent is a symlink' in result.stderr


@pytest.mark.skipif(os.geteuid() == 0, reason='permission refusal requires an unprivileged test user')
def test_model_copy_preflight_rejects_unwritable_new_model_parent(cache, tmp_path):
    lock = models.capture(cache, ['test/model@'+'a'*40])
    target = tmp_path/'target'
    hub = target/'hub'
    hub.mkdir(parents=True)
    hub.chmod(0o555)
    try:
        result = receive(target, lock, 'prepare')
        assert result.returncode != 0 and 'destination cache parent is not writable' in result.stderr
        assert not (hub/'models--test--model').exists()
    finally:
        hub.chmod(0o755)


@pytest.mark.skipif(sys.platform != 'linux', reason='Spark transport requires Linux rsync with protected arguments')
def test_model_copy_preserves_shared_parent_permissions(cache, tmp_path):
    lock = models.capture(cache, ['test/model@'+'a'*40])
    target = tmp_path/'target'
    hub = target/'hub'
    (hub/'models--test--model').mkdir(parents=True)
    hub.chmod(0o555)
    (cache/'hub').chmod(0o777)
    try:
        assert receive(target, lock, 'prepare').returncode == 0
        listing = tmp_path/'copy-files'
        listing.write_bytes(b'\0'.join(p.encode() for p in sorted(set(lock['files']) | set(lock['symlinks'])))+b'\0')
        subprocess.run(['rsync', *models.RSYNC_FLAGS, '--files-from='+str(listing),
                        str(cache)+'/', str(target)+'/'], check=True, capture_output=True)
        assert hub.stat().st_mode & 0o777 == 0o555
        assert receive(target, lock, 'verify').returncode == 0
        assert not (target/'token').exists()
        assert not list(target.rglob('.spark-copy-check-*'))
    finally:
        hub.chmod(0o755)


def test_model_lock_rejects_external_snapshot_link(cache, tmp_path):
    outside = tmp_path / 'outside'
    outside.write_text('unrelated content')
    path = cache / 'hub/models--test--model/snapshots' / ('a'*40) / 'other'
    path.symlink_to(outside)
    with pytest.raises(ValueError,match='escapes'): models.capture(cache,['test/model@'+'a'*40])


def test_collect_on_worker_does_not_need_self_ssh(tmp_path, local, monkeypatch):
    output = tmp_path / 'output'
    output.mkdir()
    (output / 'vectors.npz').write_bytes(b'artifact')
    report = {'artifact_sha256':hashlib.sha256(b'artifact').hexdigest()}
    (output / 'acceptance.json').write_text(json.dumps(report))
    monkeypatch.setattr(vector.subprocess,'run',lambda *a,**k:pytest.fail('self SSH attempted'))
    vector.collect(local,{'artifact_dir':str(output),'acceptance':report},tmp_path / 'collected')
    assert (tmp_path / 'collected/vectors.npz').read_bytes() == b'artifact'
    (output / 'vectors.npz').write_bytes(b'changed')
    with pytest.raises(ValueError,match='checksum'):
        vector.collect(local,{'artifact_dir':str(output),'acceptance':report},tmp_path / 'collected')


@pytest.fixture
def batch(tmp_path, monkeypatch):
    monkeypatch.setattr(node,'STATE',tmp_path)
    monkeypatch.setattr(node,'run',lambda *a,**k:json.dumps([{'Architecture':'arm64'}]))
    monkeypatch.setattr(node,'doctor',lambda n:{'gpu_processes':[],'gpu_containers':[],
        'research_window':None,'memory_mib':{'MemAvailable':65536}})
    return {'owner':'vector-test','digest':'a'*64,'node':{'architecture':'aarch64'},
        'compose':{'services':{'worker':{'image':'repo/image@sha256:'+'b'*64}}}}


def test_batch_reservation_recovery_does_not_restart(batch):
    assert node.reserve_batch(batch)['existing'] is False
    saved = node.reservation()
    assert saved['kind'] == 'batch' and saved['phase'] == 'reserved'
    saved.update(phase='started',container_ids=['original'])
    node.atomic(node.STATE / 'gpu.json',saved)
    assert node.reserve_batch(batch)['existing'] is True
    assert node.reservation() == saved
    with pytest.raises(RuntimeError,match='ownership'):
        node.reserve_batch({**batch,'owner':'another'})
    assert node.reservation() == saved


@pytest.mark.parametrize('busy', ['gpu', 'research'])
def test_batch_preserves_existing_work(batch, monkeypatch, busy):
    monkeypatch.setattr(node,'doctor',lambda n:{'gpu_processes':['123'] if busy=='gpu' else [],
        'gpu_containers':[],'research_window':'entered' if busy=='research' else None})
    with pytest.raises(RuntimeError): node.reserve_batch(batch)
    assert node.reservation() is None


def test_batch_rejects_mutable_image_before_reserving(batch):
    batch['compose']['services']['worker']['image'] = 'repo/image:latest'
    with pytest.raises(RuntimeError,match='immutable'): node.reserve_batch(batch)
    assert node.reservation() is None


def test_wait_rejects_replaced_job_even_if_exit_successful():
    plan = {'owner':'vector-test','digest':'a'*64}
    report = {'reservation':{**plan,'container_ids':['original']},
              'containers':[{'id':'replacement','state':'exited','exit_code':0}]}
    with pytest.raises(ValueError,match='replaced'): vector.checked_container(report,plan)
    report['containers'][0]['id'] = 'original'
    assert vector.checked_container(report,plan)['exit_code'] == 0


def test_saved_batch_plan_checks_labels_without_rerendering():
    p = {'node':{},'bundle':{},'spec':{'image':'pinned'},'model_cache':'/cache','job_id':'test','user':'1000:1000'}
    digest = hashlib.sha256(vector.canonical(p).encode()).hexdigest()
    p.update(digest=digest,owner='vector-test-'+digest[:12])
    worker = {'image':'pinned','container_name':'spark-'+p['owner'],
              'labels':{'io.spark.owner':p['owner'],'io.spark.digest':digest}}
    p['compose'] = {'services':{'worker':worker}}
    vector.validate_saved(p,{})
    worker['labels']['io.spark.owner'] = 'another-job'
    with pytest.raises(ValueError,match='ownership'): vector.validate_saved(p,{})
