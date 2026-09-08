import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

ROOT=Path(__file__).resolve().parents[1]

def module(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result)
    return result

fetch=module('model_fetch',ROOT/'scripts/fetch-pinned-spark-model.py')
sync=module('model_sync_for_fetch',ROOT/'scripts/sync-spark-models.py')


def fixture(tmp_path):
    manifest={'version':1,'repo':'test/model','revision':'a'*40,'image':'test/image@sha256:'+'b'*64,
              'hub_version':'1.23.0','files':{}}
    cache=tmp_path/'cache';base=cache/'hub/models--test--model'
    snapshot=base/'snapshots'/manifest['revision'];snapshot.mkdir(parents=True)
    (base/'blobs').mkdir()
    for name,body in [('config.json',b'{}'),('model.safetensors',b'weights-data')]:
        sha=hashlib.sha256(body).hexdigest()
        (base/'blobs'/sha).write_bytes(body)
        (snapshot/name).symlink_to('../../blobs/'+sha)
        manifest['files'][name]={'size':len(body),'sha256':sha}
    return manifest,cache,snapshot


def test_verified_download_lock_is_existing_peer_sync_contract(tmp_path):
    manifest,cache,snapshot=fixture(tmp_path)
    result=fetch.verify(cache,manifest)
    assert result==sync.capture(cache,['test/model@'+'a'*40])
    path=tmp_path/'copy-lock.json';fetch.save(path,result);fetch.save(path,result)
    with pytest.raises(ValueError,match='differs'): fetch.save(path,{'different':True})
    assert json.loads(path.read_text())==result


def test_changed_bytes_and_external_symlinks_fail_verification(tmp_path):
    manifest,cache,snapshot=fixture(tmp_path)
    target=(snapshot/'model.safetensors').resolve()
    target.write_bytes(b'weights-evil')
    with pytest.raises(ValueError,match='checksum'): fetch.verify(cache,manifest)
    outside=tmp_path/'outside';outside.write_bytes(b'weights-data')
    (snapshot/'model.safetensors').unlink();(snapshot/'model.safetensors').symlink_to(outside)
    with pytest.raises(ValueError,match='escapes'): fetch.verify(cache,manifest)


def test_unexpected_files_and_paths_fail(tmp_path):
    manifest,cache,snapshot=fixture(tmp_path)
    (snapshot/'injected.py').write_text('untracked')
    with pytest.raises(ValueError,match='inventory'): fetch.verify(cache,manifest)
    for path in ['../escape','/absolute','a/../escape','.']:
        bad={**manifest,'files':{**manifest['files'],path:{'size':0,'sha256':'a'*64}}}
        with pytest.raises(ValueError,match='path'): fetch.validate(bad)


def test_download_budget_refuses_before_library_or_network(tmp_path):
    manifest,cache,snapshot=fixture(tmp_path)
    with pytest.raises(ValueError,match='budget'): fetch.fetch(cache,manifest,1,2)
