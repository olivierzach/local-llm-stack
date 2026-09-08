"""Observe durable jobs and collect only their isolated, terminal artifacts."""
import hashlib
import importlib.machinery
import importlib.util
import json
from pathlib import Path
import socket
import sys
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT / 'tools'))
from spark_cluster import loop_node

loader = importlib.machinery.SourceFileLoader('spark_loop',str(ROOT / 'scripts/spark-loop'))
loop = importlib.util.module_from_spec(importlib.util.spec_from_loader('spark_loop',loader))
loader.exec_module(loop)


def test_wait_timeout_retains_job_and_never_restarts(tmp_path, monkeypatch):
    actions = []
    def remote(n,p,h):
        actions.append(p['action'])
        return {'status':'submitted','observed_state':'active'}
    monkeypatch.setattr(loop,'remote',remote)
    ticks = iter([0,1,6])
    monkeypatch.setattr(loop.time,'monotonic',lambda:next(ticks))
    monkeypatch.setattr(loop.time,'sleep',lambda n:None)
    with pytest.raises(RuntimeError,match='retained'): loop.wait_job({}, {},tmp_path,5)
    assert actions == ['status','status']
    assert json.loads((tmp_path / 'status.json').read_text())['observed_state']=='active'


@pytest.mark.parametrize('result', [{'status':'failed'}, {'status':'succeeded','restoration_error':'cleanup mismatch'}])
def test_wait_does_not_call_failed_recovery_success(tmp_path, monkeypatch, result):
    monkeypatch.setattr(loop,'remote',lambda *a:result)
    with pytest.raises(ValueError): loop.wait_job({}, {},tmp_path,5)
    assert json.loads((tmp_path / 'status.json').read_text())==result


def test_wait_observes_finalization_before_returning_success(tmp_path,monkeypatch):
    pending={'status':'succeeded'}
    finished={'status':'succeeded','restoration':{'status':'released'}}
    results=iter([pending,finished])
    monkeypatch.setattr(loop,'remote',lambda *a:next(results))
    monkeypatch.setattr(loop.time,'sleep',lambda n:None)
    assert loop.wait_job({}, {},tmp_path,5)==finished


@pytest.fixture
def job_request(tmp_path):
    directory = tmp_path / 'looped-llm-lab/runs/cluster/job-a'
    directory.mkdir(parents=True)
    (directory / 'result.json').write_text('{"done":true}')
    return {'node':{'projects':str(tmp_path),'hostname':socket.gethostname(),'ssh':'unused'},
        'snapshot':{'identity':'a'*64}, 'config':{'job_id':'job-a','container':{'runs_dir':str(directory)}},
        'artifacts':['result.json']}


def test_terminal_artifact_checksum_and_self_collection(job_request,tmp_path,monkeypatch):
    receipt = loop_node.artifacts(job_request,{'status':'succeeded'})
    assert receipt['files']['result.json']['sha256']==hashlib.sha256(b'{"done":true}').hexdigest()
    monkeypatch.setattr(loop,'remote',lambda *a:receipt)
    monkeypatch.setattr(loop.subprocess,'run',lambda *a,**k:pytest.fail('unexpected self SSH'))
    loop.fetch_artifacts(job_request['node'],job_request,tmp_path / 'collected',['result.json'])
    assert (tmp_path / 'collected/artifacts/result.json').read_text()=='{"done":true}'


@pytest.mark.parametrize('case',['running','traversal','foreign','symlink'])
def test_artifact_collection_refuses_unowned_or_mutating_data(job_request,tmp_path,case):
    state = {'status':'succeeded'}
    if case == 'running': state['status']='submitted'
    if case == 'traversal': job_request['artifacts']=['../job-b/result.json']
    if case == 'foreign': job_request['config']['container']['runs_dir']=str(tmp_path)
    if case == 'symlink':
        outside=tmp_path / 'outside'
        outside.write_text('private unrelated file')
        directory=Path(job_request['config']['container']['runs_dir'])
        (directory / 'link').symlink_to(outside)
        job_request['artifacts']=['link']
    with pytest.raises(RuntimeError): loop_node.artifacts(job_request,state)


def test_local_staging_keeps_project_receiver_without_ssh(tmp_path,monkeypatch):
    def snapshot(source,artifacts,target,max_bytes):
        (target/'snapshot.json').write_text('{}')
        return {'identity':'a'*64,'files':[]}
    module=SimpleNamespace(_snapshot=snapshot,_RECEIVER="import json,sys; print(json.dumps({'identity':sys.argv[2],'verified':True,'path':sys.argv[1]+'/'+sys.argv[2]}))")
    n={'hostname':socket.gethostname(),'projects':str(tmp_path),'ssh':'must-not-use'}
    result=loop.stage_snapshot(module,tmp_path,n)
    assert result['verified'] and result['files']==0
