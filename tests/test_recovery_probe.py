import copy
import importlib.util
from pathlib import Path
import sys

import pytest

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
from spark_cluster import cli, config, recovery_node
spec=importlib.util.spec_from_file_location('recovery_probe',ROOT/'scripts/probe-spark-recovery.py')
probe=importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


def test_fault_refuses_foreign_replaced_or_nonacceptance_workers():
    request={'owner':'accept-recovery-check-123','digest':'a'*64,'container_id':'b'*64}
    saved={**{k:request[k] for k in ('owner','digest')},'phase':'started','container_ids':['b'*64]}
    container={'Id':'b'*64,'Config':{'Labels':{'io.spark.owner':request['owner'],'io.spark.digest':'a'*64}},'State':{'Status':'running'}}
    assert recovery_node.checked_id(request,saved,container)=='b'*64
    for target in ('namespace','lease','label','id','phase','state','short-id'):
        r,s,c=copy.deepcopy((request,saved,container))
        if target=='namespace':r['owner']='production'
        if target=='lease':s['owner']='another-owner'
        if target=='label':c['Config']['Labels']['io.spark.digest']='c'*64
        if target=='id':c['Id']='c'*64
        if target=='phase':s['phase']='creating'
        if target=='state':c['State']['Status']='exited'
        if target=='short-id':r['container_id']='b'*12
        with pytest.raises(RuntimeError):recovery_node.checked_id(r,s,c)


@pytest.fixture
def fake(tmp_path,monkeypatch):
    inv,recipe,deployment=config.load(ROOT,ROOT/'cluster/inventory.json',ROOT/'cluster/deployments/fast-e8f1.json')
    p=config.plan(inv,recipe,{**deployment,'name':'accept-recovery-test'})
    state={'active':False,'crashed':False,'starts':0,'stops':0,'faults':0,'blocked':False,'fail_fault':False,'fail_cleanup':False}
    def up(plan,timeout,output):
        state.update(active=True,crashed=False,starts=state['starts']+1)
        cli.save_json(output/'acceptance.json',{'ready':True,'reply':'ready'})
    def inspect(plan):
        identity=('a' if state['starts']==1 else 'b')*64
        running=state['active'] and not state['crashed']
        return {'healthy':running,'nodes':{'e8f1':{
            'reservation':{'owner':p['owner'],'digest':p['digest'],'container_ids':[identity]} if state['active'] else None,
            'containers':[{'id':identity,'state':'running' if running else 'exited','exit_code':0 if running else 137}] if state['active'] else []}}}
    def call(plan,node,action):
        if action=='preflight':return {'launchable':not state['blocked']}
        assert action=='stop'
        state['stops']+=1
        if state['fail_cleanup']:raise RuntimeError('ownership ambiguous')
        state['active']=False
        return {'released':True}
    def remote(*args):
        state['faults']+=1
        if state['fail_fault']:raise RuntimeError('fault transport failed')
        state['crashed']=True
        return {'reservation_retained':True}
    monkeypatch.setattr(cli,'up',up)
    monkeypatch.setattr(cli,'inspect',inspect)
    monkeypatch.setattr(cli,'call',call)
    monkeypatch.setattr(cli,'remote',remote)
    return p,state,tmp_path/'result'


def test_real_completion_required_before_fault_and_after_restart(fake):
    p,state,output=fake
    result=probe.run_probe(p,output,30,'e8f1')
    assert result['passed'] and result['cleanup_verified']
    assert state['starts']==2 and state['stops']==2 and state['faults']==1
    assert not state['active']
    assert [c['check'] for c in result['checks']]==['preflight','initial-completion','worker-killed','failed-state-retains-lease','failed-worker-cleanup','restart-completion']
    with pytest.raises(FileExistsError):probe.run_probe(p,output,30,'e8f1')
    assert state['starts']==2


def test_busy_preflight_does_not_start_or_clean_another_workload(fake):
    p,state,output=fake
    state['blocked']=True
    result=probe.run_probe(p,output,30,'e8f1')
    assert not result['passed'] and 'preflight blocked' in result['error']
    assert state['starts']==state['stops']==state['faults']==0


@pytest.mark.parametrize('failure',['fail_fault','fail_cleanup'])
def test_fault_or_cleanup_failure_never_reports_success(fake,failure):
    p,state,output=fake
    state[failure]=True
    result=probe.run_probe(p,output,30,'e8f1')
    assert not result['passed'] and result['error']
    assert state['starts']==1
    if failure=='fail_fault':assert result['cleanup_verified'] and not state['active']
    else:assert state['active'] and 'recover with saved plan' in result['error']
