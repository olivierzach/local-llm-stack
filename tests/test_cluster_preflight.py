import copy
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'tools'))
from spark_cluster import cli, config, node


@pytest.fixture
def setup(tmp_path, monkeypatch):
    p = config.plan(*config.load(ROOT, ROOT/'cluster/inventory.json', ROOT/'cluster/deployments/fast-e8f1.json'))
    req = cli.request(p, 'e8f1', 'preflight')
    req['node']['cache'] = str(tmp_path/'cache')
    snapshot = Path(req['node']['cache'])/'hub'/('models--'+req['recipe']['model'].replace('/', '--'))/'snapshots'/req['recipe']['revision']
    snapshot.mkdir(parents=True)
    (snapshot/'config.json').write_text('{}')
    (snapshot/'model.safetensors').write_bytes(b'fixture')
    report = {'reservation':None, 'research_window':'released', 'gpu_processes':[], 'gpu_containers':[],
              'memory_mib':{'MemAvailable':120000}, 'fabric':[]}
    monkeypatch.setattr(node, 'STATE', tmp_path/'state')
    monkeypatch.setattr(node, 'verify_host', lambda n:None)
    monkeypatch.setattr(node, 'doctor', lambda n:copy.deepcopy(report))
    monkeypatch.setattr(node, 'run', lambda args:json.dumps([{'Architecture':'arm64'}]))
    monkeypatch.setattr(node, 'check_port', lambda *args:None)
    def forbidden(*args, **kwargs):raise AssertionError('preflight must not write or reserve')
    monkeypatch.setattr(node, 'atomic', forbidden)
    monkeypatch.setattr(node, 'locked', forbidden)
    monkeypatch.setattr(node, 'reserve', forbidden)
    return req, report, snapshot


def test_preflight_does_not_create_state_or_reserve_gpu(setup):
    req, _, _ = setup
    result = node.main(req)
    assert result['launchable']
    assert not node.STATE.exists()
    assert next(c for c in result['checks'] if c['check']=='model-cache')['details']['weight_shards']==1


def test_preflight_reports_all_blockers_and_still_checks_cache(setup, monkeypatch):
    req, report, snapshot = setup
    report.update(reservation={'owner':'research'}, research_window='entered', gpu_processes=['123'])
    report['memory_mib']['MemAvailable']=1
    (snapshot/'model.safetensors').unlink()
    def occupied(*args):raise OSError('port occupied')
    monkeypatch.setattr(node, 'check_port', occupied)
    result = node.main(req)
    failed = {c['check'] for c in result['checks'] if not c['passed']}
    assert not result['launchable']
    assert failed == {'reservation','research-window','gpu-idle','shared-memory','model-cache','api-port'}
    assert not node.STATE.exists()


def test_inspection_failure_does_not_hide_independent_cache_error(setup, monkeypatch):
    req, _, snapshot = setup
    def unavailable(*args):raise RuntimeError('docker unavailable')
    monkeypatch.setattr(node, 'doctor', unavailable)
    (snapshot/'config.json').unlink()
    result = node.main(req)
    assert not result['launchable']
    assert {'host-inspection','model-cache'} <= {c['check'] for c in result['checks'] if not c['passed']}


def test_cli_reports_both_nodes_when_one_is_unreachable(tmp_path, monkeypatch):
    seen=[]
    def call(plan, node_id, action):
        seen.append((node_id, action))
        if node_id=='66f1':raise RuntimeError('unreachable')
        return {'launchable':True,'checks':[]}
    monkeypatch.setattr(cli, 'call', call)
    code = cli.main(['preflight','--deployment',str(ROOT/'cluster/deployments/large-tp2-66f1.json'),'--output',str(tmp_path)])
    result=json.loads((tmp_path/'preflight.json').read_text())
    assert code==1 and not result['launchable']
    assert set(n for n,_ in seen)=={'66f1','e8f1'}
    assert result['nodes']['e8f1']['launchable']
    assert result['nodes']['66f1']['error']=='unreachable'
