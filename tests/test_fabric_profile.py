import importlib.util
import os
from pathlib import Path
import signal
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'tools'))
from spark_cluster import fabric_node
spec = importlib.util.spec_from_file_location('fabric_profile',ROOT/'scripts/profile-spark-fabric.py')
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


def test_bandwidth_requires_correct_units_size_and_one_positive_row():
    log = 'BW average[Gb/sec]\n 65536 4000 0.00 27.54 0.01 20.50\n'
    assert fabric_node.parse_bandwidth(log,65536)['average_gbps'] == 27.54
    for invalid in (log.replace('Gb/sec','MiB/sec'),log.replace('65536','64'),log+log,
                    log.replace('27.54','0.00'),log.replace('4000','0')):
        with pytest.raises(ValueError): fabric_node.parse_bandwidth(invalid,65536)


def test_bounds_refuse_before_inspecting_or_starting(monkeypatch):
    monkeypatch.setattr(fabric_node,'inspect',lambda node: pytest.fail('unexpected host access'))
    for settings in ({'seconds':0,'qps':1,'bytes':65536}, {'seconds':5,'qps':True,'bytes':65536},
                     {'seconds':5,'qps':1,'bytes':999999999}):
        with pytest.raises(ValueError): fabric_node.measure({'node':{},'settings':settings,'cases':[{}]})


def test_own_process_cleanup_preserves_unrelated_process():
    own = subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'],start_new_session=True)
    other = subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)'],start_new_session=True)
    try:
        fabric_node.stop(own)
        assert own.poll() == -signal.SIGKILL
        assert other.poll() is None
    finally:
        fabric_node.stop(own)
        fabric_node.stop(other)


def test_watchdog_and_listener_ownership(monkeypatch):
    case={'rdma':'rdma0','gid_index':3,'port':28550,'ip':'10.10.20.1','peer_ip':'10.10.20.2'}
    args=fabric_node.arguments(case,{'seconds':5,'qps':1,'bytes':65536},False)
    assert args[:5] == ['timeout','--signal=TERM','--kill-after=3s','30s','ib_write_bw']
    assert args[-1] == case['peer_ip'] and '--use_cuda' not in args
    process=type('Process',(),{'pid':123})()
    monkeypatch.setattr(os,'getpgid',lambda pid: 123 if pid == 124 else 999)
    assert fabric_node.owned_listener(process,'users:(("ib_write_bw",pid=124,fd=3))')
    assert not fabric_node.owned_listener(process,'users:(("other",pid=998,fd=3))')


def test_counter_deltas_retain_resets_and_do_not_invent_missing_counters():
    before={'rails':[{'interface':'rail0','net_counters':{'rx_errors':2,'gone':2},
                     'rdma_counters':{'packets':100},'rdma_hw_counters':{}}]}
    after={'rails':[{'interface':'rail0','net_counters':{'rx_errors':3},
                     'rdma_counters':{'packets':0},'rdma_hw_counters':{'new':5}}]}
    result=profile.counter_deltas(before,after)['rail0']
    assert result['net_counters'] == {'rx_errors':1}
    assert result['rdma_counters'] == {'packets':-100}
    assert result['rdma_hw_counters'] == {}


def test_perftest_version_exit_one_is_accepted_only_for_valid_version(monkeypatch):
    result=type('Result',(),{'returncode':1,'stdout':'Version: 6.20\n','stderr':''})()
    monkeypatch.setattr(subprocess,'run',lambda *a,**k: result)
    assert fabric_node.command(['ib_write_bw','--version']) == 'Version: 6.20'
    with pytest.raises(RuntimeError): fabric_node.command(['ib_write_bw','--help'])
    result.stdout='Could not start'
    with pytest.raises(RuntimeError): fabric_node.command(['ib_write_bw','--version'])


def test_cpu_affinity_is_scoped_to_test_and_rejects_unavailable_cpu(tmp_path, monkeypatch):
    case={'rdma':'rdma0','interface':'rail0','gid_index':3,'port':28550,
          'ip':'10.10.20.1','peer_ip':'10.10.20.2','cpu':5}
    settings={'seconds':5,'qps':1,'bytes':65536}
    args=fabric_node.arguments(case,settings,True)
    assert args[:8] == ['timeout','--signal=TERM','--kill-after=3s','30s','taskset','-c','5','ib_write_bw']
    monkeypatch.setattr(Path,'home',lambda:tmp_path)
    monkeypatch.setattr(os,'sched_getaffinity',lambda pid:{15},raising=False)
    monkeypatch.setattr(fabric_node,'inspect',lambda node:{'rails':[case]})
    monkeypatch.setattr(subprocess,'Popen',lambda *a,**k:pytest.fail('unexpected child process'))
    with pytest.raises(ValueError,match='CPU'):
        fabric_node.measure({'node':{},'settings':settings,'role':'client','cases':[case]})
