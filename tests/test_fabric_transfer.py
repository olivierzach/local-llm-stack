import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('spark_transfer', ROOT / 'scripts/spark_transfer.py')
transfer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transfer)


@pytest.fixture
def setup(monkeypatch):
    inventory = json.loads((ROOT / 'cluster/inventory.json').read_text())
    monkeypatch.setattr(transfer.platform, 'node', lambda: 'spark-66f1')
    seen = []
    answers = {'route': [{'dev': 'enp1s0f0np0'}],
               'address': [{'addr_info': [{'local': '10.10.20.1'}]}], 'hostname': 'spark-e8f1'}
    def output(argv, **kwargs):
        seen.append(argv)
        if argv[0] == 'ssh': return answers['hostname'] + '\n'
        return json.dumps(answers[argv[2]])
    monkeypatch.setattr(transfer.subprocess, 'check_output', output)
    return inventory, seen, answers


def test_pins_physical_transport_and_disables_existing_multiplex_socket(setup):
    inventory, seen, _ = setup
    ssh, alias, report = transfer.transport('spark-e8f1-wired', inventory)
    assert alias == 'spark-e8f1-wired'
    for option in ('HostName=10.10.20.2', 'BindAddress=10.10.20.1',
                   'BindInterface=enp1s0f0np0', 'ControlPath=none', 'ProxyJump=none', 'ProxyCommand=none'):
        assert option in ssh
    assert seen[-1] == ssh + [alias, 'hostname']
    assert report['direct_fabric_verified']


@pytest.mark.parametrize('route', [[{'dev': 'wlP9s9'}], [{'dev': 'enp1s0f0np0', 'gateway': '192.168.1.1'}]])
def test_lan_or_routed_path_rejected_before_ssh(setup, route):
    inventory, seen, answers = setup
    answers['route'] = route
    with pytest.raises(ValueError, match='directly connected'):
        transfer.transport('e8f1', inventory)
    assert all(argv[0] != 'ssh' for argv in seen)


def test_missing_address_and_wrong_peer_fail_closed(setup):
    inventory, _, answers = setup
    answers['address'] = []
    with pytest.raises(ValueError, match='source address'):
        transfer.transport('e8f1', inventory)
    answers['address'] = [{'addr_info': [{'local': '10.10.20.1'}]}]
    answers['hostname'] = 'wrong-host'
    with pytest.raises(ValueError, match='different host'):
        transfer.transport('e8f1', inventory)


def test_both_node_assignments_and_second_rail(setup, monkeypatch):
    inventory, _, answers = setup
    monkeypatch.setattr(transfer.platform, 'node', lambda: 'spark-e8f1')
    answers.update(route=[{'dev': 'enP2p1s0f0np0'}],
                   address=[{'addr_info': [{'local': '10.10.21.2'}]}], hostname='spark-66f1')
    ssh, alias, report = transfer.transport('66f1', inventory, 1)
    assert alias == 'spark-66f1-wired'
    assert 'BindAddress=10.10.21.2' in ssh and 'HostName=10.10.21.1' in ssh
    assert report['rail'] == 1


def test_non_spark_and_same_node_are_rejected(setup, monkeypatch):
    inventory, seen, _ = setup
    with pytest.raises(ValueError, match='different inventoried peer'):
        transfer.transport('66f1', inventory)
    monkeypatch.setattr(transfer.platform, 'node', lambda: 'mac-mini')
    with pytest.raises(ValueError, match='inventoried Spark'):
        transfer.transport('e8f1', inventory)
    assert not seen
