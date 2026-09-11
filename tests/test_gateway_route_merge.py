import importlib.machinery
import importlib.util
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import config, gateway, gateway_node


def test_route_merge_preserves_installed_aliases_under_lock(tmp_path, monkeypatch):
    monkeypatch.setattr(gateway_node, 'ROOT', tmp_path)
    monkeypatch.setattr(gateway_node.platform, 'node', lambda: 'spark-test')
    monkeypatch.setattr(gateway_node.platform, 'machine', lambda: 'aarch64')
    monkeypatch.setattr(gateway_node, 'checked', lambda saved: {'State': {'Running': True}})
    (tmp_path / 'state.json').write_text('{}')
    (tmp_path / 'config').mkdir()
    registry = tmp_path / 'config/registry.json'
    registry.write_text(json.dumps({'version': 1, 'routes': {'existing': {'base_url': 'unchanged'}}}))
    # A nonempty saved owner is required, as in a real installed gateway.
    (tmp_path / 'state.json').write_text('{"digest": "owned"}')
    result = gateway_node.main({'node': {'hostname': 'spark-test', 'architecture': 'aarch64'},
                               'action': 'routes', 'merge': True,
                               'registry': {'version': 1, 'routes': {'new': {'base_url': 'new'}}}})
    assert result['registry'] == json.loads(registry.read_text())
    assert result['registry']['routes'] == {'existing': {'base_url': 'unchanged'}, 'new': {'base_url': 'new'}}


def test_cli_distinct_alias_keeps_backend_name_and_saves_complete_registry(tmp_path, monkeypatch):
    loader = importlib.machinery.SourceFileLoader('gateway_cli_merge', str(ROOT / 'scripts/spark-gateway'))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    cli = importlib.util.module_from_spec(spec)
    loader.exec_module(cli)
    monkeypatch.setattr(cli, 'ROOT', tmp_path)
    plan = config.plan(*config.load(ROOT, ROOT / 'cluster/inventory.json',
                                   ROOT / 'cluster/deployments/large-tp2-mtp2-nccl2307-66f1.json'))
    saved = tmp_path / 'plan.json'
    saved.write_text(json.dumps(plan))
    route = next(iter(gateway.from_plans([plan])['routes'].values()))
    expected = {'version': 1, 'routes': {'old-alias': route, 'local-qwen3-next-80b': route}}
    def remote(node, request, source):
        assert request['merge'] is True
        assert list(request['registry']['routes']) == ['local-qwen3-next-80b']
        assert request['registry']['routes']['local-qwen3-next-80b']['upstream_model'] == 'local-large'
        return {'routes_replaced': True, 'aliases': list(expected['routes']), 'registry': expected}
    monkeypatch.setattr(cli, 'remote', remote)
    monkeypatch.setattr(sys, 'argv', ['spark-gateway', 'routes', '--node', '66f1', '--inventory',
        str(ROOT / 'cluster/inventory.json'), '--plan', str(saved), '--merge', '--alias', 'local-qwen3-next-80b'])
    cli.main()
    assert json.loads((tmp_path / 'data/cluster/gateways/66f1/registry.json').read_text()) == expected
