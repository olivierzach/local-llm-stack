import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('configure_context_routes', ROOT / 'scripts/configure-context-routes.py')
routes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(routes)


def test_activation_and_rollback_preserve_unrelated_configuration(tmp_path):
    env = tmp_path / '.env'
    env.write_text('# operator settings\nLITELLM_MASTER_KEY=private-fixture\nFAST_MODEL=custom/model\n')
    env.chmod(0o600)
    registry = {'version': 1, 'routes': {}}
    result = routes.configure(tmp_path, registry)
    assert result['enabled'] and not result['services_restarted'] and not result['models_started']
    assert 'LITELLM_MASTER_KEY=private-fixture\nFAST_MODEL=custom/model\n' in env.read_text()
    assert 'CONTEXT_GUARD_ROUTE_REGISTRY=data/context-guard-routes/registry.json\n' in env.read_text()
    assert env.stat().st_mode & 0o777 == 0o600
    routes.configure(tmp_path, registry)
    assert env.read_text().count('CONTEXT_GUARD_ROUTE_REGISTRY=') == 1
    routes.configure(tmp_path, disable=True)
    assert env.read_text().endswith('CONTEXT_GUARD_ROUTE_REGISTRY=\n')
    assert json.loads((tmp_path / 'data/context-guard-routes/registry.json').read_text()) == registry


def test_invalid_routes_cannot_change_live_registry_or_env(tmp_path):
    env = tmp_path / '.env'
    env.write_text('PRESERVE=yes\n')
    with pytest.raises(ValueError):
        routes.configure(tmp_path, {'version': 99, 'routes': {}})
    assert env.read_text() == 'PRESERVE=yes\n'
    assert not (tmp_path / 'data').exists()


def test_merge_preserves_existing_model_contract_and_credentials(tmp_path):
    env = tmp_path / '.env'
    env.write_text('LITELLM_MASTER_KEY=private-fixture\n')
    route = {'base_url': 'http://10.10.20.2:8011/v1', 'upstream_model': 'deepseek',
             'context_tokens': 65536, 'max_output_tokens': 4096,
             'capabilities': {'text': True, 'vision': False, 'tools': True, 'streaming': True}}
    routes.configure(tmp_path, {'version': 1, 'routes': {'local-deepseek': route}})
    new = {**route, 'base_url': 'http://10.10.20.2:8121/v1', 'upstream_model': 'local-large'}
    routes.configure(tmp_path, {'version': 1, 'routes': {'local-qwen3-next-80b': new}}, merge=True)
    saved = json.loads((tmp_path / 'data/context-guard-routes/registry.json').read_text())
    assert saved['routes'] == {'local-deepseek': route, 'local-qwen3-next-80b': new}
    assert 'LITELLM_MASTER_KEY=private-fixture\n' in env.read_text()
    previous = env.read_text()
    with pytest.raises(ValueError, match='disable'):
        routes.configure(tmp_path, disable=True, merge=True)
    assert env.read_text() == previous


def test_cli_alias_preserves_the_backend_model_name(tmp_path, monkeypatch):
    from spark_cluster import config
    (tmp_path / '.env').write_text('PRESERVE=yes\n')
    p = config.plan(*config.load(ROOT, ROOT / 'cluster/inventory.json',
        ROOT / 'cluster/deployments/large-tp2-mtp2-ordered-e8f1.json'))
    saved = tmp_path / 'plan.json'
    saved.write_text(json.dumps(p))
    monkeypatch.setattr(routes.sys, 'argv', ['configure-context-routes', '--root', str(tmp_path),
        '--plan', str(saved), '--merge', '--alias', 'local-qwen3-next-80b'])
    routes.main()
    registry = json.loads((tmp_path / 'data/context-guard-routes/registry.json').read_text())
    assert set(registry['routes']) == {'local-qwen3-next-80b'}
    assert registry['routes']['local-qwen3-next-80b']['upstream_model'] == 'local-large'
    assert registry['routes']['local-qwen3-next-80b']['deployment_digest'] == p['digest']
