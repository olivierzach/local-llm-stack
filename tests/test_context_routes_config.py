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
    assert 'CONTEXT_GUARD_ROUTE_REGISTRY=/routes/registry.json\n' in env.read_text()
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
