import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    'openclaw_context_limit', Path(__file__).resolve().parents[1] / 'scripts/update-openclaw-context-limit.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


@pytest.mark.parametrize('deadline', [300, 7200])
def test_existing_client_update_preserves_other_models_keys_and_agent_settings(deadline):
    before = {'models': {'providers': {'local': {'apiKey': 'private-fixture', 'baseUrl': 'http://example.invalid',
        'timeoutSeconds': deadline, 'models': [{'id': 'selected', 'contextWindow': 32768, 'maxTokens': 8192},
        {'id': 'other', 'contextWindow': 120000}]} }}, 'agents': {'defaults': {'model': {'primary': 'cloud/model'}}}}
    after = module.update(before, 'local', 'selected', 1048576)
    assert before['models']['providers']['local']['models'][0]['contextWindow'] == 32768
    assert after['models']['providers']['local']['timeoutSeconds'] == max(deadline, 3600)
    after['models']['providers']['local']['timeoutSeconds'] = deadline
    after['models']['providers']['local']['models'][0]['contextWindow'] = 32768
    assert after == before
