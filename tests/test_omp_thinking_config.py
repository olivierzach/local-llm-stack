import importlib.util
from pathlib import Path
import yaml

spec = importlib.util.spec_from_file_location('omp_thinking', Path(__file__).resolve().parents[1]/'scripts/update-omp-deepseek-thinking.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)


def test_thinking_update_preserves_credentials_limits_other_models_and_nested_fields():
    original = {'providers': {'local': {'apiKey': 'private-fixture', 'baseUrl': 'http://example.invalid',
        'modelOverrides': {'selected': {'reasoning': False, 'contextWindow': 1048576, 'maxTokens': 4096,
            'compat': {'streamIdleTimeoutMs': 3600000, 'extraBody': {'chat_template_kwargs': {'other': 7}}}},
            'other': {'reasoning': False, 'contextWindow': 8192}}}}}
    result = yaml.safe_load(m.update(yaml.safe_dump(original), 'local', 'selected'))
    model = result['providers']['local']['modelOverrides']['selected']
    assert model['compat']['extraBody']['chat_template_kwargs']['other'] == 7
    assert model['contextWindow'] == 1048576 and model['maxTokens'] == 4096
    assert model['compat']['streamIdleTimeoutMs'] == 3600000
    result['providers']['local']['modelOverrides']['selected'] = original['providers']['local']['modelOverrides']['selected']
    assert result == original
