from pathlib import Path
import runpy

import yaml

MOD = runpy.run_path(str(Path(__file__).resolve().parents[1] / 'scripts/configure-omp-glm53.py'))


def test_glm_addition_keeps_provider_credentials_other_models_and_defaults():
    text = '''# Existing settings
providers:
  spark-context-guard:
    baseUrl: http://127.0.0.1:4010/v1
    apiKey: TEST_ENV_KEY
    modelOverrides:
      local-deepseek-v4-flash:
        contextWindow: 1048576
        maxTokens: 4096
  another:
    modelOverrides: {}
modelRoles: {default: spark-context-guard/local-deepseek-v4-flash}
'''
    recipe = dict(alias='local-glm53-flash', context_tokens=262144, max_output_tokens=8192)
    changed = MOD['update'](text, 'spark-context-guard', recipe)
    result = yaml.safe_load(changed)
    added = result['providers']['spark-context-guard']['modelOverrides'].pop(recipe['alias'])
    assert result == yaml.safe_load(text)
    assert added['input'] == ['text', 'image']
    assert added['compat']['extraBody']['chat_template_kwargs']['enable_thinking'] is False
    assert added['compat']['whenThinking']['extraBody']['chat_template_kwargs']['enable_thinking'] is True
    assert MOD['update'](changed, 'spark-context-guard', recipe) == changed
