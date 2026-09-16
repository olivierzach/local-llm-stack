import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    'serving_profile', Path(__file__).resolve().parents[1] / 'scripts/profile-spark-serving.py')
profile = importlib.util.module_from_spec(spec)
spec.loader.exec_module(profile)


@pytest.mark.parametrize('target', [256, 1024, 8192, 65536])
def test_sizing_uses_tokenizer_and_stays_within_input_budget(target):
    count = lambda text: len(text.split()) + 11
    text, actual = profile.sized_prompt(count, target, 'unique-prefix')
    assert actual == count(text)
    assert target - 32 <= actual <= target
    assert text.startswith('unique-prefix\n')
    assert text.endswith(profile.SUFFIX)


def test_sizing_refuses_a_tokenizer_that_ignores_input():
    with pytest.raises(RuntimeError, match='cannot size'):
        profile.sized_prompt(lambda text: 12, 1024, 'unique')
