import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    'long_context_probe', Path(__file__).resolve().parents[1] / 'scripts/probe-spark-long-context.py')
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


@pytest.mark.parametrize('target', [1024, 32768, 260032])
def test_retrieval_prompt_is_sized_and_codes_are_separated(target):
    codes = {'alpha': 'a1b2c3d4', 'beta': 'e5f6a7b8', 'gamma': 'c9d0e1f2'}
    count = lambda text: len(text.split()) + 11
    text, actual = probe.fit(count, target, codes, 'unique-prefix')
    assert actual == count(text) and target - 32 <= actual <= target
    assert text.startswith('unique-prefix')
    positions = [text.index(value) / len(text) for value in codes.values()]
    assert .2 < positions[0] < .35
    assert .45 < positions[1] < .6
    assert .7 < positions[2] < .85
    assert all(text.count(value) == 1 for value in codes.values())
