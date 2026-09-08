import importlib.util
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location('openclaw_probe', Path(__file__).resolve().parents[1] / 'scripts/probe-spark-openclaw.py')
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


@pytest.mark.parametrize('invalid', [None, 'no-call', 'wrong-file', 'wrong-id', 'result-error',
                                   'wrong-provider', 'prompt-leak', 'final-mismatch', 'summary-mismatch'])
def test_openclaw_requires_actual_matching_tool_trace(invalid):
    identity = {'provider': 'spark-e8f1', 'model': 'local-coder'}
    envelope = {**identity, 'ok': True, 'status': 'ok', 'final': 'nonce',
                'toolSummary': {'calls': 1, 'tools': ['read'], 'failures': 0}}
    messages = [
        {'role': 'user', 'content': 'Read verification.txt.'},
        {**identity, 'role': 'assistant', 'content': [{'type': 'toolCall', 'id': 'call-1',
            'name': 'read', 'arguments': {'path': 'verification.txt'}}]},
        {'role': 'toolResult', 'toolCallId': 'call-1', 'toolName': 'read', 'isError': False,
         'content': [{'type': 'text', 'text': 'Verification value: nonce'}]},
        {**identity, 'role': 'assistant', 'content': [{'type': 'text', 'text': 'nonce'}]},
    ]
    if invalid == 'no-call': messages.pop(1)
    if invalid == 'wrong-file': messages[1]['content'][0]['arguments']['path'] = '../outside'
    if invalid == 'wrong-id': messages[2]['toolCallId'] = 'unrelated'
    if invalid == 'result-error': messages[2]['isError'] = True
    if invalid == 'wrong-provider': messages[3]['provider'] = 'another'
    if invalid == 'prompt-leak': messages[0]['content'] = 'Return nonce.'
    if invalid == 'final-mismatch': messages[3]['content'][0]['text'] = 'guessed'
    if invalid == 'summary-mismatch': envelope['toolSummary']['calls'] = 0
    if invalid:
        with pytest.raises(RuntimeError):
            probe.validate(envelope, messages, Path('/fixture/verification.txt'), 'nonce', 'spark-e8f1', 'local-coder')
    else:
        assert probe.validate(envelope, messages, Path('/fixture/verification.txt'), 'nonce', 'spark-e8f1', 'local-coder') == 1
