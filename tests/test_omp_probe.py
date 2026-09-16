import importlib.util
from pathlib import Path

import pytest

spec=importlib.util.spec_from_file_location('omp_probe',Path(__file__).resolve().parents[1]/'scripts/probe-spark-omp.py')
probe=importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


@pytest.mark.parametrize('invalid',[None,'no-call','wrong-file','wrong-call-id','tool-result-only'])
def test_tool_acceptance_requires_matching_read_and_final_answer(invalid):
    events=[{'type':'tool_execution_start','toolName':'read','toolCallId':'call-1','args':{'path':'/fixture'}},
        {'type':'tool_execution_end','toolCallId':'call-1','result':'nonce'},
        {'type':'agent_end','messages':[{'role':'toolResult','content':[{'type':'text','text':'nonce'}]},
            {'role':'assistant','content':[{'type':'text','text':'nonce'}]}]}]
    if invalid=='no-call': events.pop(0)
    if invalid=='wrong-file': events[0]['args']['path']='/another'
    if invalid=='wrong-call-id': events[1]['toolCallId']='different'
    if invalid=='tool-result-only': events[-1]['messages'][-1]['content'][0]['text']='guessed'
    if invalid:
        with pytest.raises(RuntimeError): probe.validate(events,Path('/fixture'),'nonce')
    else: assert probe.validate(events,Path('/fixture'),'nonce')==1
