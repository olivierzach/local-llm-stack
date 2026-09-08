import importlib.util
import json
from pathlib import Path

import pytest

spec=importlib.util.spec_from_file_location('benchmark',Path(__file__).resolve().parents[1]/'scripts/benchmark-spark-inference.py')
benchmark=importlib.util.module_from_spec(spec)
spec.loader.exec_module(benchmark)


class Response:
    def __init__(self,lines): self.lines=lines
    def __enter__(self): return self
    def __exit__(self,*args): pass
    def raise_for_status(self): pass
    def iter_lines(self,**kwargs): return iter(self.lines)


class Session(Response):
    headers={}
    def post(self,*args,**kwargs): return self


@pytest.mark.parametrize('bad',['missing-done','missing-usage','reasoning',None])
def test_measure_requires_real_usage_and_complete_text_stream(monkeypatch,bad):
    delta={'content':'hello'} if bad!='reasoning' else {'reasoning':'hidden'}
    chunks=[{'choices':[{'delta':delta}]}]
    if bad!='missing-usage': chunks.append({'choices':[],'usage':{'prompt_tokens':20,'completion_tokens':4}})
    lines=['data: '+json.dumps(c) for c in chunks]
    if bad!='missing-done': lines.append('data: [DONE]')
    monkeypatch.setattr(benchmark.requests,'Session',lambda:Session(lines))
    ticks=iter([0,.1,.5])
    monkeypatch.setattr(benchmark.time,'monotonic',lambda:next(ticks))
    if bad:
        with pytest.raises(RuntimeError): benchmark.measure('http://test/v1',None,'model','prompt',32)
    else:
        result=benchmark.measure('http://test/v1',None,'model','prompt',32)
        assert result['ttft_s']==.1 and result['completion_tokens']==4
        assert result['decode_tokens_per_second']==pytest.approx(7.5)
