#!/usr/bin/env python3
"""Probe FamChat's configured Context Guard from its container, without saving a chat."""
import argparse
import json
from pathlib import Path
import subprocess

PROBE = r'''
import json,os,sqlite3,sys
import requests
model,expected=sys.argv[1:]
con=sqlite3.connect('file:/app/backend/data/webui.db?mode=ro',uri=True)
values=dict(con.execute("SELECT key,value FROM config WHERE key IN ('openai.api_base_urls','openai.api_keys')"))
urls=json.loads(values['openai.api_base_urls']) if 'openai.api_base_urls' in values else [os.environ['OPENAI_API_BASE_URL']]
keys=json.loads(values['openai.api_keys']) if 'openai.api_keys' in values else [os.environ.get('OPENAI_API_KEY','')]
base='http://context-guard:4010/v1'
index=urls.index(base)
s=requests.Session();s.trust_env=False
s.headers['Authorization']='Bearer '+keys[index]
r=s.get(base+'/models',timeout=15);r.raise_for_status()
assert any(m['id']==model for m in r.json()['data']), 'model not advertised through FamChat provider'
r=s.post(base+'/chat/completions',json={'model':model,'temperature':0,'max_tokens':16,'messages':[{'role':'user','content':'Reply with exactly: ready'}]},timeout=120)
r.raise_for_status()
assert not expected or r.headers.get('X-Spark-Deployment')==expected, 'provider returned another deployment'
answer=r.json()['choices'][0]['message']['content']
assert answer.strip()=='ready', 'unexpected provider response'
print(json.dumps({'passed':True,'model':model,'base_url':base,'deployment_digest':r.headers.get('X-Spark-Deployment'),'reply':answer,'scope':'Configured provider URL and credential from FamChat container; no browser interaction or stored chat.'}))
'''


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--container',default='local-llm-stack-open-webui-1')
    p.add_argument('--model',default='local-deepseek-v4-flash')
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--expected-deployment',default='')
    args=p.parse_args()
    result=subprocess.run(['docker','exec','-i',args.container,'python','-',args.model,args.expected_deployment],
                          input=PROBE,text=True,capture_output=True,timeout=150)
    if result.returncode:
        # Do not relay third-party exception text that could include configuration.
        raise RuntimeError('FamChat provider probe failed; inspect provider configuration and availability')
    report=json.loads(result.stdout)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))


if __name__=='__main__':
    main()
