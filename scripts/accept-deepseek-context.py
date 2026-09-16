#!/usr/bin/env python3
"""Qualify an existing DeepSeek deployment with optional context sweep and host-memory sampling."""
import argparse,json,shlex,socket,subprocess,sys,time
from pathlib import Path
root=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(root/'tools'))
from spark_cluster.config import read,validate_saved_plan
from spark_cluster.cli import inspect,save_json
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--saved-plan',type=Path,required=True)
parser.add_argument('--output',type=Path,required=True)
parser.add_argument('--sweep',action='store_true')
args=parser.parse_args()
plan=read(args.saved_plan);validate_saved_plan(plan)
if not plan['recipe'].get('deepseek_v4') or socket.gethostname() not in [n['hostname'] for n in plan['nodes'].values()]:
 parser.error('run on a participating Spark with a DeepSeek plan')
if not inspect(plan)['healthy']:raise RuntimeError('the selected deployment must be healthy')
out=args.output.expanduser().resolve();out.mkdir(parents=True,exist_ok=False)
save_json(out/'plan.json',plan)
sweep=args.sweep;python=str(root/'.venv/bin/python')
report={'complete':False,'deployment_digest':plan['digest'],'phase':'starting-monitors','started_at':time.time()}
def save():save_json(out/'campaign.json',report)
save()
monitors=[]
def command(node,argv):
 return argv if node['hostname']==socket.gethostname() else ['ssh','-o','BatchMode=yes',node['ssh'],shlex.join(argv)]
try:
 subprocess.run([python,str(root/'scripts/snapshot-spark-serving.py'),'--saved-plan',str(out/'plan.json'),
                 '--output',str(out/'runtime-before')],check=True,timeout=180)
 for name,node in plan['nodes'].items():
  sample=out/('memory-'+name+'.jsonl')
  log=(out/('monitor-'+name+'.log')).open('w')
  proc=subprocess.Popen(command(node,['python3',str(Path.home()/'projects/local-llm-stack-cluster/current/scripts/sample-spark-memory.py'),'--output',str(sample)]),stdout=log,stderr=subprocess.STDOUT)
  monitors.append((name,node,sample,proc,log))
 with (out/'campaign.log').open('w') as log:
  if sweep:
   report['phase']='initial-regression';save()
   subprocess.run([python,str(root/'scripts/probe-deepseek-repeatability.py'),'--saved-plan',str(out/'plan.json'),'--trials','20','--output',str(out/'initial-regression.json')],cwd=root,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=600)
   report['phase']='context-sweep';save()
   subprocess.run([python,str(root/'scripts/sweep-deepseek-context.py'),'--saved-plan',str(out/'plan.json'),'--output',str(out/'sweep')],cwd=root,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=21600)
  report['phase']='full-acceptance';save()
  subprocess.run(['make','deepseek-tp-accept','PLAN='+str(out/'plan.json'),'OUTPUT='+str(out/'acceptance')],cwd=root,stdout=log,stderr=subprocess.STDOUT,check=True,timeout=14400)
 report.update(complete=True,phase='passed')
except BaseException as exc:
 report.update(phase='failed',error=repr(exc));raise
finally:
 for name,node,sample,proc,log in monitors:
  subprocess.run(command(node,['touch',str(sample.with_suffix('.stop'))]),check=True,timeout=20)
  try:proc.wait(timeout=25)
  except subprocess.TimeoutExpired:proc.terminate()
  log.close()
  if node['hostname']!=socket.gethostname():
   for path in [sample,sample.with_suffix('.summary.json')]:
    data=subprocess.check_output(command(node,['cat',str(path)]),timeout=30)
    path.write_bytes(data)
 report['ended_at']=time.time();save();print(json.dumps(report),flush=True)
 subprocess.run([python,str(root/'scripts/snapshot-spark-serving.py'),'--saved-plan',str(out/'plan.json'),
                 '--output',str(out/'runtime-after')],check=True,timeout=180)
