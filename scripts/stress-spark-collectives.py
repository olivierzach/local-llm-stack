"""Bounded two-Spark mixed/unified NCCL reproducer; run in reserved idle GPU containers."""
import argparse, datetime, json, time
from pathlib import Path
import torch
import torch.distributed as dist
from vllm.distributed.device_communicators.pynccl import PyNcclCommunicator
p=argparse.ArgumentParser()
p.add_argument('--rank',type=int,choices=[0,1],required=True)
p.add_argument('--master',required=True)
p.add_argument('--port',type=int,default=29661)
p.add_argument('--mode',choices=['mixed','torch','pynccl'],default='mixed')
p.add_argument('--steps',type=int,default=6000)
a=p.parse_args()
if not 1 <= a.steps <= 100000 or not 1024 <= a.port <= 65535:p.error('steps 1..100000; port 1024..65535')
torch.cuda.set_device(0)
dist.init_process_group('nccl',init_method=f'tcp://{a.master}:{a.port}',rank=a.rank,world_size=2,timeout=datetime.timedelta(seconds=90),device_id=torch.device('cuda:0'))
g=dist.new_group([0,1],backend='gloo')
nc=PyNcclCommunicator(g,device=0)
if a.mode!='torch' and nc.disabled:raise RuntimeError('mixed/pynccl test requires an active PyNccl communicator')
print(json.dumps({'rank': a.rank, 'torch_build_nccl': torch.cuda.nccl.version(),
                  'pynccl_runtime': getattr(nc, 'nccl_version', None),
                  'mapped_libraries': sorted({line.split()[-1] for line in
                      Path('/proc/self/maps').read_text().splitlines() if 'libnccl' in line})}), flush=True)
start=time.monotonic()
for step in range(a.steps):
    n=1+step%3
    x=torch.full((n,2048),a.rank+1.,device='cuda',dtype=torch.bfloat16)
    for _ in range(32):
        if a.mode=='torch':
            y=x.clone();dist.all_reduce(y)
        else:y=nc.all_reduce(x)
        x=y*.25+(a.rank+1)*.5
    for size in (3072,75968,3072,75968):
        z=torch.full((n,size),a.rank+1.,device='cuda',dtype=torch.bfloat16)
        out=torch.empty((2*n,size),device='cuda',dtype=z.dtype)
        if a.mode=='pynccl':nc.all_gather(out,z)
        else:dist.all_gather_into_tensor(out,z)
        # Consume collective output on the default compute stream.
        check=out[:,0].clone()
    got=check.cpu().tolist()
    assert got==[1.]*n+[2.]*n,got
    if step%100==0:assert float(y[0,0].cpu())==3.0
    if step%100==0:print(json.dumps({'rank':a.rank,'mode':a.mode,'step':step,'seconds':time.monotonic()-start}),flush=True)
torch.cuda.synchronize()
print(json.dumps({'rank':a.rank,'mode':a.mode,'complete':True,'steps':a.steps,'seconds':time.monotonic()-start}),flush=True)
dist.destroy_process_group()
