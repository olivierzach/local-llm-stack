#!/usr/bin/env python3
"""Exercise a host venv's CUDA training kernels under the shared GPU lease."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'tools'))
from spark_cluster import node

PROBE = '''
import json, torch
assert torch.cuda.is_available(), 'CUDA is unavailable'
assert torch.cuda.get_device_capability() == (12, 1), 'expected Spark GB10'
torch.manual_seed(7)
torch.backends.cuda.matmul.allow_tf32 = False
a = torch.randn(128, 128, device='cuda')
b = torch.randn(128, 128, device='cuda')
torch.testing.assert_close((a @ b).cpu(), a.cpu() @ b.cpu(), rtol=1e-4, atol=2e-4)
c = torch.ones(8, 8, device='cuda', dtype=torch.bfloat16)
assert torch.equal((c @ c).cpu(), torch.full((8, 8), 8, dtype=torch.bfloat16))
model = torch.nn.Linear(32, 16).cuda()
optimizer = torch.optim.AdamW(model.parameters(), lr=0.001)
before = model.weight.detach().clone()
loss = model(torch.randn(8, 32, device='cuda')).square().mean()
assert torch.isfinite(loss)
loss.backward()
optimizer.step()
assert not torch.equal(before, model.weight)
torch.cuda.synchronize()
print(json.dumps({'passed': True, 'torch': torch.__version__, 'cuda_build': torch.version.cuda,
 'gpu': torch.cuda.get_device_name(), 'capability': list(torch.cuda.get_device_capability()),
 'float32_matmul': True, 'bf16_matmul': True, 'adamw_step': True, 'loss': loss.item(),
 'peak_cuda_bytes': torch.cuda.max_memory_allocated(),
 'scope': 'Small CUDA correctness and optimizer smoke; not performance or model-quality acceptance.'}))
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--venv', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--inventory', type=Path, default=ROOT / 'cluster/inventory.json')
    parser.add_argument('--cleanup-request', type=Path)
    args = parser.parse_args()
    if args.cleanup_request:
        request = json.loads(args.cleanup_request.read_text())
        if not request['owner'].startswith('accept-python-'):
            parser.error('request is outside Python acceptance namespace')
        print(json.dumps(node.main({**request, 'action': 'release-workload'})))
        return 0
    if not args.venv or not args.output:
        parser.error('--venv and a fresh --output are required')
    python = args.venv.expanduser().absolute() / 'bin/python'
    if not python.is_file():
        parser.error('venv Python does not exist')
    hosts = [n for n in json.loads(args.inventory.read_text())['nodes'].values() if n['hostname'] == platform.node()]
    if len(hosts) != 1:
        parser.error('current host must match exactly one inventory node')
    output = args.output.expanduser().absolute()
    output.mkdir(parents=True, exist_ok=False)
    digest = hashlib.sha256(json.dumps([hosts[0], str(python), str(output)]).encode()).hexdigest()
    request = {'owner': 'accept-python-' + digest[:24], 'digest': digest,
               'node': hosts[0], 'min_available_mib': 16384}
    node.atomic(output / 'request.json', request)
    result = {'passed': False, 'venv': str(python.parent.parent)}
    reserved = False
    try:
        node.main({**request, 'action': 'reserve-workload'})
        reserved = True
        probe = subprocess.run([str(python), '-c', PROBE], capture_output=True, text=True, timeout=180)
        (output / 'probe.log').write_text(probe.stdout + probe.stderr)
        probe.check_returncode()
        result['probe'] = json.loads(probe.stdout.strip().splitlines()[-1])
        result['passed'] = result['probe']['passed'] is True
    except Exception as error:
        result['error'] = str(error)
    finally:
        if reserved:
            try:
                result['cleanup'] = node.main({**request, 'action': 'release-workload'})
                result['cleanup_verified'] = node.reservation() is None
            except Exception as error:
                result.update(passed=False, cleanup_error=str(error))
        node.atomic(output / 'acceptance.json', result)
    print(json.dumps(result, indent=2))
    return int(not result['passed'])


if __name__ == '__main__':
    raise SystemExit(main())
