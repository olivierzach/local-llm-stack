"""Fault injection only for an explicitly owned acceptance-test container."""
import fcntl
import json
from pathlib import Path
import platform
import re
import subprocess
import sys


def checked_id(request, saved, container):
    identity = request['container_id']
    if not re.fullmatch(r'[a-f0-9]{64}', identity):
        raise RuntimeError('fault injection requires an immutable container ID')
    if not re.fullmatch(r'accept-recovery-[a-z0-9-]+', request['owner']):
        raise RuntimeError('fault injection is restricted to acceptance deployments')
    if not saved or saved.get('owner') != request['owner'] or saved.get('digest') != request['digest']:
        raise RuntimeError('fault injection reservation mismatch')
    if saved.get('phase') != 'started' or saved.get('container_ids') != [identity]:
        raise RuntimeError('fault injection requires the one journaled running worker')
    labels = container.get('Config', {}).get('Labels') or {}
    if container.get('Id') != identity or labels.get('io.spark.owner') != request['owner'] or labels.get('io.spark.digest') != request['digest']:
        raise RuntimeError('fault injection container ownership mismatch')
    if container.get('State', {}).get('Status') != 'running':
        raise RuntimeError('fault injection worker is not running')
    return identity


def main(request):
    if platform.node() != request['node']['hostname'] or platform.machine() != request['node']['architecture']:
        raise RuntimeError('fault injection host mismatch')
    state = Path.home()/'.local/state/local-llm-cluster'
    # Require existing admission state; do not create a lease or a new lock.
    with (state/'mutex').open('r+') as mutex:
        fcntl.flock(mutex, fcntl.LOCK_EX)
        saved = json.loads((state/'gpu.json').read_text())
        identity = request['container_id']
        if not re.fullmatch(r'[a-f0-9]{64}', identity):
            raise RuntimeError('invalid immutable container ID')
        result = subprocess.run(['docker','inspect',identity], capture_output=True, text=True, timeout=30)
        if result.returncode:raise RuntimeError('owned fault-test worker could not be inspected')
        checked_id(request, saved, json.loads(result.stdout)[0])
        result = subprocess.run(['docker','kill','--signal=KILL',identity], capture_output=True, text=True, timeout=30)
        if result.returncode:raise RuntimeError('owned fault-test worker could not be killed')
        if json.loads((state/'gpu.json').read_text()) != saved:
            raise RuntimeError('fault injection unexpectedly changed the reservation')
        return {'container_id':identity,'signal':'SIGKILL','reservation_retained':True}


if __name__ == '__main__':
    try:print(json.dumps({'ok':True,'result':main(json.load(sys.stdin))}))
    except Exception as exc:
        print(json.dumps({'ok':False,'error':str(exc)}))
        sys.exit(1)
