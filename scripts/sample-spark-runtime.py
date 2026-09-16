#!/usr/bin/env python3
"""Read-only local GPU, shared-memory and fabric samples for an owned test worker.

Stops when the selected container stops or the duration expires. RDMA data
counters are converted from four-octet units to bytes; ordinary network counters
alone do not account for offloaded RDMA. Power is GPU telemetry, not wall power.
"""
import argparse
import json
from pathlib import Path
import re
import subprocess
import time


def sample(container):
    result = {'time': time.time(), 'rdma': {}, 'network': {}}
    for device in Path('/sys/class/infiniband').glob('*'):
        counters = {}
        for key in ('port_rcv_data', 'port_xmit_data', 'port_rcv_packets',
                    'port_xmit_packets', 'port_rcv_errors', 'port_xmit_discards'):
            path = device / 'ports/1/counters' / key
            if path.exists():
                value = int(path.read_text())
                counters[key.replace('_data', '_bytes')] = value * (4 if key.endswith('_data') else 1)
        result['rdma'][device.name] = counters
    for device in Path('/sys/class/net').glob('*'):
        if device.name.startswith(('en', 'wl')):
            result['network'][device.name] = {key: int((device / 'statistics' / key).read_text())
                                             for key in ('rx_bytes', 'tx_bytes', 'rx_errors', 'tx_errors')}
    result['memory_kib'] = {key: int(value.split()[0]) for key, value in
                           (line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
                           if key in ('MemTotal', 'MemAvailable', 'SwapFree')}
    gpu = subprocess.run(['nvidia-smi', '--query-gpu=utilization.gpu,power.draw,temperature.gpu',
                          '--format=csv,noheader,nounits'], capture_output=True, text=True, timeout=10)
    result['gpu_csv_columns'] = ['utilization_percent', 'power_watts', 'temperature_celsius']
    result['gpu_csv'] = gpu.stdout.strip()
    if gpu.returncode: result['gpu_error'] = gpu.stderr.strip()[-500:]
    state = subprocess.run(['docker', 'inspect', '--format', '{{.State.Status}}', container],
                           capture_output=True, text=True, timeout=10)
    result['container'] = container
    result['container_state'] = state.stdout.strip() if state.returncode == 0 else 'unavailable'
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--container', required=True, help='exact container ID from sparkctl status')
    parser.add_argument('--interval', type=float, default=5)
    parser.add_argument('--duration', type=int, default=3600)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if not (re.fullmatch(r'[a-f0-9]{12,64}', args.container) and .5 <= args.interval <= 60
            and 1 <= args.duration <= 14400):
        parser.error('invalid container ID or sampling bounds')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + args.duration
    with args.output.open('x') as output:
        while time.monotonic() < deadline:
            began = time.monotonic()
            result = sample(args.container)
            output.write(json.dumps(result) + '\n')
            output.flush()
            if result['container_state'] != 'running': break
            time.sleep(max(0, min(args.interval - (time.monotonic() - began), deadline - time.monotonic())))


if __name__ == '__main__':
    main()
