#!/usr/bin/env python3
"""Sample Linux shared-memory pressure during a bounded serving test; never mutate workloads."""
import argparse
import json
from pathlib import Path
import time


def snapshot():
    mem = {line.split(':')[0]: int(line.split()[1]) for line in Path('/proc/meminfo').read_text().splitlines()}
    vm = dict(line.split() for line in Path('/proc/vmstat').read_text().splitlines())
    pressure = {}
    for line in Path('/proc/pressure/memory').read_text().splitlines():
        name, *values = line.split()
        pressure[name] = dict(v.split('=') for v in values)
    return {'time': time.time(), 'available_kib': mem['MemAvailable'],
            'swap_used_kib': mem['SwapTotal'] - mem['SwapFree'],
            'pswpin': int(vm['pswpin']), 'pswpout': int(vm['pswpout']), 'pressure': pressure}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, required=True)
    p.add_argument('--seconds', type=int, default=21600)
    args = p.parse_args()
    if args.output.exists() or not 10 <= args.seconds <= 21600:
        p.error('use a new file and duration 10..21600 seconds')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    stop = args.output.with_suffix('.stop')
    if stop.exists(): p.error('remove the previous stop marker or choose a new output')
    first = snapshot(); last = first; minimum = first['available_kib']; samples = 0
    with args.output.open('x') as f:
        while True:
            last = snapshot(); minimum = min(minimum, last['available_kib']); samples += 1
            f.write(json.dumps(last) + '\n'); f.flush()
            if stop.exists() or time.time() - first['time'] >= args.seconds:
                break
            time.sleep(10)
    summary = {'started_at': first['time'], 'ended_at': last['time'], 'samples': samples,
               'min_available_gib': minimum / 1024**2, 'initial_swap_gib': first['swap_used_kib'] / 1024**2,
               'final_swap_gib': last['swap_used_kib'] / 1024**2,
               'swapin_pages': last['pswpin'] - first['pswpin'], 'swapout_pages': last['pswpout'] - first['pswpout'],
               'pressure_full_us': int(last['pressure']['full']['total']) - int(first['pressure']['full']['total'])}
    args.output.with_suffix('.summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary))


if __name__ == '__main__': main()
