"""Bounded host-memory RDMA diagnostics; transported over SSH, no resident service."""
from concurrent.futures import ThreadPoolExecutor
import fcntl
import ipaddress
import json
import os
from pathlib import Path
import platform
import re
import signal
import subprocess
import sys
import time


def command(args):
    result = subprocess.run(args, text=True, capture_output=True, timeout=15)
    version_exit = (args == ['ib_write_bw', '--version'] and result.returncode == 1 and
                    re.fullmatch(r'Version: [0-9.]+', result.stdout.strip()))
    if result.returncode and not version_exit:
        raise RuntimeError(f'{args[0]} failed: {result.stderr[-500:]}')
    return result.stdout.strip()


def value(path):
    try: return Path(path).read_text().strip()
    except OSError: return None


def counters(path):
    result = {}
    for file in Path(path).glob('*'):
        text = value(file)
        if text is not None and text.isdecimal(): result[file.name] = int(text)
    return result


def inspect(node):
    if platform.node() != node['hostname'] or platform.machine() != node['architecture']:
        raise RuntimeError('host identity does not match inventory')
    rails = []
    for rail in node['fabric']:
        net = Path('/sys/class/net')/rail['interface']
        rdma = Path('/sys/class/infiniband')/rail['rdma']/'ports/1'
        addresses = json.loads(command(['ip', '-j', '-4', 'addr', 'show', 'dev', rail['interface']]))
        if rail['ip'] not in {a['local'] for row in addresses for a in row['addr_info']}:
            raise RuntimeError('fabric address does not match inventory')
        if value(net/'carrier') != '1': raise RuntimeError('fabric carrier is down')
        gids = []
        for entry in (rdma/'gids').glob('*'):
            address = ipaddress.ip_address(value(entry))
            if (str(address.ipv4_mapped) == rail['ip'] and
                value(rdma/'gid_attrs/types'/entry.name) == 'RoCE v2' and
                value(rdma/'gid_attrs/ndevs'/entry.name) == rail['interface']):
                gids.append(int(entry.name))
        if len(gids) != 1: raise RuntimeError('expected one matching IPv4 RoCE v2 GID')
        device = (net/'device').resolve()
        pci = []
        for path in [device, *device.parents]:
            speed = value(path/'current_link_speed')
            if speed is not None:
                pci.append({'device': path.name, 'speed': speed, 'width': value(path/'current_link_width')})
        verbs = command(['ibv_devinfo', '-d', rail['rdma'], '-i', '1'])
        active_mtu = re.search(r'active_mtu:\s+(\d+)', verbs)
        max_mtu = re.search(r'max_mtu:\s+(\d+)', verbs)
        firmware = re.search(r'fw_ver:\s+(\S+)', verbs)
        rails.append({**rail, 'gid_index': gids[0], 'mtu': int(value(net/'mtu')),
            'speed_mbps': value(net/'speed'), 'rdma_active_mtu': int(active_mtu[1]) if active_mtu else None,
            'rdma_max_mtu': int(max_mtu[1]) if max_mtu else None,
            'rdma_firmware': firmware[1] if firmware else None,
            'pci_links': pci, 'net_counters': counters(net/'statistics'),
            'rdma_counters': counters(rdma/'counters'), 'rdma_hw_counters': counters(rdma/'hw_counters')})
    return {'hostname': platform.node(), 'kernel': platform.release(),
        'gpu_driver': command(['nvidia-smi','--query-gpu=driver_version','--format=csv,noheader']), 'perftest_version': command(['ib_write_bw', '--version']),
        'gpu_processes': command(['nvidia-smi', '--query-compute-apps=pid,process_name', '--format=csv,noheader']).splitlines(),
        'load_average': list(os.getloadavg()), 'allowed_cpus': sorted(os.sched_getaffinity(0)),
        'cpu_policies': {p.name:{key:value(p/key) for key in ('scaling_governor','scaling_cur_freq','cpuinfo_max_freq','related_cpus')}
                         for p in Path('/sys/devices/system/cpu/cpufreq').glob('policy*')}, 'rails': rails, 'timestamp': time.time()}


def arguments(case, settings, server):
    args = ['timeout', '--signal=TERM', '--kill-after=3s', str(settings['seconds']+25)+'s', 'ib_write_bw', '-d', case['rdma'], '-i', '1', '-x', str(case['gid_index']),
        '-p', str(case['port']), '--bind_source_ip', case['ip'], '-D', str(settings['seconds']),
        '-f', '1', '-q', str(settings['qps']), '-s', str(settings['bytes']), '-Q', '1',
        '-N', '--report_gbits', '--cpu_util', '--perform_warm_up']
    if case.get('cpu') is not None: args[4:4] = ['taskset', '-c', str(case['cpu'])]
    if not server: args.append(case['peer_ip'])
    return args


def parse_bandwidth(output, size):
    if 'BW average[Gb/sec]' not in output: raise ValueError('bandwidth units/header missing')
    rows = []
    for line in output.splitlines():
        if re.fullmatch(r'\s*\d+\s+\d+\s+[\d.]+\s+[\d.]+\s+[\d.]+(?:\s+[\d.]+)?\s*', line):
            columns = line.split()
            if int(columns[0]) == size:
                rows.append({'bytes': size, 'iterations': int(columns[1]), 'average_gbps': float(columns[3]),
                             'cpu_util_percent': float(columns[5]) if len(columns)>5 else None})
    if len(rows) != 1 or rows[0]['iterations'] <= 0 or not 0 < rows[0]['average_gbps'] < 10000:
        raise ValueError('missing or invalid bandwidth measurement')
    return rows[0]


def owned_listener(process, text):
    # GNU timeout supervises the actual listener in this invocation's process group.
    for pid in re.findall(r'pid=(\d+),', text):
        try:
            if os.getpgid(int(pid)) == process.pid: return True
        except ProcessLookupError:
            pass
    return False


def stop(process):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGKILL)
    process.wait()


def measure(request):
    node, settings, cases = request['node'], request['settings'], request['cases']
    if (type(settings['seconds']) is not int or not 5 <= settings['seconds'] <= 30 or
        type(settings['qps']) is not int or settings['qps'] not in (1, 2, 4, 8) or
        type(settings['bytes']) is not int or settings['bytes'] not in (65536, 1048576, 8388608)):
        raise ValueError('diagnostic bounds exceeded')
    if request['role'] not in ('server', 'client') or not 1 <= len(cases) <= 2:
        raise ValueError('invalid diagnostic role/rail count')
    state = Path.home()/'.local/state/local-llm-cluster'
    state.mkdir(parents=True, exist_ok=True)
    with (state/'fabric-profile.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        before = inspect(node)
        for case in cases:
            if type(case['port']) is not int or not 20000 <= case['port'] <= 60000:
                raise ValueError('invalid diagnostic port')
            ipaddress.IPv4Address(case['peer_ip'])
            if case.get('cpu') is not None and (type(case['cpu']) is not int or case['cpu'] not in os.sched_getaffinity(0)):
                raise ValueError('requested CPU is not available to this process')
            if not any(all(case[k] == rail[k] for k in ('ip', 'interface', 'rdma', 'gid_index')) for rail in before['rails']):
                raise ValueError('diagnostic rail differs from current inventory')
        server = request['role'] == 'server'
        processes = []
        started = time.time()
        try:
            for case in cases:
                processes.append(subprocess.Popen(arguments(case, settings, server), stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT, text=True, start_new_session=True))
            if server:
                deadline = time.monotonic()+10
                while True:
                    if any(p.poll() is not None for p in processes):
                        raise RuntimeError('RDMA server exited before listening: '+''.join(p.communicate()[0] for p in processes if p.poll() is not None)[-1500:])
                    listening = [command(['ss', '-ltnp', f'sport = :{case["port"]}']) for case in cases]
                    if all(owned_listener(p, row) for p, row in zip(processes, listening)): break
                    if time.monotonic() >= deadline: raise TimeoutError('RDMA server did not listen')
                    time.sleep(.05)
                print(json.dumps({'event': 'listening', 'watchdog_pids': [p.pid for p in processes],
                    'watchdog_seconds': settings['seconds']+25, 'started_at': started}), flush=True)
            def collect(item):
                case, process = item
                output = process.communicate(timeout=settings['seconds']+20)[0]
                if process.returncode: raise RuntimeError('RDMA test failed: '+output[-1500:])
                return {'interface': case['interface'], 'requested_cpu': case.get('cpu'), 'pid': process.pid, 'exit_code': process.returncode,
                    'measurement': parse_bandwidth(output, settings['bytes']), 'log': output}
            with ThreadPoolExecutor(max_workers=len(cases)) as pool:
                records = list(pool.map(collect, zip(cases, processes)))
            return {'before': before, 'after': inspect(node), 'started_at': started,
                    'finished_at': time.time(), 'records': records}
        finally:
            for process in processes: stop(process)


def main():
    try:
        request = json.load(sys.stdin)
        result = inspect(request['node']) if request['action'] == 'inspect' else measure(request)
        print(json.dumps({'event': 'result', 'ok': True, 'result': result}), flush=True)
    except Exception as exc:
        print(json.dumps({'event': 'result', 'ok': False, 'error': str(exc)}), flush=True)
        sys.exit(1)


if __name__ == '__main__': main()
