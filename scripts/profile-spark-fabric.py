#!/usr/bin/env python3
"""Profile host-memory RDMA in both directions; no GPU use or host reconfiguration."""
from concurrent.futures import ThreadPoolExecutor
import argparse
import json
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'tools'))
from spark_cluster.config import read, validate_inventory
from spark_cluster.cli import save_json

SOURCE = ROOT/'tools/spark_cluster/fabric_node.py'


class Invocation:
    def __init__(self, node, request):
        argv = ['python3', '-c', SOURCE.read_text()]
        if socket.gethostname() != node['hostname']:
            argv = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
                    '-o', 'ServerAliveInterval=10', '-o', 'ServerAliveCountMax=3', node['ssh'], shlex.join(argv)]
        self.stderr = tempfile.TemporaryFile(mode='w+t')
        self.ready, self.done = threading.Event(), threading.Event()
        self.response, self.error, self.ready_info = None, None, None
        self.process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.stderr, text=True)
        self.process.stdin.write(json.dumps({**request, 'node': node}))
        self.process.stdin.close()
        self.reader = threading.Thread(target=self.receive, daemon=True)
        self.reader.start()

    def receive(self):
        try:
            for line in self.process.stdout:
                message = json.loads(line)
                if message.get('event') == 'listening':
                    self.ready_info = message
                    self.ready.set()
                elif message.get('event') == 'result': self.response = message
                else: raise RuntimeError('unexpected diagnostic response')
        except Exception as exc:
            self.error = exc
        finally:
            self.done.set()

    def listening(self):
        deadline = time.monotonic()+25
        while not self.ready.wait(.05):
            if self.done.is_set():
                self.result()
                raise RuntimeError('server completed without a listening acknowledgement')
            if time.monotonic() >= deadline: raise TimeoutError('server readiness observation timed out')

    def result(self):
        if not self.done.wait(150): raise TimeoutError('diagnostic observation timed out')
        code = self.process.wait(timeout=10)
        if self.error: raise self.error
        if code or not self.response or not self.response.get('ok'):
            self.stderr.seek(0)
            raise RuntimeError((self.response or {}).get('error', self.stderr.read()[-700:]))
        return self.response['result']

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try: self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.reader.join(timeout=5)
        self.process.stdout.close()
        self.stderr.close()
        # Node-side GNU timeout bounds test children even if this SSH session dies.


def inspect(node):
    job = Invocation(node, {'action': 'inspect'})
    try: return job.result()
    finally: job.close()


def counter_deltas(before, after):
    result = {}
    for left, right in zip(before['rails'], after['rails']):
        result[left['interface']] = {}
        for group in ('net_counters', 'rdma_counters', 'rdma_hw_counters'):
            result[left['interface']][group] = {key: right[group][key]-value for key, value in left[group].items()
                                               if key in right[group]}
    return result


def run_case(nodes, snapshots, sender, receiver, rails, settings, port, cpus=None):
    def cases(node, peer):
        return [{**snapshots[node]['rails'][rail], 'peer_ip': snapshots[peer]['rails'][rail]['ip'],
                 'port': port+rail, 'cpu': cpus[rail] if cpus else None} for rail in rails]
    server, client = None, None
    try:
        server = Invocation(nodes[receiver], {'action': 'measure', 'role': 'server',
                            'settings': settings, 'cases': cases(receiver, sender)})
        server.listening()
        client = Invocation(nodes[sender], {'action': 'measure', 'role': 'client',
                            'settings': settings, 'cases': cases(sender, receiver)})
        results = {sender: client.result(), receiver: server.result()}
        rates = [row['measurement']['average_gbps'] for row in results[sender]['records']]
        return {'sender': sender, 'receiver': receiver, 'rails': rails, 'settings': settings,
            'sum_concurrent_rail_average_gbps': sum(rates), 'nodes': results,
            'counter_deltas': {name: counter_deltas(value['before'], value['after']) for name, value in results.items()}}
    finally:
        if client: client.close()
        if server: server.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory', type=Path, default=ROOT/'cluster/inventory.json')
    parser.add_argument('--nodes', nargs=2, default=['66f1','e8f1'])
    parser.add_argument('--seconds', type=int, choices=range(5,31), default=5)
    parser.add_argument('--qps', type=int, nargs='+', choices=[1,2,4,8], default=[1,4])
    parser.add_argument('--sizes', type=int, nargs='+', choices=[65536,1048576,8388608], default=[65536,8388608])
    parser.add_argument('--cpus', type=int, nargs=2, help='optional CPU IDs for rail 0 and rail 1 on both nodes; pins test processes only')
    parser.add_argument('--port', type=int, default=28550)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    inv = read(args.inventory)
    validate_inventory(inv)
    if len(set(args.nodes)) != 2 or any(n not in inv['nodes'] for n in args.nodes): parser.error('select two different inventory nodes')
    if not 20000 <= args.port <= 59999: parser.error('port must be between 20000 and 59999')
    if len(set(args.qps)) != len(args.qps) or len(set(args.sizes)) != len(args.sizes): parser.error('duplicate test settings')
    args.output.mkdir(parents=True, exist_ok=False)
    nodes = {n:inv['nodes'][n] for n in args.nodes}
    with ThreadPoolExecutor(max_workers=2) as pool:
        snapshots = dict(zip(nodes, pool.map(inspect, nodes.values())))
    if args.cpus and (len(set(args.cpus)) != 2 or any(cpu not in s['allowed_cpus'] for cpu in args.cpus for s in snapshots.values())):
        raise ValueError('select two distinct CPUs available on both nodes')
    if any(len(s['rails']) != 2 for s in snapshots.values()): raise ValueError('this sweep requires two logical rails per node')
    a,b = args.nodes
    for left,right in zip(snapshots[a]['rails'],snapshots[b]['rails']):
        if left['mtu'] != right['mtu']: raise ValueError('fabric MTUs differ; refusing a mismatched sweep')
    report = {'format':1, 'inventory':nodes, 'requested_cpus':args.cpus, 'initial':snapshots, 'runs':[], 'complete':False,
        'qualification':'Host-memory RDMA write throughput, not model-copy speed or GPU collective bandwidth. Concurrent rail rates are sums of perftest averages from overlapping runs, not a separately synchronized aggregate measurement. Logical rail speeds must not be added to infer physical cable capacity. GPU processes/load/counters are recorded; unrelated workload activity can affect results.'}
    save_json(args.output/'report.json', report)
    for sender,receiver in [(a,b),(b,a)]:
        for qps in args.qps:
            for size in args.sizes:
                for rails in ([0],[1],[0,1]):
                    settings = {'seconds':args.seconds,'qps':qps,'bytes':size}
                    try:
                        result = run_case(nodes,snapshots,sender,receiver,rails,settings,args.port,args.cpus)
                    except Exception as exc:
                        report['error'] = str(exc)
                        report['failed_case'] = {'sender':sender,'receiver':receiver,'rails':rails,'settings':settings}
                        save_json(args.output/'report.json',report)
                        raise
                    report['runs'].append(result)
                    save_json(args.output/'report.json', report)
                    print(json.dumps({k:v for k,v in result.items() if k not in ('nodes','counter_deltas')}), flush=True)
    report['complete'] = True
    save_json(args.output/'report.json',report)


if __name__ == '__main__':
    try: main()
    except Exception as exc:
        print(f'profile-spark-fabric: {exc}',file=sys.stderr)
        sys.exit(1)
