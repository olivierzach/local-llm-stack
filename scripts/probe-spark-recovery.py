#!/usr/bin/env python3
"""Start a fresh acceptance deployment, crash its owned worker, recover and clean up."""
import argparse
import json
from pathlib import Path
import signal
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT/'tools'))
from spark_cluster import cli, config


def require(value, message):
    if not value:raise RuntimeError(message)


def run_probe(p, output, timeout, fault_node):
    config.validate_saved_plan(p)
    require(p['deployment']['name'].startswith('accept-recovery-'), 'use an isolated acceptance deployment')
    require(fault_node in p['nodes'], 'fault node is outside deployment')
    output.mkdir(parents=True, exist_ok=False)
    cli.save_json(output/'plan.json', p)
    report = {'passed':False,'owner':p['owner'],'digest':p['digest'],'fault_node':fault_node,
              'checks':[], 'cleanup':{},
              'scope':'Owned worker SIGKILL after successful completion, retained-lease cleanup, fresh restart and another real completion. No gateway changes or in-flight request replay test.'}
    attempted = False
    error = None

    def record(label, value):
        report['checks'].append({'check':label, **value})
        cli.save_json(output/'recovery.json', report)
        cli.emit({'phase':label,'owner':p['owner']})

    def clean():
        results = {}
        for node_id in reversed(list(p['nodes'])):
            try:results[node_id] = cli.call(p,node_id,'stop')
            except Exception as exc:results[node_id] = {'error':str(exc)}
        return results

    try:
        preflight = {n:cli.call(p,n,'preflight') for n in p['nodes']}
        record('preflight', {'nodes':preflight})
        require(all(v['launchable'] for v in preflight.values()), 'preflight blocked; no GPU start attempted')
        attempted = True
        cli.up(p, timeout, output/'initial')
        before = cli.inspect(p)
        require(before['healthy'], 'initial deployment is not healthy')
        initial_ids = {n:[c['id'] for c in s['containers']] for n,s in before['nodes'].items()}
        require(len(initial_ids[fault_node]) == 1, 'fault injection needs one worker on the selected node')
        record('initial-completion', {'acceptance':config.read(output/'initial/acceptance.json'),'container_ids':initial_ids})
        request = {'node':p['nodes'][fault_node], 'owner':p['owner'],'digest':p['digest'],
                   'container_id':initial_ids[fault_node][0]}
        fault = cli.remote(p['nodes'][fault_node], request, ROOT/'tools/spark_cluster/recovery_node.py')
        record('worker-killed', fault)
        deadline = time.monotonic()+30
        while True:
            after = cli.inspect(p)
            stopped = after['nodes'][fault_node]['containers']
            if stopped and all(c['state'] in ('exited','dead') for c in stopped):break
            require(time.monotonic()<deadline, 'crashed worker did not become terminal')
            time.sleep(1)
        saved = after['nodes'][fault_node]['reservation']
        require(not after['healthy'], 'crashed deployment still reports healthy')
        require(saved is not None and saved['owner']==p['owner'] and saved['digest']==p['digest'] and
                saved['container_ids']==initial_ids[fault_node], 'crashed worker lost its exact recovery reservation')
        require(stopped[0]['id']==initial_ids[fault_node][0] and stopped[0]['exit_code']!=0,
                'fault did not produce an abnormal exit of the original worker')
        record('failed-state-retains-lease', {'status':after})
        cleanup = clean()
        require(not any('error' in r for r in cleanup.values()), 'failed-worker cleanup needs manual recovery')
        cleared = cli.inspect(p)
        require(all(not s['containers'] and s['reservation'] is None for s in cleared['nodes'].values()),
                'failed-worker cleanup left containers or reservations')
        record('failed-worker-cleanup', {'nodes':cleanup})
        cli.up(p, timeout, output/'restarted')
        restarted = cli.inspect(p)
        require(restarted['healthy'], 'restarted deployment is not healthy')
        for n,s in restarted['nodes'].items():
            require(s['containers'] and not set(initial_ids[n]) & {c['id'] for c in s['containers']},
                    'restart did not create fresh worker identities')
        record('restart-completion', {'acceptance':config.read(output/'restarted/acceptance.json'), 'status':restarted})
    except BaseException as exc:
        error = str(exc) or type(exc).__name__
    finally:
        if attempted:
            report['cleanup'] = clean()
            if any('error' in value for value in report['cleanup'].values()):
                error = (error+'; ' if error else '')+'cleanup incomplete; recover with saved plan'
            else:
                try:
                    final = cli.inspect(p)
                    report['final_status'] = final
                    require(all(not s['containers'] and (s['reservation'] or {}).get('owner') != p['owner']
                                for s in final['nodes'].values()), 'owned workers or reservations remain after cleanup')
                    report['cleanup_verified'] = True
                except Exception as exc:
                    error = (error+'; ' if error else '')+'cleanup verification failed: '+str(exc)
        if error:report['error'] = error
        report['passed'] = error is None
        cli.save_json(output/'recovery.json', report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--deployment',type=Path,required=True)
    parser.add_argument('--inventory',type=Path,default=ROOT/'cluster/inventory.json')
    parser.add_argument('--run-id',required=True,help='fresh identifier; existing run directories are refused')
    parser.add_argument('--fault-node',help='default: deployment coordinator')
    parser.add_argument('--timeout',type=int,default=600,help='each startup deadline, 10..7200 seconds')
    parser.add_argument('--output',type=Path)
    args = parser.parse_args()
    config.name(args.run_id)
    require(len(args.run_id)<=24, 'run ID must be at most 24 characters')
    require(10<=args.timeout<=7200, 'startup timeout must be 10..7200 seconds')
    inv, recipe, deployment = config.load(ROOT,args.inventory,args.deployment)
    deployment = {**deployment,'name':'accept-recovery-'+args.run_id}
    p = config.plan(inv,recipe,deployment)
    def interrupted(signum, frame):raise RuntimeError('acceptance interrupted by signal '+str(signum))
    for sig in (signal.SIGTERM, signal.SIGINT):signal.signal(sig,interrupted)
    result = run_probe(p, args.output or ROOT/'data/cluster/recovery'/args.run_id,
                       args.timeout, args.fault_node or deployment['coordinator'])
    cli.emit(result)
    return int(not result['passed'])


if __name__ == '__main__':
    try:sys.exit(main())
    except Exception as exc:
        print('probe-spark-recovery: '+str(exc),file=sys.stderr)
        sys.exit(1)
