"""Build a direct-fabric SSH transport; never fall back to a LAN or jump host."""
import ipaddress
import json
import platform
import re
import subprocess


def transport(peer, inventory, rail=0):
    nodes = inventory['nodes']
    sources = [n for n in nodes.values() if n['hostname'] == platform.node()]
    targets = [n for key, n in nodes.items() if peer in (key, n['ssh'], n['hostname'])]
    if len(sources) != 1 or len(targets) != 1 or sources[0] == targets[0]:
        raise ValueError('run on one inventoried Spark and select a different inventoried peer')
    source, target = sources[0], targets[0]
    if type(rail) is not int or not 0 <= rail < min(len(source['fabric']), len(target['fabric'])):
        raise ValueError('fabric rail is not present on both nodes')
    local, remote = source['fabric'][rail], target['fabric'][rail]
    src, dst = str(ipaddress.IPv4Address(local['ip'])), str(ipaddress.IPv4Address(remote['ip']))
    interface = local['interface']
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', interface):
        raise ValueError('invalid fabric interface')
    route = json.loads(subprocess.check_output(['ip', '-j', 'route', 'get', dst, 'from', src], text=True))
    if len(route) != 1 or route[0].get('dev') != interface or route[0].get('gateway'):
        raise ValueError('peer route is not the directly connected fabric interface; no copy attempted')
    addresses = json.loads(subprocess.check_output(['ip', '-j', 'address', 'show', 'dev', interface], text=True))
    if not any(a.get('local') == src for entry in addresses for a in entry.get('addr_info', [])):
        raise ValueError('expected source address is missing from the fabric interface')
    # Command-line settings override even a Mac-oriented alias with ProxyJump.
    # Disable multiplexing so an existing LAN socket cannot be reused.
    ssh = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10',
           '-o', 'ServerAliveInterval=15', '-o', 'ServerAliveCountMax=4',
           '-o', 'HostName=' + dst, '-o', 'Port=22', '-o', 'ProxyJump=none',
           '-o', 'ProxyCommand=none', '-o', 'ControlPath=none',
           '-o', 'BindInterface=' + interface, '-o', 'BindAddress=' + src]
    alias = target['ssh']
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.@-]*', alias):
        raise ValueError('invalid peer SSH alias')
    actual = subprocess.check_output(ssh + [alias, 'hostname'], text=True, timeout=20).strip()
    if actual != target['hostname']:
        raise ValueError('fabric SSH reached a different host; no copy attempted')
    return ssh, alias, {'source': source['hostname'], 'destination': actual,
                        'interface': interface, 'source_ip': src, 'destination_ip': dst,
                        'rail': rail, 'direct_fabric_verified': True}
