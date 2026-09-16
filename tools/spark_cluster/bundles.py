"""Immutable, checksummed regular-file bundles over local execution or SSH."""
import hashlib
import json
from pathlib import Path
import shlex
import socket
import subprocess
import tarfile
import tempfile


def checksum(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(4 * 1024 * 1024), b''): h.update(chunk)
    return h.hexdigest()


RECEIVER = r'''
import hashlib,json,os,pathlib,shutil,sys,tarfile,tempfile
root=pathlib.Path(sys.argv[1]); identity=sys.argv[2]
root.mkdir(parents=True,exist_ok=True)
temporary=pathlib.Path(tempfile.mkdtemp(prefix='.pending-',dir=root))
def verify(directory,manifest):
    actual=set()
    for path in directory.rglob('*'):
        if path.is_symlink():raise ValueError('bundle contains a symlink')
        if path.is_file():actual.add(str(path.relative_to(directory)))
    if actual!=set(manifest['files'])|{'.spark-bundle.json'}:
        raise ValueError('existing bundle file set changed')
    if json.loads((directory/'.spark-bundle.json').read_text())!=manifest:
        raise ValueError('existing bundle manifest changed')
    for relative,expected in manifest['files'].items():
        p=pathlib.Path(relative)
        if p.is_absolute() or '..' in p.parts: raise ValueError('unsafe bundle path')
        path=directory/p
        if path.is_symlink() or not path.is_file() or path.stat().st_size!=expected['size']:
            raise ValueError('bundle file missing/changed')
        h=hashlib.sha256()
        with path.open('rb') as f:
            for chunk in iter(lambda:f.read(4*1024*1024),b''):h.update(chunk)
        if h.hexdigest()!=expected['sha256']:raise ValueError('bundle checksum mismatch')
try:
    names=set()
    with tarfile.open(fileobj=sys.stdin.buffer,mode='r|') as archive:
        for member in archive:
            p=pathlib.Path(member.name)
            if not member.isfile() or p.is_absolute() or '..' in p.parts or member.name in names:
                raise ValueError('unsafe archive member')
            names.add(member.name)
            path=temporary/p;path.parent.mkdir(parents=True,exist_ok=True)
            with archive.extractfile(member) as source,path.open('wb') as target:
                shutil.copyfileobj(source,target,4*1024*1024)
                target.flush();os.fsync(target.fileno())
            path.chmod(0o644)
    manifest=json.loads((temporary/'.spark-bundle.json').read_text())
    calculated=hashlib.sha256(json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    if calculated!=identity or names!=set(manifest['files'])|{'.spark-bundle.json'}:
        raise ValueError('bundle identity/file set mismatch')
    verify(temporary,manifest)
    destination=root/identity
    if destination.is_symlink():raise ValueError('bundle destination is a symlink')
    if destination.exists():
        verify(destination,manifest)
    else:
        try:os.rename(temporary,destination)
        except FileExistsError:verify(destination,manifest)
        fd=os.open(root,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
    print(json.dumps({'identity':identity,'path':str(destination),'verified':True}))
finally:
    if temporary.exists():shutil.rmtree(temporary)
'''


def publish(root, node):
    files = {}
    for path in sorted(root.rglob('*')):
        if path.is_symlink(): raise ValueError('bundle symlinks are not allowed')
        if path.is_dir(): continue
        if not path.is_file() or path.name == '.spark-bundle.json': raise ValueError('unexpected bundle file')
        files[str(path.relative_to(root))] = {'size': path.stat().st_size, 'sha256': checksum(path)}
    manifest = {'version': 1, 'files': files}
    identity = hashlib.sha256(json.dumps(manifest, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    destination = str(Path(node['projects']).parent / '.local/state/local-llm-cluster/bundles')
    with tempfile.TemporaryDirectory() as temporary:
        header = Path(temporary) / 'manifest.json'
        header.write_text(json.dumps(manifest, sort_keys=True))
        archive_path = Path(temporary) / 'bundle.tar'
        with tarfile.open(archive_path, 'w') as archive:
            archive.add(header, arcname='.spark-bundle.json', recursive=False)
            for relative in files: archive.add(root / relative, arcname=relative, recursive=False)
        command = ['python3', '-c', RECEIVER, destination, identity]
        if socket.gethostname() != node['hostname']:
            command = ['ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=10', node['ssh'], shlex.join(command)]
        with archive_path.open('rb') as stream:
            result = subprocess.run(command, stdin=stream, text=True, capture_output=True, timeout=3600)
        if result.returncode: raise RuntimeError('bundle publish failed: ' + result.stderr[-1000:])
        receipt = json.loads(result.stdout)
        if receipt.get('identity') != identity or receipt.get('verified') is not True:
            raise RuntimeError('bundle acknowledgement mismatch')
        return {**receipt, 'files': len(files), 'bytes': sum(f['size'] for f in files.values())}
