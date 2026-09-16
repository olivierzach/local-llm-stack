#!/usr/bin/env python3
"""Fetch an explicit public model revision, verify upstream SHA-256s, emit a peer-copy lock.

Run fetch inside the manifest's pinned runtime image. Verify needs only Python's
standard library. No credential is read or forwarded to Hugging Face.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import tempfile
import time


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(8*1024*1024),b''): h.update(block)
    return h.hexdigest()


def validate(manifest):
    if set(manifest) != {'version','repo','revision','image','hub_version','files'} or type(manifest['version']) is not int or manifest['version'] != 1:
        raise ValueError('unsupported download manifest')
    if not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',manifest['repo']): raise ValueError('invalid model repository')
    if not re.fullmatch(r'[0-9a-f]{40}',manifest['revision']): raise ValueError('revision must be a full commit')
    if not re.fullmatch(r'[A-Za-z0-9./_-]+@sha256:[0-9a-f]{64}',manifest['image']): raise ValueError('image must be pinned')
    if not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+',manifest['hub_version']): raise ValueError('hub version must be pinned')
    if not isinstance(manifest['files'],dict) or not 1 <= len(manifest['files']) <= 4096:
        raise ValueError('invalid file inventory')
    for name,info in manifest['files'].items():
        path=PurePosixPath(name)
        if (not path.parts or path.is_absolute() or '..' in path.parts or path.as_posix() != name or
            not re.fullmatch(r'[A-Za-z0-9_./-]+',name)):
            raise ValueError('unsafe download path')
        if (set(info) != {'size','sha256'} or type(info['size']) is not int or info['size'] < 0 or
            not re.fullmatch(r'[0-9a-f]{64}',info['sha256'])):
            raise ValueError('invalid size/checksum')
    if 'config.json' not in manifest['files'] or not any(name.endswith('.safetensors') for name in manifest['files']):
        raise ValueError('model config or safetensors weights missing')
    return sum(info['size'] for info in manifest['files'].values())


def paths(cache,manifest):
    directory='models--'+manifest['repo'].replace('/','--')
    base=cache/'hub'/directory
    snapshot=base/'snapshots'/manifest['revision']
    for path in [cache,cache/'hub',base,base/'blobs',base/'snapshots',snapshot]:
        if path.is_symlink(): raise ValueError('cache directory symlinks are not supported')
    return directory,base,snapshot


def verify(cache,manifest):
    validate(manifest)
    directory,base,snapshot=paths(cache,manifest)
    if not snapshot.is_dir(): raise ValueError('snapshot is missing')
    found={str(path.relative_to(snapshot)) for path in snapshot.rglob('*') if not path.is_dir()}
    if found != set(manifest['files']): raise ValueError('snapshot file inventory differs from download manifest')
    files,links={},{}
    for relative,expected in sorted(manifest['files'].items()):
        path=snapshot/relative
        for parent in path.parents:
            if parent == cache: break
            if parent.is_symlink(): raise ValueError('snapshot parent is a symlink')
        target=path.resolve(strict=True)
        if not target.is_relative_to(base.resolve()) or not target.is_file(): raise ValueError('file escapes model cache')
        if target.stat().st_size != expected['size'] or digest(target) != expected['sha256']:
            raise ValueError('upstream checksum/size mismatch: '+relative)
        files[str(target.relative_to(cache))]=dict(expected)
        if path.is_symlink(): links[str(path.relative_to(cache))]=os.readlink(path)
        print(json.dumps({'verified':relative,'bytes':expected['size']}),flush=True)
    return {'version':1,'models':[{'repo':manifest['repo'],'revision':manifest['revision'],'directory':directory}],
            'files':files,'symlinks':links}


def save(path,data):
    path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists():
        if path.is_symlink() or json.loads(path.read_text()) != data:
            raise ValueError('existing lock differs; choose another output')
        return
    fd,temp=tempfile.mkstemp(dir=path.parent)
    try:
        with os.fdopen(fd,'w') as stream:
            json.dump(data,stream,indent=2,sort_keys=True);stream.write('\n')
            stream.flush();os.fsync(stream.fileno())
        # Link exclusively so a concurrent writer cannot be overwritten.
        os.link(temp,path)
    finally:
        os.unlink(temp)


def fetch(cache,manifest,max_bytes,workers):
    total=validate(manifest)
    if total > max_bytes: raise ValueError('manifest exceeds explicit download-byte budget')
    _,_,snapshot=paths(cache,manifest)
    cache.mkdir(parents=True,exist_ok=True)
    missing=sum(info['size'] for name,info in manifest['files'].items()
                if not (snapshot/name).is_file() or (snapshot/name).stat().st_size != info['size'])
    if shutil.disk_usage(cache).free < missing + 10*1024**3:
        raise ValueError('insufficient disk space with 10 GiB reserve')
    import huggingface_hub
    if huggingface_hub.__version__ != manifest['hub_version']:
        raise ValueError('download library differs; use the pinned runtime image')
    def download(name):
        print(json.dumps({'fetching':name}),flush=True)
        result=huggingface_hub.hf_hub_download(repo_id=manifest['repo'],revision=manifest['revision'],
            filename=name,cache_dir=str(cache/'hub'),token=False)
        print(json.dumps({'fetched':name}),flush=True)
        return result
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures=[pool.submit(download,name) for name in manifest['files']]
        for future in as_completed(futures):
            try: future.result()
            except BaseException:
                for pending in futures: pending.cancel()
                raise
    return verify(cache,manifest)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['fetch','verify'])
    parser.add_argument('--manifest',type=Path,required=True)
    parser.add_argument('--cache',type=Path,required=True)
    parser.add_argument('--lock-output',type=Path,required=True)
    parser.add_argument('--max-download-bytes',type=int,help='required explicit byte budget for fetch')
    parser.add_argument('--workers',type=int,choices=range(1,5),default=2)
    args=parser.parse_args()
    if args.action == 'fetch' and (args.max_download_bytes is None or args.max_download_bytes < 1):
        parser.error('fetch requires a positive --max-download-bytes')
    manifest=json.loads(args.manifest.read_text());validate(manifest)
    cache=args.cache.expanduser().absolute()
    started=time.monotonic()
    lock=(fetch(cache,manifest,args.max_download_bytes,args.workers) if args.action == 'fetch' else verify(cache,manifest))
    save(args.lock_output,lock)
    print(json.dumps({'verified':True,'repo':manifest['repo'],'revision':manifest['revision'],
        'manifest_sha256':digest(args.manifest),'files':len(lock['files']),
        'bytes':sum(f['size'] for f in lock['files'].values()),'elapsed_s':time.monotonic()-started,
        'lock':str(args.lock_output)}),flush=True)


if __name__ == '__main__': main()
