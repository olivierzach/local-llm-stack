import importlib.util
import json
from pathlib import Path
import subprocess
import sys


def test_digest_only_pull_is_visible_in_plan(tmp_path, monkeypatch, capsys):
    root = Path(__file__).resolve().parents[1]
    spec = importlib.util.spec_from_file_location('image_sync', root / 'scripts/sync-spark-images.py')
    mod = importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    identity = 'sha256:' + 'a' * 64
    lock = tmp_path / 'images.json'
    lock.write_text(json.dumps({'version': 1, 'images': [{'id': identity, 'tag': 'test/glm:pinned'}]}))
    def checked(argv):
        if argv[0] == 'docker':
            return identity if '-aq' in argv else ''
        assert 'docker image ls -aq --no-trunc' in argv
        return ''
    monkeypatch.setattr(mod, 'checked', checked)
    monkeypatch.setattr(mod.subprocess, 'run', lambda *a, **k: subprocess.CompletedProcess(a[0], 1, '', ''))
    monkeypatch.setattr(sys, 'argv', ['sync', '--lock', str(lock), '--peer', 'peer'])
    mod.main()
    assert json.loads(capsys.readouterr().out)['missing'] == ['test/glm:pinned']
