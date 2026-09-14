import importlib.util
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
spec = importlib.util.spec_from_file_location('prepare_glm53', ROOT / 'scripts/prepare-glm53-tp.py')
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


def test_preparation_cannot_launch_or_stop_gpu_services(tmp_path, monkeypatch):
    output = tmp_path / 'receipts'
    monkeypatch.setattr(prepare.socket, 'gethostname', lambda: 'spark-66f1')
    monkeypatch.setattr(sys, 'argv', ['prepare', '--peer', 'e8f1', '--output', str(output), '--apply'])
    monkeypatch.setattr(prepare, 'transport', lambda *args: (['ssh', '-o', 'HostName=10.10.20.2'],
        'spark-e8f1-wired', {'direct_fabric_verified': True}))
    calls = []
    def run(argv, **kwargs):
        calls.append([str(a) for a in argv])
        if 'stdout' in kwargs:
            kwargs['stdout'].write('{"verified_files":74}')
    monkeypatch.setattr(prepare, 'run', run)
    image_id = json.loads((ROOT / 'cluster/glm53-images.lock.json').read_text())['images'][0]['id']
    monkeypatch.setattr(prepare.subprocess, 'check_output',
        lambda argv, **kw: image_id + '\n' if argv[0] == 'docker' else '1000\n1000\n')
    prepare.main()
    assert calls
    assert all('--gpus' not in c and '--privileged' not in c for c in calls)
    assert all('sparkctl' not in c and 'systemctl' not in c for c in calls)
    downloads = [c for c in calls if '/scripts/fetch-pinned-spark-model.py' in c]
    assert len(downloads) == 2
    assert downloads[0][downloads[0].index('--memory') + 1] == '2g'
    copies = [c for c in calls if any(a.endswith('/sync-spark-models.py') for a in c)]
    assert len(copies) == 2 and all('--fabric-inventory' in c for c in copies)
    receipt = json.loads((output / 'preparation.json').read_text())
    assert receipt['complete'] and receipt['phase'] == 'staged-awaiting-gpu-testing'
    assert not receipt['gpu_workloads_changed']


def test_default_is_plan_only_and_wrong_source_rejected(tmp_path, monkeypatch):
    monkeypatch.setattr(prepare.socket, 'gethostname', lambda: 'spark-66f1')
    monkeypatch.setattr(sys, 'argv', ['prepare', '--peer', 'e8f1', '--output', str(tmp_path / 'unused')])
    monkeypatch.setattr(prepare, 'run', lambda *args, **kwargs: pytest.fail('plan mutated state'))
    prepare.main()
    assert not (tmp_path / 'unused').exists()
    monkeypatch.setattr(prepare.socket, 'gethostname', lambda: 'mac-mini')
    with pytest.raises(SystemExit):
        prepare.main()
