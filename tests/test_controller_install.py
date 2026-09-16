"""Exercise release upgrades against real Git bundles without downloading packages."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('controller_install', ROOT / 'scripts/install-spark-controller.py')
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


@pytest.fixture
def releases(tmp_path, monkeypatch):
    source = tmp_path / 'source'
    source.mkdir()
    subprocess.run(['git', 'init', str(source)], check=True, capture_output=True)
    for key, value in [('user.name', 'Release Test'), ('user.email', 'release@example.invalid')]:
        subprocess.run(['git', '-C', str(source), 'config', key, value], check=True)
    (source / 'scripts').mkdir()
    (source / 'tools').mkdir()
    (source / 'tools/controller-requirements.lock').write_text('# test fixture has no dependencies\n')
    (source / '.gitignore').write_text('.venv/\ndata/cluster\n')
    revisions = []
    for label in ('first', 'second'):
        for name in installer.COMMANDS:
            (source / 'scripts' / name).write_text('print(' + repr(label) + ')\n')
        subprocess.run(['git', '-C', str(source), 'add', '.'], check=True)
        subprocess.run(['git', '-C', str(source), 'commit', '-m', label], check=True, capture_output=True)
        revisions.append(subprocess.check_output(['git', '-C', str(source), 'rev-parse', 'HEAD'], text=True).strip())
    bundle = tmp_path / 'controller.bundle'
    subprocess.run(['git', '-C', str(source), 'bundle', 'create', str(bundle), 'HEAD'], check=True)
    original_run = installer.run

    def without_package_downloads(argv, **kwargs):
        args = [str(arg) for arg in argv]
        if args[1:3] == ['-m', 'venv']:
            binaries = Path(args[3]) / 'bin'
            binaries.mkdir(parents=True)
            (binaries / 'python').symlink_to(sys.executable)
            return
        if args[1:3] == ['-m', 'pip']:
            return
        return original_run(argv, **kwargs)

    monkeypatch.setattr(installer, 'run', without_package_downloads)
    return bundle, revisions, tmp_path / 'installed controller'


def test_upgrade_rollback_and_recovery_state_survive(releases):
    bundle, (first, second), prefix = releases
    installer.install(bundle, first, prefix)
    record = prefix / 'state' / 'recovery.json'
    record.write_text('{"owner":"already-running"}')
    wrapper = prefix / 'bin/sparkctl'
    assert subprocess.check_output([str(wrapper)], text=True).strip() == 'first'
    installer.install(bundle, second, prefix)
    assert subprocess.check_output([str(wrapper)], text=True).strip() == 'second'
    assert (prefix / 'previous').resolve() == prefix / 'releases' / first
    assert (prefix / 'current/data/cluster/recovery.json').read_text() == record.read_text()
    # Reinstalling an existing validated release is also the rollback operation.
    installer.install(bundle, first, prefix)
    assert subprocess.check_output([str(wrapper)], text=True).strip() == 'first'
    assert json.loads(record.read_text())['owner'] == 'already-running'


def test_failed_release_keeps_current_and_does_not_remove_shared_state(releases, monkeypatch):
    bundle, (first, second), prefix = releases
    installer.install(bundle, first, prefix)
    (prefix / 'state/keep').write_text('recovery')
    def fail(_):
        raise RuntimeError('new command failed its smoke check')
    monkeypatch.setattr(installer, 'smoke', fail)
    with pytest.raises(RuntimeError, match='smoke check'):
        installer.install(bundle, second, prefix)
    assert (prefix / 'current').resolve() == prefix / 'releases' / first
    assert not (prefix / 'releases' / second).exists()
    assert (prefix / 'state/keep').read_text() == 'recovery'


def test_dirty_existing_release_is_not_activated(releases):
    bundle, (first, second), prefix = releases
    installer.install(bundle, first, prefix)
    installer.install(bundle, second, prefix)
    (prefix / 'releases' / first / 'scripts/sparkctl').write_text('raise RuntimeError("modified")\n')
    with pytest.raises(RuntimeError, match='has changed'):
        installer.install(bundle, first, prefix)
    assert (prefix / 'current').resolve() == prefix / 'releases' / second


def test_existing_user_directory_and_symbolic_revision_are_rejected(releases):
    bundle, revisions, prefix = releases
    prefix.mkdir()
    keep = prefix / 'user-work'
    keep.write_text('preserve')
    with pytest.raises(RuntimeError, match='not a managed'):
        installer.install(bundle, revisions[0], prefix)
    with pytest.raises(ValueError, match='explicit full Git'):
        installer.install(bundle, 'HEAD', prefix)
    assert keep.read_text() == 'preserve'
