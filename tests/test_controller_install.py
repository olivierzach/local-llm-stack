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
BASELINE_COMMANDS = ('sparkctl', 'spark-gateway', 'spark-client', 'spark-loop', 'spark-vector')
RECOVERY_COMMANDS = ('spark-recover', 'spark-monitor', 'spark-node', 'spark-services')


@pytest.fixture
def releases(tmp_path, monkeypatch, request):
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
    for label, commands in (('first', BASELINE_COMMANDS), ('second', BASELINE_COMMANDS + RECOVERY_COMMANDS)):
        for name in commands:
            if name != getattr(request, 'param', None):
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


@pytest.mark.parametrize('legacy_receipt', [False, True], ids=['current-receipt', 'legacy-receipt'])
def test_upgrade_rollback_and_recovery_state_survive(releases, legacy_receipt):
    bundle, (first, second), prefix = releases
    installer.install(bundle, first, prefix)
    if legacy_receipt:
        # c7adb112's installer did not yet record source hashes.
        receipt_path = prefix / 'releases' / first / '.controller-release.json'
        receipt = json.loads(receipt_path.read_text())
        del receipt['source_sha256']
        receipt_path.write_text(json.dumps(receipt))
    for command in RECOVERY_COMMANDS:
        assert not (prefix / 'bin' / command).exists()
    unrelated = prefix / 'bin/operator-tool'
    unrelated.write_text('#!/bin/sh\necho operator\n')
    unrelated.chmod(0o755)
    # This name was formerly used as the installer's temporary wrapper.
    user_temporary = prefix / 'bin/sparkctl.tmp'
    user_temporary.write_text('operator-owned temporary file\n')
    secret = prefix / 'state/secrets.env'
    secret.write_text('API_KEY=keep-me\n')
    secret.chmod(0o600)
    record = prefix / 'state' / 'recovery.json'
    record.write_text('{"owner":"already-running"}')
    wrapper = prefix / 'bin/sparkctl'
    assert subprocess.check_output([str(wrapper)], text=True).strip() == 'first'
    installer.install(bundle, second, prefix)
    assert subprocess.check_output([str(wrapper)], text=True).strip() == 'second'
    for command in RECOVERY_COMMANDS:
        assert subprocess.check_output([str(prefix / 'bin' / command)], text=True).strip() == 'second'
    assert (prefix / 'previous').resolve() == prefix / 'releases' / first
    assert (prefix / 'current/data/cluster/recovery.json').read_text() == record.read_text()
    # Reinstalling an existing validated release is also the rollback operation.
    installer.install(bundle, first, prefix)
    assert subprocess.check_output([str(wrapper)], text=True).strip() == 'first'
    assert json.loads(record.read_text())['owner'] == 'already-running'
    for command in RECOVERY_COMMANDS:
        assert not (prefix / 'bin' / command).exists()
    for command in BASELINE_COMMANDS:
        assert subprocess.check_output([str(prefix / 'bin' / command)], text=True).strip() == 'first'
    assert secret.read_text() == 'API_KEY=keep-me\n'
    assert secret.stat().st_mode & 0o777 == 0o600
    assert subprocess.check_output([str(unrelated)], text=True).strip() == 'operator'
    assert user_temporary.read_text() == 'operator-owned temporary file\n'
    assert (prefix / 'previous').resolve() == prefix / 'releases' / second


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


@pytest.mark.parametrize('direction', ['upgrade', 'rollback'])
@pytest.mark.parametrize('kind', ['file', 'symlink', 'directory'])
def test_user_owned_command_collision_refuses_activation(releases, direction, kind):
    bundle, (first, second), prefix = releases
    active, requested = (first, second) if direction == 'upgrade' else (second, first)
    installer.install(bundle, active, prefix)
    target = prefix / 'bin/spark-node'
    if target.exists():
        target.unlink()
    original = prefix / 'operator-command'
    original.write_text('#!/bin/sh\necho operator\n')
    if kind == 'file':
        target.write_bytes(original.read_bytes())
    elif kind == 'symlink':
        target.symlink_to(original)
    else:
        target.mkdir()
        (target / 'keep').write_text('operator data')
    with pytest.raises(RuntimeError, match='not installer-owned'):
        installer.install(bundle, requested, prefix)
    assert (prefix / 'current').resolve() == prefix / 'releases' / active
    assert subprocess.check_output([str(prefix / 'bin/sparkctl')], text=True).strip() == (
        'first' if active == first else 'second')
    assert original.read_text() == '#!/bin/sh\necho operator\n'
    if kind == 'symlink':
        assert target.is_symlink() and target.resolve() == original
    elif kind == 'directory':
        assert (target / 'keep').read_text() == 'operator data'
    else:
        assert target.read_bytes() == original.read_bytes()


def test_untracked_optional_command_is_not_executed_or_activated(releases):
    bundle, (first, _), prefix = releases
    installer.install(bundle, first, prefix)
    release = prefix / 'releases' / first
    sentinel = prefix / 'unexpected-execution'
    (release / 'scripts/spark-node').write_text(
        'from pathlib import Path\nPath(' + repr(str(sentinel)) + ').write_text("ran")\n')
    installer.install(bundle, first, prefix)
    assert not sentinel.exists()
    assert not (prefix / 'bin/spark-node').exists()
    assert subprocess.check_output([str(prefix / 'bin/sparkctl')], text=True).strip() == 'first'


@pytest.mark.parametrize('releases', ['spark-vector'], indirect=True)
def test_release_missing_baseline_command_is_rejected(releases):
    bundle, (first, _), prefix = releases
    with pytest.raises(RuntimeError, match='missing baseline commands: spark-vector'):
        installer.install(bundle, first, prefix)
    assert not (prefix / 'current').exists()
    assert not (prefix / 'releases' / first).exists()


def test_source_receipt_mismatch_prevents_rollback(releases):
    bundle, (first, second), prefix = releases
    installer.install(bundle, first, prefix)
    installer.install(bundle, second, prefix)
    receipt_path = prefix / 'releases' / first / '.controller-release.json'
    receipt = json.loads(receipt_path.read_text())
    receipt['source_sha256']['scripts/sparkctl'] = '0' * 64
    receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(RuntimeError, match='payload has changed'):
        installer.install(bundle, first, prefix)
    assert subprocess.check_output([str(prefix / 'bin/sparkctl')], text=True).strip() == 'second'
