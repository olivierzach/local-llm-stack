import importlib.util
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[1]

@pytest.fixture
def setup(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location('native_fabric', ROOT/'scripts/configure-native-fabric.py')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module.node, 'verify_host', lambda _: None)
    (tmp_path/'.env').write_text('# keep this\nLITELLM_MASTER_KEY=test-secret\nQWEN38_BIND_HOST=127.0.0.1\n')
    return module, tmp_path

@pytest.mark.parametrize('suffix', ['1', '2'])
def test_config_is_host_specific_preserves_credentials_and_is_idempotent(setup, suffix):
    module, root = setup
    local={'hostname':'spark-test', 'fabric':[{'interface':'direct0','ip':'10.10.20.'+suffix}]}
    addresses=[{'ifname':'direct0','addr_info':[{'local':local['fabric'][0]['ip']}]}]
    env=root/'.env'
    original=env.read_text()
    preview=module.configure(root,local,addresses)
    assert not preview['applied'] and env.read_text()==original and not (root/'data').exists()
    result=module.configure(root,local,addresses,True)
    expected='# keep this\nLITELLM_MASTER_KEY=test-secret\nDEEPSEEKV4_BIND_HOST=10.10.20.'+suffix+'\nQWEN38_BIND_HOST=10.10.20.'+suffix+'\n'
    assert env.read_text()==expected
    backup=root/'data/context-guard-routes/before-native-fabric.env'
    assert backup.read_text()==original and backup.stat().st_mode & 0o777==0o600
    assert 'test-secret' not in str(result)
    assert not result['services_restarted'] and not result['models_started']
    module.configure(root,local,addresses,True)
    assert env.read_text()==expected and backup.read_text()==original

@pytest.mark.parametrize('address,interface', [('10.10.20.1','wrong0'),('8.8.8.8','direct0')])
def test_invalid_fabric_cannot_change_env(setup,address,interface):
    module,root=setup
    env=root/'.env'
    original=env.read_bytes()
    local={'hostname':'spark-test','fabric':[{'interface':'direct0','ip':address}]}
    with pytest.raises(ValueError):
        module.configure(root,local,[{'ifname':interface,'addr_info':[{'local':address}]}],True)
    assert env.read_bytes()==original and not (root/'data').exists()
