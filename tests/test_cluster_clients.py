"""Client profiles preserve the routing contract and leave secrets out of files."""
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster.clients import profiles, client_environment
from spark_cluster.config import ConfigError, load, plan
from spark_cluster.gateway import from_plans


def registry():
    return from_plans([plan(*load(ROOT, ROOT / "cluster/inventory.json", ROOT / "cluster/deployments/fast-e8f1.json"))])


@pytest.mark.parametrize("context,port", [("66f1", 4111), ("e8f1", 4112)])
def test_clients_keep_alias_limits_and_capabilities(context, port):
    p = profiles(registry(), context, port)
    provider = "spark-" + context
    omp = p["omp/models.yml"]["providers"][provider]
    claw = p["openclaw/openclaw.json"]["models"]["providers"][provider]
    assert omp["baseUrl"] == claw["baseUrl"] == f"http://127.0.0.1:{port}/v1"
    for model in (omp["models"][0], claw["models"][0]):
        assert model["id"] == "local-fast"
        assert model["contextWindow"] == 8192
        assert model["maxTokens"] == 2048
        assert model["input"] == ["text"]
    assert omp["models"][0]["supportsTools"] is False
    assert claw["models"][0]["compat"]["supportsTools"] is False
    assert p["llm/extra-openai-models.yaml"][0]["model_name"] == "local-fast"
    assert p["aichat/config.yaml"]["function_calling"] is False
    assert p["aichat/config.yaml"]["compress_threshold"] == 0
    assert p["omp/config.yml"]["modelRoles"]["tiny"] == provider + "/local-fast"


def test_secret_only_in_process_environment(tmp_path):
    key = "a-private-test-credential-123456"
    p = profiles(registry(), "e8f1", 4110)
    env = client_environment(tmp_path, key)
    assert key not in json.dumps(p)
    assert env["OPENAI_API_KEY"] == key
    assert Path(env["OPENCLAW_STATE_DIR"]).is_relative_to(tmp_path)


def test_long_context_omp_watchdog_tracks_route_without_leaking_client_specific_fields():
    r = from_plans([plan(*load(ROOT, ROOT/'cluster/inventory.json',
        ROOT/'cluster/deployments/deepseek-tp2-a16-dspark2-graphs-1m-e8f1.json'))])
    p = profiles(r, 'e8f1', 4110)
    omp = p['omp/models.yml']['providers']['spark-e8f1']['models'][0]
    claw = p['openclaw/openclaw.json']['models']['providers']['spark-e8f1']['models'][0]
    assert omp['contextWindow'] == 1048576
    assert omp['compat']['streamIdleTimeoutMs'] == 3600000
    assert 'streamIdleTimeoutMs' not in claw['compat']
    config = p['openclaw/openclaw.json']
    assert config['models']['providers']['spark-e8f1']['timeoutSeconds'] == 3600
    assert config['agents']['defaults']['timeoutSeconds'] == 3600
    short = profiles(registry(), 'e8f1', 4110)
    assert 'streamIdleTimeoutMs' not in short['omp/models.yml']['providers']['spark-e8f1']['models'][0]['compat']


def test_missing_routes_and_invalid_ports_fail():
    with pytest.raises(ConfigError): profiles({"version": 1, "routes": {}}, "e8f1", 4110)
    with pytest.raises(ConfigError): profiles(registry(), "e8f1", 80)


def test_replica_placement_does_not_change_any_client_profile():
    inv,recipe,deployment=load(ROOT,ROOT/'cluster/inventory.json',ROOT/'cluster/deployments/fast-e8f1.json')
    first=plan(inv,recipe,deployment)
    second=plan(inv,recipe,{**deployment,'name':'fast-66f1','nodes':['66f1'],'coordinator':'66f1'})
    single=from_plans([first])
    replicated=from_plans([first,second],replicas=True)
    assert profiles(single,'e8f1',4112)==profiles(replicated,'e8f1',4112)


def test_vision_capability_reaches_all_client_profiles():
    r = from_plans([plan(*load(ROOT, ROOT/'cluster/inventory.json', ROOT/'cluster/deployments/vision-e8f1.json'))])
    p = profiles(r, 'e8f1', 4110)
    assert p['omp/models.yml']['providers']['spark-e8f1']['models'][0]['input'] == ['text', 'image']
    assert p['openclaw/openclaw.json']['models']['providers']['spark-e8f1']['models'][0]['input'] == ['text', 'image']
    assert p['aichat/config.yaml']['clients'][0]['models'][0]['supports_vision'] is True
    assert p['llm/extra-openai-models.yaml'][0]['vision'] is True


def test_aichat_attachments_mount_only_selected_directory_read_only(tmp_path):
    from spark_cluster.clients import attachment_mount
    images = tmp_path/'images with spaces'
    images.mkdir()
    result = attachment_mount(images)
    assert result == ['--mount', f'type=bind,src={images.resolve()},dst={images.resolve()},readonly']
    file = tmp_path/'file'
    file.write_text('not a directory')
    with pytest.raises(ConfigError): attachment_mount(file)
    comma = tmp_path/'unsafe,mount-option'
    comma.mkdir()
    with pytest.raises(ConfigError): attachment_mount(comma)


@pytest.mark.parametrize('coordinator', ['66f1', 'e8f1'])
def test_glm_generated_omp_profile_supports_thinking_tools_and_images(coordinator):
    r = from_plans([plan(*load(ROOT, ROOT/'cluster/inventory.json',
        ROOT/f'cluster/deployments/glm53-tp2-256k-dflash2-{coordinator}.json'))])
    p = profiles(r, coordinator, 4110)
    model = p['omp/models.yml']['providers']['spark-' + coordinator]['models'][0]
    assert model['reasoning'] and model['supportsTools']
    assert model['input'] == ['text', 'image']
    assert model['compat']['reasoningContentField'] == 'reasoning'
    assert model['compat']['extraBody']['chat_template_kwargs']['enable_thinking'] is False
    assert model['compat']['whenThinking']['extraBody']['chat_template_kwargs']['enable_thinking'] is True
    assert profiles(registry(), coordinator, 4110)['omp/models.yml']['providers']['spark-' + coordinator]['models'][0]['reasoning'] is False
