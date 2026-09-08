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
    assert p["omp/config.yml"]["modelRoles"]["tiny"] == provider + "/local-fast"


def test_secret_only_in_process_environment(tmp_path):
    key = "a-private-test-credential-123456"
    p = profiles(registry(), "e8f1", 4110)
    env = client_environment(tmp_path, key)
    assert key not in json.dumps(p)
    assert env["OPENAI_API_KEY"] == key
    assert Path(env["OPENCLAW_STATE_DIR"]).is_relative_to(tmp_path)


def test_missing_routes_and_invalid_ports_fail():
    with pytest.raises(ConfigError): profiles({"version": 1, "routes": {}}, "e8f1", 4110)
    with pytest.raises(ConfigError): profiles(registry(), "e8f1", 80)
