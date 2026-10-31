"""Warmup payload construction and health-root derivation, no GPU needed."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tools"))
from spark_cluster import warmup


def test_health_root_strips_v1():
    assert warmup.health_root("http://10.10.20.1:8124/v1") == "http://10.10.20.1:8124"
    assert warmup.health_root("http://10.10.20.1:8124") == "http://10.10.20.1:8124"


def test_dspark_payload_runs_enough_draft_steps():
    spec = {"method": "dspark", "num_speculative_tokens": 2,
            "draft_sample_method": "probabilistic", "attention_backend": "B12X"}
    payload = warmup.warmup_speculative_payload("local-deepseek-v4-flash-mia", spec)
    assert payload["model"] == "local-deepseek-v4-flash-mia"
    # >= 2xK drafts so proposals are accepted/rejected, capped at 64
    assert 16 <= payload["max_tokens"] <= 64
    assert payload["temperature"] == 0


def test_payload_caps_and_defaults():
    small = warmup.warmup_speculative_payload("m", {"num_speculative_tokens": 8})
    assert small["max_tokens"] == 64  # 8*8=64
    default = warmup.warmup_speculative_payload("m", None)
    assert default["max_tokens"] == 16
