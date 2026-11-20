import builtins
import importlib.util
from pathlib import Path

import pytest

from fastapi import HTTPException
from huggingface_hub import constants

ROOT = Path(__file__).resolve().parents[1]
PIN = "3de623fc3c33e44ffbe2bad470d0f45bccf2eb21"


@pytest.fixture
def gateway(monkeypatch, tmp_path):
    monkeypatch.delenv("FLUX_MODEL", raising=False)
    monkeypatch.delenv("FLUX_REVISION", raising=False)
    monkeypatch.setattr(constants, "HF_HUB_CACHE", str(tmp_path / "hub"))
    # Importing the gateway or rejecting an absent cache must not import the
    # model runtime, much less allocate a model. Keep the real HF resolver.
    original_import = builtins.__import__

    def no_model_runtime(name, *args, **kwargs):
        if name.split(".")[0] in {"torch", "diffusers"}:
            raise AssertionError(f"Unexpected model runtime import: {name}")
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_model_runtime)

    def load():
        spec = importlib.util.spec_from_file_location("flux_gateway", ROOT / "flux/serve.py")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    return load


def cache_manifest(repo, revision):
    root = Path(constants.HF_HUB_CACHE) / ("models--" + repo.replace("/", "--"))
    snapshot = root / "snapshots" / revision
    snapshot.mkdir(parents=True)
    blob = root / "blobs" / "manifest"
    blob.parent.mkdir(exist_ok=True)
    blob.write_text("{}")
    (snapshot / "model_index.json").symlink_to("../../blobs/manifest")
    return snapshot


def test_refs_only_cache_rejects_before_loading_runtime(gateway):
    module = gateway()
    root = Path(constants.HF_HUB_CACHE) / "models--black-forest-labs--FLUX.1-dev"
    (root / "refs").mkdir(parents=True)
    (root / "refs/main").write_text(PIN)
    with pytest.raises(HTTPException) as error:
        module._load()
    assert error.value.status_code == 503
    assert PIN in error.value.detail
    assert module._pipe is None


def test_pinned_snapshot_ignores_main_and_preserves_snapshot_path(gateway):
    module = gateway()
    snapshot = cache_manifest(module.MODEL_ID, PIN)
    refs = snapshot.parent.parent / "refs"
    refs.mkdir()
    (refs / "main").write_text("a" * 40)
    assert module._local_snapshot() == snapshot
    # A blob path instead of a snapshot path would lose all component paths.
    assert module._local_snapshot() != (snapshot / "model_index.json").resolve().parent


@pytest.mark.parametrize("missing", ["revision", "dangling-manifest"])
def test_other_revision_or_dangling_manifest_is_not_ready(gateway, missing):
    module = gateway()
    snapshot = cache_manifest(module.MODEL_ID, "a" * 40 if missing == "revision" else PIN)
    if missing == "dangling-manifest":
        (snapshot / "model_index.json").resolve().unlink()
    with pytest.raises(HTTPException) as error:
        module._load()
    assert error.value.status_code == 503
    assert module._pipe is None


def test_model_override_uses_its_own_cache_and_pin(gateway, monkeypatch):
    monkeypatch.setenv("FLUX_MODEL", "example/custom-flux")
    monkeypatch.setenv("FLUX_REVISION", "b" * 40)
    snapshot = cache_manifest("example/custom-flux", "b" * 40)
    module = gateway()
    assert module._local_snapshot() == snapshot


@pytest.mark.parametrize("revision", [None, "", "main", "abc123"])
def test_model_override_requires_its_own_full_pin(gateway, monkeypatch, revision):
    cache_manifest("black-forest-labs/FLUX.1-dev", PIN)
    monkeypatch.setenv("FLUX_MODEL", "example/custom-flux")
    if revision is not None:
        monkeypatch.setenv("FLUX_REVISION", revision)
    module = gateway()
    with pytest.raises(HTTPException) as error:
        module._load()
    assert error.value.status_code == 503
    assert "FLUX_REVISION" in error.value.detail
    assert "example/custom-flux" in error.value.detail
