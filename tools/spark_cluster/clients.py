"""Generate isolated client profiles from the gateway's model contract.

JSON is valid YAML: emit one unambiguous format, including for YAML consumers.
Credentials are injected into the launched process, never the profile files.
"""
from pathlib import Path

from .config import integer, name, require
from .gateway import validate_registry


def profiles(registry, context, port):
    validate_registry(registry)
    name(context)
    integer(port, 1024, 65535)
    require(bool(registry["routes"]), "client profiles require at least one model route")
    provider = "spark-" + context
    base = f"http://127.0.0.1:{port}/v1"
    omp, claw, aichat, llm = [], [], [], []
    for alias, route in sorted(registry["routes"].items()):
        caps = route["capabilities"]
        common = {"id": alias, "name": alias, "reasoning": False,
                  "input": ["text", "image"] if caps["vision"] else ["text"],
                  "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0},
                  "contextWindow": route["context_tokens"], "maxTokens": route["max_output_tokens"]}
        compat = {"supportsStore": False, "supportsDeveloperRole": False,
                  "supportsReasoningEffort": False, "maxTokensField": "max_tokens"}
        omp_compat = dict(compat)
        if route.get('request_timeout_s'):
            omp_compat['streamIdleTimeoutMs'] = route['request_timeout_s'] * 1000
        omp.append({**common, "api": "openai-completions", "supportsTools": caps["tools"], "compat": omp_compat})
        claw.append({**common, "compat": {**compat, "supportsTools": caps["tools"]}})
        aichat.append({"name": alias, "max_input_tokens": route["context_tokens"],
                       "max_output_tokens": route["max_output_tokens"], "supports_vision": caps["vision"],
                       "supports_function_calling": caps["tools"]})
        llm.append({"model_id": provider + "/" + alias, "model_name": alias, "api_base": base,
                    "api_key_name": "openai", "can_stream": caps["streaming"],
                    "supports_tools": caps["tools"], "vision": caps["vision"]})
    default = sorted(registry["routes"])[0]
    return {
        "omp/config.yml": {"modelRoles": {role: provider + "/" + default for role in
            ("default", "smol", "slow", "plan", "commit", "tiny", "task", "advisor")},
            "enabledModels": [provider + "/" + alias for alias in sorted(registry["routes"])]},
        "omp/models.yml": {"providers": {provider: {"baseUrl": base, "apiKey": "SPARK_GATEWAY_KEY",
            "api": "openai-completions", "auth": "apiKey", "authHeader": True, "models": omp}}},
        "openclaw/openclaw.json": {"models": {"mode": "merge", "providers": {provider: {
            "baseUrl": base, "apiKey": "${SPARK_GATEWAY_KEY}", "api": "openai-completions", "models": claw}}},
            "agents": {"defaults": {"model": {"primary": provider + "/" + default}}}},
        "aichat/config.yaml": {"model": "spark:" + default, "stream": True, "save": False,
            "function_calling": registry["routes"][default]["capabilities"]["tools"],
            "clients": [{"type": "openai-compatible", "name": "spark", "api_base": base, "models": aichat}]},
        "llm/extra-openai-models.yaml": llm,
    }


def client_environment(root, key):
    root = Path(root).resolve()
    return {"SPARK_GATEWAY_KEY": key, "SPARK_API_KEY": key, "OPENAI_API_KEY": key,
            "PI_CODING_AGENT_DIR": str(root / "omp"),
            "OPENCLAW_CONFIG_PATH": str(root / "openclaw/openclaw.json"),
            "OPENCLAW_STATE_DIR": str(root / "openclaw/state"),
            "AICHAT_CONFIG_DIR": str(root / "aichat"), "LLM_USER_PATH": str(root / "llm")}


def attachment_mount(directory):
    """Expose only an explicitly selected attachment folder to container AIChat."""
    directory = Path(directory).expanduser().resolve(strict=True)
    require(directory.is_dir(), "attachment directory must be a directory")
    require("," not in str(directory), "Docker attachment paths must not contain commas")
    return ["--mount", f"type=bind,src={directory},dst={directory},readonly"]
