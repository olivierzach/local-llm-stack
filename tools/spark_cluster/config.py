"""Strict versioned configuration and side-effect-free deployment planning."""
from __future__ import annotations

import hashlib
import ipaddress
import json
from pathlib import Path
import re


class ConfigError(ValueError):
    pass


def require(condition, message):
    if not condition:
        raise ConfigError(message)


def fields(value, required, optional=()):
    require(isinstance(value, dict), "expected an object")
    require(not (set(required) - value.keys()), f"missing fields: {set(required) - value.keys()}")
    require(not (value.keys() - set(required) - set(optional)),
            f"unknown fields: {value.keys() - set(required) - set(optional)}")


def name(value):
    require(isinstance(value, str) and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,47}", value),
            f"invalid identifier: {value!r}")
    return value


def integer(value, low, high):
    require(type(value) is int and low <= value <= high, f"expected integer {low}..{high}")


def absolute(value):
    require(isinstance(value, str) and value.startswith("/") and
            ".." not in Path(value).parts and not any(c in value for c in "\n\r\x00:$"),
            "expected an absolute path without traversal or Compose interpolation")


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def read(path):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            require(key not in result, f"duplicate JSON key: {key}")
            result[key] = value
        return result
    return json.loads(Path(path).read_text(), object_pairs_hook=unique)


def validate_inventory(inv):
    fields(inv, ("version", "nodes"))
    require(inv["version"] == 1, "unsupported inventory version")
    require(isinstance(inv["nodes"], dict) and inv["nodes"], "nodes must be a nonempty mapping")
    hosts, ips = set(), set()
    for key, node in inv["nodes"].items():
        name(key)
        fields(node, ("hostname", "ssh", "architecture", "gpus", "cache", "projects", "fabric"))
        require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", node["hostname"]), "invalid hostname")
        require(node["hostname"] not in hosts, "duplicate hostname")
        hosts.add(node["hostname"])
        require(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.@-]*", node["ssh"]), "invalid SSH target")
        require(node["architecture"] in ("aarch64", "x86_64"), "unsupported architecture")
        integer(node["gpus"], 1, 8)
        absolute(node["cache"])
        absolute(node["projects"])
        require(isinstance(node["fabric"], list) and node["fabric"], "fabric required")
        interfaces = set()
        for rail in node["fabric"]:
            fields(rail, ("interface", "ip", "rdma"))
            for key in ("interface", "rdma"):
                require(re.fullmatch(r"[A-Za-z0-9_]+", rail[key]), "invalid interface")
            ipaddress.IPv4Address(rail["ip"])
            require(rail["ip"] not in ips and rail["interface"] not in interfaces,
                    "duplicate fabric address/interface")
            ips.add(rail["ip"])
            interfaces.add(rail["interface"])


def validate_recipe(r):
    fields(r, ("version", "kind", "image", "model", "revision", "alias", "context_tokens",
               "max_output_tokens", "capabilities", "dtype", "gpu_memory_utilization",
               "min_available_mib", "max_num_seqs", "extra_args", "parallelism", "validation"),
           ("tool_call_parser", "default_chat_template_kwargs", "image_processing", "runtime_cache", "load_strategy", "mamba_cache_mode", "speculative_config", "async_scheduling", "nccl_launch_order_implicit", "flashinfer_sampler", "disable_pynccl", "nccl_library", "deepseek_v4"))
    require(r["version"] == 1 and r["kind"] == "vllm", "unsupported recipe version/kind")
    require(re.fullmatch(r"[a-zA-Z0-9./_-]+@sha256:[0-9a-f]{64}", r["image"]),
            "image must be an immutable registry digest")
    require(re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", r["model"]), "invalid HF model")
    require(re.fullmatch(r"[0-9a-f]{40}", r["revision"]), "model revision must be a commit SHA")
    name(r["alias"])
    integer(r["context_tokens"], 256, 2097152)
    integer(r["max_output_tokens"], 1, r["context_tokens"] - 1)
    integer(r["min_available_mib"], 1024, 4194304)
    integer(r["max_num_seqs"], 1, 4096)
    require(type(r["gpu_memory_utilization"]) in (float, int) and
            0.01 <= r["gpu_memory_utilization"] <= 0.95, "invalid GPU memory utilization")
    require(r["dtype"] in ("auto", "bfloat16", "float16", "float32"), "invalid dtype")
    fields(r["capabilities"], ("text", "vision", "tools", "streaming"))
    require(all(type(v) is bool for v in r["capabilities"].values()), "capabilities must be booleans")
    require(r.get("tool_call_parser") in (None, "hermes", "deepseek_v4"), "unsupported tool parser")
    require(r["capabilities"]["tools"] == bool(r.get("tool_call_parser")),
            "tool capability requires an explicit supported parser")
    if "default_chat_template_kwargs" in r:
        kwargs = r["default_chat_template_kwargs"]
        if "deepseek_v4" in r:
            fields(kwargs, ("thinking", "reasoning_effort"))
            require(type(kwargs["thinking"]) is bool, "thinking must be a boolean")
            require(kwargs["reasoning_effort"] in ("low", "high", "max"), "unsupported reasoning effort")
            if kwargs["reasoning_effort"] == "max":
                require(r["context_tokens"] >= 393216, "max reasoning requires at least 384K context")
        else:
            fields(kwargs, ("enable_thinking",))
            require(type(kwargs["enable_thinking"]) is bool, "enable_thinking must be a boolean")
    if "deepseek_v4" in r:
        ds = r["deepseek_v4"]
        fields(ds, ("backend", "kv_cache_dtype", "block_size", "max_num_batched_tokens"),
               ("cudagraph_capture_size",))
        require(ds["backend"] == "b12x", "unsupported DeepSeek V4 backend")
        require(ds["kv_cache_dtype"] == "fp8", "unsupported DeepSeek V4 KV dtype")
        require(type(ds["block_size"]) is int and ds["block_size"] == 256, "DeepSeek V4 requires block size 256")
        integer(ds["max_num_batched_tokens"], 256, 32768)
        if "cudagraph_capture_size" in ds:
            integer(ds["cudagraph_capture_size"], 1, 128)
            require("--enforce-eager" not in r["extra_args"], "CUDA graphs conflict with eager execution")
        require(r["model"] == "deepseek-ai/DeepSeek-V4-Flash-0731", "DeepSeek V4 profile requires the 0731 checkpoint")
        require(r.get("tool_call_parser") == "deepseek_v4", "DeepSeek V4 requires its tokenizer/tool parser")
    elif r.get("tool_call_parser") == "deepseek_v4":
        raise ConfigError("DeepSeek V4 parser requires its runtime profile")
    if "mamba_cache_mode" in r:
        require(r["mamba_cache_mode"] in ("none", "align"), "unsupported hybrid cache mode")
    if "async_scheduling" in r:
        require(type(r["async_scheduling"]) is bool, "async_scheduling must be a boolean")
    if "nccl_launch_order_implicit" in r:
        require(type(r["nccl_launch_order_implicit"]) is bool,
                "nccl_launch_order_implicit must be a boolean")
    if "flashinfer_sampler" in r:
        require(type(r["flashinfer_sampler"]) is bool, "flashinfer_sampler must be a boolean")
    if "disable_pynccl" in r:
        require(type(r["disable_pynccl"]) is bool, "disable_pynccl must be a boolean")
    if "nccl_library" in r:
        fields(r['nccl_library'], ('version', 'sha256'))
        require(isinstance(r['nccl_library']['version'], str) and re.fullmatch(r'2\.[0-9]+\.[0-9]+', r['nccl_library']['version']), 'invalid NCCL version')
        require(isinstance(r['nccl_library']['sha256'], str) and re.fullmatch(r'[a-f0-9]{64}', r['nccl_library']['sha256']), 'invalid NCCL SHA-256')
    if "speculative_config" in r:
        speculative = r["speculative_config"]
        require(isinstance(speculative, dict), "speculative_config must be an object")
        if speculative.get("method") == "dspark":
            fields(speculative, ("method", "num_speculative_tokens", "draft_sample_method", "attention_backend"))
            require("deepseek_v4" in r, "DSpark requires the DeepSeek V4 runtime profile")
            require(speculative["draft_sample_method"] == "probabilistic", "unsupported DSpark sampling")
            require(speculative["attention_backend"] == "B12X", "DSpark requires B12X attention")
        else:
            fields(speculative, ("method", "num_speculative_tokens"))
            require(speculative["method"] == "mtp", "unsupported speculative method")
            require("deepseek_v4" not in r, "0731 carries DSpark, not an MTP head")
        integer(speculative["num_speculative_tokens"], 1, 8)
    if "load_strategy" in r:
        require(r["load_strategy"] in ("lazy", "eager", "prefetch"), "unsupported weight load strategy")
    if "runtime_cache" in r:
        require(type(r["runtime_cache"]) is bool, "runtime_cache must be a boolean")
    if "image_processing" in r:
        require(r["capabilities"]["vision"], "image processing requires vision capability")
        image = r["image_processing"]
        fields(image, ("max_images", "min_pixels", "max_pixels"))
        integer(image["max_images"], 1, 8)
        integer(image["min_pixels"], 1024, 16777216)
        integer(image["max_pixels"], image["min_pixels"], 16777216)
    # Keep lifecycle-critical options under manifest control. Extend this allowlist
    # alongside a pinned runtime recipe and its validation, not arbitrary overrides.
    require(isinstance(r["extra_args"], list) and
            all(a in ("--enforce-eager", "--enable-prefix-caching", "--disable-custom-all-reduce")
                for a in r["extra_args"]), "unsupported extra vLLM argument")
    require(isinstance(r["parallelism"], list) and r["parallelism"] and
            set(r["parallelism"]) <= {"single", "tensor", "pipeline"}, "invalid parallelism")


def load(root, inventory, deployment):
    inv = read(inventory)
    validate_inventory(inv)
    d = read(deployment)
    fields(d, ("version", "name", "recipe", "nodes", "coordinator", "mode",
               "tensor_parallel", "pipeline_parallel", "port"), ("master_port",))
    require(d["version"] == 1, "unsupported deployment version")
    name(d["name"])
    name(d["recipe"])
    require(isinstance(d["nodes"], list) and d["nodes"] and
            all(isinstance(n, str) for n in d["nodes"]), "nodes must be a nonempty list")
    require(len(set(d["nodes"])) == len(d["nodes"]), "duplicate node placement")
    require(set(d["nodes"]) <= inv["nodes"].keys(), "unknown node")
    require(d["coordinator"] in d["nodes"], "coordinator must be a deployment member")
    integer(d["port"], 1024, 65535)
    if "master_port" in d:
        integer(d["master_port"], 1024, 65535)
        require(d["master_port"] != d["port"], "master and API ports must differ")
    integer(d["tensor_parallel"], 1, 64)
    integer(d["pipeline_parallel"], 1, 64)
    r = read(Path(root) / "cluster" / "recipes" / (d["recipe"] + ".json"))
    validate_recipe(r)
    require(d["mode"] in r["parallelism"], "recipe does not support this parallelism mode")
    gpus = sum(inv["nodes"][n]["gpus"] for n in d["nodes"])
    require(d["tensor_parallel"] * d["pipeline_parallel"] == gpus,
            "TP × PP must equal allocated GPUs; use independent deployments for replicas")
    if d["mode"] == "single":
        require(len(d["nodes"]) == 1 and gpus == 1, "single mode requires one GPU on one node")
    elif d["mode"] == "tensor":
        require(d["tensor_parallel"] > 1 and d["pipeline_parallel"] == 1, "invalid tensor placement")
    elif d["mode"] == "pipeline":
        require(d["pipeline_parallel"] > 1, "invalid pipeline placement")
    return inv, r, d


def runtime_cache_name(recipe, deployment, node_id, architecture):
    # Validation prose and the public alias do not change compiled computation.
    compute = {key: value for key, value in recipe.items() if key not in ("validation", "alias", "load_strategy")}
    ordered = [deployment["coordinator"]] + [n for n in deployment["nodes"] if n != deployment["coordinator"]]
    identity = {"cache_format": 1, "recipe": compute, "architecture": architecture,
                "mode": deployment["mode"], "tensor_parallel": deployment["tensor_parallel"],
                "pipeline_parallel": deployment["pipeline_parallel"], "rank": ordered.index(node_id)}
    return "spark-runtime-" + hashlib.sha256(canonical(identity).encode()).hexdigest()


def plan(inv, recipe, deployment):
    """Return an immutable desired state. Rendering never contacts a host."""
    require(deployment["mode"] in recipe["parallelism"], "recipe does not support this parallelism mode")
    if recipe.get("speculative_config"):
        require(deployment["pipeline_parallel"] == 1,
                "MTP with pipeline parallelism is not validated by this controller")
    if deployment["mode"] != "single":
        require("master_port" in deployment, "distributed deployments require an explicit master_port")
    identity = {"renderer": 2, "recipe": recipe, "deployment": deployment,
                "nodes": {n: inv["nodes"][n] for n in deployment["nodes"]}}
    digest = hashlib.sha256(canonical(identity).encode()).hexdigest()
    owner = deployment["name"] + "-" + digest[:12]
    result = {"version": 1, "owner": owner, "digest": digest, **identity, "compose": {}}
    for node_id, node in identity["nodes"].items():
        ip = node["fabric"][0]["ip"]
        model_path = "/cache/hub/models--" + recipe["model"].replace("/", "--") + "/snapshots/" + recipe["revision"]
        cmd = ["-m", "vllm.entrypoints.openai.api_server", "--model", model_path,
               "--served-model-name", recipe["alias"], "--host", ip,
               "--port", str(deployment["port"]), "--dtype", recipe["dtype"],
               "--max-model-len", str(recipe["context_tokens"]),
               "--gpu-memory-utilization", str(recipe["gpu_memory_utilization"]),
               "--max-num-seqs", str(recipe["max_num_seqs"])] + recipe["extra_args"]
        if "mamba_cache_mode" in recipe:
            cmd += ["--mamba-cache-mode", recipe["mamba_cache_mode"]]
        if "speculative_config" in recipe:
            cmd += ["--speculative-config", canonical(recipe["speculative_config"])]
        if "async_scheduling" in recipe:
            cmd += ["--async-scheduling" if recipe["async_scheduling"] else "--no-async-scheduling"]
        if "load_strategy" in recipe:
            cmd += ["--safetensors-load-strategy", recipe["load_strategy"]]
        if recipe.get("tool_call_parser"):
            cmd += ["--enable-auto-tool-choice", "--tool-call-parser", recipe["tool_call_parser"]]
        if "default_chat_template_kwargs" in recipe:
            cmd += ["--default-chat-template-kwargs", canonical(recipe["default_chat_template_kwargs"])]
        if "deepseek_v4" in recipe:
            ds = recipe["deepseek_v4"]
            cmd += ["--tokenizer-mode", "deepseek_v4", "--reasoning-parser", "deepseek_v4",
                    "--reasoning-config", canonical({"reasoning_parser": "deepseek_v4", "reasoning_start_str": "", "reasoning_end_str": ""}),
                    "--kv-cache-dtype", ds["kv_cache_dtype"], "--block-size", str(ds["block_size"]),
                    "--max-num-batched-tokens", str(ds["max_num_batched_tokens"]),
                    "--moe-backend", "b12x", "--linear-backend", "b12x", "--attention-backend", "B12X"]
            if "cudagraph_capture_size" in ds:
                cmd += ["--max-cudagraph-capture-size", str(ds["cudagraph_capture_size"]),
                        "--compilation-config", canonical({"cudagraph_mode": "FULL_AND_PIECEWISE", "custom_ops": ["all"]})]
        if "image_processing" in recipe:
            image = recipe["image_processing"]
            cmd += ["--limit-mm-per-prompt", canonical({"image": image["max_images"], "video": 0}),
                    "--mm-processor-kwargs", canonical({"min_pixels": image["min_pixels"], "max_pixels": image["max_pixels"]})]
        service = {
            "image": recipe["image"], "pull_policy": "never", "init": True,
            "container_name": "spark-" + owner, "labels": {"io.spark.owner": owner, "io.spark.digest": digest},
            "network_mode": "host", "ipc": "host", "gpus": "all", "restart": "no",
            "entrypoint": ["python3"], "command": cmd,
            "environment": {"HF_HOME": "/cache", "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
                            "VLLM_NO_USAGE_STATS": "1", "DO_NOT_TRACK": "1"},
            "volumes": [{"type": "bind", "source": node["cache"], "target": "/cache", "read_only": True}],
            "logging": {"driver": "json-file", "options": {"max-size": "20m", "max-file": "3"}},
            "healthcheck": {"test": ["CMD", "python3", "-c",
                           f"import urllib.request; urllib.request.urlopen('http://{ip}:{deployment['port']}/health', timeout=2)"],
                            "interval": "10s", "timeout": "3s", "retries": 6, "start_period": "180s"}
        }
        if "flashinfer_sampler" in recipe:
            service["environment"]["VLLM_USE_FLASHINFER_SAMPLER"] = (
                "1" if recipe["flashinfer_sampler"] else "0")
        if "deepseek_v4" in recipe:
            # Fixed, reviewed kernel switches; no arbitrary environment overrides
            # can replace admission, offline loading or the direct fabric settings.
            service["environment"].update({
                "CUTE_DSL_ARCH": "sm_121a", "VLLM_USE_AOT_COMPILE": "1",
                "VLLM_USE_BREAKABLE_CUDAGRAPH": "0", "VLLM_USE_MEGA_AOT_ARTIFACT": "1",
                "VLLM_MEMORY_PROFILE_INCLUDE_ATTN": "1", "VLLM_USE_B12X_WO_PROJECTION": "1",
                "VLLM_USE_B12X_MHC": "1", "VLLM_USE_B12X_FP8_GEMM": "1",
                "VLLM_USE_B12X_MOE": "1", "VLLM_USE_B12X_SPARSE_INDEXER": "1",
                "VLLM_USE_V2_MODEL_RUNNER": "1", "VLLM_MOE_SKIP_PADDING": "0",
                "B12X_MLA_SM120_UNIFIED": "1", "B12X_MOE_FORCE_A8": "1"})
        if "disable_pynccl" in recipe:
            service["environment"]["VLLM_DISABLE_PYNCCL"] = (
                "1" if recipe["disable_pynccl"] else "0")
        if "nccl_library" in recipe:
            library = recipe['nccl_library']
            source = str(Path(node['cache']).parent / 'runtime-libraries/nccl' / library['sha256'] / 'libnccl.so.2')
            target = '/usr/local/lib/python3.12/dist-packages/nvidia/nccl/lib/libnccl.so.2'
            service['volumes'].append({'type': 'bind', 'source': source, 'target': target, 'read_only': True})
            service['environment']['VLLM_NCCL_SO_PATH'] = target
        if recipe.get("runtime_cache"):
            service["volumes"].append({"type": "volume", "source": "runtime-cache", "target": "/root/.cache"})
            service["environment"].update({"VLLM_CACHE_ROOT": "/root/.cache/vllm",
                "TORCHINDUCTOR_CACHE_DIR": "/root/.cache/torchinductor", "TRITON_CACHE_DIR": "/root/.cache/triton",
                "CUDA_CACHE_PATH": "/root/.cache/nvidia"})
        if deployment["mode"] != "single":
            # Pinned vLLM 0.25.1 supports native multi-node MP. Each container is
            # supervised by Docker; coordinator rank is a deployment choice.
            coordinator = deployment["coordinator"]
            master_ip = inv["nodes"][coordinator]["fabric"][0]["ip"]
            ordered = [coordinator] + [n for n in deployment["nodes"] if n != coordinator]
            service["entrypoint"] = ["vllm"]
            service["command"] = ["serve", model_path] + cmd[4:] + [
                "--distributed-executor-backend", "mp",
                "--tensor-parallel-size", str(deployment["tensor_parallel"]),
                "--pipeline-parallel-size", str(deployment["pipeline_parallel"]),
                "--nnodes", str(len(ordered)), "--node-rank", str(ordered.index(node_id)),
                "--master-addr", master_ip, "--master-port", str(deployment["master_port"])]
            if node_id != coordinator:
                service["command"].append("--headless")
            service["environment"].update({
                "VLLM_HOST_IP": ip, "NCCL_SOCKET_IFNAME": "=" + node["fabric"][0]["interface"],
                "GLOO_SOCKET_IFNAME": node["fabric"][0]["interface"],
                "NCCL_IB_HCA": "=" + ",".join(r["rdma"] + ":1" for r in node["fabric"]),
                "NCCL_IB_GID_INDEX": "3", "NCCL_IB_DISABLE": "0",
                "NCCL_DEBUG": "INFO", "NCCL_DEBUG_SUBSYS": "INIT,NET"})
            if "nccl_launch_order_implicit" in recipe:
                service["environment"]["NCCL_LAUNCH_ORDER_IMPLICIT"] = (
                    "1" if recipe["nccl_launch_order_implicit"] else "0")
            service["devices"] = ["/dev/infiniband:/dev/infiniband"]
            service["cap_add"] = ["IPC_LOCK"]
            service["ulimits"] = {"memlock": {"soft": -1, "hard": -1}}
            service["healthcheck"]["test"][-1] = (
                f"import urllib.request; urllib.request.urlopen('http://{master_ip}:{deployment['port']}/health', timeout=2)")
        result["compose"][node_id] = {"name": "spark-" + owner, "services": {"worker": service}}
        if recipe.get("runtime_cache"):
            result["compose"][node_id]["volumes"] = {"runtime-cache": {
                "name": runtime_cache_name(recipe, deployment, node_id, node["architecture"]), "external": True}}
    coordinator = inv["nodes"][deployment["coordinator"]]
    result["endpoint"] = {"alias": recipe["alias"],
                          "base_url": f"http://{coordinator['fabric'][0]['ip']}:{deployment['port']}/v1",
                          "context_tokens": recipe["context_tokens"],
                          "max_output_tokens": recipe["max_output_tokens"],
                          "capabilities": recipe["capabilities"], "ready": False}
    return result


def validate_saved_plan(p):
    """Validate recovery identity without re-rendering an older deployment.

    Saved plans are recovery handles, not signatures. Remote operations still
    verify the on-host reservation, container IDs and ownership labels.
    """
    fields(p, ("version", "owner", "digest", "recipe", "deployment", "nodes", "compose", "endpoint"),
           ("renderer",))
    require(p["version"] == 1, "unsupported saved plan version")
    validate_inventory({"version": 1, "nodes": p["nodes"]})
    validate_recipe(p["recipe"])
    identity = {k: p[k] for k in ("recipe", "deployment", "nodes")}
    if "renderer" in p:
        integer(p["renderer"], 1, 2147483647)
        identity["renderer"] = p["renderer"]
    digest = hashlib.sha256(canonical(identity).encode()).hexdigest()
    require(p["digest"] == digest and p["owner"] == p["deployment"]["name"] + "-" + digest[:12],
            "saved plan identity does not match its inputs")
    require(set(p["compose"]) == set(p["nodes"]) == set(p["deployment"]["nodes"]),
            "saved plan node membership mismatch")
    require(p["deployment"]["coordinator"] in p["nodes"], "invalid saved coordinator")
    for compose in p["compose"].values():
        require(compose["name"] == "spark-" + p["owner"] and set(compose["services"]) == {"worker"},
                "saved Compose ownership mismatch")
        worker = compose["services"]["worker"]
        require(worker["container_name"] == "spark-" + p["owner"] and
                worker["labels"] == {"io.spark.owner": p["owner"], "io.spark.digest": digest} and
                worker["image"] == p["recipe"]["image"], "saved worker ownership mismatch")
