"""Read-only, bounded Linux telemetry helper, transported as source by sparkctl."""
from pathlib import Path
import csv
import io
import json
import math
import os
import platform
import re
import selectors
import subprocess
import sys
import time
from urllib.request import HTTPRedirectHandler, ProxyHandler, build_opener

# Linux IB port_*_data counters count four-octet words, not bytes.
RDMA_COUNTERS = {
    "port_xmit_data": ("tx_bytes", 4), "port_rcv_data": ("rx_bytes", 4),
    "port_xmit_packets": ("tx_packets", 1), "port_rcv_packets": ("rx_packets", 1),
    "port_rcv_errors": ("rx_errors", 1), "port_xmit_discards": ("tx_discards", 1),
    "port_rcv_remote_physical_errors": ("rx_remote_physical_errors", 1),
    "symbol_error": ("symbol_errors", 1), "link_error_recovery": ("link_recoveries", 1),
    "link_downed": ("link_downs", 1), "port_xmit_wait": ("tx_wait_ticks", 1),
}
RDMA_HARDWARE = (
    "rx_write_requests", "rx_read_requests", "rx_atomic_requests", "out_of_buffer",
    "out_of_sequence", "packet_seq_err", "local_ack_timeout_err", "rnr_nak_retry_err",
    "implied_nak_seq_err", "req_cqe_error", "resp_cqe_error", "duplicate_request",
    "np_cnp_sent", "np_ecn_marked_roce_packets", "rp_cnp_handled", "rp_cnp_ignored",
)
GPU_FIELDS = {
    "utilization.gpu": ("busy_ratio", .01), "temperature.gpu": ("temperature_celsius", 1),
    "power.draw": ("power_watts", 1), "memory.total": ("memory_total_bytes", 1048576),
    "memory.used": ("memory_used_bytes", 1048576), "memory.free": ("memory_free_bytes", 1048576),
}
NET_COUNTERS = ("rx_bytes", "tx_bytes", "rx_packets", "tx_packets", "rx_errors", "tx_errors", "rx_dropped", "tx_dropped")
# mlx5 *_phy counters describe the external physical port, not a vport or PCIe path.
PHYSICAL_COUNTERS = {
    "tx_bytes_phy": "tx_bytes", "rx_bytes_phy": "rx_bytes",
    "rx_crc_errors_phy": "rx_crc_errors", "rx_discards_phy": "rx_discards",
    "link_down_events_phy": "link_down_events", "rx_corrected_bits_phy": "rx_corrected_bits",
    "rx_pause_ctrl_phy": "rx_pause_ctrl", "tx_pause_ctrl_phy": "tx_pause_ctrl",
}
ETHTOOL_MAX_OUTPUT = 256 * 1024


def bounded_ethtool(interface, timeout):
    """Drain a bounded stdout pipe; never run a shell, sudo or a mutating operation."""
    deadline = time.monotonic() + timeout
    process = subprocess.Popen(["ethtool", "-S", interface], stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    output = bytearray()
    try:
        with selectors.DefaultSelector() as selector:
            os.set_blocking(process.stdout.fileno(), False)
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise subprocess.TimeoutExpired(process.args, timeout)
                chunk = os.read(process.stdout.fileno(), min(8192, ETHTOOL_MAX_OUTPUT + 1 - len(output)))
                if not chunk:
                    break
                output.extend(chunk)
                if len(output) > ETHTOOL_MAX_OUTPUT:
                    raise ValueError("physical counter output too large")
        code = process.wait(timeout=max(.001, deadline - time.monotonic()))
        if code:
            raise ValueError("physical counters unavailable")
        return output.decode("ascii")
    finally:
        if process.poll() is None:
            process.kill()
        process.stdout.close()
        try:
            process.wait(timeout=.2)
        except subprocess.TimeoutExpired:
            pass


def parse_physical_counters(output):
    """Accept only exact external-port counters; never sum ring/vport counters."""
    counters = {}
    for line in output.splitlines():
        name, separator, value = line.strip().partition(":")
        if separator and name in PHYSICAL_COUNTERS:
            value = value.strip()
            key = PHYSICAL_COUNTERS[name]
            if key in counters:
                raise ValueError("duplicate physical counter")
            if value.isascii() and value.isdecimal() and len(value) <= 20:
                number = int(value)
                if number <= 2**64 - 1:
                    counters[key] = number
    return counters


def physical_metrics(ports, budget=2, per_call_timeout=.75):
    """One canonical netdev per mapped physical port, under a shared deadline."""
    deadline = time.monotonic() + budget
    result = {}
    for port in ports[:64]:
        link, interface = port["physical_link"], port["interface"]
        if link in result:
            continue
        values = {"interface": interface, "counters": {}}
        result[link] = values
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,31}", interface):
            continue
        try:
            output = bounded_ethtool(interface, min(per_call_timeout, remaining))
            values["counters"] = parse_physical_counters(output)
            values["sampled_at"] = time.monotonic()
        except (OSError, ValueError, UnicodeError, subprocess.TimeoutExpired):
            pass
    return result


def text(path):
    try:
        with Path(path).open() as stream:
            return stream.read(65536).strip()
    except (OSError, UnicodeError):
        return None


def number(path):
    value = text(path)
    return int(value) if value is not None and value.isdecimal() else None


def gpu_metrics(budget=4):
    """Query only supported read-only fields. An unsupported field cannot hide others."""
    deadline = time.monotonic() + budget
    rows = {}
    available = False
    for field, (key, scale) in GPU_FIELDS.items():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            result = subprocess.run(["nvidia-smi", "--query-gpu=index," + field,
                                     "--format=csv,noheader,nounits"],
                                    capture_output=True, text=True, timeout=min(1, remaining))
            if result.returncode:
                continue
            available = True
            for row in list(csv.reader(io.StringIO(result.stdout)))[:32]:
                if len(row) != 2 or not row[0].strip().isdecimal():
                    continue
                gpu = rows.setdefault(row[0].strip(), {})
                try:
                    value = float(row[1].strip()) * scale
                    if math.isfinite(value) and value >= 0:
                        gpu[key] = value
                except ValueError:
                    pass  # N/A is missing, never zero on unified-memory GPUs.
        except (OSError, subprocess.TimeoutExpired):
            continue
    return {"up": available, "devices": rows}


def collect(node, physical_ports=()):
    if platform.node() != node["hostname"] or platform.machine() != node["architecture"]:
        raise ValueError("host identity mismatch")
    host = {}
    memory = text("/proc/meminfo")
    if memory:
        allowed = {"MemTotal": "memory_total_bytes", "MemAvailable": "memory_available_bytes",
                   "SwapTotal": "swap_total_bytes", "SwapFree": "swap_free_bytes"}
        for line in memory.splitlines():
            parts = line.split()
            if len(parts) == 3 and parts[0][:-1] in allowed and parts[1].isdecimal() and parts[2] == "kB":
                host[allowed[parts[0][:-1]]] = int(parts[1]) * 1024
    cpu = text("/proc/stat")
    if cpu:
        try:
            ticks = [int(v) for v in cpu.splitlines()[0].split()[1:9]]
            hz = os.sysconf("SC_CLK_TCK")
            for mode, value in zip(("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"), ticks):
                host["cpu_" + mode + "_seconds_total"] = value / hz
        except (ValueError, OSError):
            pass
    try:
        host["load1"] = os.getloadavg()[0]
        host["cpu_count"] = os.cpu_count()
    except OSError:
        pass
    disks = {}
    for label, path in (("root", "/"), ("cache", node["cache"])):
        try:
            fs = os.statvfs(path)
            disks[label] = {"size_bytes": fs.f_blocks * fs.f_frsize,
                            "available_bytes": fs.f_bavail * fs.f_frsize}
        except OSError:
            disks[label] = {}
    interfaces = {}
    # All local netdevs allow management/Wi-Fi observation without guessing addresses.
    for net in sorted(Path("/sys/class/net").glob("*"))[:128]:
        data = {"present": True, "counters": {}}
        for key in ("carrier", "speed"):
            value = number(net / key)
            if value is not None:
                data["speed_bits_per_second" if key == "speed" else key] = value * 1000000 if key == "speed" else value
        for key in NET_COUNTERS:
            value = number(net / "statistics" / key)
            if value is not None:
                data["counters"][key] = value
        interfaces[net.name] = data
    rdma = {}
    for rail in node["fabric"]:
        interfaces.setdefault(rail["interface"], {"present": False, "counters": {}})
        root = Path("/sys/class/infiniband") / rail["rdma"] / "ports" / "1"
        data = {"present": root.is_dir(), "counters": {}}
        state = text(root / "state")
        if state and state.split(":")[0].isdecimal():
            data["active"] = int(state.split(":")[0]) == 4
        for source, (key, multiplier) in RDMA_COUNTERS.items():
            value = number(root / "counters" / source)
            if value is not None:
                data["counters"][key] = value * multiplier
        for key in RDMA_HARDWARE:
            value = number(root / "hw_counters" / key)
            if value is not None:
                data["counters"][key] = value
        rdma[rail["rdma"]] = data
    return {"version": 1, "hostname": platform.node(), "architecture": platform.machine(),
            "collected_at": time.time(), "host": host, "disks": disks,
            "interfaces": interfaces, "rdma": rdma, "physical": physical_metrics(physical_ports),
            "gpu": gpu_metrics()}


def collect_engine(node, engine, timeout):
    """Read one owned engine over its coordinator's network, never generate tokens."""
    if platform.node() != node["hostname"] or platform.machine() != node["architecture"]:
        raise ValueError("engine host identity mismatch")
    if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 120:
        raise ValueError("invalid engine deadline")
    worker = engine["worker"]
    owner, digest = engine["owner"], engine["digest"]
    if (not re.fullmatch(r"[a-zA-Z0-9_.-]{1,128}", owner)
            or not re.fullmatch(r"[a-f0-9]{64}", digest)
            or worker["container_name"] != "spark-" + owner):
        raise ValueError("invalid engine identity")
    address = node.get("serving", {}).get("address", node["fabric"][0]["ip"])
    port = engine["port"]
    if type(port) is not int or not 1 <= port <= 65535:
        raise ValueError("invalid engine port")
    base = f"http://{address}:{port}"
    deadline = time.monotonic() + timeout

    def remaining():
        value = deadline - time.monotonic()
        if value <= 0:
            raise TimeoutError("engine collection deadline")
        return value

    def identity():
        result = subprocess.run(
            ["docker", "inspect", "--type", "container", "--format",
             '{"id":{{json .Id}},"name":{{json .Name}},"running":{{json .State.Running}},'
             '"started":{{json .State.StartedAt}},"restarts":{{json .RestartCount}},'
             '"labels":{{json .Config.Labels}},"image":{{json .Config.Image}},'
             '"command":{{json .Config.Cmd}},"entrypoint":{{json .Config.Entrypoint}}}',
             worker["container_name"]], capture_output=True, text=True, timeout=remaining(), check=True)
        if len(result.stdout) > 131072:
            raise ValueError("engine identity too large")
        item = json.loads(result.stdout)
        if (item["name"] != "/" + worker["container_name"] or item["running"] is not True
                or item["labels"].get("io.spark.owner") != owner
                or item["labels"].get("io.spark.digest") != digest
                or item["image"] != worker["image"] or item["command"] != worker["command"]
                or item["entrypoint"] != worker["entrypoint"]):
            raise ValueError("live engine differs from saved plan")
        return item["id"], item["started"], item["restarts"]

    class NoRedirect(HTTPRedirectHandler):
        def redirect_request(self, *args, **kwargs):
            raise ValueError("engine redirect refused")

    opener = build_opener(ProxyHandler({}), NoRedirect())

    def get(path, limit):
        with opener.open(base + path, timeout=remaining()) as response:
            body = response.read(limit + 1)
        if len(body) > limit:
            raise ValueError("engine response too large")
        return body.decode("utf-8")

    before = identity()
    models = json.loads(get("/v1/models", 131072))
    rows = models.get("data")
    if (not isinstance(rows, list) or len(rows) != 1 or not isinstance(rows[0], dict)
            or rows[0].get("id") != engine["alias"] or rows[0].get("root") != engine["model_root"]
            or rows[0].get("max_model_len") != engine["context_tokens"]):
        raise ValueError("engine model identity mismatch")
    metrics = get("/metrics", 2 * 1024 * 1024)
    if identity() != before:
        raise ValueError("engine changed during collection")
    return {"owner": owner, "digest": digest, "metrics": metrics}


def main():
    try:
        request = json.loads(sys.stdin.read(131072))
        if request.get("action") != "monitor":
            raise ValueError("unsupported read-only action")
        if "engine" in request:
            collected = collect_engine(request["node"], request["engine"], request["timeout"])
        else:
            collected = collect(request["node"], request.get("physical_ports", ()))
        result = {"ok": True, "result": collected}
    except Exception:
        # Never return command output, environment, paths or request contents.
        result = {"ok": False, "error": "telemetry collection failed"}
    print(json.dumps(result, allow_nan=False))


if __name__ == "__main__":
    main()
