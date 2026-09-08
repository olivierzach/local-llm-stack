"""Run bounded diagnostics inside an already-owned distributed deployment."""
from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import shlex
import socket
import subprocess


def parse_records(text):
    # NCCL's C logger may prepend buffered output to a Python JSON line.
    decoder = json.JSONDecoder()
    records = []
    for line in text.splitlines():
        start = line.find('{"rank":')
        if start < 0: continue
        try:
            record, _ = decoder.raw_decode(line[start:])
            records.append(record)
        except ValueError:
            pass
    return records


def collectives(plan, output, call):
    if len(plan["nodes"]) != 2:
        raise ValueError("collective benchmark currently supports exactly two reserved nodes")
    for node_id in plan["nodes"]:
        status = call(plan, node_id, "status")
        reservation = status["reservation"]
        if not reservation or reservation["owner"] != plan["owner"] or reservation["digest"] != plan["digest"]:
            raise RuntimeError("collective benchmark requires ownership on every node")
        if not status["containers"] or any(c["state"] != "running" for c in status["containers"]):
            raise RuntimeError("collective benchmark requires running owned containers")
    coordinator = plan["deployment"]["coordinator"]
    ordered = [coordinator] + [n for n in plan["nodes"] if n != coordinator]
    master = plan["nodes"][coordinator]["fabric"][0]["ip"]
    source = (Path(__file__).resolve().parents[2] / "scripts/benchmark-collectives.py").read_text()
    output.mkdir(parents=True, exist_ok=True)
    def run_one(rank):
        node = plan["nodes"][ordered[rank]]
        cmd = ["docker", "exec", "-i", "spark-" + plan["owner"], "timeout", "180s", "python3", "-",
               "--rank", str(rank), "--world", str(len(ordered)), "--master", master]
        if socket.gethostname() != node["hostname"]:
            cmd = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", node["ssh"], shlex.join(cmd)]
        result = subprocess.run(cmd, input=source, capture_output=True, text=True, timeout=210)
        (output / f"collectives-{ordered[rank]}.log").write_text(result.stdout + result.stderr)
        records = parse_records(result.stdout)
        if result.returncode or len(records) != 11:
            raise RuntimeError(f"collective benchmark failed on {ordered[rank]}; inspect saved log")
        return records
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run_one, rank) for rank in range(2)]
        results = [f.result() for f in futures]
    (output / "collectives.json").write_text(json.dumps(results, indent=2) + "\n")
    return {"ranks": 2, "correctness_checks": 20, "results": str(output / "collectives.json"),
            "rank_zero": results[0]}
