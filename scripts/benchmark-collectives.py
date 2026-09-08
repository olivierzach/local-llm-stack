#!/usr/bin/env python3
"""Two-rank CUDA-buffer collective correctness and wall-clock throughput.

Run inside the reserved distributed containers with no inference traffic. This
measures the installed NCCL path, including Spark's required host staging. It
does not claim GPUDirect support or replace a standard nccl-tests benchmark.
"""
import argparse
import datetime
import json
import time

import torch
import torch.distributed as dist


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--rank", type=int, required=True)
    p.add_argument("--world", type=int, required=True)
    p.add_argument("--master", required=True)
    p.add_argument("--port", type=int, default=29631)
    args = p.parse_args()
    torch.cuda.set_device(0)
    dist.init_process_group("nccl", init_method=f"tcp://{args.master}:{args.port}",
                            rank=args.rank, world_size=args.world, timeout=datetime.timedelta(seconds=90),
                            device_id=torch.device("cuda:0"))
    try:
        print(json.dumps({"rank": args.rank, "torch": torch.__version__, "nccl": torch.cuda.nccl.version(),
                          "device": torch.cuda.get_device_name(0), "transport": "see NCCL log; Spark uses host staging"}), flush=True)
        for size in (1024, 65536, 1048576, 16777216, 268435456):
            tensor = torch.full((size // 4,), float(args.rank + 1), dtype=torch.float32, device="cuda")
            gathered = torch.empty(size // 4 * args.world, dtype=torch.float32, device="cuda")
            for operation in ("all_reduce", "all_gather"):
                tensor.fill_(args.rank + 1)
                def collective():
                    if operation == "all_reduce": dist.all_reduce(tensor)
                    else: dist.all_gather_into_tensor(gathered, tensor)
                collective()
                torch.cuda.synchronize()
                if operation == "all_reduce":
                    correct = bool(torch.all(tensor == args.world*(args.world+1)/2).item())
                else:
                    correct = all(bool(torch.all(part == rank+1).item())
                                  for rank, part in enumerate(gathered.chunk(args.world)))
                if not correct: raise RuntimeError("collective validation failed")
                tensor.zero_()
                for _ in range(5): collective()
                torch.cuda.synchronize()
                dist.barrier()
                torch.cuda.synchronize()
                start = time.perf_counter()
                for _ in range(20): collective()
                torch.cuda.synchronize()
                elapsed = torch.tensor([time.perf_counter()-start], dtype=torch.float64, device="cuda")
                dist.all_reduce(elapsed, op=dist.ReduceOp.MAX)
                seconds = elapsed.item() / 20
                # Per-rank input bytes. For all_gather this is NOT total output bytes.
                factor = 2*(args.world-1)/args.world if operation == "all_reduce" else args.world-1
                print(json.dumps({"rank": args.rank, "operation": operation, "input_bytes_per_rank": size,
                                  "iterations": 20, "correct": correct, "latency_ms": seconds*1000,
                                  "input_GB_s": size/seconds/1e9,
                                  "estimated_bus_Gb_s": size/seconds/1e9*factor*8}), flush=True)
            del tensor, gathered
    finally:
        dist.destroy_process_group()


if __name__ == "__main__": main()
