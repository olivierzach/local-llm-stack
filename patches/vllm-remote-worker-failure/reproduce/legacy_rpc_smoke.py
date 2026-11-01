"""Compatibility smoke only: real legacy dispatch/pipes, stub model methods."""

import json
import multiprocessing
from types import SimpleNamespace

from vllm.v1.executor.multiproc_executor import WorkerProc


class Requests:
    def __init__(self, method, output_rank):
        self.requests = iter([(method, (), {}, output_rank)] * 2)

    def dequeue(self, *, indefinite):
        return next(self.requests)


results = []
for method, output_rank, fatal in (
    ("execute_model", 0, True),
    ("compile_or_warm_up_model", None, True),
    ("add_lora", None, False),
):
    reader, writer = multiprocessing.Pipe(duplex=False)

    def fail():
        raise RuntimeError("synthetic RPC compatibility error")

    outputs = []
    worker = WorkerProc.__new__(WorkerProc)
    worker.rank = 1
    worker.worker = SimpleNamespace(**{method: fail})
    worker.error_writer = writer
    worker.rpc_broadcast_mq = Requests(method, output_rank)
    worker.handle_output = outputs.append
    try:
        try:
            worker.worker_busy_loop()
        except StopIteration:
            pass
        assert reader.poll(0) == fatal
        if fatal:
            reader.recv_bytes()
            assert not reader.poll(0), "fatal notification repeated"
        assert len(outputs) == (2 if output_rank is None else 0)
        assert all(isinstance(output, RuntimeError) for output in outputs)
        results.append({"method": method, "fatal_notification": fatal, "passed": True})
    finally:
        reader.close()
        writer.close()
print(json.dumps({"legacy_rpc_compatibility": results}))
