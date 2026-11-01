"""Test-only worker extension; never installed in the serving deployment."""

import hashlib
import json
import os
import time
from pathlib import Path


class FailureProbeExtension:
    def e2e_report(self):
        import vllm
        from vllm.distributed.device_communicators import shm_broadcast
        from vllm.v1.executor import multiproc_executor

        source = Path(multiproc_executor.__file__)
        queue_source = Path(shm_broadcast.__file__)
        return {
            "rank": self.rank,
            "pid": os.getpid(),
            "version": vllm.__version__,
            "executor_path": str(source),
            "executor_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            "queue_path": str(queue_source),
            "queue_sha256": hashlib.sha256(queue_source.read_bytes()).hexdigest(),
            "model_class": type(self.get_model()).__name__,
        }

    def e2e_arm_failure(self, target_rank=1, run_id="unspecified", failure_mode="exception"):
        if self.rank != int(target_rank):
            return {"rank": self.rank, "armed": False, "run_id": run_id}
        if getattr(self, "_e2e_failure_hook", None) is not None:
            raise RuntimeError("A failure is already armed")

        def fail_before_forward(model, args):
            self._e2e_failure_hook.remove()
            self._e2e_failure_hook = None
            print(
                "VLLM_FAILURE_E2E "
                + json.dumps(
                    {
                        "event": "injected_forward_" + failure_mode,
                        "rank": self.rank,
                        "pid": os.getpid(),
                        "run_id": run_id,
                        "unix_time": time.time(),
                    }
                ),
                flush=True,
            )
            if failure_mode == "exit":
                os._exit(17)
            raise RuntimeError(f"Synthetic model-forward failure: {run_id}")

        self._e2e_failure_hook = self.get_model().register_forward_pre_hook(
            fail_before_forward
        )
        return {"rank": self.rank, "armed": True, "run_id": run_id}
