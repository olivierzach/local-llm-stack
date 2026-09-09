# Spark host Python environment

The host development environment is separate from the lightweight cluster
controller venv and each model's pinned container. Model artifact parity does
not imply host Python package parity. The September 9 audit found 167 packages
(excluding pip) in 66f1's stack environment, versus eleven in e8f1's environment.
Both passed `pip check`, but only the former contained the host training tools.

`cluster/python/spark-host-cp312-aarch64.lock` captures the source versions and
the SHA-256 hashes of their Linux aarch64 / Python 3.12 wheels. It includes Torch,
Transformers, Accelerate, PEFT, TRL, datasets, Jupyter and their dependencies.
It does not replace drivers, CUDA system packages, model images or credentials.

Build a fresh environment on either Spark:

```bash
cd ~/projects/local-llm-stack
make python-parity-prepare VENV="$HOME/.local/share/spark-host-envs/20260909"
make python-parity-check VENV="$HOME/.local/share/spark-host-envs/20260909"
```

The preparer refuses an existing destination, installs only hash-verified wheels,
and checks every locked version and dependency consistency. A failed build is
retained for inspection; use a new path for another attempt. No active venv is
replaced. The check does not execute a GPU workload, and extra packages are
reported separately. To use a wheel directory already copied over the fabric,
add `WHEELHOUSE=/path/to/wheels`; this disables pip's network access.

Use the new environment explicitly until its CPU imports, CUDA smoke and stack
regression tests pass. GPU validation must obtain the shared workload reservation
and wait for an idle research window. Existing model containers keep their own
Python environments. Replacing the baseline `.venv` is a separate maintenance
step: wait for that node's jobs to finish, retain the original directory for
rollback, and point `.venv` at the validated environment. Do not move the new
environment afterward, since its entrypoint scripts contain its absolute path.

On a current 66f1 checkout, `make python-parity-check` audits the original venv
without changing it. On a new machine, the same lock and prepare command recreate
the host environment without requiring a live source Spark or a coding agent.
