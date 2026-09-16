#!/usr/bin/env python3
"""Run the legacy guard, with optional per-alias cluster route overrides."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from spark_cluster.legacy_gateway import main

if __name__ == '__main__':
    raise SystemExit(main())
