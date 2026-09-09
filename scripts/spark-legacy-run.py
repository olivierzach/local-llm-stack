#!/usr/bin/env python3
"""Guard normal Make GPU operations with the shared node admission protocol."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'tools'))
from spark_cluster.legacy import main

if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except Exception as error:
        print('spark-legacy-run: ' + str(error), file=sys.stderr)
        raise SystemExit(1)
