"""Render a caller-supplied launch manifest; print only, never execute.

The manifest is an external JSON object mapping host-a and host-b to argument
arrays. Historical executed manifests are private evidence, not bundled here.
Set HOST_A_IP, HOST_B_IP and HF_CACHE; review all mounts and NIC/RDMA arguments
for an isolated test environment before separately running the printed command.
"""

import argparse
import json
import os
import shlex
from pathlib import Path
from string import Template

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "manifest", type=Path,
    help="External JSON launch manifest; not included in this source package",
)
parser.add_argument("host", choices=["host-a", "host-b"])
args = parser.parse_args()
required = ("HOST_A_IP", "HOST_B_IP", "HF_CACHE")
missing = [name for name in required if not os.environ.get(name)]
if missing:
    parser.error("Set environment variables: " + ", ".join(missing))
if not args.manifest.is_file():
    parser.error("Provide an existing external launch manifest; no evidence is bundled")

manifest = json.loads(args.manifest.read_text())
command = [
    Template(
        argument.replace("192.0.2.10", "${HOST_A_IP}").replace(
            "192.0.2.11", "${HOST_B_IP}"
        )
    ).substitute(os.environ)
    for argument in manifest[args.host]
]
print(shlex.join(command))
