#!/usr/bin/env python3
"""Build an isolated SIGTERM stack-dump image without modifying production Go source."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="local/oteldb:shutdown-probe")
    args = parser.parse_args()
    output = HERE / "bin" / "shutdown-probe"
    output.mkdir(parents=True, exist_ok=True)
    source = HERE / "shutdown_probe.go.txt"
    overlay = output / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {
        str(ROOT / "cmd/oteldb/zz_shutdown_probe.go"): str(source)}}, indent=2) + "\n")
    subprocess.run(["go", "build", "-overlay=" + str(overlay), "-trimpath", "-buildvcs=false",
                    "-o", str(output / "oteldb"), "./cmd/oteldb"], cwd=ROOT,
                   env=dict(os.environ, CGO_ENABLED="0", GOOS="linux"), check=True)
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    version = subprocess.check_output(["go", "list", "-m", "-f", "{{.Version}}",
                                       "github.com/oteldb/storage"], cwd=ROOT, text=True).strip()
    subprocess.run(["docker", "build", "--build-arg", "REVISION=" + revision,
                    "--build-arg", "STORAGE_REVISION=" + version,
                    "--label", "dev.oteldb.shutdown-probe.sha256=" + hashlib.sha256(source.read_bytes()).hexdigest(),
                    "-f", str(HERE / "Dockerfile"), "-t", args.image, str(output)], check=True)


if __name__ == "__main__":
    main()
