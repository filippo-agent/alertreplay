#!/usr/bin/env python3
"""Capture example CLI output; build/start/backfill/stop all happen in one run."""
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parent.parent


def binary(name, variable):
    override = os.environ.get(variable)
    if override:
        path = shutil.which(override)
        if not path:
            raise SystemExit(f"{variable}={override!r}: executable not found")
        return path
    local = ROOT / ".tools" / name
    if local.is_file() and os.access(local, os.X_OK):
        return str(local)
    path = shutil.which(name)
    if not path:
        raise SystemExit(f"Install Prometheus 3.15.0 in .tools/ or set {variable}")
    return path


def main():
    prometheus = binary("prometheus", "ALERTREPLAY_PROMETHEUS")
    promtool = binary("promtool", "ALERTREPLAY_PROMTOOL")
    with tempfile.TemporaryDirectory(prefix="alertreplay-example-") as directory:
        temp = Path(directory)
        executable = temp / "alertreplay"
        subprocess.run(["go", "build", "-o", str(executable), "."], cwd=ROOT, check=True)
        subprocess.run([
            promtool, "tsdb", "create-blocks-from", "openmetrics",
            str(ROOT / "testdata" / "synthetic.om"), str(temp / "data"),
        ], check=True)
        config = temp / "prometheus.yml"
        config.write_text("global:\n  scrape_interval: 1m\nscrape_configs: []\n")
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        log_path = temp / "prometheus.log"
        with log_path.open("w") as log:
            process = subprocess.Popen([
                prometheus, f"--config.file={config}",
                f"--storage.tsdb.path={temp / 'data'}",
                "--storage.tsdb.retention.time=100y",
                "--query.lookback-delta=30s",
                f"--web.listen-address=127.0.0.1:{port}",
            ], stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 15
                while True:
                    if process.poll() is not None:
                        raise RuntimeError(f"Prometheus exited:\n{log_path.read_text()}")
                    try:
                        with urllib.request.urlopen(url + "/-/ready", timeout=1):
                            break
                    except (urllib.error.URLError, TimeoutError):
                        if time.monotonic() >= deadline:
                            raise RuntimeError(f"Prometheus readiness timed out:\n{log_path.read_text()}")
                        time.sleep(0.1)
                result = subprocess.run([
                    str(executable), "-url", url,
                    "-start", "2026-09-01T00:00:00Z",
                    "-end", "2026-09-01T01:00:00Z", "-qps", "1000",
                    "testdata/synthetic.rules.yml",
                ], cwd=ROOT, text=True, capture_output=True, check=True)
                destination = ROOT / "testdata" / "example-output.txt"
                destination.write_text(result.stdout)
                print(result.stdout, end="")
                print(f"Saved {destination}")
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
