"""Replay the historical MiniKV admission comparison under a Docker memory cap."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import uuid

WORKERS = 16
TICK = 0.020
WINDOW_TICKS = 500
VALUE = b"x" * 1024
# This revision contains the experiment peer and the published comparison.
PEER_REVISION = "ff182ad95d5e664bc38a83cb3ecd8472947cb1d5"


@dataclass
class RequestResult:
    hot: bool
    tick: int
    hit: bool
    success: bool
    online_ns: int
    get_ns: int
    origin_cpu_ns: int
    origin_queries: int
    origin_timeout: bool
    status: bytes


class Client:
    def __init__(self, port):
        self.conn = socket.create_connection(("127.0.0.1", port), timeout=3)
        self.reader = self.conn.makefile("rb")

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.reader.close()
        self.conn.close()

    def get(self, key):
        self.conn.sendall(b"GET 8\n" + key)
        header = self.reader.readline()
        if header == b"VALUE 1024\n":
            if self.reader.read(len(VALUE)) != VALUE:
                raise ValueError("invalid cached payload")
            return b"HIT"
        if header in (b"MISS\n", b"BUSY\n"):
            return header.rstrip()
        raise ValueError(f"unexpected GET header {header!r}")

    def fill(self, key, nx=True):
        header = b"SET 8 1024 600000 NX\n" if nx else b"SET 8 1024 600000\n"
        self.conn.sendall(header + key + VALUE)
        status = self.reader.readline().rstrip()
        if status not in (b"STORED", b"EXISTS", b"NOT_CACHED", b"NO_SPACE", b"BUSY"):
            raise ValueError(f"unexpected SET status {status!r}")
        return status


@contextmanager
def peer(binary, image, arm):
    name = f"minikv-admission-{uuid.uuid4().hex[:12]}"
    command = [
        "docker", "run", "--rm", "-i", "--name", name, "--pull=never",
        "--memory=64m", "--memory-swap=64m", "--cpus=2",
        "-e", "GOMEMLIMIT=40MiB", "-e", "GOMAXPROCS=2",
        "-e", f"MINIKV_ADMISSION_ARM={arm}",
        "-p", "127.0.0.1::7321",
        "--mount", f"type=bind,src={binary.parent},dst=/work,readonly",
        "--entrypoint", "/work/peer", image, "-test.run=^TestAdmissionPeer$",
    ]
    with subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, text=True) as proc:
        try:
            for line in proc.stdout:
                if line.strip() == "MINIKV_ADMISSION_READY":
                    break
            else:
                raise RuntimeError(f"cache peer did not start: {arm}")
            binding = subprocess.check_output(["docker", "port", name, "7321/tcp"], text=True)
            port = int(binding.strip().rsplit(":", 1)[1])
            deadline = time.monotonic() + 5
            while True:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=.5):
                        break
                except OSError:
                    if proc.poll() is not None or time.monotonic() >= deadline:
                        raise
                    time.sleep(.05)
            yield port
            proc.stdin.close()
            if proc.wait(timeout=5) != 0:
                raise RuntimeError(f"cache peer failed: {proc.stdout.read()}")
        finally:
            if proc.poll() is None:
                subprocess.run(["docker", "stop", "-t", "1", name], stdout=subprocess.DEVNULL)
                proc.wait(timeout=5)


def key(prefix, i):
    return f"{prefix}{i:04d}".encode()


def workload(phase, index):
    if phase == "scan" and index % 5 == 0:
        return False, key("cold", index // 5)
    if phase == "scan":
        index -= index // 5 + 1
    if phase == "migration":
        return True, key("next", index % 256)
    return True, key("warm", index % 128)


def request(client, origin, hot, item, tick, scheduled_ns):
    started = time.monotonic_ns()
    status = client.get(item)
    get_ns = time.monotonic_ns() - started
    hit = status == b"HIT"
    success = status in (b"HIT", b"MISS")
    cpu_ns = 0
    queries = 0
    origin_timeout = False
    if status == b"MISS":
        success = origin.acquire(timeout=1)
        if success:
            try:
                before = time.thread_time_ns()
                hashlib.pbkdf2_hmac("sha256", item, b"origin", 1500)
                cpu_ns = time.thread_time_ns() - before
                queries = 1
            finally:
                origin.release()
            status = client.fill(item)
        else:
            origin_timeout = True
    return RequestResult(hot, tick, hit, success, time.monotonic_ns() - scheduled_ns,
                         get_ns, cpu_ns, queries, origin_timeout, status)


def replay(port, phase, ticks):
    origin = threading.BoundedSemaphore(8)
    ready = threading.Barrier(WORKERS, timeout=5)
    # Give the TCP connections time to establish before the first scheduled tick.
    start_ns = time.monotonic_ns() + 250_000_000
    tick_ns = round(TICK * 1e9)

    def lane(i):
        rows = []
        with Client(port) as client:
            ready.wait()
            for tick in range(ticks):
                scheduled = start_ns + tick * tick_ns
                delay = scheduled - time.monotonic_ns()
                if delay > 0:
                    time.sleep(delay / 1e9)
                hot, item = workload(phase, tick * WORKERS + i)
                rows.append(request(client, origin, hot, item, tick, scheduled))
        return rows

    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        return [row for rows in pool.map(lane, range(WORKERS)) for row in rows]


def percentile_us(samples, quantile):
    return round(sorted(samples)[math.ceil(len(samples) * quantile) - 1] / 1000, 1)


def summarize(rows, phase):
    hot = [row for row in rows if row.hot]
    summary = {
        "requests": len(rows),
        "hot_requests": len(hot),
        "hot_origin_queries": sum(row.origin_queries for row in hot),
        "origin_queries": sum(row.origin_queries for row in rows),
        "origin_cpu_ms": round(sum(row.origin_cpu_ns for row in rows) / 1e6, 2),
        "origin_timeouts": sum(row.origin_timeout for row in rows),
        "successes": sum(row.success for row in rows),
        "busy": sum(row.status == b"BUSY" for row in rows),
        "no_space": sum(row.status == b"NO_SPACE" for row in rows),
        "not_cached": sum(row.status == b"NOT_CACHED" for row in rows),
        "hot_online_p99_us": percentile_us([row.online_ns for row in hot], .99),
        "hot_get_p999_us": percentile_us([row.get_ns for row in hot], .999),
    }
    if phase == "migration":
        windows = [0] * (len(rows) // (WORKERS * WINDOW_TICKS))
        for row in rows:
            windows[row.tick // WINDOW_TICKS] += row.hit
        rates = [hits / (WORKERS * WINDOW_TICKS) for hits in windows]
        summary["window_hit_rates"] = rates
        summary["warmup_seconds"] = next(
            (10 * (i + 3) for i in range(len(rates) - 2) if min(rates[i:i + 3]) >= .95), None,
        )
    return summary


def measure(binary, image, arm, pair, smoke):
    with peer(binary, image, arm) as port:
        with Client(port) as client:
            for i in range(504):
                status = client.fill(key("warm", i), nx=False)
                if status != b"STORED":
                    raise RuntimeError(f"warmup SET failed: {status!r}")
        phases = {"scan": 320, "migration": 2000}
        if smoke:
            phases = {"scan": 8, "migration": WINDOW_TICKS}
        for phase, ticks in phases.items():
            result = summarize(replay(port, phase, ticks), phase)
            print(json.dumps({"pair": pair, "arm": arm, "phase": phase, **result}), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pairs", type=int, default=3)
    parser.add_argument("--image", default="redis:7-alpine", help="locally available Linux image")
    parser.add_argument("--smoke", action="store_true", help="run a shorter transport check")
    args = parser.parse_args()
    if args.pairs < 1:
        parser.error("--pairs must be positive")
    module = Path(__file__).resolve().parents[1]
    arch = subprocess.check_output(["docker", "info", "--format", "{{.Architecture}}"], text=True).strip()
    goarch = {"aarch64": "arm64", "x86_64": "amd64", "arm64": "arm64", "amd64": "amd64"}[arch]
    with tempfile.TemporaryDirectory(dir="/tmp") as scratch:
        source = Path(scratch).resolve()
        archive = subprocess.check_output(["git", "archive", PEER_REVISION, "minikv"], cwd=module.parent)
        subprocess.run(["tar", "-x", "-C", source], input=archive, check=True)
        binary = source / "bin" / "peer"
        binary.parent.mkdir()
        subprocess.run(
            ["go", "test", "-c", "-o", binary, "./internal/server"], cwd=source / "minikv", check=True,
            env={**os.environ, "GOOS": "linux", "GOARCH": goarch, "CGO_ENABLED": "0", "GOWORK": "off"},
        )
        for pair in range(1, args.pairs + 1):
            arms = ("direct", "two-fill") if pair % 2 else ("two-fill", "direct")
            for arm in arms:
                measure(binary, args.image, arm, pair, args.smoke)


if __name__ == "__main__":
    main()
