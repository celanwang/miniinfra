"""Run MiniKV and Redis GET/SET capacity curves in resource-limited containers."""

import argparse
from contextlib import contextmanager
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid


def docker(*args):
    return subprocess.check_output(["docker", *map(str, args)], text=True).strip()


def mount(binaries):
    return f"type=bind,src={binaries},dst=/work,readonly"


@contextmanager
def engine(binaries, image, name, go_memory_limit):
    container = f"minikv-capacity-{uuid.uuid4().hex[:12]}"
    flags = [
        "run", "-d", "--name", container, "--pull=never", "--network", "none",
        "--cpus=2", "--memory=160m", "--memory-swap=160m",
        "--sysctl", "net.ipv4.tcp_wmem=4096 32768 131072",
    ]
    if name == "minikv":
        flags += [
            "-e", "MINIKV_CAPACITY_PEER=1", "-e", "GOMAXPROCS=2",
            "--mount", mount(binaries), "--entrypoint", "/work/peer",
        ]
        if go_memory_limit:
            flags += ["-e", f"GOMEMLIMIT={go_memory_limit}"]
        args = ["-test.run=^TestCapacityPeer$", "-test.timeout=0"]
        probe = ["wget", "-qO-", "http://127.0.0.1:9122/stats"]
    else:
        flags += ["--entrypoint", "redis-server"]
        args = [
            "--bind", "127.0.0.1", "--port", "7321", "--save", "",
            "--appendonly", "no", "--maxmemory", "64mb",
            "--maxmemory-policy", "allkeys-lru", "--io-threads", "1",
        ]
        probe = ["redis-cli", "-p", "7321", "PING"]

    try:
        docker(*flags, image, *args)
        deadline = time.monotonic() + 8
        while True:
            ready = subprocess.run(
                ["docker", "exec", container, *probe],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            if ready.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError(f"{name} startup failed: {docker('logs', container)}")
            time.sleep(.05)
        yield container
    finally:
        subprocess.run(["docker", "rm", "-f", container], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)


def rss(container):
    status = docker("exec", container, "cat", "/proc/1/status")
    memory = {}
    for line in status.splitlines():
        label, _, value = line.partition(":")
        if label in ("VmRSS", "VmHWM"):
            memory[label + "KiB"] = int(value.split()[0])
    if len(memory) != 2:
        raise RuntimeError(f"missing process RSS: {memory}")
    return memory


def replay(binaries, image, name, profile, rate, duration, tick, go_memory_limit):
    with engine(binaries, image, name, go_memory_limit) as container:
        flags = [
            "run", "--rm", "--pull=never", "--network", f"container:{container}",
            "--pid", f"container:{container}", "--cpus=4",
            "--memory=256m", "--memory-swap=256m", "-e", "GOMAXPROCS=4",
            "--mount", mount(binaries), "--entrypoint", "/work/load", image,
            "-profile", profile, "-rate", rate, "-duration", duration, "-tick", tick,
        ]
        if name == "redis":
            flags.append("-redis")
        metadata = {"Engine": name, "Profile": profile, "Rate": rate,
                    "Duration": duration, "Tick": tick,
                    "GoMemoryLimit": go_memory_limit if name == "minikv" else None}
        driver = subprocess.run(["docker", *map(str, flags)], capture_output=True, text=True)
        if driver.returncode:
            if docker("inspect", "--format", "{{.State.OOMKilled}}", container) != "true":
                raise RuntimeError(f"load driver exited {driver.returncode}: {driver.stderr}")
            result = {"OOMKilled": True, "DriverStderr": driver.stderr.strip()}
        else:
            result = {**rss(container), **json.loads(driver.stdout)}
        print(json.dumps({**metadata, **result}), flush=True)


def build(binaries, module):
    arch = docker("info", "--format", "{{.Architecture}}")
    goarch = {"aarch64": "arm64", "x86_64": "amd64", "arm64": "arm64", "amd64": "amd64"}[arch]
    env = {**os.environ, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": goarch}
    subprocess.run(["go", "test", "-c", "-o", binaries / "peer", "./internal/server"],
                   cwd=module, env=env, check=True)
    subprocess.run(["go", "build", "-o", binaries / "load", "./experiments/load"],
                   cwd=module, env=env, check=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default="redis:7-alpine")
    parser.add_argument("--rates", default="10000,30000,60000,100000,150000")
    parser.add_argument("--profiles", default="small,mixed,slow,capacity")
    parser.add_argument("--duration", default="6s")
    parser.add_argument("--tick", default="1ms", help="schedule slots in fixed-duration batches")
    parser.add_argument("--go-memory-limit", help="set GOMEMLIMIT on the MiniKV server for this run")
    args = parser.parse_args()
    profiles = args.profiles.split(",")
    if any(p not in ("small", "mixed", "slow", "capacity") for p in profiles):
        parser.error("invalid profile")
    rates = [int(rate) for rate in args.rates.split(",")]
    if any(rate <= 0 for rate in rates):
        parser.error("rates must be positive")

    module = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(dir="/tmp") as scratch:
        binaries = Path(scratch).resolve()
        build(binaries, module)
        for i, (profile, rate) in enumerate(
            (p, r) for p in profiles for r in ([0] if p == "capacity" else rates)
        ):
            engines = ("minikv", "redis") if i % 2 == 0 else ("redis", "minikv")
            for name in engines:
                replay(binaries, args.image, name, profile, rate, args.duration, args.tick, args.go_memory_limit)


if __name__ == "__main__":
    main()
