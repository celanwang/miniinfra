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
def engine(binaries, image, name, go_memory_limit, cpuset, block_profile):
    container = f"minikv-capacity-{uuid.uuid4().hex[:12]}"
    flags = [
        "run", "-d", "--name", container, "--pull=never", "--network", "none",
        "--cpus=2", "--memory=160m", "--memory-swap=160m",
        "--sysctl", "net.ipv4.tcp_wmem=4096 32768 131072",
    ]
    if cpuset:
        flags += ["--cpuset-cpus", cpuset]
    if name == "minikv":
        flags += [
            "-e", "MINIKV_CAPACITY_PEER=1", "-e", "GOMAXPROCS=2",
            "--mount", mount(binaries), "--entrypoint", "/work/peer",
        ]
        if go_memory_limit:
            flags += ["-e", f"GOMEMLIMIT={go_memory_limit}"]
        if block_profile:
            flags += ["-e", "MINIKV_LOCK_PROFILE=1"]
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


def replay(binaries, image, name, profile, rate, duration, tick, go_memory_limit, server_cpus, driver_cpus, pressure_lengths="fixed", block_profile=False, ttl_pattern="clustered", page_window=1, page_key_bytes=12):
    with engine(binaries, image, name, go_memory_limit, server_cpus, block_profile) as container:
        flags = [
            "run", "--rm", "--pull=never", "--network", f"container:{container}",
            "--cpus=4",
            "--memory=256m", "--memory-swap=256m", "-e", "GOMAXPROCS=4",
            "--mount", mount(binaries), "--entrypoint", "/work/load", image,
            "-profile", profile, "-rate", rate, "-duration", duration, "-tick", tick,
        ]
        if profile == "pressure":
            flags += ["-pressure-lengths", pressure_lengths]
            if block_profile:
                flags += ["-block-profile"]
        elif profile == "ttl":
            flags += ["-ttl-pattern", ttl_pattern]
        elif profile == "page":
            flags += ["-page-window", page_window, "-page-key-bytes", page_key_bytes]
        else:
            flags[2:2] = ["--pid", f"container:{container}"]
        if driver_cpus:
            flags[2:2] = ["--cpuset-cpus", driver_cpus]
        if name == "redis":
            flags.append("-redis")
        metadata = {"Engine": name, "Profile": profile, "Rate": rate,
                    "Duration": duration, "Tick": tick,
                    "GoMemoryLimit": go_memory_limit if name == "minikv" else None}
        if profile == "pressure":
            metadata["PressureLengths"] = pressure_lengths
        if profile == "ttl":
            metadata["TTLPattern"] = ttl_pattern
        if profile == "page":
            metadata["PageWindow"] = page_window
            metadata["PageKeyBytes"] = page_key_bytes
        driver = subprocess.run(["docker", *map(str, flags)], capture_output=True, text=True)
        oom = docker("inspect", "--format", "{{.State.OOMKilled}}", container) == "true"
        if driver.returncode and not oom:
            raise RuntimeError(f"load driver exited {driver.returncode}: {driver.stderr}")
        result = json.loads(driver.stdout) if not driver.returncode else {}
        if oom:
            result.update(OOMKilled=True, DriverStderr=driver.stderr.strip())
        else:
            result.update(rss(container))
        print(json.dumps({**metadata, **result}), flush=True)


def build(binaries, module, peer_module=None):
    arch = docker("info", "--format", "{{.Architecture}}")
    goarch = {"aarch64": "arm64", "x86_64": "amd64", "arm64": "arm64", "amd64": "amd64"}[arch]
    env = {**os.environ, "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": goarch}
    subprocess.run(["go", "test", "-c", "-o", binaries / "peer", "./internal/server"],
                   cwd=peer_module or module, env=env, check=True)
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
    parser.add_argument("--server-cpus", help="pin the server to a Docker CPU set")
    parser.add_argument("--driver-cpus", help="pin the driver to a Docker CPU set")
    parser.add_argument("--pressure-lengths", default="fixed", choices=("fixed", "repeated", "adjacent", "publish", "publish-locality",
                                          "publish-aligned", "large-to-half", "half-to-large",
                                          "cold-half", "alternating", "business", "eviction"))
    parser.add_argument("--peer-module", type=Path, help="build the MiniKV peer from another checkout")
    parser.add_argument("--block-profile", action="store_true", help="collect an instrumented Go block profile")
    parser.add_argument("--ttl-pattern", default="clustered", choices=("clustered", "spread", "byte-fit", "byte-limit"))
    parser.add_argument("--page-window", type=int, default=1, choices=(1, 4))
    parser.add_argument("--page-key-bytes", type=int, default=12, help="key length for the page profile (12–1024)")
    args = parser.parse_args()
    profiles = args.profiles.split(",")
    if any(p not in ("small", "mixed", "slow", "status", "hit", "capacity", "pressure", "ttl", "page") for p in profiles):
        parser.error("invalid profile")
    rates = [int(rate) for rate in args.rates.split(",")]
    if any(rate <= 0 for rate in rates):
        parser.error("rates must be positive")

    module = Path(__file__).resolve().parents[1]
    with tempfile.TemporaryDirectory(dir="/tmp") as scratch:
        binaries = Path(scratch).resolve()
        build(binaries, module, args.peer_module.resolve() if args.peer_module else None)
        for i, (profile, rate) in enumerate(
            (p, r) for p in profiles for r in ([0] if p in ("capacity", "pressure", "ttl") else rates)
        ):
            engines = ("minikv", "redis") if i % 2 == 0 else ("redis", "minikv")
            if profile in ("pressure", "ttl", "page", "status", "hit"):
                engines = ("minikv",)
            for name in engines:
                replay(binaries, args.image, name, profile, rate, args.duration, args.tick,
                       args.go_memory_limit, args.server_cpus, args.driver_cpus, args.pressure_lengths, args.block_profile,
                       args.ttl_pattern, args.page_window, args.page_key_bytes)


if __name__ == "__main__":
    main()
