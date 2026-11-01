#!/usr/bin/env python3
"""xet-p2p vs rsync over a real network path: this machine -> a Modal sandbox.

Method
------
- One sandbox (debian_slim + rsync, 1 CPU) with three raw TCP tunnels, one per server so nothing
  is switched mid-run: 4434 = xet-p2p agent started per command, 4435 = rsync daemon (started
  once), 4436 = xet-p2p agent kept with --keep-agent. All share the same network path.
- Tools: `rsync -a`, `rsync -az` (rsync:// daemon protocol, delta transfer on, unencrypted),
  `xet-p2p` (TLS over TCP, fresh agent each command), `xet-p2p --keep-agent` (TLS over TCP,
  agent kept between commands).
- R repetitions (default 5). Each repetition generates new data (seeded by scenario and
  repetition, identical for every tool within the repetition), so no run can benefit from content
  an earlier repetition left behind. The two xet-p2p variants use separate sandbox CASes.
- Within a repetition the tool order is shuffled per scenario (seeded), so drifting network
  conditions do not systematically favour one tool.
- A network baseline is taken at the start of every repetition: TCP connect RTT and raw upload
  throughput through the tunnel.
- "Warm" scenarios sync the first version, wait SETTLE_SECS, sync it again (both unmeasured: the
  steady state of a tree that has been synced before), edit it (mtime +10 s so rsync's quick
  check notices), then measure. Every measured result is verified by SHA-256 inside the sandbox.
- Reported: median [min-max] over repetitions; raw per-run data is written as JSON too.

Usage (needs the Modal SDK's Python, a Linux xet-p2p agent build, and a local xet-p2p):
    XET_P2P_AGENT_LINUX_BIN=target/x86_64-unknown-linux-musl/release/xet-p2p \\
    <modal-python> bench/modal_vs_rsync.py --xet target/release/xet-p2p \\
        --out docs/BENCHMARK_M5_MODAL.md --raw bench/results/modal_vs_rsync.json
"""

import argparse
import hashlib
import json
import os
import random
import re
import shutil
import socket
import statistics
import subprocess
import tempfile
import time

# DNS64 without working NAT64 makes the Modal SDK hang on synthesized IPv6 addresses.
_getaddrinfo = socket.getaddrinfo
socket.getaddrinfo = lambda *a, **k: sorted(_getaddrinfo(*a, **k), key=lambda i: i[0] != socket.AF_INET)

import modal  # noqa: E402

MIB = 1024 * 1024
XET_PORT, RSYNC_PORT, KEEP_PORT = 4434, 4435, 4436
TOOLS = ["rsync -a", "rsync -az", "xet-p2p", "xet-p2p --keep-agent"]


# ---------- deterministic data ----------

def rand_bytes(seed, n):
    return random.Random(seed).randbytes(n)


WORDS = ("INFO WARN request completed user id latency_ms status=200 GET /api/v1/items cache miss "
         "hit shard worker retry ok connection closed bytes elapsed upstream token refresh").split()


def log_text(seed, n):
    r = random.Random(seed)
    out, size, line = [], 0, 0
    while size < n:
        line += 1
        s = f"2026-10-31T12:{line % 60:02d}:{r.randrange(60):02d} " + " ".join(
            r.choice(WORDS) for _ in range(r.randrange(4, 12))) + f" {r.randrange(100000)}\n"
        out.append(s)
        size += len(s)
    return "".join(out).encode()[:n]


def write(root, rel, data):
    p = os.path.join(root, rel)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "wb") as f:
        f.write(data)


SETTLE_SECS = 3


def bump(path):
    t = time.time() + 10
    os.utime(path, (t, t))


def tree(root, seed, n=500):
    r = random.Random(seed)
    for i in range(n):
        write(root, f"d{i % 20:02d}/f{i:04d}.txt", log_text(seed * 100000 + i, r.randrange(1024, 16 * 1024)))


def scenarios():
    """(name, setup(src, seed), edit(src, seed) or None)."""

    def insert_mid(s, seed):
        p = os.path.join(s, "f.bin")
        v = open(p, "rb").read()
        open(p, "wb").write(v[:4 * MIB] + rand_bytes(seed + 7, 4096) + v[4 * MIB:])
        bump(p)

    def ckpt(s, seed):
        write(s, "model.ckpt", b"".join(rand_bytes(seed * 100 + t, 512 * 1024) for t in range(32)))

    def ckpt_edit(s, seed):
        p = os.path.join(s, "model.ckpt")
        v = bytearray(open(p, "rb").read())
        for t in (5, 20):
            v[t * 512 * 1024:(t + 1) * 512 * 1024] = rand_bytes(seed * 100 + 90 + t, 512 * 1024)
        open(p, "wb").write(v)
        bump(p)

    def append_log(s, seed):
        p = os.path.join(s, "app.log")
        open(p, "ab").write(log_text(seed + 7, 256 * 1024))
        bump(p)

    def edit_tree(s, seed):
        for i in range(0, 500, 50):
            p = os.path.join(s, f"d{i % 20:02d}/f{i:04d}.txt")
            open(p, "ab").write(b"edited line\n")
            bump(p)

    return [
        ("cold, 8 MiB random file", lambda s, k: write(s, "f.bin", rand_bytes(k, 8 * MIB)), None),
        ("warm, 4 KiB inserted mid 8 MiB file", lambda s, k: write(s, "f.bin", rand_bytes(k, 8 * MIB)), insert_mid),
        ("warm, checkpoint: 2 of 32 tensors rewritten (16 MiB)", ckpt, ckpt_edit),
        ("cold, 8 MiB compressible log", lambda s, k: write(s, "app.log", log_text(k, 8 * MIB)), None),
        ("warm, 256 KiB appended to 8 MiB log", lambda s, k: write(s, "app.log", log_text(k, 8 * MIB)), append_log),
        ("cold, 500 small files (1-16 KiB)", lambda s, k: tree(s, k), None),
        ("warm, 10 of 500 small files edited", lambda s, k: tree(s, k), edit_tree),
        ("warm, 500 small files unchanged", lambda s, k: tree(s, k), lambda s, k: None),
    ]


def digest_tree(root):
    out = {}
    for d, _, files in os.walk(root):
        for f in files:
            p = os.path.join(d, f)
            out[os.path.relpath(p, root)] = hashlib.sha256(open(p, "rb").read()).hexdigest()
    return out


def remote_digests(sb, path):
    p = sb.exec("sh", "-c", f"cd {path} && find . -type f -exec sha256sum {{}} +", timeout=120)
    out = p.stdout.read()
    p.wait()
    res = {}
    for line in out.splitlines():
        h, f = line.split(None, 1)
        res[f[2:] if f.startswith("./") else f] = h
    return res


RSYNC_TEMP = re.compile(r"(^|/)\.[^/]+\.[A-Za-z0-9]{6}$")


def check_tree(remote, local, what):
    """Every source file must be at the destination with identical content. Extra files fail too,
    except rsync temp files (`.name.XXXXXX`) left by an interrupted (then retried) run: rsync
    without --delete never removes them. Returns those leftovers."""
    bad = {f for f in local if remote.get(f) != local[f]}
    extra = set(remote) - set(local)
    leftovers = sorted(f for f in extra if RSYNC_TEMP.search(f))
    assert not bad and len(leftovers) == len(extra), \
        f"{what}: tree differs (wrong or missing: {sorted(bad)[:5]}, extra: {sorted(extra)[:5]})"
    if leftovers:
        print(f"  note: {what}: {len(leftovers)} rsync temp file(s) left by an interrupted run", flush=True)
    return leftovers


# ---------- tools ----------

def run_rsync(src, url, compress):
    t = time.time()
    r = subprocess.run(["rsync", "-az" if compress else "-a", "--stats", f"{src}/", url],
                       capture_output=True, text=True, timeout=1800)
    secs = time.time() - t
    if r.returncode != 0:
        raise RuntimeError(f"rsync failed: {r.stderr.strip()[:500]}")
    num = lambda name: int(re.search(rf"{name}:\s*([\d,]+)", r.stdout).group(1).replace(",", ""))
    sent, recv = num("Total sent"), num("Total received")
    return {"sent": sent, "total": sent + recv, "wall": secs, "transfer": secs, "bootstrap": 0.0}


def run_xet(xet, src, target, cas, keep):
    cmd = [xet, "push", src, target, "--cas-dir", cas, "--json"] + (["--keep-agent", "15"] if keep else [])
    t = time.time()
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    secs = time.time() - t
    if r.returncode != 0:
        raise RuntimeError(f"xet-p2p failed: {r.stderr.strip()[-800:]}")
    j = json.loads(r.stdout)
    return {"sent": j["udp_bytes_sent"], "total": j["udp_bytes_sent"] + j["udp_bytes_received"],
            "wall": secs, "transfer": j["elapsed_secs"], "bootstrap": j["bootstrap_secs"],
            "reused_agent": j["reused_agent"], "transport": j["transport"]}


def baseline(sb, ip, port):
    """TCP connect RTT and raw upload throughput through the tunnel on `port`.

    The probe server accepts until killed: Modal's tunnel may open connections of its own."""
    srv = ("import socket, threading\n"
           "s=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n"
           f"s.bind(('0.0.0.0', {port})); s.listen(16)\n"
           "def h(c):\n"
           "    try:\n"
           "        while c.recv(1<<20): pass\n"
           "        c.sendall(b'k')\n"
           "    except OSError: pass\n"
           "    c.close()\n"
           "while True:\n"
           "    c,_=s.accept(); threading.Thread(target=h, args=(c,), daemon=True).start()\n")
    sb.filesystem.write_text(srv, "/tmp/xetbench_probe.py")
    sb.exec("python3", "/tmp/xetbench_probe.py")
    time.sleep(2)
    try:
        up = None
        for _ in range(2):
            try:
                c = socket.create_connection((ip, port), timeout=20)
                c.settimeout(120)
                t = time.time()
                c.sendall(b"y" * (4 * MIB))
                c.shutdown(socket.SHUT_WR)
                c.recv(1)
                up = 4 / (time.time() - t)
                c.close()
                break
            except OSError:
                time.sleep(1)
        rtts = []
        for _ in range(5):
            t = time.time()
            c = socket.create_connection((ip, port), timeout=20)
            rtts.append((time.time() - t) * 1000)
            c.shutdown(socket.SHUT_WR); c.recv(1); c.close()
        return {"rtt_ms": statistics.median(rtts), "up_mib_s": up}
    finally:
        kill = ("import os, signal\n"
                "for p in os.listdir('/proc'):\n"
                "    if p.isdigit() and int(p) != os.getpid():\n"
                "        try: cmd = open(f'/proc/{p}/cmdline', 'rb').read()\n"
                "        except OSError: continue\n"
                "        if b'xetbench_probe' in cmd: os.kill(int(p), signal.SIGKILL)\n")
        sb.exec("python3", "-c", kill).wait()
        time.sleep(0.5)


def with_retries(fn, what, attempts=5):
    """Network paths drop connections now and then. Retry a failed run (after a pause) instead of
    aborting the whole benchmark; how many retries each result needed is recorded."""
    for i in range(attempts):
        try:
            return fn(), i
        except Exception as e:
            if i + 1 == attempts:
                raise
            print(f"  retrying {what} after: {str(e)[:160]}", flush=True)
            time.sleep(10 * (i + 1))  # ride out network outages of a minute or more


# ---------- report ----------

def fmt_bytes(b):
    if b >= 10 * MIB:
        return f"{b / MIB:.1f} MiB"
    if b >= 10 * 1024:
        return f"{b / 1024:.0f} KiB"
    return f"{b:.0f} B"


def cell(values, fmt):
    if not values:
        return "—"
    med = statistics.median(values)
    lo, hi = min(values), max(values)
    return f"{fmt(med)} [{fmt(lo)}–{fmt(hi)}]" if lo != hi else fmt(med)


def report(meta, runs, path):
    names = [n for n, _, _ in scenarios()]
    get = lambda name, tool, key: [r[key] for r in runs if r["scenario"] == name and r["tool"] == tool]
    secs = lambda v: f"{v:.1f}"
    L = []
    L.append("| Scenario | " + " | ".join(TOOLS) + " |")
    L.append("|---|" + "---:|" * len(TOOLS))
    for n in names:
        L.append(f"| {n} | " + " | ".join(cell(get(n, t, "sent"), fmt_bytes) for t in TOOLS) + " |")
    B = ["| Scenario | " + " | ".join(TOOLS) + " |", "|---|" + "---:|" * len(TOOLS)]
    for n in names:
        B.append(f"| {n} | " + " | ".join(cell(get(n, t, "total"), fmt_bytes) for t in TOOLS) + " |")
    W = ["| Scenario | " + " | ".join(TOOLS) + " |", "|---|" + "---:|" * len(TOOLS)]
    for n in names:
        W.append(f"| {n} | " + " | ".join(cell(get(n, t, "wall"), secs) for t in TOOLS) + " |")
    X = ["| Scenario | xet-p2p transfer | xet-p2p bootstrap | --keep-agent transfer | --keep-agent bootstrap |",
         "|---|---:|---:|---:|---:|"]
    for n in names:
        X.append(f"| {n} | {cell(get(n, 'xet-p2p', 'transfer'), secs)} | {cell(get(n, 'xet-p2p', 'bootstrap'), secs)} | "
                 f"{cell(get(n, 'xet-p2p --keep-agent', 'transfer'), secs)} | "
                 f"{cell(get(n, 'xet-p2p --keep-agent', 'bootstrap'), secs)} |")
    N = ["| Repetition | TCP connect RTT | raw upload |", "|---|---:|---:|"]
    for i, b in enumerate(meta["baselines"]):
        up = f"{b['up_mib_s']:.2f} MiB/s" if b["up_mib_s"] else "probe failed"
        N.append(f"| {i + 1} | {b['rtt_ms']:.0f} ms | {up} |")
    with open(path, "w") as f:
        f.write(json.dumps({k: v for k, v in meta.items() if k != "baselines"}) + "\n")
        for title, rows in [("BASELINE", N), ("BYTES_SENT", L), ("BYTES_BOTH", B), ("WALL", W), ("XET_SPLIT", X)]:
            f.write(title + "\n" + "\n".join(rows) + "\n\n")


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xet", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--raw", required=True)
    ap.add_argument("--reps", type=int, default=5)
    args = ap.parse_args()
    xet = os.path.abspath(args.xet)
    work = tempfile.mkdtemp(prefix="xetbench-")
    runs, meta, sb = [], {"baselines": []}, None
    try:
        app = modal.App.lookup("xet-p2p-test", create_if_missing=True)
        image = modal.Image.debian_slim().apt_install("rsync")
        sb = modal.Sandbox.create("sleep", "infinity", app=app, image=image, cpu=1.0,
                                  unencrypted_ports=[XET_PORT, RSYNC_PORT, KEEP_PORT], timeout=3 * 3600)
        print(f"sandbox {sb.object_id} up", flush=True)
        tunnels = sb.tunnels(timeout=60)
        addr = {}
        for port in (XET_PORT, RSYNC_PORT, KEEP_PORT):
            host, p = tunnels[port].tcp_socket
            ip = [a for a in _getaddrinfo(host, p, type=socket.SOCK_STREAM) if a[0] == socket.AF_INET][0][4][0]
            addr[port] = (ip, p)
        u = sb.exec("uname", "-sm"); meta["platform"] = u.stdout.read().strip(); u.wait()
        meta["reps"] = args.reps
        sb.filesystem.write_text("[bench]\n  path = /workspace/rsync\n  read only = false\n"
                                 "  use chroot = false\n  uid = root\n  gid = root\n", "/tmp/rsyncd.conf")
        sb.exec("mkdir", "-p", "/workspace/rsync", "/workspace/xa", "/workspace/xb").wait()
        sb.exec("rsync", "--daemon", "--no-detach", f"--port={RSYNC_PORT}", "--config=/tmp/rsyncd.conf")
        time.sleep(2)
        rip, rport = addr[RSYNC_PORT]
        keep_uri_root = f"modal://{sb.object_id}/workspace/xb"
        fresh_uri_root = f"modal://{sb.object_id}/workspace/xa"

        for rep in range(args.reps):
            b = baseline(sb, *addr[XET_PORT])
            meta["baselines"].append(b)
            print(f"rep {rep + 1}: baseline RTT {b['rtt_ms']:.0f} ms, upload {b['up_mib_s'] or 0:.2f} MiB/s", flush=True)
            for si, (name, setup, edit) in enumerate(scenarios()):
                seed = (si + 1) * 1000 + rep
                order = TOOLS[:]
                random.Random(seed).shuffle(order)
                for tool in order:
                    tag = f"r{rep}s{si}{TOOLS.index(tool)}"
                    src = os.path.join(work, tag)
                    setup(src, seed)
                    if tool.startswith("rsync"):
                        z = tool == "rsync -az"
                        url = f"rsync://{rip}:{rport}/bench/{tag}/"
                        run = lambda: run_rsync(src, url, z)
                        remote = f"/workspace/rsync/{tag}"
                    else:
                        keep = tool.endswith("keep-agent")
                        root, port = (keep_uri_root, KEEP_PORT) if keep else (fresh_uri_root, XET_PORT)
                        target = f"{root}/{tag}?port={port}&root={root.split(sb.object_id)[1]}"
                        cas = os.path.join(work, f"cas-{tag}")
                        run = lambda: run_xet(xet, src, target, cas, keep)
                        remote = f"{root.split(sb.object_id)[1]}/{tag}"
                    retries = 0
                    if edit:
                        # Steady state: the tree was synced a while ago and again since (so
                        # xet-p2p's stamp caches hold settled records on both sides; files
                        # changed within 2 s of being hashed are never trusted). Same for every tool.
                        _, r0 = with_retries(run, f"{tool} {name} (setup sync)")
                        time.sleep(SETTLE_SECS)
                        _, r0b = with_retries(run, f"{tool} {name} (resync)")
                        retries += r0 + r0b
                        edit(src, seed)
                    m, r1 = with_retries(run, f"{tool} {name}")
                    m["retries"] = retries + r1
                    m["leftovers"] = check_tree(remote_digests(sb, remote), digest_tree(src), f"{tool} {name} rep {rep}")
                    m.update({"tool": tool, "scenario": name, "rep": rep})
                    runs.append(m)
                    print(f"  rep {rep + 1} {tool:22} {name}: {fmt_bytes(m['sent'])}, {m['wall']:.1f}s "
                          f"(transfer {m['transfer']:.1f}s, bootstrap {m['bootstrap']:.1f}s)", flush=True)
                    shutil.rmtree(src)
                    shutil.rmtree(os.path.join(work, f"cas-{tag}"), ignore_errors=True)
                    os.makedirs(os.path.dirname(os.path.abspath(args.raw)), exist_ok=True)
                    json.dump({"meta": meta, "runs": runs}, open(args.raw, "w"), indent=1)
            report(meta, runs, args.out)
    finally:
        if sb is not None:
            sb.terminate()
            print(f"sandbox {sb.object_id} terminated", flush=True)
        shutil.rmtree(work, ignore_errors=True)
    print(f"wrote {args.out} and {args.raw}")


if __name__ == "__main__":
    main()
