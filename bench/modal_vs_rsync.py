#!/usr/bin/env python3
"""xet-p2p vs rsync over a real network path: this machine -> a Modal sandbox.

Both tools go through the *same* Modal raw TCP tunnel to the same sandbox:

- rsync: an rsync daemon in the sandbox on the tunnelled port (`rsync://`, delta transfer on,
  with and without zlib). The daemon protocol is unencrypted; xet-p2p pays for TLS.
- xet-p2p: `push SRC modal://SANDBOX/...` (chunk mode, TLS over TCP through the tunnel).

The port is shared, so all rsync runs happen with the daemon up, then it is stopped and all
xet-p2p runs happen. Every scenario uses its own deterministic data (no cross-scenario sharing),
"warm" scenarios sync the first version unmeasured, and every final result is checked by SHA-256
inside the sandbox. The sandbox is created for the run and terminated at the end, even on error.

Usage (needs the Modal SDK's Python, a Linux xet-p2p build, and a local xet-p2p):
    XET_P2P_AGENT_LINUX_BIN=target/x86_64-unknown-linux-musl/release/xet-p2p \\
    <modal-python> bench/modal_vs_rsync.py --xet target/release/xet-p2p --out docs/BENCHMARK_M5_MODAL.md
"""

import argparse
import hashlib
import json
import os
import random
import re
import shutil
import socket
import subprocess
import tempfile
import time

# DNS64 without working NAT64 makes the Modal SDK hang on synthesized IPv6 addresses.
_getaddrinfo = socket.getaddrinfo
socket.getaddrinfo = lambda *a, **k: sorted(_getaddrinfo(*a, **k), key=lambda i: i[0] != socket.AF_INET)

import modal  # noqa: E402

MIB = 1024 * 1024
PORT = 4434


# ---------- deterministic data ----------

def rand_bytes(seed, n):
    return random.Random(seed).randbytes(n)


WORDS = ("INFO WARN request completed user id latency_ms status=200 GET /api/v1/items cache miss "
         "hit shard worker retry ok connection closed bytes elapsed upstream token refresh").split()


def log_text(seed, n):
    r = random.Random(seed)
    out, line = [], 0
    size = 0
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


def bump(path):
    # rsync's quick check has 1 s resolution; a real later edit has a later mtime.
    t = time.time() + 10
    os.utime(path, (t, t))


def tree(root, seed, n=1000):
    r = random.Random(seed)
    for i in range(n):
        write(root, f"d{i % 20:02d}/f{i:04d}.txt", log_text(seed * 100000 + i, r.randrange(1024, 16 * 1024)))


def insert(path, at, extra):
    with open(path, "rb") as f:
        v = f.read()
    with open(path, "wb") as f:
        f.write(v[:at] + extra + v[at:])
    bump(path)


def scenarios():
    def ckpt_setup(s):
        write(s, "model.ckpt", b"".join(rand_bytes(300 + t, MIB) for t in range(32)))

    def ckpt_edit(s):
        p = os.path.join(s, "model.ckpt")
        v = bytearray(open(p, "rb").read())
        for t in (5, 20):
            v[t * MIB:(t + 1) * MIB] = rand_bytes(900 + t, MIB)
        open(p, "wb").write(v)
        bump(p)

    def append_log(s):
        p = os.path.join(s, "app.log")
        open(p, "ab").write(log_text(6, 512 * 1024))
        bump(p)

    def edit_tree(s):
        for i in range(0, 1000, 100):
            p = os.path.join(s, f"d{i % 20:02d}/f{i:04d}.txt")
            open(p, "ab").write(b"edited line\n")
            bump(p)

    return [
        ("cold, 16 MiB random file", lambda s: write(s, "f.bin", rand_bytes(1, 16 * MIB)), None),
        ("warm, 4 KiB inserted mid 16 MiB random file", lambda s: write(s, "f.bin", rand_bytes(2, 16 * MIB)),
         lambda s: insert(os.path.join(s, "f.bin"), 8 * MIB, rand_bytes(3, 4096))),
        ("warm, checkpoint: 2 of 32 x 1 MiB tensors rewritten", ckpt_setup, ckpt_edit),
        ("cold, 16 MiB compressible log", lambda s: write(s, "app.log", log_text(4, 16 * MIB)), None),
        ("warm, 512 KiB appended to 16 MiB log", lambda s: write(s, "app.log", log_text(5, 16 * MIB)), append_log),
        ("cold, 1000 small files (1-16 KiB text)", lambda s: tree(s, 7), None),
        ("warm, 10 of 1000 small files edited", lambda s: tree(s, 8), edit_tree),
        ("warm, 1000 small files unchanged", lambda s: tree(s, 9), lambda s: None),
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


def stop_port_users(sb):
    """Stops the rsync daemon and waits until the tunnelled port can be bound again.

    debian_slim has no pkill, so processes are found through /proc."""
    kill = (
        "import os, signal\n"
        "for p in os.listdir('/proc'):\n"
        "    if p.isdigit() and int(p) != os.getpid():\n"
        "        try: cmd = open(f'/proc/{p}/cmdline', 'rb').read()\n"
        "        except OSError: continue\n"
        "        if cmd.startswith(b'rsync'): os.kill(int(p), signal.SIGKILL)\n"
    )
    sb.exec("python3", "-c", kill).wait()
    # SO_REUSEADDR, as the agent's listener uses: lingering TIME_WAIT connections do not block it.
    probe = (f"import socket, time\nfor _ in range(50):\n    s = socket.socket()\n"
             f"    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n"
             f"    try: s.bind(('0.0.0.0', {PORT})); s.listen(1); s.close(); print('free'); break\n"
             f"    except OSError: time.sleep(0.2)\n")
    p = sb.exec("python3", "-c", probe)
    ok = p.stdout.read().strip() == "free"
    p.wait()
    if not ok:
        ps = sb.exec("sh", "-c", "for p in /proc/[0-9]*; do tr '\\0' ' ' < $p/cmdline; echo; done")
        holders = [l for l in ps.stdout.read().splitlines() if l.strip()]
        ps.wait()
        raise RuntimeError(f"port {PORT} still in use after stopping rsync; processes: {holders}")


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
    return {"sent": sent, "total": sent + recv, "secs": secs, "transfer_secs": secs}


def run_xet(xet, src, target, cas):
    t = time.time()
    r = subprocess.run([xet, "push", src, target, "--cas-dir", cas, "--json"],
                       capture_output=True, text=True, timeout=1800)
    secs = time.time() - t
    if r.returncode != 0:
        raise RuntimeError(f"xet-p2p failed: {r.stderr.strip()[-800:]}")
    j = json.loads(r.stdout)
    return {"sent": j["udp_bytes_sent"], "total": j["udp_bytes_sent"] + j["udp_bytes_received"],
            "secs": secs, "transfer_secs": j["elapsed_secs"], "transport": j["transport"],
            "payload": j["payload_bytes_sent"]}


# ---------- main ----------

def fmt(b):
    if b >= 10 * MIB:
        return f"{b / MIB:.1f} MiB"
    if b >= 10 * 1024:
        return f"{b / 1024:.0f} KiB"
    return f"{b} B"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--xet", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    xet = os.path.abspath(args.xet)
    work = tempfile.mkdtemp(prefix="xetbench-")
    rows = []
    sb = None
    try:
        app = modal.App.lookup("xet-p2p-test", create_if_missing=True)
        image = modal.Image.debian_slim().apt_install("rsync")
        t = time.time()
        sb = modal.Sandbox.create("sleep", "infinity", app=app, image=image, cpu=1.0,
                                  unencrypted_ports=[PORT], timeout=3600)
        print(f"sandbox {sb.object_id} up in {time.time() - t:.0f}s", flush=True)
        host, port = sb.tunnels(timeout=60)[PORT].tcp_socket
        ip = [a for a in _getaddrinfo(host, port, type=socket.SOCK_STREAM) if a[0] == socket.AF_INET][0][4][0]
        uname = sb.exec("uname", "-sm"); platform = uname.stdout.read().strip(); uname.wait()
        sb.filesystem.write_text("[bench]\n  path = /workspace/rsync\n  read only = false\n"
                                 "  use chroot = false\n  uid = root\n  gid = root\n", "/tmp/rsyncd.conf")
        sb.exec("mkdir", "-p", "/workspace/rsync", "/workspace/xet").wait()

        results = {name: {} for name, _, _ in scenarios()}

        # rsync, with the daemon on the tunnelled port.
        daemon = sb.exec("rsync", "--daemon", "--no-detach", f"--port={PORT}", "--config=/tmp/rsyncd.conf")
        time.sleep(2)
        for i, (name, setup, edit) in enumerate(scenarios()):
            for compress in (False, True):
                key = "rsync -az" if compress else "rsync -a"
                src = os.path.join(work, f"rs{i}{'z' if compress else ''}")
                setup(src)
                url = f"rsync://{ip}:{port}/bench/s{i}{'z' if compress else ''}/"
                if edit:
                    run_rsync(src, url, compress)
                    edit(src)
                m = run_rsync(src, url, compress)
                remote = remote_digests(sb, f"/workspace/rsync/s{i}{'z' if compress else ''}")
                assert remote == digest_tree(src), f"{key} {name}: remote tree differs"
                results[name][key] = m
                print(f"{key:9} {name}: {fmt(m['sent'])} in {m['secs']:.1f}s", flush=True)
                shutil.rmtree(src)
        stop_port_users(sb)

        # xet-p2p, each scenario with its own fresh local CAS.
        for i, (name, setup, edit) in enumerate(scenarios()):
            src, cas = os.path.join(work, f"x{i}"), os.path.join(work, f"cas{i}")
            setup(src)
            target = f"modal://{sb.object_id}/workspace/xet/s{i}?port={PORT}"
            if edit:
                run_xet(xet, src, target, cas)
                edit(src)
            m = run_xet(xet, src, target, cas)
            remote = remote_digests(sb, f"/workspace/xet/s{i}")
            assert remote == digest_tree(src), f"xet-p2p {name}: remote tree differs"
            results[name]["xet-p2p"] = m
            print(f"xet-p2p   {name}: {fmt(m['sent'])} in {m['secs']:.1f}s "
                  f"(transfer {m['transfer_secs']:.1f}s, {m['transport']})", flush=True)
            shutil.rmtree(src)
        rows = results
    finally:
        if sb is not None:
            sb.terminate()
            print(f"sandbox {sb.object_id} terminated", flush=True)
        shutil.rmtree(work, ignore_errors=True)

    tools = ["rsync -a", "rsync -az", "xet-p2p"]
    lines = [
        "| Scenario | " + " | ".join(tools) + " |",
        "|---|" + "---:|" * len(tools),
    ]
    for name, r in rows.items():
        lines.append(f"| {name} | " + " | ".join(fmt(r[t]["sent"]) for t in tools) + " |")
    both = ["| Scenario | " + " | ".join(tools) + " |", "|---|" + "---:|" * len(tools)]
    for name, r in rows.items():
        both.append(f"| {name} | " + " | ".join(fmt(r[t]["total"]) for t in tools) + " |")
    times = ["| Scenario | rsync -a | rsync -az | xet-p2p (transfer) | xet-p2p (end to end) |",
             "|---|---:|---:|---:|---:|"]
    for name, r in rows.items():
        times.append(f"| {name} | {r['rsync -a']['secs']:.1f} | {r['rsync -az']['secs']:.1f} | "
                     f"{r['xet-p2p']['transfer_secs']:.1f} | {r['xet-p2p']['secs']:.1f} |")
    with open(args.out, "w") as f:
        f.write(json.dumps({"platform": platform, "tunnel": f"{host}:{port}"}) + "\n")
        f.write("\n".join(["BYTES_SENT"] + lines + ["", "BYTES_BOTH"] + both + ["", "TIMES"] + times) + "\n")
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
