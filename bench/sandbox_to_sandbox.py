#!/usr/bin/env python3
"""xet-p2p vs rsync between two Modal sandboxes, on larger datasets.

Method
------
- Two sandboxes (debian_slim + rsync, 4 CPUs, 8 GiB each). A sends, B receives. B exposes raw TCP
  tunnels: 4434 = `xet-p2p serve` (TLS over TCP, pinned certificates from `xet-p2p keygen`),
  4435 = rsync daemon, 4436 = raw throughput probe. A reaches B through B's tunnel, so both tools
  share the same path (Modal's edge relays it; Modal tunnels carry no UDP, so no QUIC here).
  Private sandbox networking (i6pn) needs region pinning, which this account's plan does not allow.
- Tools, run inside A: `rsync -a`, `rsync -az` (daemon protocol, delta transfer on, unencrypted),
  `xet-p2p` (TLS over TCP, chunk mode, agent already running on B).
- Scenarios (sizes x --scale): a 2 GiB checkpoint of 64 tensors (cold; 4 tensors rewritten;
  unchanged; the same checkpoint pushed to a second destination), 20k small text files (cold;
  50 edited), 16 x 64 MiB dataset shards with 2 new shards appended, and a 1 GiB log with 16 MiB
  appended.
- R repetitions (default 3). New data per repetition (seeded by scenario and repetition, identical
  for every tool within it); tool order shuffled per scenario. B's xet-p2p CAS is wiped between
  repetitions, so a repetition never benefits from an earlier one; within a repetition it is kept
  (the second-destination scenario depends on that, as it would in real use).
- "Warm" = steady state: sync, wait 3 s, sync again (unmeasured), edit, then measure.
- Times are measured inside A (no orchestration overhead). Every measured tree is verified by
  SHA-256 on both sides.

Usage (needs the Modal SDK's Python and a Linux xet-p2p build):
    XET_P2P_AGENT_LINUX_BIN=target/x86_64-unknown-linux-musl/release/xet-p2p \\
    <modal-python> bench/sandbox_to_sandbox.py --out bench/results/s2s.md --raw bench/results/s2s.json
"""
import argparse
import hashlib
import json
import os
import random
import re
import shutil
import statistics
import subprocess
import sys
import time

MIB = 1024 * 1024
XET_PORT, RSYNC_PORT, PROBE_PORT = 4434, 4435, 4436
TOOLS = ["rsync -a", "rsync -az", "xet-p2p"]
X = "/opt/x/xet-p2p"
KEYS = "/opt/keys"
SETTLE_SECS = 3

# =====================================================================================
# Driver: runs inside the sandboxes (`python3 bench.py driver <json>`), stdlib only.
# =====================================================================================

WORDS = ("fn let mut struct impl self return if else match for while loop use pub mod crate "
         "async await Result Option Some None Ok Err String Vec Box Arc clone into from new "
         "data value index count total error config path file buffer chunk hash").split()


def write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        f.write(data)


def text(r, n):
    out, size = [], 0
    while size < n:
        line = " ".join(r.choices(WORDS, k=r.randint(4, 14))) + "\n"
        out.append(line)
        size += len(line)
    return "".join(out).encode()[:n]


def log_lines(r, n):
    out, size, step = [], 0, r.randint(0, 10**6)
    while size < n:
        step += 1
        line = (f"2026-11-01T08:{step // 60 % 60:02d}:{step % 60:02d}.{r.randint(0, 999):03d}Z INFO "
                f"worker={r.randint(0, 63)} step={step} loss={r.random() * 4:.5f} "
                f"lr={r.random() * 1e-3:.2e} tokens={r.randint(10**5, 10**6)}\n")
        out.append(line)
        size += len(line)
    return "".join(out).encode()[:n]


def gen(scenario, seed, root, scale):
    r = random.Random(seed)
    if scenario["data"] == "ckpt":
        n = scenario["tensors"]
        for i in range(n):
            write(f"{root}/model-{i:03d}.bin", r.randbytes(int(scenario["tensor_mib"] * MIB * scale)))
    elif scenario["data"] == "tree":
        for i in range(int(scenario["files"] * scale)):
            write(f"{root}/src/m{i % 97:02d}/f{i}.rs", text(r, r.randint(1024, 16 * 1024)))
    elif scenario["data"] == "shards":
        for i in range(scenario["shards"]):
            write(f"{root}/shard-{i:05d}.bin", r.randbytes(int(scenario["shard_mib"] * MIB * scale)))
    elif scenario["data"] == "log":
        write(f"{root}/train.log", log_lines(r, int(scenario["log_mib"] * MIB * scale)))


def bump(path):
    t = time.time() + 10
    os.utime(path, (t, t))


def edit(scenario, seed, root, scale):
    r = random.Random(seed + 7)
    kind = scenario.get("edit")
    if kind == "rewrite_tensors":
        for i in r.sample(range(scenario["tensors"]), scenario["rewrite"]):
            p = f"{root}/model-{i:03d}.bin"
            write(p, r.randbytes(os.path.getsize(p)))
            bump(p)
    elif kind == "edit_files":
        files = sorted(os.path.join(d, f) for d, _, fs in os.walk(root) for f in fs)
        for p in r.sample(files, scenario["edits"]):
            data = bytearray(open(p, "rb").read())
            at = r.randrange(len(data))
            data[at:at] = b" // edited\n"
            write(p, bytes(data))
            bump(p)
    elif kind == "append_shards":
        base = scenario["shards"]
        for i in range(base, base + scenario["new_shards"]):
            write(f"{root}/shard-{i:05d}.bin", r.randbytes(int(scenario["shard_mib"] * MIB * scale)))
    elif kind == "append_log":
        p = f"{root}/train.log"
        with open(p, "ab") as f:
            f.write(log_lines(r, int(scenario["append_mib"] * MIB * scale)))
        bump(p)


def digest(root):
    h = hashlib.sha256()
    for d, _, fs in sorted(os.walk(root)):
        for f in sorted(fs):
            p = os.path.join(d, f)
            fh = hashlib.sha256()
            with open(p, "rb") as fp:
                for block in iter(lambda: fp.read(MIB), b""):
                    fh.update(block)
            h.update(f"{os.path.relpath(p, root)}\0{fh.hexdigest()}\n".encode())
    return h.hexdigest()


def run_tool(tool, src, dest, cas, b_host):
    t = time.time()
    if tool.startswith("rsync"):
        host, port = b_host["rsync"]
        cmd = ["rsync", "-az" if tool == "rsync -az" else "-a", "--stats", f"{src}/",
               f"rsync://{host}:{port}/bench/{dest}/"]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
        secs = time.time() - t
        if r.returncode != 0:
            raise RuntimeError(f"rsync failed: {r.stderr.strip()[:500]}")
        # GNU rsync prints "Total bytes sent:", openrsync "Total sent:".
        num = lambda w: int(re.search(rf"Total (?:bytes )?{w}:\s*([\d,]+)", r.stdout).group(1).replace(",", ""))
        sent, recv = num("sent"), num("received")
        return {"sent": sent, "total": sent + recv, "wall": secs, "transfer": secs}
    host, port = b_host["xet"]
    cmd = [X, "push", src, f"tcp://{host}:{port}/{dest}", "--cert", f"{KEYS}/client.crt",
           "--key", f"{KEYS}/client.key", "--peer-cert", f"{KEYS}/server.crt", "--cas-dir", cas, "--json"]
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    secs = time.time() - t
    if r.returncode != 0:
        raise RuntimeError(f"xet-p2p failed: {r.stderr.strip()[-800:]}")
    j = json.loads(r.stdout)
    return {"sent": j["udp_bytes_sent"], "total": j["udp_bytes_sent"] + j["udp_bytes_received"],
            "wall": secs, "transfer": j["elapsed_secs"], "connect": j["connect_secs"],
            "cached_files": j["ingest"]["cached_files"], "trusted": j["receiver"]["files_trusted"],
            "payload": j["payload_bytes_sent"] if "payload_bytes_sent" in j else None}


def with_retries(fn, attempts=3):
    for i in range(attempts):
        try:
            return fn(), i
        except Exception as e:  # tunnels drop connections now and then
            if i + 1 == attempts:
                raise
            print(f"retrying after: {str(e)[:200]}", file=sys.stderr, flush=True)
            time.sleep(10)


def driver(req):
    op = req["op"]
    if op == "digest":
        return {"digest": digest(req["path"])}
    if op == "gen":
        shutil.rmtree(req["root"], ignore_errors=True)
        gen(req["scenario"], req["seed"], req["root"], req["scale"])
        return {"bytes": sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(req["root"]) for f in fs)}
    if op == "probe":
        import socket
        host, port = req["host"]
        rtts = []
        for _ in range(5):
            t = time.time()
            c = socket.create_connection((host, port), timeout=20)
            rtts.append((time.time() - t) * 1000)
            c.close()
        c = socket.create_connection((host, port), timeout=20)
        block = b"\0" * MIB
        t = time.time()
        for _ in range(req["mib"]):
            c.sendall(block)
        c.shutdown(socket.SHUT_WR)
        c.recv(1)
        return {"rtt_ms": statistics.median(rtts), "up_mib_s": req["mib"] / (time.time() - t)}
    if op == "measure":
        sc, tool, seed, scale = req["scenario"], req["tool"], req["seed"], req["scale"]
        src, cas, dest = req["src"], req["cas"], req["dest"]
        shutil.rmtree(src, ignore_errors=True)
        shutil.rmtree(cas, ignore_errors=True)
        subprocess.run(["cp", "-a", req["base"], src], check=True)
        run = lambda d=dest: run_tool(tool, src, d, cas, req["b"])
        retries = 0
        if sc["kind"] == "warm":
            for _ in range(2):
                retries += with_retries(run)[1]
                time.sleep(SETTLE_SECS)
            edit(sc, seed, src, scale)
        elif sc["kind"] == "second_dest":
            retries += with_retries(lambda: run(dest + "-first"))[1]
        m, r = with_retries(run)
        m["retries"] = retries + r
        m["src_digest"] = digest(src)
        shutil.rmtree(src, ignore_errors=True)
        shutil.rmtree(cas, ignore_errors=True)
        return m
    raise ValueError(op)


# =====================================================================================
# Orchestrator: runs on this machine with the Modal SDK.
# =====================================================================================

def scenarios():
    ck = {"data": "ckpt", "tensors": 64, "tensor_mib": 32}
    tree = {"data": "tree", "files": 20000}
    return [
        ("cold, 2 GiB checkpoint (64 tensors)", {**ck, "kind": "cold"}),
        ("warm, 4 of 64 tensors rewritten", {**ck, "kind": "warm", "edit": "rewrite_tensors", "rewrite": 4}),
        ("warm, 2 GiB checkpoint unchanged", {**ck, "kind": "warm"}),
        ("same checkpoint to a second destination", {**ck, "kind": "second_dest"}),
        ("cold, 20k small source files", {**tree, "kind": "cold"}),
        ("warm, 50 of 20k files edited", {**tree, "kind": "warm", "edit": "edit_files", "edits": 50}),
        ("warm, 2 x 64 MiB shards added to 16", {"data": "shards", "shards": 16, "shard_mib": 64,
                                                 "kind": "warm", "edit": "append_shards", "new_shards": 2}),
        ("warm, 16 MiB appended to 1 GiB log", {"data": "log", "log_mib": 1024, "kind": "warm",
                                                "edit": "append_log", "append_mib": 16}),
    ]


def fmt_bytes(n):
    if n >= 1024 * MIB:
        return f"{n / (1024 * MIB):.2f} GiB"
    if n >= MIB:
        return f"{n / MIB:.1f} MiB"
    return f"{n / 1024:.0f} KiB"


def cell(vals, f):
    if not vals:
        return "—"
    med = statistics.median(vals)
    lo, hi = min(vals), max(vals)
    return f(med) if lo == hi else f"{f(med)} [{f(lo)}–{f(hi)}]"


def report(meta, runs, path):
    names = [n for n, _ in scenarios()]
    get = lambda n, t, k: [r[k] for r in runs if r["scenario"] == n and r["tool"] == t and r.get(k) is not None]
    secs = lambda v: f"{v:.1f}"
    out = [f"<!-- generated by bench/sandbox_to_sandbox.py; {json.dumps(meta)} -->", "", "WALL (s)",
           "| Scenario | " + " | ".join(TOOLS) + " |", "|---|" + "---:|" * len(TOOLS)]
    for n in names:
        out.append(f"| {n} | " + " | ".join(cell(get(n, t, "wall"), secs) for t in TOOLS) + " |")
    out += ["", "BYTES SENT", "| Scenario | " + " | ".join(TOOLS) + " |", "|---|" + "---:|" * len(TOOLS)]
    for n in names:
        out.append(f"| {n} | " + " | ".join(cell(get(n, t, "sent"), fmt_bytes) for t in TOOLS) + " |")
    out += ["", "XET SPLIT (s)", "| Scenario | transfer | connect | files cached (sender) | files trusted (receiver) |",
            "|---|---:|---:|---:|---:|"]
    for n in names:
        out.append(f"| {n} | {cell(get(n, 'xet-p2p', 'transfer'), secs)} | {cell(get(n, 'xet-p2p', 'connect'), secs)} | "
                   f"{cell(get(n, 'xet-p2p', 'cached_files'), str)} | {cell(get(n, 'xet-p2p', 'trusted'), str)} |")
    out += ["", "BASELINE", "| Repetition | TCP connect RTT A→B | raw throughput A→B |", "|---|---:|---:|"]
    for i, b in enumerate(meta["baselines"]):
        out.append(f"| {i + 1} | {b['rtt_ms']:.0f} ms | {b['up_mib_s']:.0f} MiB/s |")
    with open(path, "w") as f:
        f.write("\n".join(out) + "\n")


def orchestrate(args):
    import socket
    _gai = socket.getaddrinfo
    socket.getaddrinfo = lambda *a, **k: sorted(_gai(*a, **k), key=lambda i: i[0] != socket.AF_INET)
    import modal

    agent = os.environ.get("XET_P2P_AGENT_LINUX_BIN")
    if not agent:
        sys.exit("set XET_P2P_AGENT_LINUX_BIN to a Linux build of xet-p2p")
    app = modal.App.lookup("xet-p2p-test", create_if_missing=True)
    image = modal.Image.debian_slim().apt_install("rsync")
    boxes = []

    def sh(sb, cmd, t=3600):
        p = sb.exec("sh", "-c", cmd, timeout=t)
        out, err = p.stdout.read(), p.stderr.read()
        p.wait()
        if p.returncode != 0:
            raise RuntimeError(f"{cmd[:80]!r} failed ({p.returncode}): {err.strip()[-800:]}")
        return out

    def drive(sb, req, t=3600):
        out = sh(sb, f"python3 /opt/x/bench.py driver '{json.dumps(req)}'", t)
        return json.loads(out.strip().splitlines()[-1])

    meta = {"reps": args.reps, "scale": args.scale, "baselines": []}
    runs = []
    try:
        for role in ("A", "B"):
            kw = {"unencrypted_ports": [XET_PORT, RSYNC_PORT, PROBE_PORT]} if role == "B" else {}
            sb = modal.Sandbox.create("sleep", "infinity", app=app, image=image, cpu=4.0, memory=8192,
                                      timeout=4 * 3600, **kw)
            boxes.append(sb)
            print(f"sandbox {role} {sb.object_id} up", flush=True)
        a, b = boxes
        for sb in boxes:
            sh(sb, "mkdir -p /opt/x")
            sb.filesystem.copy_from_local(agent, X)
            sb.filesystem.copy_from_local(os.path.abspath(__file__), "/opt/x/bench.py")
            sh(sb, f"chmod 755 {X}")
        meta["platform"] = sh(a, "uname -sm").strip()
        meta["cpus"] = sh(a, "nproc").strip()
        meta["disk_a"] = sh(a, "df -h / | tail -1").strip()
        meta["disk_b"] = sh(b, "df -h / | tail -1").strip()
        print(f"A: {meta['platform']}, {meta['cpus']} CPUs; disk A {meta['disk_a']}; disk B {meta['disk_b']}", flush=True)

        # Pinned identities: A is the client, B the server; each gets the other's certificate.
        sh(a, f"{X} keygen --out {KEYS} --name client")
        sh(b, f"{X} keygen --out {KEYS} --name server")
        a.filesystem.write_text(sh(b, f"cat {KEYS}/server.crt"), f"{KEYS}/server.crt")
        b.filesystem.write_text(sh(a, f"cat {KEYS}/client.crt"), f"{KEYS}/client.crt")

        tun = b.tunnels(timeout=60)
        host = {k: list(tun[p].tcp_socket) for k, p in (("xet", XET_PORT), ("rsync", RSYNC_PORT), ("probe", PROBE_PORT))}
        b.filesystem.write_text("[bench]\n  path = /recv/rsync\n  read only = false\n"
                                "  use chroot = false\n  uid = root\n  gid = root\n", "/tmp/rsyncd.conf")
        sh(b, "mkdir -p /recv/rsync /recv/xet")
        b.exec("rsync", "--daemon", "--no-detach", f"--port={RSYNC_PORT}", "--config=/tmp/rsyncd.conf")
        sink = ("import socket, threading\ns=socket.socket(); s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n"
                f"s.bind(('0.0.0.0', {PROBE_PORT})); s.listen(16)\n"
                "def h(c):\n    try:\n        while c.recv(1<<20): pass\n        c.sendall(b'k')\n    except OSError: pass\n    c.close()\n"
                "while True:\n    c,_=s.accept(); threading.Thread(target=h, args=(c,), daemon=True).start()\n")
        b.filesystem.write_text(sink, "/tmp/sink.py")
        b.exec("python3", "/tmp/sink.py")

        def start_xet():
            sh(b, "for p in /proc/[0-9]*; do [ \"$(readlink $p/exe 2>/dev/null)\" = /opt/x/xet-p2p ] && kill ${p#/proc/}; done; "
                  "sleep 1; rm -rf /recv/cas /recv/xet /tmp/ready.json; mkdir -p /recv/xet; "
                  f"nohup {X} serve --no-quic --listen-tcp 0.0.0.0:{XET_PORT} --root /recv/xet --cas-dir /recv/cas "
                  f"--cert {KEYS}/server.crt --key {KEYS}/server.key --allow-client {KEYS}/client.crt "
                  "--ready-file /tmp/ready.json > /tmp/serve.log 2>&1 < /dev/null & "
                  "for i in $(seq 1 100); do [ -s /tmp/ready.json ] && exit 0; sleep 0.1; done; cat /tmp/serve.log; exit 1")

        time.sleep(2)
        for rep in range(args.reps):
            start_xet()
            sh(b, "rm -rf /recv/rsync/*")
            base = drive(a, {"op": "probe", "host": host["probe"], "mib": 256})
            meta["baselines"].append(base)
            print(f"rep {rep + 1}: A→B RTT {base['rtt_ms']:.0f} ms, raw {base['up_mib_s']:.0f} MiB/s", flush=True)
            for si, (name, sc) in enumerate(scenarios()):
                seed = (si + 1) * 1000 + rep
                g = drive(a, {"op": "gen", "scenario": sc, "seed": seed, "root": "/work/base", "scale": args.scale})
                order = TOOLS[:]
                random.Random(seed).shuffle(order)
                for tool in order:
                    tag = f"r{rep}s{si}t{TOOLS.index(tool)}"
                    m = drive(a, {"op": "measure", "scenario": sc, "tool": tool, "seed": seed, "scale": args.scale,
                                  "base": "/work/base", "src": "/work/src", "cas": "/work/cas", "dest": tag, "b": host})
                    remote = f"/recv/rsync/{tag}" if tool.startswith("rsync") else f"/recv/xet/{tag}"
                    got = drive(b, {"op": "digest", "path": remote})["digest"]
                    assert got == m.pop("src_digest"), f"{tool} {name} rep {rep}: tree differs"
                    m.update({"tool": tool, "scenario": name, "rep": rep, "data_bytes": g["bytes"]})
                    runs.append(m)
                    print(f"  rep {rep + 1} {tool:10} {name}: {fmt_bytes(m['sent'])} sent, {m['wall']:.1f}s", flush=True)
                    sh(b, f"rm -rf {remote} {remote}-first")
                    with open(args.raw, "w") as f:
                        json.dump({"meta": meta, "runs": runs}, f, indent=1)
            report(meta, runs, args.out)
        report(meta, runs, args.out)
        print(f"wrote {args.out} and {args.raw}", flush=True)
    finally:
        for sb in boxes:
            try:
                sb.terminate()
                print(f"sandbox {sb.object_id} terminated", flush=True)
            except Exception as e:
                print(f"could not terminate {sb.object_id}: {e}", flush=True)


if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "driver":
        print(json.dumps(driver(json.loads(sys.argv[2]))))
        sys.exit(0)
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--raw", required=True)
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--scale", type=float, default=1.0, help="multiply dataset sizes (e.g. 0.05 to smoke-test)")
    orchestrate(ap.parse_args())
