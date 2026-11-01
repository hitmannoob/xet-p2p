#!/usr/bin/env python3
"""A persistent pair of Modal sandboxes for fast measure-change-measure cycles.

    devpair.py up [--shards 2 --extra 1]   create A (sender) and B (receiver), keys, rsync daemon,
                                           and download FineWeb-Edu shards into A once
    devpair.py deploy BIN                  upload a Linux xet-p2p build to both; restart B's agent
    devpair.py run [--tools xet,rsync]     cold -> unchanged x2 -> grown -> second destination,
                   [--profile]             per tool, verified; --profile prints xet-p2p's phases
    devpair.py sh a|b CMD                  run a shell command in A or B
    devpair.py down                        terminate both

State (sandbox ids, tunnel addresses) lives in bench/.devpair.json. The sandboxes stop by
themselves after --hours (default 2), so a forgotten pair does not run on.
"""
import argparse
import json
import os
import socket
import sys
import time

STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".devpair.json")
REPO, SUBDIR = "HuggingFaceFW/fineweb-edu", "sample/100BT"
X, K = "/opt/x/xet-p2p", "/opt/keys"
XET_PORT, RSYNC_PORT = 4434, 4435
STEPS = ["cold", "unchanged", "unchanged-steady", "grown", "second-dest"]
DIGEST = "cd {0} && find . -type f -print0 | sort -z | xargs -0 -P4 -n4 sha256sum | sort -k2 | sha256sum | cut -c1-64"


def modal():
    gai = socket.getaddrinfo
    socket.getaddrinfo = lambda *a, **k: sorted(gai(*a, **k), key=lambda i: i[0] != socket.AF_INET)
    import modal as m
    return m


def sh(sb, cmd, t=3600, check=True):
    p = sb.exec("sh", "-c", cmd, timeout=t)
    out, err = p.stdout.read(), p.stderr.read()
    p.wait()
    if check and p.returncode != 0:
        sys.exit(f"{cmd[:120]!r} failed ({p.returncode}): {err.strip()[-1500:]}")
    return out.strip()


def load():
    if not os.path.exists(STATE):
        sys.exit("no pair: run `devpair.py up` first")
    st = json.load(open(STATE))
    m = modal()
    return st, m.Sandbox.from_id(st["a"]), m.Sandbox.from_id(st["b"])


def up(args):
    m = modal()
    app = m.App.lookup("xet-p2p-test", create_if_missing=True)
    image = m.Image.debian_slim().apt_install("rsync").pip_install("huggingface_hub[hf_xet]")
    a = m.Sandbox.create("sleep", "infinity", app=app, image=image, cpu=4.0, memory=8192,
                         timeout=int(args.hours * 3600), region="us-east")
    b = m.Sandbox.create("sleep", "infinity", app=app, image=image, cpu=4.0, memory=8192,
                         timeout=int(args.hours * 3600), region="us-east",
                         unencrypted_ports=[XET_PORT, RSYNC_PORT])
    tun = b.tunnels(timeout=60)
    st = {"a": a.object_id, "b": b.object_id, "xet": list(tun[XET_PORT].tcp_socket),
          "rsync": list(tun[RSYNC_PORT].tcp_socket), "shards": args.shards, "extra": args.extra}
    json.dump(st, open(STATE, "w"), indent=1)
    print(f"A {a.object_id}, B {b.object_id}", flush=True)
    b.filesystem.write_text("[bench]\n  path = /recv/rsync\n  read only = false\n  use chroot = false\n"
                            "  uid = root\n  gid = root\n", "/tmp/rsyncd.conf")
    sh(b, "mkdir -p /recv/rsync")
    b.exec("rsync", "--daemon", "--no-detach", f"--port={RSYNC_PORT}", "--config=/tmp/rsyncd.conf")
    names = [f"{SUBDIR}/000_{i:05d}.parquet" for i in range(args.shards + args.extra)]
    t = time.time()
    sh(a, "python3 -c \"from huggingface_hub import hf_hub_download as d; from concurrent.futures import "
          f"ThreadPoolExecutor as P; n={names!r}; "
          f"list(P(8).map(lambda f: d('{REPO}', f, repo_type='dataset', local_dir='/data/hf'), n))\"")
    print(f"downloaded {len(names)} shards in {time.time() - t:.0f} s; now `devpair.py deploy BIN`", flush=True)


def deploy(args):
    st, a, b = load()
    for sb in (a, b):
        sh(sb, "mkdir -p /opt/x")
        sb.filesystem.copy_from_local(args.bin, X)
        sh(sb, f"chmod 755 {X}")
    if not st.get("keys"):
        sh(a, f"rm -rf {K} && {X} keygen --out {K} --name client")
        sh(b, f"rm -rf {K} && {X} keygen --out {K} --name server")
        a.filesystem.write_text(sh(b, f"cat {K}/server.crt") + "\n", f"{K}/server.crt")
        b.filesystem.write_text(sh(a, f"cat {K}/client.crt") + "\n", f"{K}/client.crt")
        st["keys"] = True
        json.dump(st, open(STATE, "w"), indent=1)
    print("deployed", flush=True)


def restart_agent(b):
    sh(b, "for p in /proc/[0-9]*; do [ \"$(readlink $p/exe 2>/dev/null)\" = /opt/x/xet-p2p ] && kill ${p#/proc/}; done; "
          "sleep 1; rm -rf /recv/cas /recv/xet /tmp/ready.json; mkdir -p /recv/xet; "
          f"RUST_LOG=warn,transfer=debug nohup {X} serve --no-quic --listen-tcp 0.0.0.0:{XET_PORT} --root /recv/xet "
          f"--cas-dir /recv/cas --cert {K}/server.crt --key {K}/server.key --allow-client {K}/client.crt "
          "--ready-file /tmp/ready.json > /tmp/serve.log 2>&1 < /dev/null & "
          "for i in $(seq 1 100); do [ -s /tmp/ready.json ] && exit 0; sleep 0.1; done; cat /tmp/serve.log; exit 1")


def run(args):
    st, a, b = load()
    (xh, xp), (rh, rp) = st["xet"], st["rsync"]
    names = sorted(sh(a, f"ls /data/hf/{SUBDIR}").split())
    base, extra = names[:st["shards"]], names[st["shards"]:st["shards"] + st["extra"]]
    link = lambda files: " && ".join(f"ln /data/hf/{SUBDIR}/{n} /work/src/{n}" for n in files)
    results = []
    for tool in args.tools.split(","):
        if tool == "xet":
            restart_agent(b)
        sh(b, "rm -rf /recv/rsync/*")
        sh(a, f"rm -rf /work && mkdir -p /work/src && {link(base)} && sleep 3")
        for step in STEPS:
            if step == "grown":
                sh(a, link(extra))
            if step == "second-dest":
                sh(b, "rm -rf /recv/rsync/d1 /recv/xet/d1")
            dest = "d2" if step == "second-dest" else "d1"
            if tool == "rsync":
                cmd = f"rsync -a --stats /work/src/ rsync://{rh}:{rp}/bench/{dest}/"
            else:
                mark = int(sh(b, "wc -l < /tmp/serve.log") or 0)
                cmd = (f"XET_P2P_TRACE=/tmp/trace/p {X} push /work/src tcp://{xh}:{xp}/{dest} --cert {K}/client.crt "
                       f"--key {K}/client.key --peer-cert {K}/server.crt --cas-dir /work/cas --json")
            out = sh(a, f"rm -rf /tmp/trace && mkdir -p /tmp/trace; s=$(date +%s.%N); {cmd} > /tmp/out.txt; e=$(date +%s.%N); "
                        "python3 -c \"import sys; print(float(sys.argv[2]) - float(sys.argv[1]))\" $s $e")
            wall = float(out.splitlines()[-1])
            stdout = sh(a, "cat /tmp/out.txt")
            if tool == "rsync":
                import re
                sent = int(re.search(r"Total (?:bytes )?sent:\s*([\d,]+)", stdout).group(1).replace(",", ""))
            else:
                sent = json.loads(stdout)["udp_bytes_sent"]
            if step in ("cold", "grown", "second-dest"):
                remote = f"/recv/{'rsync' if tool == 'rsync' else 'xet'}/{dest}"
                assert sh(a, DIGEST.format("/work/src")) == sh(b, DIGEST.format(remote)), f"{tool} {step}: differs"
            results.append((tool, step, wall, sent))
            print(f"{tool:6} {step:17} {wall:6.1f} s  sent {sent / 2**30:7.2f} GiB", flush=True)
            if args.profile and tool == "xet":
                for line in sh(a, "cat /tmp/trace/*.txt 2>/dev/null || true").splitlines()[2:12]:
                    print(f"         sender   {line[:110]}", flush=True)
                log = sh(b, f"tail -n +{mark + 1} /tmp/serve.log | sed 's/\\x1b\\[[0-9;]*m//g' | "
                            "grep 'transfer::fetch' | grep -E 'missing content|chunk runs received|reconstructed' || true")
                for line in log.splitlines():
                    print(f"         receiver {line.split('transfer::fetch: ')[-1]}", flush=True)
            time.sleep(3)
    if len({t for t, *_ in results}) > 1:
        print("\nstep               " + "".join(f"{t:>10}" for t in args.tools.split(",")))
        for step in STEPS:
            row = {t: w for t, s, w, _ in results if s == step}
            print(f"{step:17} " + "".join(f"{row.get(t, 0):9.1f}s" for t in args.tools.split(",")))


def shell(args):
    _, a, b = load()
    print(sh(a if args.which == "a" else b, args.command, check=False))


def down(args):
    if not os.path.exists(STATE):
        return print("no pair")
    st = json.load(open(STATE))
    m = modal()
    for k in ("a", "b"):
        try:
            m.Sandbox.from_id(st[k]).terminate()
        except Exception as e:  # already gone
            print(f"{st[k]}: {e}")
    os.remove(STATE)
    print("terminated", st["a"], st["b"])


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("up")
    p.add_argument("--shards", type=int, default=2)
    p.add_argument("--extra", type=int, default=1)
    p.add_argument("--hours", type=float, default=2)
    p = sub.add_parser("deploy")
    p.add_argument("bin")
    p = sub.add_parser("run")
    p.add_argument("--tools", default="xet,rsync")
    p.add_argument("--profile", action="store_true")
    p = sub.add_parser("sh")
    p.add_argument("which", choices=["a", "b"])
    p.add_argument("command")
    sub.add_parser("down")
    args = ap.parse_args()
    {"up": up, "deploy": deploy, "run": run, "sh": shell, "down": down}[args.cmd](args)
