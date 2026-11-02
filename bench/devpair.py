#!/usr/bin/env python3
"""A persistent pair of Modal sandboxes for fast measure-change-measure cycles.

    devpair.py up [--shards 2 --extra 1]   create A (sender) and B (receiver), keys, rsync daemon,
                                           and download FineWeb-Edu shards into A once
    devpair.py deploy BIN                  upload a Linux xet-p2p build to both; restart B's agent
    devpair.py run [--tools xet,rsync]     cold -> unchanged x2 -> grown -> second destination,
                   [--profile]             per tool, verified; --profile prints xet-p2p's phases
    devpair.py agents [--n 8 --rounds 3]   coding-agent fleet -> collector: first sync per agent,
                                           then incremental rounds (see agents_sim.py)
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


def restart_agent(b, trace=False, link=False):
    sh(b, "for p in /proc/[0-9]*; do case \"$(tr '\\0' ' ' < $p/cmdline 2>/dev/null)\" in '/opt/x/xet-p2p serve'*) kill ${p#/proc/};; esac; done; "
          "sleep 1; rm -rf /recv/cas /recv/xet /tmp/ready.json /tmp/trace; mkdir -p /recv/xet /tmp/trace; "
          f"RUST_LOG=warn,transfer=debug {'XET_P2P_TRACE=/tmp/trace/serve ' if trace else ''}"
          f"nohup {X} serve --no-quic --listen-tcp 0.0.0.0:{XET_PORT} --root /recv/xet "
          f"--cas-dir /recv/cas --cert {K}/server.crt --key {K}/server.key --allow-client {K}/client.crt "
          f"{'--link-identical ' if link else ''}"
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


REPO_TGZ = "https://codeload.github.com/django/django/tar.gz/refs/tags/5.1"


def agents(args):
    import re
    st, a, b = load()
    (xh, xp), (rh, rp) = st["xet"], st["rsync"]
    a.filesystem.copy_from_local(os.path.join(os.path.dirname(os.path.abspath(__file__)), "agents_sim.py"),
                                 "/opt/x/agents_sim.py")
    sh(a, "[ -d /data/repo ] || python3 -c \"import io, tarfile, urllib.request; "
          f"t = tarfile.open(fileobj=io.BytesIO(urllib.request.urlopen('{REPO_TGZ}').read())); "
          "m = [x for x in t.getmembers() if '/' in x.name and (x.isfile() or x.isdir())]; "
          "[setattr(x, 'name', x.name.split('/', 1)[1]) for x in m]; t.extractall('/data/repo', members=m)\"")
    print("base repo:", sh(a, "find /data/repo -type f | wc -l"), "files,",
          sh(a, "du -sh /data/repo | cut -f1"), flush=True)
    tools = args.tools.split(",")
    rows = []

    def sync(tool, i):
        src, dest = f"/agents/a{i}", f"a{i}"
        if tool == "xet":
            trace = f"XET_P2P_TRACE=/tmp/trace/push-a{i}-$(date +%s%N) " if args.profile else ""
            cmd = (f"{trace}{X} push {src} tcp://{xh}:{xp}/{dest} --cert {K}/client.crt --key {K}/client.key "
                   f"--peer-cert {K}/server.crt --cas-dir /work/cas{i} --json")
        else:
            # In daemon mode --link-dest is relative to the module root (a relative ../ path is
            # silently ignored).
            link = " --link-dest=/a1" if tool == "rsync-link" and i > 1 else ""
            cmd = f"rsync -az{link} --stats {src}/ rsync://{rh}:{rp}/bench/{dest}/"
        out = sh(a, f"s=$(date +%s.%N); {cmd} > /tmp/out.txt; e=$(date +%s.%N); "
                    "python3 -c \"import sys; print(float(sys.argv[2]) - float(sys.argv[1]))\" $s $e")
        wall, stdout = float(out.splitlines()[-1]), sh(a, "cat /tmp/out.txt")
        if tool == "xet":
            sent = json.loads(stdout)["udp_bytes_sent"]
        else:
            sent = int(re.search(r"Total (?:bytes )?sent:\s*([\d,]+)", stdout).group(1).replace(",", ""))
        return wall, sent

    for tool in tools:
        if tool == "xet":
            restart_agent(b, trace=args.profile)
        sh(b, "rm -rf /recv/rsync/*")
        sh(a, f"rm -rf /work /tmp/trace && mkdir -p /work /tmp/trace && "
              f"python3 /opt/x/agents_sim.py init {args.n} && sleep 3")
        for i in range(1, args.n + 1):
            w, n = sync(tool, i)
            rows.append((tool, "first, agent 1" if i == 1 else "first, agents 2..N", w, n))
        for rnd in range(1, args.rounds + 1):
            sh(a, f"python3 /opt/x/agents_sim.py step {rnd} && sleep 3")
            for i in range(1, args.n + 1):
                w, n = sync(tool, i)
                rows.append((tool, "incremental rounds", w, n))
        for i in range(1, args.n + 1):
            remote = f"/recv/{'xet' if tool == 'xet' else 'rsync'}/a{i}"
            assert sh(a, DIGEST.format(f"/agents/a{i}")) == sh(b, DIGEST.format(remote)), f"{tool} a{i}: differs"
        print(f"{tool}: done, {args.n} agent trees verified", flush=True)
        if tool == "xet" and args.profile:
            first = sh(a, f"ls -t /tmp/trace/push-a{args.n}-*.txt | tail -1")
            last = sh(a, f"ls -t /tmp/trace/push-a{args.n}-*.txt | head -1")
            print(f"--- sender profile, first sync of agent {args.n}:\n" + sh(a, f"cat {first}"), flush=True)
            if last != first:
                print(f"--- sender profile, last sync of agent {args.n}:\n" + sh(a, f"cat {last}"), flush=True)
            sh(b, "for p in /proc/[0-9]*; do case \"$(tr '\\0' ' ' < $p/cmdline 2>/dev/null)\" in "
                  "'/opt/x/xet-p2p serve'*) kill -TERM ${p#/proc/};; esac; done; sleep 3")
            print("--- receiver profile (all syncs):\n" + sh(b, "cat /tmp/trace/serve.*.txt"), flush=True)

    phases = ["first, agent 1", "first, agents 2..N", "incremental rounds"]
    print(f"\n{'phase':22}" + "".join(f"{t:>24}" for t in tools))
    for ph in phases:
        cells = []
        for t in tools:
            m = [(w, n) for tt, p, w, n in rows if tt == t and p == ph]
            cells.append(f"{sum(w for w, _ in m):7.1f} s {sum(n for _, n in m) / 2**20:8.1f} MiB")
        print(f"{ph:22}" + "".join(f"{c:>24}" for c in cells))


def rollouts(args):
    """One agent, many rollouts from the same base; the collector keeps each as rollout-k."""
    import re
    import statistics
    st, a, b = load()
    (xh, xp), (rh, rp) = st["xet"], st["rsync"]
    a.filesystem.copy_from_local(os.path.join(os.path.dirname(os.path.abspath(__file__)), "agents_sim.py"),
                                 "/opt/x/agents_sim.py")
    tools = args.tools.split(",")
    rows = []
    for tool in tools:
        if tool.startswith("xet"):
            restart_agent(b, trace=args.profile, link=tool == "xet-link")
        sh(b, "rm -rf /recv/rsync/*")
        sh(a, "rm -rf /work /tmp/trace && mkdir -p /work /tmp/trace && python3 /opt/x/agents_sim.py rollout 0 && sleep 3")
        for k in range(1, args.n + 1):
            sh(a, f"python3 /opt/x/agents_sim.py rollout {k}")
            dest = f"rollout-{k}"
            if tool.startswith("xet"):
                trace = "XET_P2P_TRACE=/tmp/trace/push " if args.profile and k == args.n else ""
                cmd = (f"{trace}{X} push /agent/run tcp://{xh}:{xp}/{dest} --cert {K}/client.crt --key {K}/client.key "
                       f"--peer-cert {K}/server.crt --cas-dir /work/cas --json")
            else:
                link = f" --link-dest=/rollout-{k - 1}" if tool == "rsync-link" and k > 1 else ""
                cmd = f"rsync -az{link} --stats /agent/run/ rsync://{rh}:{rp}/bench/{dest}/"
            out = sh(a, f"s=$(date +%s.%N); {cmd} > /tmp/out.txt; e=$(date +%s.%N); "
                        "python3 -c \"import sys; print(float(sys.argv[2]) - float(sys.argv[1]))\" $s $e")
            wall, stdout = float(out.splitlines()[-1]), sh(a, "cat /tmp/out.txt")
            if tool.startswith("xet"):
                sent = json.loads(stdout)["udp_bytes_sent"]
            else:
                sent = int(re.search(r"Total (?:bytes )?sent:\s*([\d,]+)", stdout).group(1).replace(",", ""))
            remote = f"/recv/{'xet' if tool.startswith('xet') else 'rsync'}/{dest}"
            assert sh(a, DIGEST.format("/agent/run")) == sh(b, DIGEST.format(remote)), f"{tool} {dest}: differs"
            rows.append((tool, k, wall, sent))
            print(f"{tool:10} rollout {k:2}: {wall:5.1f} s  sent {sent / 1024:9.0f} KiB", flush=True)
            time.sleep(1)
        if tool.startswith("xet") and args.profile:
            print(f"--- {tool} sender, rollout {args.n}:\n" + sh(a, "cat /tmp/trace/push.*.txt"), flush=True)
            sh(b, "for p in /proc/[0-9]*; do case \"$(tr '\\0' ' ' < $p/cmdline 2>/dev/null)\" in "
                  "'/opt/x/xet-p2p serve'*) kill -TERM ${p#/proc/};; esac; done; sleep 3")
            print(f"--- {tool} receiver, all rollouts:\n" + sh(b, "cat /tmp/trace/serve.*.txt"), flush=True)
    print(f"\n{'':24}" + "".join(f"{t:>22}" for t in tools))
    for label, pick in (("rollout 1 (cold)", lambda k: k == 1), ("rollouts 2..N, median", lambda k: k > 1)):
        cells = []
        for t in tools:
            m = [(w, n) for tt, k, w, n in rows if tt == t and pick(k)]
            cells.append(f"{statistics.median(w for w, _ in m):5.1f} s {statistics.median(n for _, n in m) / 1024:8.0f} KiB")
        print(f"{label:24}" + "".join(f"{c:>22}" for c in cells))


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
    p = sub.add_parser("rollouts")
    p.add_argument("--n", type=int, default=10)
    p.add_argument("--tools", default="xet,rsync-z,rsync-link")
    p.add_argument("--profile", action="store_true")
    p = sub.add_parser("sh")
    p.add_argument("which", choices=["a", "b"])
    p.add_argument("command")
    p = sub.add_parser("agents")
    p.add_argument("--n", type=int, default=8)
    p.add_argument("--rounds", type=int, default=3)
    p.add_argument("--tools", default="xet,rsync-z,rsync-link")
    p.add_argument("--profile", action="store_true")
    sub.add_parser("down")
    args = ap.parse_args()
    {"up": up, "deploy": deploy, "run": run, "agents": agents, "rollouts": rollouts, "sh": shell,
     "down": down}[args.cmd](args)
