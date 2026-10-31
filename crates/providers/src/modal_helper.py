# xet-p2p Modal adapter helper. Run by the Rust adapter with the Python that has the Modal SDK.
#
# Usage: python -c <this script> <command>   with a JSON request on stdin; writes one JSON object
# to stdout. Private keys arrive on stdin only (never argv), and are written with mode 0600.
#
# The helper only attaches to an existing sandbox (Sandbox.from_id). It never creates one.
import hashlib
import json
import shlex
import socket
import sys
import time

# Prefer IPv4: on networks with DNS64 but broken NAT64, the synthesized IPv6 addresses come first
# and never connect, and the SDK's gRPC client does not fall back. IPv6 stays as a fallback.
_getaddrinfo = socket.getaddrinfo
socket.getaddrinfo = lambda *a, **k: sorted(_getaddrinfo(*a, **k), key=lambda i: i[0] != socket.AF_INET)

import modal  # noqa: E402


def run(sb, *cmd, timeout=60):
    p = sb.exec(*cmd, timeout=timeout)
    out = p.stdout.read()
    err = p.stderr.read()
    p.wait()
    if p.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd[:2])}... failed ({p.returncode}): {err.strip()[:400]}")
    return out


def tunnel_for(sb, port):
    t = sb.tunnels(timeout=30).get(port)
    if t is None or not getattr(t, "unencrypted_host", None):
        return None
    host, p = t.tcp_socket
    return [host, p]


def cmd_info(req):
    sb = modal.Sandbox.from_id(req["id"])
    uname = run(sb, "uname", "-sm").split()
    return {
        "os": uname[0].lower() if uname else "unknown",
        "arch": uname[1] if len(uname) > 1 else "unknown",
        "tunnel": tunnel_for(sb, req["port"]),
    }


def write_file(sb, path, data, mode):
    # Modal's filesystem API (sb.open() was retired); then restrict permissions.
    sb.filesystem.write_bytes(data, path)
    run(sb, "chmod", mode, path)


def cmd_start(req):
    sb = modal.Sandbox.from_id(req["id"])
    state = {"pid": None}
    try:
        return _start(sb, req, state)
    except Exception:
        # A failed start must not leave an agent or session credentials behind.
        try:
            if state["pid"]:
                run(sb, "sh", "-c", f"kill {int(state['pid'])} 2>/dev/null || true")
            run(sb, "rm", "-rf", req["session_dir"])
        except Exception:
            pass
        raise


def _start(sb, req, started):
    tunnel = tunnel_for(sb, req["port"])
    if tunnel is None:
        raise RuntimeError(
            f"sandbox has no unencrypted (raw TCP) tunnel for port {req['port']}; create it with "
            f"unencrypted_ports=[{req['port']}] (Modal cannot add tunnels to a running sandbox)"
        )
    state, session = req["state_dir"], req["session_dir"]

    # Agent binary: upload only if this exact build is not already there.
    h = hashlib.sha256()
    with open(req["agent"], "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    digest = h.hexdigest()
    remote_bin = f"{state}/bin/xet-p2p-{digest[:16]}"
    run(sb, "mkdir", "-p", f"{state}/bin", session, req["root"])
    have = run(sb, "sh", "-c", f"sha256sum {shlex.quote(remote_bin)} 2>/dev/null || true").split()
    if not have or have[0] != digest:
        sb.filesystem.copy_from_local(req["agent"], remote_bin)
        run(sb, "chmod", "755", remote_bin)

    # Create the key file empty with 0600 first, so the key is never readable by others.
    run(sb, "sh", "-c", f"umask 077 && : > {shlex.quote(session)}/server.key")
    write_file(sb, f"{session}/server.key", req["server_key_pem"].encode(), "600")
    write_file(sb, f"{session}/server.crt", req["server_cert_pem"].encode(), "644")
    write_file(sb, f"{session}/client.crt", req["client_cert_pem"].encode(), "644")

    args = [
        remote_bin, "serve", "--no-quic", "--listen-tcp", f"0.0.0.0:{req['port']}",
        "--root", req["root"], "--cas-dir", req["cas_dir"],
        "--cert", f"{session}/server.crt", "--key", f"{session}/server.key",
        "--allow-client", f"{session}/client.crt", "--ready-file", f"{session}/ready.json",
    ]
    line = " ".join(shlex.quote(a) for a in args)
    pid = run(sb, "sh", "-c", f"nohup {line} > {shlex.quote(session)}/agent.log 2>&1 & echo $!").strip()
    started["pid"] = pid

    deadline = time.time() + 30
    while time.time() < deadline:
        ready = run(sb, "sh", "-c", f"cat {shlex.quote(session)}/ready.json 2>/dev/null || true")
        if ready.strip():
            return {"pid": int(pid), "tcp": tunnel}
        alive = run(sb, "sh", "-c", f"kill -0 {pid} 2>/dev/null && echo up || echo down").strip()
        if alive != "up":
            log = run(sb, "sh", "-c", f"tail -c 2000 {shlex.quote(session)}/agent.log || true")
            raise RuntimeError(f"agent exited: {log.strip()}")
        time.sleep(0.5)
    raise RuntimeError("agent did not become ready in 30 s")


def cmd_stop(req):
    sb = modal.Sandbox.from_id(req["id"])
    if req.get("pid"):
        run(sb, "sh", "-c", f"kill {int(req['pid'])} 2>/dev/null || true")
    run(sb, "rm", "-rf", req["session_dir"])
    return {"stopped": True}


COMMANDS = {"info": cmd_info, "start": cmd_start, "stop": cmd_stop}

if __name__ == "__main__":
    try:
        req = json.load(sys.stdin)
        print(json.dumps(COMMANDS[sys.argv[1]](req)))
    except Exception as e:  # reported to the adapter as a provider error
        print(json.dumps({"error": f"{type(e).__name__}: {e}"}))
        sys.exit(1)
