# xet-p2p Modal adapter helper. Run by the Rust adapter with the Python that has the Modal SDK.
#
# A long-lived process for one xet-p2p command: it reads one JSON request per line on stdin and
# writes one JSON response per line on stdout, so the SDK is imported and the sandbox looked up
# once. Every remote step is batched to keep round trips to Modal's API to a minimum:
#
#   attach     Sandbox.from_id + tunnels()                      (no remote command)
#   bootstrap  one remote shell script: CPU check, agent binary check, replace a stale kept agent
#              on the port, credentials (from stdin, written under umask 077), launch, wait for
#              readiness *inside the sandbox*
#              (+ one upload the first time this agent build is used)
#   stop       one remote command: kill the agent, delete its session directory
#
# The helper only attaches to existing sandboxes. It never creates or terminates one.
import base64
import hashlib
import json
import socket
import sys

# Prefer IPv4: on networks with DNS64 but broken NAT64, the synthesized IPv6 addresses come first
# and never connect, and the SDK's gRPC client does not fall back. IPv6 stays as a fallback.
_getaddrinfo = socket.getaddrinfo
socket.getaddrinfo = lambda *a, **k: sorted(_getaddrinfo(*a, **k), key=lambda i: i[0] != socket.AF_INET)

import modal  # noqa: E402

BOOTSTRAP = r"""
set -u
# Read the credentials first: Modal rejects stdin writes once the process has exited, and the
# checks below may exit early.
IFS= read -r K; IFS= read -r C; IFS= read -r CL
BIN=$1 SHA=$2 ARCH=$3 ROOT=$4 CAS=$5 SESSION=$6 PORT=$7 IDLE=$8 STATE=$9
KEPT="$STATE/port-$PORT.kept"
sha() { if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1"; else shasum -a 256 "$1"; fi | cut -d' ' -f1; }
M=$(uname -m)
if [ "$ARCH" != any ] && [ "$M" != "$ARCH" ]; then echo "sandbox CPU is $M but the agent is built for $ARCH" >&2; exit 11; fi
if [ ! -f "$BIN" ] || [ "$(sha "$BIN" 2>/dev/null)" != "$SHA" ]; then echo '{"need_binary": true}'; exit 0; fi
chmod 755 "$BIN"
# Is $1 a live xet-p2p agent listening on this port? (Not a zombie, not a recycled pid.)
ours() {
  if [ -r "/proc/$1/cmdline" ]; then
    [ "$(cut -d' ' -f3 "/proc/$1/stat" 2>/dev/null)" != Z ] || return 1
    CMD=$(tr '\0' ' ' < "/proc/$1/cmdline")
  else
    CMD=$(ps -p "$1" -o args= 2>/dev/null) || return 1
  fi
  case "$CMD" in *" serve "*"--listen-tcp 0.0.0.0:$PORT "*) return 0;; esac
  return 1
}
# A kept agent from an earlier command still holds the port, but its client could not reach it
# or no longer has its credentials. Replace it: stop it and delete its credentials.
if [ -f "$KEPT" ]; then
  read -r OPID OSESSION < "$KEPT" || true
  if [ -n "${OPID:-}" ] && ours "$OPID"; then
    kill "$OPID" 2>/dev/null; J=0
    while ours "$OPID" && [ "$J" -lt 50 ]; do J=$((J + 1)); sleep 0.1; done
    ours "$OPID" && kill -9 "$OPID" 2>/dev/null && sleep 0.2
  fi
  [ -n "${OSESSION:-}" ] && rm -rf "$OSESSION"
  rm -f "$KEPT"
fi
umask 077
mkdir -p "$SESSION" && mkdir -p "$ROOT" || exit 14
printf %s "$K" | base64 -d > "$SESSION/server.key" && printf %s "$C" | base64 -d > "$SESSION/server.crt" \
  && printf %s "$CL" | base64 -d > "$SESSION/client.crt" || { rm -rf "$SESSION"; exit 15; }
set -- "$BIN" serve --no-quic --listen-tcp "0.0.0.0:$PORT" --root "$ROOT" --cas-dir "$CAS" \
  --cert "$SESSION/server.crt" --key "$SESSION/server.key" --allow-client "$SESSION/client.crt" \
  --ready-file "$SESSION/ready.json"
if [ "$IDLE" != 0 ]; then set -- "$@" --idle-exit-secs "$IDLE" --cleanup-on-exit "$SESSION"; fi
nohup "$@" > "$SESSION/agent.log" 2>&1 < /dev/null &
PID=$!
I=0
while [ ! -s "$SESSION/ready.json" ]; do
  if ! kill -0 "$PID" 2>/dev/null; then
    if grep -q "Address.*in use" "$SESSION/agent.log"; then
      echo "port $PORT is already in use in the sandbox (another xet-p2p command or another program is listening on it)" >&2
    else
      echo "agent exited: $(tail -c 1500 "$SESSION/agent.log")" >&2
    fi
    rm -rf "$SESSION"; exit 12
  fi
  I=$((I + 1)); if [ "$I" -gt 300 ]; then kill "$PID"; rm -rf "$SESSION"; echo "agent not ready in 30 s" >&2; exit 13; fi
  sleep 0.1
done
if [ "$IDLE" != 0 ]; then echo "$PID $SESSION" > "$KEPT"; fi
echo "{\"pid\": $PID, \"uname\": \"$(uname -sm)\"}"
"""

_sandboxes = {}


def sandbox(sid):
    if sid not in _sandboxes:
        _sandboxes[sid] = modal.Sandbox.from_id(sid)
    return _sandboxes[sid]


def run(sb, script, args, stdin=None, timeout=120):
    p = sb.exec("sh", "-c", script, "sh", *args, timeout=timeout)
    if stdin is not None:
        p.stdin.write(stdin)
        p.stdin.write_eof()
        p.stdin.drain()
    out, err = p.stdout.read(), p.stderr.read()
    p.wait()
    return p.returncode, out, err


def elf_arch(path):
    """CPU of an ELF binary (as `uname -m` names it), or "any" if it is not ELF."""
    with open(path, "rb") as f:
        head = f.read(20)
    if head[:4] != b"\x7fELF":
        return "any"
    machine = int.from_bytes(head[18:20], "little")
    return {0x3E: "x86_64", 0xB7: "aarch64"}.get(machine, f"elf-{machine:#x}")


def cmd_attach(req):
    sb = sandbox(req["id"])
    t = sb.tunnels(timeout=30).get(req["port"])
    tunnel = list(t.tcp_socket) if t is not None and getattr(t, "unencrypted_host", None) else None
    return {"tunnel": tunnel}


def cmd_bootstrap(req):
    sb = sandbox(req["id"])
    h = hashlib.sha256()
    with open(req["agent"], "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    digest = h.hexdigest()
    remote_bin = f"{req['state_dir']}/bin/xet-p2p-{digest[:16]}"
    secrets = "\n".join(base64.b64encode(req[k].encode()).decode()
                        for k in ("server_key_pem", "server_cert_pem", "client_cert_pem")) + "\n"
    args = [remote_bin, digest, elf_arch(req["agent"]), req["root"], req["cas_dir"],
            req["session_dir"], str(req["port"]), str(req.get("idle_exit_secs") or 0), req["state_dir"]]
    for attempt in range(2):
        code, out, err = run(sb, BOOTSTRAP, args, stdin=secrets, timeout=180)
        if code != 0:
            raise RuntimeError(err.strip()[:1500] or f"bootstrap failed ({code})")
        reply = json.loads(out.strip().splitlines()[-1])
        if not reply.get("need_binary"):
            return reply
        if attempt == 0:  # first use of this agent build in this sandbox
            run(sb, 'mkdir -p "$1"', [f"{req['state_dir']}/bin"])
            sb.filesystem.copy_from_local(req["agent"], remote_bin)
    raise RuntimeError("agent binary did not verify after upload")


def cmd_stop(req):
    sb = sandbox(req["id"])
    pid = int(req.get("pid") or 0)
    script = '[ "$1" -gt 0 ] && kill "$1" 2>/dev/null; rm -rf "$2"; true'
    run(sb, script, [str(pid), req["session_dir"]])
    return {"stopped": True}


COMMANDS = {"attach": cmd_attach, "bootstrap": cmd_bootstrap, "stop": cmd_stop}

if __name__ == "__main__":
    for line in sys.stdin:
        if not line.strip():
            continue
        try:
            req = json.loads(line)
            reply = COMMANDS[req["cmd"]](req)
        except Exception as e:  # reported to the adapter as a provider error
            reply = {"error": f"{type(e).__name__}: {e}"}
        print(json.dumps(reply), flush=True)
