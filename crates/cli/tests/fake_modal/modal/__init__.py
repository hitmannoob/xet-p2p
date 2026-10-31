# A stand-in for the Modal SDK, for offline tests of the xet-p2p Modal adapter.
#
# Implements exactly the calls the adapter's helper makes (Sandbox.from_id, exec, open,
# tunnels) against this machine: "sandboxes" are listed in $FAKE_MODAL_SANDBOXES as
# {"id": [ports with a raw TCP tunnel]}, commands run locally, and tunnels point at 127.0.0.1.
import json
import os
import subprocess


class _Stream:
    def __init__(self, data):
        self._data = data

    def read(self):
        return self._data


class _Process:
    def __init__(self, cmd, timeout):
        r = subprocess.run(list(cmd), capture_output=True, text=True, timeout=timeout)
        self.stdout, self.stderr, self.returncode = _Stream(r.stdout), _Stream(r.stderr), r.returncode

    def wait(self):
        return self.returncode


class _Tunnel:
    def __init__(self, port):
        self.host, self.port = "fake-tls.modal.invalid", 443
        self.unencrypted_host, self.unencrypted_port = "127.0.0.1", port

    @property
    def tcp_socket(self):
        return (self.unencrypted_host, self.unencrypted_port)


class _Filesystem:
    def write_bytes(self, data, remote_path):
        with open(remote_path, "wb") as f:
            f.write(data)

    def copy_from_local(self, local_path, remote_path):
        import shutil
        shutil.copyfile(local_path, remote_path)


class Sandbox:
    filesystem = _Filesystem()

    @staticmethod
    def from_id(sandbox_id):
        ports = json.loads(os.environ.get("FAKE_MODAL_SANDBOXES", "{}")).get(sandbox_id)
        if ports is None:
            raise LookupError(f"Sandbox {sandbox_id} not found")
        sb = Sandbox()
        sb._ports = ports
        return sb

    def exec(self, *cmd, timeout=None):
        return _Process(cmd, timeout)

    def open(self, path, mode):
        # Mirrors Modal: the legacy file API is gone.
        raise RuntimeError("UnimplementedError: The legacy Sandbox filesystem API is no longer supported.")

    def tunnels(self, timeout=None):
        return {p: _Tunnel(p) for p in self._ports}
