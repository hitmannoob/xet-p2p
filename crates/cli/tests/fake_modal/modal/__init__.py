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


class _Stdin:
    def __init__(self, proc):
        self._proc = proc

    def write(self, data):
        # Like Modal: writing to a finished exec is a ConflictError.
        if self._proc._popen.poll() is not None:
            raise RuntimeError("ConflictError: Exec has already completed; stdin is no longer accepting writes")
        self._proc._popen.stdin.write(data if isinstance(data, str) else data.decode())

    def write_eof(self):
        self._proc._popen.stdin.close()

    def drain(self):
        pass


class _LazyStream:
    def __init__(self, proc, which):
        self._proc, self._which = proc, which

    def read(self):
        self._proc._finish()
        return self._proc._out if self._which == "out" else self._proc._err


class _Process:
    """Starts immediately, like a Modal exec."""

    def __init__(self, cmd, timeout):
        self._timeout, self._done = timeout, False
        self._popen = subprocess.Popen(list(cmd), stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE, text=True)
        self.stdin = _Stdin(self)
        self.stdout, self.stderr = _LazyStream(self, "out"), _LazyStream(self, "err")
        self.returncode = None

    def _finish(self):
        if self._done:
            return
        if not self._popen.stdin.closed:
            self._popen.stdin.close()
        self._out, self._err = self._popen.communicate(timeout=self._timeout)
        self.returncode, self._done = self._popen.returncode, True

    def wait(self):
        self._finish()
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
