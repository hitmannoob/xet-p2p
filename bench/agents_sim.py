#!/usr/bin/env python3
"""Simulated coding-agent fleet, for benchmarking trajectory collection. Runs inside a sandbox.

    agents_sim.py init N     /agents/a{i}: a copy of the base repo (/data/repo) plus trajectory/
                             with a first step, for i in 1..N
    agents_sim.py step R     round R for every agent: edit 3 source files, append ~50 KB of JSONL
                             (tool calls quoting real repo files, as agent logs do) and write a test log

Deterministic per (agent, round). Uses only the standard library.
"""
import json
import os
import random
import shutil
import sys

BASE, ROOT = "/data/repo", "/agents"


def sources():
    out = []
    for d, _, files in os.walk(BASE):
        out += [os.path.relpath(os.path.join(d, f), BASE) for f in files if f.endswith(".py")]
    return sorted(out)


def log_lines(r, agent, rnd, files, n_bytes):
    lines, size, step = [], 0, rnd * 1000
    while size < n_bytes:
        step += 1
        path = r.choice(files)
        try:
            body = open(os.path.join(BASE, path), errors="replace").read()
        except OSError:
            body = ""
        start = r.randrange(max(1, len(body) - 3000))
        event = {
            "agent": agent, "round": rnd, "step": step,
            "thought": f"Looking at {path} to understand how the failing test reaches this code.",
            "action": {"tool": "view_file", "path": path, "lines": [start // 40, start // 40 + 75]},
            "observation": body[start:start + 3000],
            "usage": {"prompt_tokens": r.randint(2000, 9000), "completion_tokens": r.randint(50, 600)},
        }
        line = json.dumps(event) + "\n"
        lines.append(line)
        size += len(line)
    return "".join(lines)


def init(n):
    files = sources()
    shutil.rmtree(ROOT, ignore_errors=True)
    for i in range(1, n + 1):
        d = f"{ROOT}/a{i}"
        shutil.copytree(BASE, f"{d}/workspace", symlinks=False)
        os.makedirs(f"{d}/trajectory")
        r = random.Random(i)
        with open(f"{d}/trajectory/steps.jsonl", "w") as f:
            f.write(log_lines(r, i, 0, files, 20_000))


def step(rnd):
    files = sources()
    for name in sorted(os.listdir(ROOT)):
        i = int(name[1:])
        d = f"{ROOT}/{name}"
        r = random.Random(i * 1000 + rnd)
        for path in r.sample(files, 3):
            p = f"{d}/workspace/{path}"
            with open(p, "a") as f:
                f.write(f"\n# agent {i} round {rnd}: guard against the empty case\n"
                        f"def _patched_{rnd}_{r.randint(0, 9999)}(value):\n    return value or None\n")
        with open(f"{d}/trajectory/steps.jsonl", "a") as f:
            f.write(log_lines(r, i, rnd, files, 50_000))
        with open(f"{d}/trajectory/test_round{rnd}.log", "w") as f:
            for k in range(400):
                f.write(f"tests/test_{r.choice(files).replace('/', '_')}::case_{k} "
                        f"{'PASSED' if r.random() > 0.05 else 'FAILED'} [{k / 4:.0f}%]\n")


if __name__ == "__main__":
    {"init": init, "step": step}[sys.argv[1]](int(sys.argv[2]))
