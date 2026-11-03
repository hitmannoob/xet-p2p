#!/usr/bin/env python3
"""File-format datasets for xet-p2p vs rsync, generated or downloaded inside a sandbox.

    formats_sim.py init           download / generate every base dataset under /data/formats
    formats_sim.py reset FMT DIR  DIR := a fresh copy of format FMT's base version
    formats_sim.py edit FMT DIR   apply FMT's realistic edit to DIR

Formats: weights (real Qwen2.5-0.5B safetensors; edit: the real -Instruct fine-tune), sqlite
(update 1% of rows, insert 1%), targz and tar (a Django release; edit one file, rebuild the
archive), csv (append 2% more rows), floats (500 MB raw array; overwrite a 2% slice in place).
Media, all from Blender's open movie Sintel (1080p H.264, 14:48): mp4-retag (retitle and move
the index to the front for streaming), mp4-trim (cut the first minute, stream copy), wav (PCM
soundtrack; trim a 5 s intro), flac (retag), mp3 (embed cover art), clips (178 5-second FLAC
clips; reorganize into train/ and val/, drop 10%).
Deterministic. Needs huggingface_hub (weights), the Django tree at /data/repo (archives) and
ffmpeg (media). init builds only what is missing.
"""
import os
import random
import shutil
import sqlite3
import subprocess
import sys

ROOT = "/data/formats"
MB = 1 << 20
FORMATS = ["weights", "sqlite", "targz", "tar", "csv", "floats",
           "mp4-retag", "mp4-trim", "wav", "flac", "mp3", "clips"]
SINTEL = "https://download.blender.org/durian/movies/Sintel.2010.1080p.mkv"
# format -> (base under ROOT, name in the synced tree)
BASES = {"weights": ("weights.base", "model.safetensors"), "sqlite": ("sqlite.base", "events.db"),
         "targz": ("targz.base", "django.tar.gz"), "tar": ("tar.base", "django.tar"),
         "csv": ("csv.base", "events.csv"), "floats": ("floats.base", "tensor.f32"),
         "mp4-retag": ("sintel.mp4", "sintel.mp4"), "mp4-trim": ("sintel.mp4", "sintel.mp4"),
         "wav": ("sintel.wav", "soundtrack.wav"), "flac": ("sintel.flac", "soundtrack.flac"),
         "mp3": ("sintel.mp3", "soundtrack.mp3"), "clips": ("clips", "clips")}


def hf(repo, name, dest):
    from huggingface_hub import hf_hub_download
    p = hf_hub_download(repo, name, local_dir=f"/data/hf-models/{repo}")
    shutil.copyfile(p, dest)


def build_sqlite(path, seed, rows):
    r = random.Random(seed)
    db = sqlite3.connect(path)
    db.execute("pragma page_size=4096")
    db.execute("create table events (id integer primary key, user text, kind text, amount real, note text)")
    db.executemany("insert into events values (?, ?, ?, ?, ?)",
                   ((i, f"user{r.randint(0, 99999)}", r.choice(["view", "click", "buy", "refund"]),
                     r.random() * 1000, "".join(r.choices("abcdefghij ", k=r.randint(40, 200))))
                    for i in range(rows)))
    db.commit()
    db.close()


def csv_rows(r, start, n):
    return "".join(f"{i},{r.randint(0, 10**9)},{r.random():.6f},{r.choice(['us', 'eu', 'ap'])},"
                   f"{''.join(r.choices('abcdefghij', k=24))}\n" for i in range(start, start + n))


def tar(src_dir, out, gz):
    subprocess.run(["tar", "-c" + ("z" if gz else "") + "f", out, "-C", os.path.dirname(src_dir),
                    os.path.basename(src_dir)], check=True,
                   env={**os.environ, "GZIP": "-n"})


def ffmpeg(*args):
    subprocess.run(["ffmpeg", "-nostdin", "-loglevel", "error", "-y", *args], check=True)


def replace_with(path, ext, *args):
    """Run ffmpeg from path into a temporary file, then swap it in."""
    tmp = f"{path}.tmp{ext}"
    ffmpeg("-i", path, *args, tmp)
    os.replace(tmp, path)


def missing(name):
    return not os.path.exists(f"{ROOT}/{name}")


def init():
    os.makedirs(ROOT, exist_ok=True)
    if missing("weights.edit"):
        hf("Qwen/Qwen2.5-0.5B", "model.safetensors", f"{ROOT}/weights.base")
        hf("Qwen/Qwen2.5-0.5B-Instruct", "model.safetensors", f"{ROOT}/weights.edit")
    if missing("sqlite.base"):
        build_sqlite(f"{ROOT}/sqlite.base", 1, 1_200_000)
    if missing("csv.base"):
        with open(f"{ROOT}/csv.base", "w") as f:
            r = random.Random(2)
            for start in range(0, 4_000_000, 200_000):
                f.write(csv_rows(r, start, 200_000))
    if missing("floats.base"):
        r = random.Random(3)
        with open(f"{ROOT}/floats.base", "wb") as f:
            for _ in range(500):
                f.write(r.randbytes(MB))  # trained float32 weights are close to incompressible
    if missing("tar.base"):
        shutil.rmtree("/tmp/django", ignore_errors=True)
        shutil.copytree("/data/repo", "/tmp/django/django")
        tar("/tmp/django/django", f"{ROOT}/targz.base", True)
        tar("/tmp/django/django", f"{ROOT}/tar.base", False)
    if missing("clips"):
        init_media()


def init_media():
    mkv = "/data/sintel.mkv"
    if not os.path.exists(mkv):
        import urllib.request
        urllib.request.urlretrieve(SINTEL, mkv + ".part")
        os.replace(mkv + ".part", mkv)
    # As a camera or encoder writes it: index (moov) at the end.
    ffmpeg("-i", mkv, "-map", "0:v:0", "-map", "0:a:0", "-c", "copy", f"{ROOT}/sintel.mp4")
    ffmpeg("-i", mkv, "-map", "0:a:0", "-ac", "2", "-ar", "48000", "-c:a", "pcm_s16le",
           f"{ROOT}/sintel.wav")
    ffmpeg("-i", f"{ROOT}/sintel.wav", "-c:a", "flac", "-metadata", "title=Sintel soundtrack",
           f"{ROOT}/sintel.flac")
    ffmpeg("-i", f"{ROOT}/sintel.wav", "-c:a", "libmp3lame", "-b:a", "192k",
           "-metadata", "title=Sintel soundtrack", f"{ROOT}/sintel.mp3")
    ffmpeg("-ss", "300", "-i", mkv, "-frames:v", "1", "-q:v", "2", f"{ROOT}/cover.jpg")
    os.makedirs(f"{ROOT}/clips.tmp", exist_ok=True)
    ffmpeg("-i", f"{ROOT}/sintel.wav", "-c:a", "flac", "-f", "segment", "-segment_time", "5",
           f"{ROOT}/clips.tmp/clip_%04d.flac")
    os.replace(f"{ROOT}/clips.tmp", f"{ROOT}/clips")


def reset(fmt, d):
    shutil.rmtree(d, ignore_errors=True)
    os.makedirs(d)
    base, name = BASES[fmt]
    if os.path.isdir(f"{ROOT}/{base}"):
        shutil.copytree(f"{ROOT}/{base}", f"{d}/{name}")
    else:
        shutil.copyfile(f"{ROOT}/{base}", f"{d}/{name}")
    return f"{d}/{name}"


def bump(path):
    st = os.stat(path)
    os.utime(path, (st.st_atime + 10, st.st_mtime + 10))


def edit(fmt, d):
    path = os.path.join(d, os.listdir(d)[0])
    r = random.Random(100)
    if fmt == "weights":
        shutil.copyfile(f"{ROOT}/weights.edit", path)
    elif fmt == "sqlite":
        db = sqlite3.connect(path)
        n = db.execute("select count(*) from events").fetchone()[0]
        ids = r.sample(range(n), n // 100)
        db.executemany("update events set amount = ?, note = ? where id = ?",
                       ((r.random() * 1000, "updated", i) for i in ids))
        db.executemany("insert into events values (?, ?, ?, ?, ?)",
                       ((n + i, f"user{r.randint(0, 99999)}", "buy", r.random() * 1000, "new")
                        for i in range(n // 100)))
        db.commit()
        db.close()
    elif fmt in ("targz", "tar"):
        target = "/tmp/django/django/django/db/models/query.py"
        with open(target, "a") as f:
            f.write("\n# edited by the formats benchmark\n")
        tar("/tmp/django/django", path, fmt == "targz")
        shutil.copytree("/data/repo", "/tmp/django/django", dirs_exist_ok=True)  # undo the edit
    elif fmt == "csv":
        with open(path, "a") as f:
            f.write(csv_rows(r, 4_000_000, 80_000))
    elif fmt == "floats":
        size = os.path.getsize(path)
        span = size // 50 // 4 * 4
        at = (size // 3) // 4 * 4
        with open(path, "r+b") as f:
            f.seek(at)
            f.write(r.randbytes(span))
    elif fmt == "mp4-retag":
        replace_with(path, ".mp4", "-map", "0", "-c", "copy", "-movflags", "+faststart",
                     "-metadata", "title=Sintel (2010), director's cut")
    elif fmt == "mp4-trim":
        replace_with(path, ".mp4", "-ss", "60", "-map", "0", "-c", "copy")
    elif fmt == "wav":
        replace_with(path, ".wav", "-ss", "5", "-c", "copy")
    elif fmt == "flac":
        replace_with(path, ".flac", "-c", "copy", "-metadata", "artist=Jan Morgenstern",
                     "-metadata", "album=Sintel (Original Soundtrack)")
    elif fmt == "mp3":
        tmp = f"{path}.tmp.mp3"
        ffmpeg("-i", path, "-i", f"{ROOT}/cover.jpg", "-map", "0", "-map", "1", "-c", "copy",
               "-id3v2_version", "3", "-metadata:s:v", "comment=Cover (front)", tmp)
        os.replace(tmp, path)
    elif fmt == "clips":
        clips = sorted(os.listdir(path))
        for c in r.sample(clips, len(clips) // 10):
            os.remove(f"{path}/{c}")
            clips.remove(c)
        for split in ("train", "val"):
            os.makedirs(f"{path}/{split}")
        for c in clips:
            os.rename(f"{path}/{c}", f"{path}/{'val' if r.random() < 0.2 else 'train'}/{c}")
        return
    bump(path)


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "init":
        init()
    elif cmd == "reset":
        reset(sys.argv[2], sys.argv[3])
    elif cmd == "edit":
        edit(sys.argv[2], sys.argv[3])
