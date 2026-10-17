#!/usr/bin/env python3
"""Build and package the six enhanced development binaries with Go only."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tarfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
TARGETS = tuple(f"{system}/{arch}" for system in ("windows", "linux", "darwin") for arch in ("amd64", "arm64"))


def git(*args):
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def checksum(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=TARGETS, action="append", help="build selected targets; default: all six")
    parser.add_argument("--output", type=Path, default=ROOT / ".tdl/dist", help="artifact directory")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    commit = git("rev-parse", "HEAD")
    dirty = bool(git("status", "--porcelain"))
    version = f"dev-{commit[:12]}" + ("-dirty" if dirty else "")
    date = git("show", "-s", "--format=%cI", "HEAD")
    flags = " ".join([
        "-s -w",
        f"-X github.com/iyear/tdl/pkg/consts.Version={version}",
        f"-X github.com/iyear/tdl/pkg/consts.Commit={commit[:12]}",
        f"-X github.com/iyear/tdl/pkg/consts.CommitDate={date}",
    ])
    artifacts = []
    for target in dict.fromkeys(args.target or TARGETS):
        system, arch = target.split("/")
        directory = output / f"{system}-{arch}"
        directory.mkdir(exist_ok=True)
        executable = directory / ("tdl.exe" if system == "windows" else "tdl")
        env = {**os.environ, "CGO_ENABLED": "0", "GOOS": system, "GOARCH": arch, "GOWORK": str(ROOT / "go.work")}
        print(f"Building {target}", flush=True)
        subprocess.run(["go", "build", "-trimpath", "-buildvcs=false", "-ldflags", flags, "-o", str(executable), "."], cwd=ROOT, env=env, check=True)
        label = {"windows": "Windows", "linux": "Linux", "darwin": "MacOS"}[system]
        cpu = "64bit" if arch == "amd64" else arch
        archive = output / f"tdl_{label}_{cpu}.{'zip' if system == 'windows' else 'tar.gz'}"
        files = [executable, ROOT / "README.md", ROOT / "README_zh.md", ROOT / "LICENSE"]
        if system == "windows":
            with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as packed:
                for path in files:
                    packed.write(path, path.name)
        else:
            with tarfile.open(archive, "w:gz") as packed:
                for path in files:
                    packed.add(path, arcname=path.name)
        artifacts.append({"target": target, "binary": str(executable.relative_to(output)), "archive": archive.name,
                          "binary_sha256": checksum(executable),
                          "sha256": checksum(archive)})
    (output / "tdl_checksums.txt").write_text("".join(f"{a['sha256']}  {a['archive']}\n" for a in artifacts))
    (output / "build.json").write_text(json.dumps({"version": version, "commit": commit, "commit_date": date, "dirty": dirty,
        "go": subprocess.check_output(["go", "version"], text=True).strip(), "artifacts": artifacts}, indent=2) + "\n")
    print(f"Built {len(artifacts)} targets in {output}")


if __name__ == "__main__":
    main()
