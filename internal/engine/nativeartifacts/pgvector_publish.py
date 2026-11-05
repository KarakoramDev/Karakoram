# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
"""Publish verified, unpacked pgvector files without unlinking live libraries."""
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

root = Path(sys.argv[1])
for source in sorted(Path("unpacked").rglob("*")):
    if not source.is_file():
        continue
    relative = source.relative_to("unpacked")
    target = root / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    if "karakoram-native-artifacts" not in relative.parts:
        diverted = str(target) + ".distrib"
        current = subprocess.check_output(["dpkg-divert", "--truename", str(target)], text=True).strip()
        if current == str(target):
            backup = Path(diverted)
            if target.is_symlink() or backup.is_symlink():
                raise RuntimeError("unsafe existing pgvector package payload: " + str(target))
            if backup.exists():
                # Registration can fail after the backup is published. Resume
                # only if it is a complete copy of the still-serving old file.
                if not target.is_file() or not backup.is_file():
                    raise RuntimeError("unverified pgvector package backup: " + diverted)
                with target.open("rb") as active, backup.open("rb") as saved:
                    if hashlib.file_digest(active, "sha256").digest() != hashlib.file_digest(saved, "sha256").digest():
                        raise RuntimeError("unverified pgvector package backup: " + diverted)
            elif target.exists():
                fd, temporary = tempfile.mkstemp(prefix=".pgvector-backup-", dir=target.parent)
                os.close(fd)
                try:
                    shutil.copy2(target, temporary)
                    # Publish only a complete backup and never overwrite a
                    # competing path. Neither operation alters the active inode.
                    os.link(temporary, backup)
                finally:
                    os.unlink(temporary)
            subprocess.run(["dpkg-divert", "--local", "--no-rename", "--divert", diverted, "--add", str(target)], check=True)
        elif current != diverted or subprocess.check_output(["dpkg-divert", "--listpackage", str(target)], text=True).strip() != "LOCAL":
            raise RuntimeError("conflicting pgvector diversion: " + str(target))
    fd, temporary = tempfile.mkstemp(prefix=".pgvector-", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as output, source.open("rb") as payload:
            shutil.copyfileobj(payload, output)
            os.fchmod(output.fileno(), source.stat().st_mode & 0o777)
        os.replace(temporary, target)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
