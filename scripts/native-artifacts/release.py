#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
"""Stage every Karakoram-built file referenced by the tagged release's lock."""
import hashlib
import json
from pathlib import Path
import shutil
import sys


def stage(lock, source, destination):
    entries = json.loads(Path(lock).read_text())
    required = [a for a in entries if not a['upstreamURL']]
    # Validate the entire set before copying any bytes into the release.
    for artifact in required:
        filename = artifact['file']
        if Path(filename).name != filename:
            raise ValueError('artifact file must be a basename: '+filename)
        file = Path(source)/filename
        if not file.is_file():
            raise ValueError('missing native artifact: '+str(file)+'; rebuild with scripts/native-artifacts/build.sh')
        if file.stat().st_size != artifact['size']:
            raise ValueError('native artifact size mismatch: '+filename)
        with file.open('rb') as stream:
            digest = hashlib.file_digest(stream,'sha256').hexdigest()
        if digest != artifact['sha256']:
            raise ValueError('native artifact SHA256 mismatch: '+filename)
    for artifact in required:
        shutil.copyfile(Path(source)/artifact['file'],Path(destination)/artifact['file'])


if __name__ == '__main__':
    try:
        stage(*sys.argv[1:])
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise SystemExit('native artifacts: '+str(error)) from None
