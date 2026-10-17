#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
"""Make the pinned main worker's source directory a native runtime setting."""
import hashlib
import json
from pathlib import Path
import sys

source, destination = map(Path,sys.argv[1:])
original = source.read_text()
needle = '/home/deno/functions'
if original.count(needle) != 2:
    raise SystemExit('upstream main-worker paths changed; review the native adaptation')
adapted = "const functionRoot = Deno.env.get('FUNCTIONS_ROOT')\nif (!functionRoot) throw new Error('FUNCTIONS_ROOT is required')\n" + original.replace(needle,'${functionRoot}')
destination.write_text(adapted)
destination.with_name('worker-source.json').write_text(json.dumps({
    'repo':'https://github.com/supabase/supabase',
    'commit':'bc27d2661edd09a5d0b8d76ce33da6d4071a756e',
    'path':'docker/volumes/functions/main/index.ts',
    'sourceSHA256':hashlib.sha256(original.encode()).hexdigest(),
    'nativeSHA256':hashlib.sha256(adapted.encode()).hexdigest(),
    'adaptation':'Replace the two filesystem paths with required FUNCTIONS_ROOT environment setting.'
},indent=2)+'\n')
