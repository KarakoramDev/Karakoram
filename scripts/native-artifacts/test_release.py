#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('native_release',Path(__file__).with_name('release.py'))
release = importlib.util.module_from_spec(spec)
spec.loader.exec_module(release)


class ReleaseArtifacts(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root/'source'; self.source.mkdir()
        self.dest = self.root/'release'; self.dest.mkdir()
        self.lock = self.root/'lock.json'
        self.payload = b'real test bytes\n'
        self.artifact = {'file':'app.tar.gz','sha256':hashlib.sha256(self.payload).hexdigest(),'size':len(self.payload),'upstreamURL':''}
        (self.source/'app.tar.gz').write_bytes(self.payload)
        self.lock.write_text(json.dumps([self.artifact,{'upstreamURL':'https://official.example/app'}]))

    def test_copy_built_only(self):
        release.stage(self.lock,self.source,self.dest)
        self.assertEqual((self.dest/'app.tar.gz').read_bytes(),self.payload)
        self.assertEqual([p.name for p in self.dest.iterdir()],['app.tar.gz'])

    def test_missing(self):
        (self.source/'app.tar.gz').unlink()
        with self.assertRaisesRegex(ValueError,'missing native artifact'):
            release.stage(self.lock,self.source,self.dest)
        self.assertEqual(list(self.dest.iterdir()),[])

    def test_wrong_digest(self):
        (self.source/'app.tar.gz').write_bytes(b'x'*len(self.payload))
        with self.assertRaisesRegex(ValueError,'SHA256 mismatch'):
            release.stage(self.lock,self.source,self.dest)
        self.assertEqual(list(self.dest.iterdir()),[])

    def test_wrong_size(self):
        (self.source/'app.tar.gz').write_bytes(b'x')
        with self.assertRaisesRegex(ValueError,'size mismatch'):
            release.stage(self.lock,self.source,self.dest)

    def test_path_escape(self):
        self.artifact['file']='../outside'
        self.lock.write_text(json.dumps([self.artifact]))
        with self.assertRaisesRegex(ValueError,'basename'):
            release.stage(self.lock,self.source,self.dest)


if __name__=='__main__': unittest.main()
