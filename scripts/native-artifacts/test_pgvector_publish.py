# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

PUBLISHER = Path(__file__).resolve().parents[2] / 'internal/engine/nativeartifacts/pgvector_publish.py'


class PgvectorPublisherTests(unittest.TestCase):
    def test_fresh_and_existing_package_payloads(self):
        for existing in (False, True):
            with self.subTest(existing=existing), tempfile.TemporaryDirectory() as directory:
                stage = Path(directory)
                root = stage / 'root'
                relative = Path('usr/lib/postgresql/17/lib/vector.so')
                source = stage / 'unpacked' / relative
                source.parent.mkdir(parents=True)
                source.write_bytes(b'qualified-0.8.7')
                target = root / relative
                target.parent.mkdir(parents=True)
                old = None
                if existing:
                    target.write_bytes(b'apt-0.8.6')
                    old = target.open('rb')
                diversions = {}

                def check_output(args, **kwargs):
                    path = args[-1]
                    if args[1] == '--truename':
                        return diversions.get(path, path) + '\n'
                    return 'LOCAL\n'

                def run(args, **kwargs):
                    self.assertEqual(args[:3], ['dpkg-divert', '--local', '--no-rename'])
                    diversions[args[-1]] = args[4]
                    return subprocess.CompletedProcess(args, 0)

                previous = Path.cwd()
                try:
                    os.chdir(stage)
                    with patch.object(sys, 'argv', [str(PUBLISHER), str(root)]), patch('subprocess.check_output', side_effect=check_output), patch('subprocess.run', side_effect=run):
                        runpy.run_path(str(PUBLISHER))
                        runpy.run_path(str(PUBLISHER))
                    self.assertEqual(target.read_bytes(), b'qualified-0.8.7')
                    self.assertEqual(diversions[str(target)], str(target) + '.distrib')
                    if existing:
                        self.assertEqual(old.read(), b'apt-0.8.6')
                        self.assertEqual(Path(str(target) + '.distrib').read_bytes(), b'apt-0.8.6')
                    # Later dpkg operations affect only the diverted package file.
                    Path(diversions[str(target)]).write_bytes(b'apt-upgrade')
                    self.assertEqual(target.read_bytes(), b'qualified-0.8.7')
                    self.assertFalse(list(target.parent.glob('.pgvector-*')))
                finally:
                    if old:
                        old.close()
                    os.chdir(previous)

    def test_conflicting_diversion_fails_without_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            stage = Path(directory)
            source = stage / 'unpacked/usr/share/postgresql/17/extension/vector.control'
            source.parent.mkdir(parents=True)
            source.write_text('new')
            previous = Path.cwd()
            try:
                os.chdir(stage)
                with patch.object(sys, 'argv', [str(PUBLISHER), str(stage / 'root')]), patch('subprocess.check_output', return_value='/conflicting\n'):
                    with self.assertRaisesRegex(RuntimeError, 'conflicting pgvector diversion'):
                        runpy.run_path(str(PUBLISHER))
                self.assertFalse((stage / 'root/usr/share/postgresql/17/extension/vector.control').exists())
            finally:
                os.chdir(previous)

    def test_interrupted_backup_and_registration_recover(self):
        for fault in ('registration', 'partial-copy', 'partial-backup', 'unrelated-backup'):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory:
                stage = Path(directory)
                relative = Path('usr/lib/postgresql/17/lib/vector.so')
                source = stage / 'unpacked' / relative
                source.parent.mkdir(parents=True)
                source.write_bytes(b'qualified-0.8.7')
                target = stage / 'root' / relative
                target.parent.mkdir(parents=True)
                target.write_bytes(b'apt-0.8.6')
                inode = target.stat().st_ino
                backup = Path(str(target) + '.distrib')
                if fault in ('partial-backup', 'unrelated-backup'):
                    backup.write_bytes(b'apt-' if fault == 'partial-backup' else b'unrelated')
                previous = Path.cwd()
                try:
                    os.chdir(stage)
                    with patch.object(sys, 'argv', [str(PUBLISHER), str(stage / 'root')]), patch('subprocess.check_output', return_value=str(target)+'\n'):
                        if fault == 'registration':
                            with patch('subprocess.run', side_effect=subprocess.CalledProcessError(1, 'dpkg-divert')):
                                with self.assertRaises(subprocess.CalledProcessError):
                                    runpy.run_path(str(PUBLISHER))
                            self.assertEqual(backup.read_bytes(), b'apt-0.8.6')
                        elif fault == 'partial-copy':
                            def interrupted_copy(src, dst):
                                Path(dst).write_bytes(b'apt-')
                                raise OSError('interrupted backup')
                            with patch('shutil.copy2', side_effect=interrupted_copy):
                                with self.assertRaisesRegex(OSError, 'interrupted backup'):
                                    runpy.run_path(str(PUBLISHER))
                            self.assertFalse(backup.exists())
                            self.assertFalse(list(target.parent.glob('.pgvector-*')))
                        else:
                            with patch('subprocess.run') as registration:
                                with self.assertRaisesRegex(RuntimeError, 'unverified pgvector package backup'):
                                    runpy.run_path(str(PUBLISHER))
                                registration.assert_not_called()
                        self.assertEqual(target.stat().st_ino, inode)
                        self.assertEqual(target.read_bytes(), b'apt-0.8.6')
                        if fault in ('registration', 'partial-copy'):
                            with target.open('rb') as serving, patch('subprocess.run') as registration:
                                runpy.run_path(str(PUBLISHER))
                                registration.assert_called_once()
                                self.assertEqual(serving.read(), b'apt-0.8.6')
                            self.assertEqual(target.read_bytes(), b'qualified-0.8.7')
                            self.assertEqual(backup.read_bytes(), b'apt-0.8.6')
                finally:
                    os.chdir(previous)
