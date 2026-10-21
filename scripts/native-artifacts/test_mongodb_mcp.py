#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import Mock, patch

spec = importlib.util.spec_from_file_location('mongodb_mcp', Path(__file__).with_name('mongodb_mcp.py'))
mongo = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mongo)


class MongoBundleTests(unittest.TestCase):
    def test_fixture_requires_pinned_bytes_before_extraction(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch.object(mongo.urllib.request, 'urlopen', return_value=io.BytesIO(b'changed')), patch.object(mongo.subprocess, 'run') as extract:
                with self.assertRaisesRegex(RuntimeError, 'checksum mismatch'):
                    mongo.fixture(root)
                extract.assert_not_called()
            pin = mongo.FIXTURE | {'sha256':mongo.hashlib.sha256(b'fixture').hexdigest()}
            with patch.object(mongo, 'FIXTURE', pin), patch.object(mongo.urllib.request, 'urlopen', return_value=io.BytesIO(b'fixture')), patch.object(mongo.subprocess, 'run') as extract:
                path = mongo.fixture(root)
                self.assertEqual(path.name, 'mongod')
                extract.assert_called_once()

    def test_stage_retains_lock_licenses_and_pinned_launcher(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            node = root/'node'
            (node/'bin').mkdir(parents=True)
            (node/'LICENSE').write_text('Node notices')
            destination = root/'stage'
            def install(command, cwd, **kwargs):
                self.assertEqual(command[1:], ['ci', '--omit=dev', '--ignore-scripts', '--no-audit', '--no-fund'])
                self.assertTrue((cwd/'package-lock.json').exists())
                package = cwd/'node_modules/mongodb-mcp-server'
                package.mkdir(parents=True)
                (package/'LICENSE').write_text('Apache license')
                (package/'package.json').write_text('{"version":"3.1.0"}')
            with patch.object(mongo.subprocess, 'check_output', return_value='10.9.9\n'), patch.object(mongo.subprocess, 'run', side_effect=install):
                self.assertEqual(mongo.stage(destination, node), '10.9.9')
            launcher = (destination/'bin/start').read_text()
            self.assertIn('cd "$root/app"', launcher)
            self.assertIn('exec "$root/node/bin/node" dist/esm/index.js "$@"', launcher)
            self.assertEqual((destination/'bin/start').stat().st_mode & 0o777, 0o755)
            self.assertTrue((destination/'provenance/package-lock.json').exists())
            self.assertEqual((destination/'LICENSES/Node-LICENSE').read_text(), 'Node notices')
            self.assertEqual((destination/'LICENSES/node_modules/mongodb-mcp-server/LICENSE').read_text(), 'Apache license')
            notices = json.loads((destination/'THIRD_PARTY.json').read_text())
            self.assertTrue(any(p['name']=='mongodb-mcp-server' and p['version']=='3.1.0' for p in notices))
            with self.assertRaisesRegex(RuntimeError, 'already exist'):
                mongo.stage(destination, node)

    def test_rejects_changed_npm(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root/'node').mkdir()
            with patch.object(mongo.subprocess, 'check_output', return_value='11.0.0\n'):
                with self.assertRaisesRegex(RuntimeError, 'unexpected bundled npm'):
                    mongo.stage(root/'stage', root/'node')

    def test_http_handshake_and_cleanup(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database, server = Mock(), Mock()
            database.poll.return_value = server.poll.return_value = None
            replies = [
                ({'jsonrpc':'2.0', 'id':1, 'result':{'protocolVersion':'2025-03-26'}}, {'Mcp-Session-Id':'session'}),
                (None, {}),
                ({'jsonrpc':'2.0', 'id':2, 'result':{'tools':[{'name':'find'}]}}, {}),
            ]
            methods = []
            def response(request, timeout):
                message = json.loads(request.data)
                methods.append(message['method'])
                if len(methods)>1:
                    self.assertEqual(request.get_header('Mcp-session-id'), 'session')
                payload, headers = replies.pop(0)
                result = io.BytesIO(json.dumps(payload).encode() if payload else b'')
                result.headers = headers
                return result
            with patch.object(mongo.subprocess, 'Popen', side_effect=[database, server]), patch.object(mongo.socket, 'create_connection'), patch.object(mongo.urllib.request, 'urlopen', side_effect=response):
                mongo.qualify(root, root/'mongod', root)
            self.assertEqual(methods, ['initialize', 'notifications/initialized', 'tools/list'])
            self.assertTrue((root/'mongodb-mcp-server.qualification.json').exists())
            database.terminate.assert_called_once()
            server.terminate.assert_called_once()

    def test_qualification_failure_cleans_database(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = Mock()
            database.poll.return_value = 1
            database.wait.side_effect = [subprocess.TimeoutExpired('mongod', 15), 0]
            with patch.object(mongo.subprocess, 'Popen', return_value=database):
                with self.assertRaisesRegex(RuntimeError, 'mongod exited'):
                    mongo.qualify(root, root/'mongod', root)
            database.kill.assert_called_once()
            self.assertFalse((root/'mongodb-mcp-server.qualification.json').exists())


if __name__ == '__main__':
    unittest.main()
