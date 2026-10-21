#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
"""Assemble the published npm tree with a pinned Node runtime; qualify natively."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.request

HERE = Path(__file__).resolve().parent
NAME = 'mongodb-mcp-server'

FIXTURE = {
    'file':'mongodb-linux-x86_64-ubuntu2404-8.0.16.tgz',
    'url':'https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu2404-8.0.16.tgz',
    'sha256':'7d67c164da52d31ec144778b40ec9ab6bdaa0c250be020b44dacdcf91a7de3c0',
}


def fixture(directory):
    directory = Path(directory)
    archive = directory/FIXTURE['file']
    with urllib.request.urlopen(FIXTURE['url'], timeout=600) as response, archive.open('wb') as target:
        shutil.copyfileobj(response, target)
    if sha256(archive) != FIXTURE['sha256']:
        raise RuntimeError('MongoDB fixture checksum mismatch')
    subprocess.run(['tar', '-xf', str(archive), '-C', str(directory)], check=True)
    return directory/FIXTURE['file'].removesuffix('.tgz')/'bin/mongod'

def sha256(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def stage(destination, node):
    destination, node = Path(destination), Path(node)
    if destination.exists():
        raise RuntimeError('stage must not already exist: '+str(destination))
    destination.mkdir(parents=True)
    shutil.copytree(node, destination/'node', symlinks=True)
    npm = node/'bin/npm'
    env = os.environ | {'PATH':str(node/'bin')+':'+os.environ['PATH']}
    version = subprocess.check_output([str(npm), '--version'], env=env, text=True).strip()
    if version != '10.9.9':
        raise RuntimeError('unexpected bundled npm version: '+version)
    with tempfile.TemporaryDirectory() as temporary:
        work = Path(temporary)
        for filename in ('package.json', 'package-lock.json'):
            shutil.copy2(HERE/'mongodb-mcp'/filename, work/filename)
        subprocess.run([str(npm), 'ci', '--omit=dev', '--ignore-scripts', '--no-audit', '--no-fund'], cwd=work, env=env, check=True)
        modules = work/'node_modules'
        shutil.copytree(modules/NAME, destination/'app', symlinks=True)
        shutil.copytree(modules, destination/'app/node_modules', symlinks=True, ignore=shutil.ignore_patterns('.bin'))
    (destination/'bin').mkdir()
    launcher = destination/'bin/start'
    launcher.write_text('#!/bin/sh\nset -eu\nroot=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)\ncd "$root/app"\nexport NODE_ENV=production\nexec "$root/node/bin/node" dist/esm/index.js "$@"\n')
    launcher.chmod(0o755)
    licenses = destination/'LICENSES'
    licenses.mkdir()
    shutil.copy2(node/'LICENSE', licenses/'Node-LICENSE')
    lock = json.loads((HERE/'mongodb-mcp/package-lock.json').read_text())
    notices = []
    for location, package in sorted(lock['packages'].items()):
        if not location:
            continue
        name = location.rsplit('node_modules/', 1)[-1]
        notices.append({'name':name, **package})
        installed = destination/'app'/location
        for path in sorted(installed.glob('*')):
            if path.is_file() and path.name.lower().startswith(('license', 'licence', 'notice', 'copyright')):
                target = licenses/location/path.name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, target)
    (destination/'THIRD_PARTY.json').write_text(json.dumps(notices, indent=2)+'\n')
    shutil.copytree(HERE/'mongodb-mcp', destination/'provenance')
    return version


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def qualify(root, mongod, output):
    """Exercise the actual launcher and HTTP MCP against a loopback native mongod."""
    root, output = Path(root), Path(output)
    db_port, mcp_port = free_port(), free_port()
    with tempfile.TemporaryDirectory() as data, (output/'mongodb-mcp-server.smoke.log').open('w') as log:
        database = subprocess.Popen([str(mongod), '--dbpath', data, '--bind_ip', '127.0.0.1', '--port', str(db_port), '--wiredTigerCacheSizeGB', '0.25', '--setParameter', 'diagnosticDataCollectionEnabled=false'], stdout=log, stderr=log)
        server = None
        try:
            for _ in range(120):
                if database.poll() is not None:
                    raise RuntimeError('mongod exited during startup')
                try:
                    with socket.create_connection(('127.0.0.1', db_port), timeout=0.2):
                        break
                except OSError:
                    time.sleep(0.25)
            else:
                raise RuntimeError('mongod startup timed out')
            env = os.environ | {'MDB_MCP_TRANSPORT':'http', 'MDB_MCP_HTTP_HOST':'127.0.0.1', 'MDB_MCP_HTTP_PORT':str(mcp_port), 'MDB_MCP_CONNECTION_STRING':f'mongodb://127.0.0.1:{db_port}/qualification', 'MDB_MCP_TELEMETRY':'disabled', 'MDB_MCP_READ_ONLY':'true'}
            server = subprocess.Popen([str(root/'bin/start')], env=env, stdout=log, stderr=log)
            url = f'http://127.0.0.1:{mcp_port}/mcp'
            headers = {'Content-Type':'application/json', 'Accept':'application/json, text/event-stream'}
            def rpc(message):
                request = urllib.request.Request(url, json.dumps(message).encode(), headers=headers)
                with urllib.request.urlopen(request, timeout=15) as response:
                    if response.headers.get('Mcp-Session-Id'):
                        headers['Mcp-Session-Id'] = response.headers['Mcp-Session-Id']
                    payload = response.read().decode()
                if not payload:
                    return None
                if payload.startswith('event:') or payload.startswith('data:'):
                    payload = next(line[5:].strip() for line in payload.splitlines() if line.startswith('data:'))
                result = json.loads(payload)
                if 'error' in result:
                    raise RuntimeError(result['error'])
                return result
            for _ in range(120):
                if server.poll() is not None:
                    raise RuntimeError('MCP launcher exited during startup')
                try:
                    initialized = rpc({'jsonrpc':'2.0', 'id':1, 'method':'initialize', 'params':{'protocolVersion':'2025-03-26', 'capabilities':{}, 'clientInfo':{'name':'native-artifact-qualification', 'version':'1'}}})
                    break
                except OSError:
                    time.sleep(0.25)
            else:
                raise RuntimeError('MCP startup timed out')
            rpc({'jsonrpc':'2.0', 'method':'notifications/initialized'})
            tools = rpc({'jsonrpc':'2.0', 'id':2, 'method':'tools/list', 'params':{}})
            if not tools['result']['tools']:
                raise RuntimeError('empty MCP tools list')
            (output/'mongodb-mcp-server.qualification.json').write_text(json.dumps({'initialize':initialized, 'tools/list':tools}, indent=2)+'\n')
        finally:
            for process in (server, database):
                if process is not None:
                    process.terminate()
                    try:
                        process.wait(timeout=15)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--stage', type=Path)
    parser.add_argument('--node', type=Path)
    parser.add_argument('--mongod', type=Path)
    args = parser.parse_args()
    if args.stage:
        stage(args.stage.resolve(), args.node.resolve())
        return
    import build
    output = build.OUT
    output.mkdir(parents=True, exist_ok=True)
    recipe = build.RECIPES[NAME]
    pin = build.TOOLCHAINS['node-'+recipe['node']]
    archive = build.download(pin['url'], pin['file'], pin['sha256'])
    if archive.stat().st_size != pin['size']:
        raise RuntimeError('Node archive size mismatch')
    with tempfile.TemporaryDirectory(dir=output) as temporary:
        temporary = Path(temporary)
        subprocess.run(['tar', '-xf', str(archive), '-C', str(temporary)], check=True)
        node = temporary/('node-v'+recipe['node']+'-linux-x64')
        root = output/(NAME+'-'+recipe['version']+'-stage')
        npm = stage(root, node)
    manifest = recipe | {'name':NAME, 'build':'native-v1', 'platform':'ubuntu24.04-amd64', 'npm':npm, 'toolchainTransports':[pin], 'dependencyLockSHA256':sha256(HERE/'mongodb-mcp/package-lock.json'), 'buildInputFingerprint':build.build_input_fingerprint(NAME)}
    (root/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    qualify(root, args.mongod.resolve(), output)
    filename = NAME+'-'+recipe['version']+'-ubuntu24.04-amd64.tar.gz'
    candidate = output/(filename+'.candidate')
    subprocess.run(['tar', '--sort=name', '--mtime=@0', '--owner=0', '--group=0', '--numeric-owner', '-C', str(root), '-czf', str(candidate), '.'], check=True)
    candidate.replace(output/filename)
    artifact = {'name':NAME, 'version':recipe['version'], 'file':filename, 'sha256':sha256(output/filename), 'size':(output/filename).stat().st_size, 'upstreamURL':'', 'license':recipe['license'], 'source':{'repo':'https://github.com/'+recipe['repo'], 'commit':recipe['commit']}, 'build':'native-v1', 'runtimeDeps':recipe['runtimeDeps']}
    entries = json.loads(build.LOCK.read_text())
    entries = [entry for entry in entries if entry['name'] != NAME]+[artifact]
    build.LOCK.write_text(json.dumps(entries, indent=2)+'\n')
    print(json.dumps(artifact, indent=2))


if __name__ == '__main__':
    main()
