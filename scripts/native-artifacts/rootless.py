#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
"""One Ubuntu builder and disposable clean runners without host installation.

No host home is mounted. Subordinate UID/GID mappings preserve real package and
PostgreSQL ownership; a single-ID namespace is not sufficient for this backend.
Each runner is extracted from the verified pristine Ubuntu archive, never copied
from the builder. Directories rather than overlayfs permit dpkg directory renames.
"""
import hashlib
from pathlib import Path
import shlex
import subprocess
import urllib.request

URL = 'https://cloud-images.ubuntu.com/minimal/releases/noble/release-20261017/ubuntu-24.04-minimal-cloudimg-amd64-root.tar.xz'
SHA256 = '15def31708587e1a6ffc9864caa8203ed81dc0fd68d9aa060e86799bd7ef24bf'
MAPPING = ['unshare', '--user', '--map-root-user', '--map-auto']


class Rootless:
    def __init__(self, directory):
        self.directory = Path(directory).resolve()
        self.archive = self.directory/'ubuntu24.04.tar.xz'
        self.directory.mkdir(parents=True, exist_ok=True)

    def prepare(self):
        if not self.archive.exists():
            with urllib.request.urlopen(URL, timeout=300) as response, self.archive.open('wb') as target:
                while chunk := response.read(1024*1024):
                    target.write(chunk)
        with self.archive.open('rb') as source:
            if hashlib.file_digest(source, 'sha256').hexdigest() != SHA256:
                raise RuntimeError('Ubuntu rootfs checksum mismatch')

    def command(self, instance, args):
        root = self.directory/instance
        if not root.exists():
            root.mkdir()
            subprocess.run([*MAPPING, 'tar', '--exclude=dev/*', '-xJpf', str(self.archive), '-C', str(root)], check=True)
        command = ['bwrap', '--bind', str(root), '/', '--proc', '/proc',
                   '--dev', '/dev', '--tmpfs', '/run', '--unshare-pid', '--unshare-ipc',
                   '--unshare-uts', '--die-with-parent', '--clearenv', '--cap-add', 'ALL',
                   '--setenv', 'PATH', '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin',
                   '--setenv', 'HOME', '/root', '--setenv', 'LANG', 'C.UTF-8',
                   '--setenv', 'NATIVE_ARTIFACT_ROOTLESS', '1',
                   '--ro-bind', '/etc/resolv.conf', '/etc/resolv.conf', '--chdir', '/root', '--', *args]
        script = ('rm -f '+shlex.quote(str(root/'etc/resolv.conf'))+
                  '; touch '+shlex.quote(str(root/'etc/resolv.conf'))+'; exec '+shlex.join(command))
        return [*MAPPING, '--mount', '--fork', 'bash', '-euc', script]

    def execute(self, instance, *args, **kwargs):
        return subprocess.run(self.command(instance, args), check=True, **kwargs)

    def push(self, source, instance, destination):
        with open(source, 'rb') as payload:
            self.execute(instance, 'bash', '-euc', 'cat > '+shlex.quote(destination), stdin=payload)

    def pull(self, instance, source, destination):
        with open(destination, 'wb') as payload:
            self.execute(instance, 'cat', source, stdout=payload)

    def remove(self, instance):
        root = self.directory/instance
        if root.exists():
            subprocess.run([*MAPPING, 'rm', '-rf', str(root)], check=True)
