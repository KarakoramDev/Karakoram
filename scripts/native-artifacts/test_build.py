#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('native_build',Path(__file__).with_name('build.py'))
build = importlib.util.module_from_spec(spec)
spec.loader.exec_module(build)


class OtpGuest:
    """Execute the real archive verification/cache/build shell in a local guest."""
    version = '27.3.4.6'

    def __init__(self, root):
        self.root = root
        self.out = root/'out'
        self.out.mkdir()
        (root/'root').mkdir()
        (root/'opt').mkdir()
        self.prefix = root/'opt'/('otp-'+self.version)
        self.source = root/'opt'/('otp-src-'+self.version)
        self.receipt = self.prefix/'.karakoram-build-inputs'
        self.counter = root/'compilations'
        self.packages = root/'packages'
        self.packages.write_text('gcc=1\nlibssl-dev=1\n')
        query = root/'dpkg-query'
        query.write_text('#!/bin/sh\ncat '+build.Q(str(self.packages))+'\n')
        query.chmod(0o755)
        self.bundled = []

    def pin(self, marker, fail=False):
        configure = f'''#!/bin/sh
set -eu
{"exit 17" if fail else ""}
test ! -e stale-source
prefix=${{1#--prefix=}}
cat > Makefile <<EOF
all:
\tprintf '%s\\\\n' '{marker}' >> {self.counter}
install:
\tmkdir -p "$prefix/bin"
\tprintf '%s\\\\n' '{marker}' > "$prefix/bin/erl"
\tchmod 0755 "$prefix/bin/erl"
EOF
'''
        archive = self.out/'otp.tar.gz'
        payload = configure.encode()
        with tarfile.open(archive,'w:gz') as output:
            member = tarfile.TarInfo('otp/configure')
            member.mode = 0o755
            member.size = len(payload)
            output.addfile(member,io.BytesIO(payload))
        return {'file':archive.name,'url':'https://official.example/otp.tar.gz',
                'sha256':hashlib.sha256(archive.read_bytes()).hexdigest()}

    def run(self, *args, **kwargs):
        if args[:3] == ('incus','file','push'):
            shutil.copyfile(args[3],self.root/'root'/args[4].split('/root/',1)[1])
        elif args[:3] == ('incus','file','pull'):
            payload = (self.root/'root/native-manifest.json').read_bytes()
            with tarfile.open(args[4],'w:gz') as archive:
                member = tarfile.TarInfo('./manifest.json')
                member.size = len(payload)
                archive.addfile(member,io.BytesIO(payload))
        else:
            raise AssertionError(args)

    def guest(self, script):
        if 'sha256sum -c -' in script:
            script = script.replace('/root/',str(self.root/'root')+'/')
            script = script.replace('/opt/',str(self.root/'opt')+'/')
            script = script.replace('dpkg-query',str(self.root/'dpkg-query'))
            subprocess.run(['bash','-euc',script],check=True,capture_output=True,text=True)
        elif 'native-recipe.sh' in script:
            self.bundled.append((self.prefix/'bin/erl').read_text().strip())
        elif 'git init' not in script:
            raise AssertionError(script)


class NativeBuilder(unittest.TestCase):
    def test_toolchain_pins_complete(self):
        for recipe in build.RECIPES.values():
            keys = [kind+'-'+recipe[kind] for kind in ('node','go','rust','otp','bun') if kind in recipe]
            if 'elixir' in recipe:
                keys.append('elixir-'+recipe['elixir']+'-otp-'+recipe['otp'].split('.')[0])
            for key in keys:
                pin = build.TOOLCHAINS[key]
                self.assertEqual(len(bytes.fromhex(pin['sha256'])),32)
                self.assertGreater(pin['size'],0)
                self.assertEqual(Path(pin['file']).name,pin['file'])
                self.assertTrue(pin['url'].startswith('https://'))
                self.assertTrue(pin['checksumURL'].startswith('https://'))

    def test_bootstrap_tools_are_verified_without_registry_resolution(self):
        for name, node, expected in (
            ('supabase-storage', '24.22.0', ['npm-11.12.1']),
            ('supabase-studio', '22.23.3', ['pnpm-11.13.1', 'turbo-linux-64-2.9.14']),
        ):
            with patch.object(build,'install_toolchain') as install, patch.object(build,'guest'), patch.object(build,'STUDIO_ASSETS',[]):
                path = build.toolchains({'name':name,'node':node})
            self.assertEqual([call.args[0] for call in install.call_args_list],['node-'+node,*expected])
            for key in expected:
                self.assertIn('/opt/'+key+'/bin',path)
                pin = build.TOOLCHAINS[key]
                self.assertEqual(len(bytes.fromhex(pin['sha256'])),32)
                self.assertGreater(pin['size'],0)
        recipe = (build.HERE/'recipe.sh').read_text()
        self.assertNotIn('npm install -g',recipe)
        self.assertNotIn('pnpm dlx',recipe)
        self.assertIn('turbo prune studio --docker',recipe)

    def test_mix_bootstrap_uses_pinned_verified_local_files(self):
        with patch.object(build,'install_toolchain') as install, patch.object(build,'guest'), patch.object(build,'wait_for_host'):
            build.toolchains({'otp':'27.3.4.6','elixir':'1.18.2'})
        self.assertEqual([call.args[0] for call in install.call_args_list],['otp-27.3.4.6','elixir-1.18.2-otp-27','hex-2.5.3','rebar3-3.24.0'])
        self.assertEqual([call.kwargs for call in install.call_args_list[-2:]],[{'installer':'true'},{'installer':'true'}])
        recipe = (build.HERE/'recipe.sh').read_text()
        self.assertIn('mix archive.install /root/hex-2.5.3.ez --force',recipe)
        self.assertIn('mix local.rebar rebar3 /root/rebar3-3.24.0 --force',recipe)
        self.assertNotIn('mix local.hex',recipe)
        self.assertNotIn('--if-missing',recipe)
        for key in ('hex-2.5.3','rebar3-3.24.0'):
            self.assertEqual(len(bytes.fromhex(build.TOOLCHAINS[key]['sha256'])),32)

    def test_otp_reuse_requires_matching_successful_build_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            guest = OtpGuest(Path(directory))
            pin = guest.pin('current')
            # A legacy executable alone must not qualify for reuse.
            (guest.prefix/'bin').mkdir(parents=True)
            (guest.prefix/'bin/erl').write_text('legacy\n')
            (guest.prefix/'bin/erl').chmod(0o755)
            with patch.object(build,'OUT',guest.out), patch.dict(build.TOOLCHAINS,{'otp-'+guest.version:pin}), patch.object(build,'run',side_effect=guest.run), patch.object(build,'guest',side_effect=guest.guest), patch.object(build,'wait_for_host'):
                build.install_otp(guest.version)
                self.assertEqual(guest.counter.read_text().splitlines(),['current'])
                receipt = guest.receipt.read_text()
                self.assertIn(pin['sha256'],receipt)
                self.assertIn('--without-javac --without-wx --without-odbc',receipt)
                build.install_otp(guest.version)
                self.assertEqual(guest.counter.read_text().splitlines(),['current'])
                for cause in ('missing','mismatched','compiler','builder'):
                    with self.subTest(cause=cause):
                        (guest.source/'stale-source').touch()
                        if cause == 'missing':
                            guest.receipt.unlink()
                        elif cause == 'mismatched':
                            guest.receipt.write_text('not current inputs\n')
                        elif cause == 'compiler':
                            guest.packages.write_text('gcc=2\nlibssl-dev=1\n')
                        else:
                            changed = guest.root/'changed-builder.py'
                            changed.write_text('# changed compilation inputs\n')
                        with patch.object(build,'__file__',str(changed) if cause=='builder' else build.__file__):
                            build.install_otp(guest.version)
                        self.assertFalse((guest.source/'stale-source').exists())
                self.assertEqual(len(guest.counter.read_text().splitlines()),5)

    def test_otp_cache_reuse_still_verifies_source_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            guest = OtpGuest(Path(directory))
            pin = guest.pin('current')
            with patch.object(build,'OUT',guest.out), patch.dict(build.TOOLCHAINS,{'otp-'+guest.version:pin}), patch.object(build,'run',side_effect=guest.run), patch.object(build,'guest',side_effect=guest.guest), patch.object(build,'wait_for_host'):
                build.install_otp(guest.version)
                receipt = guest.receipt.read_text()
                (guest.out/pin['file']).write_bytes(b'unverified archive')
                with patch.object(build,'download',return_value=guest.out/pin['file']):
                    with self.assertRaises(subprocess.CalledProcessError):
                        build.install_otp(guest.version)
                self.assertEqual(guest.receipt.read_text(),receipt)
                self.assertEqual(guest.counter.read_text().splitlines(),['current'])

    def test_failed_otp_build_cannot_publish_a_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            guest = OtpGuest(Path(directory))
            pin = guest.pin('old')
            with patch.object(build,'OUT',guest.out), patch.dict(build.TOOLCHAINS,{'otp-'+guest.version:pin}), patch.object(build,'run',side_effect=guest.run), patch.object(build,'guest',side_effect=guest.guest), patch.object(build,'wait_for_host'):
                build.install_otp(guest.version)
                build.TOOLCHAINS['otp-'+guest.version] = guest.pin('failed',fail=True)
                with self.assertRaises(subprocess.CalledProcessError):
                    build.install_otp(guest.version)
                self.assertFalse(guest.receipt.exists())
                self.assertFalse((guest.prefix/'bin/erl').exists())
                build.TOOLCHAINS['otp-'+guest.version] = guest.pin('recovered')
                build.install_otp(guest.version)
                self.assertTrue(guest.receipt.exists())
                self.assertEqual(guest.counter.read_text().splitlines(),['old','recovered'])

    def test_changed_same_version_otp_rebuilds_before_candidate_attestation(self):
        name = 'supabase-supavisor'
        recipe = build.RECIPES[name]
        install = build.install_toolchain
        with tempfile.TemporaryDirectory() as directory:
            guest = OtpGuest(Path(directory))
            here = guest.root/'inputs'
            here.mkdir()
            pins_file = here/'toolchains.json'
            old_pin = guest.pin('old-runtime')
            pins_file.write_text(json.dumps(old_pin))
            guests = [{'name':build.BUILDER,'status':'Running'}]
            def install_selected(key, *args, **kwargs):
                if key == 'otp-'+guest.version:
                    return install(key,*args,**kwargs)
            with patch.object(build,'HERE',here), patch.object(build,'OUT',guest.out), patch.dict(build.TOOLCHAINS,{'otp-'+guest.version:old_pin}), patch.object(build,'get_instances',return_value=guests), patch.object(build,'push_build_inputs'), patch.object(build,'install_toolchain',side_effect=install_selected), patch.object(build,'guest',side_effect=guest.guest), patch.object(build,'run',side_effect=guest.run), patch.object(build,'wait_for_host'), patch.object(build.subprocess,'check_output',return_value='123'), patch.object(build,'qualify') as qualify:
                build.build(name)
                old_fingerprint = build.build_input_fingerprint(name)
                candidate = guest.out/(name+'-'+recipe['version']+'-ubuntu24.04-amd64.tar.gz.candidate')
                new_pin = guest.pin('new-runtime')
                pins_file.write_text(json.dumps(new_pin))
                build.TOOLCHAINS['otp-'+guest.version] = new_pin
                with self.assertRaisesRegex(RuntimeError,'build inputs differ'):
                    build.build(name)
                self.assertEqual(qualify.call_count,1)
                candidate.unlink()
                (guest.source/'stale-source').touch()
                (guest.prefix/'stale-runtime').touch()
                build.build(name)
                self.assertEqual(qualify.call_count,2)
                manifest = json.loads((guest.out/(name+'.manifest.json')).read_text())
                self.assertNotEqual(manifest['buildInputFingerprint'],old_fingerprint)
                self.assertEqual(manifest['buildInputFingerprint'],build.build_input_fingerprint(name))
                self.assertEqual(manifest['toolchainTransports'][0]['sha256'],new_pin['sha256'])
                self.assertIn(new_pin['sha256'],guest.receipt.read_text())
                self.assertFalse((guest.prefix/'stale-runtime').exists())
                self.assertEqual(guest.bundled,['old-runtime','new-runtime'])
                self.assertEqual(guest.counter.read_text().splitlines(),['old-runtime','new-runtime'])

    def test_studio_declaration_inputs_are_pinned(self):
        self.assertEqual({pin['file'] for pin in build.STUDIO_ASSETS},{'studio-lib.deno.d.ts','studio-edge-runtime.d.ts','studio-deno-LICENSE','studio-functions-js-LICENSE'})
        for pin in build.STUDIO_ASSETS:
            self.assertEqual(len(bytes.fromhex(pin['sha256'])),32)
            self.assertGreater(pin['size'],0)
            self.assertTrue(pin['url'].startswith('https://'))
            self.assertTrue(pin['selectionSource'].startswith('https://github.com/supabase/supabase/blob/'))

    @patch.dict('os.environ',{'NATIVE_ARTIFACT_MEMORY_PREFIX':'example-'})
    def test_memory_guard_counts_running_reservations(self):
        guests = [
            {'name':'example-node','status':'Running','config':{'limits.memory':'19GiB'}},
            {'name':'example-test','status':'Running','config':{'limits.memory':'1024MiB'}},
            {'name':'example-stopped','status':'Stopped','config':{'limits.memory':'8GiB'}},
            {'name':'unrelated-node','status':'Running','config':{'limits.memory':'64GiB'}},
        ]
        with patch.object(build,'get_instances',return_value=guests):
            build.memory_guard('example-builder',8)
            with self.assertRaisesRegex(RuntimeError,'exceeding the 28 GiB budget'):
                build.memory_guard('example-builder',8.5)

    def test_extract_verifies_durable_input(self):
        with patch.object(build,'download',return_value=Path('/cache/runtime.tar.gz')), patch.object(build,'run') as run, patch.object(build,'guest') as guest:
            build.checked_archive('https://official.example/runtime.tar.gz','runtime.tar.gz','a'*64,'/opt/runtime')
        self.assertTrue(run.call_args.args[-1].endswith('/root/runtime.tar.gz'))
        command = guest.call_args.args[0]
        self.assertLess(command.index('sha256sum -c -'),command.index('tar -xf'))
        self.assertLess(command.index('sha256sum -c -'),command.index('rm -rf /opt/runtime'))
        self.assertLess(command.index('rm -rf /opt/runtime'),command.index('tar -xf'))
        self.assertIn('/root/runtime.tar.gz',command)
        self.assertNotIn('/tmp/',command)

    def test_extension_rust_toolchains(self):
        with patch.object(build,'install_toolchain') as install:
            build.toolchains({'name':'supabase-pg17-extensions','rust':'1.81.0'})
        self.assertEqual([call.args[0] for call in install.call_args_list],['rust-1.81.0','rust-1.80.0'])

    def test_precompiled_toolchains_replace_existing_trees(self):
        with patch.object(build,'install_toolchain') as install, patch.object(build,'guest'), patch.object(build,'wait_for_host'):
            build.toolchains({'rust':'1.81.0','bun':'1.3.14','otp':'27.3.4.6','elixir':'1.18.2'})
        installers = {call.args[0]:call.kwargs.get('installer','') for call in install.call_args_list}
        self.assertIn('rm -rf /root/rust-1.81.0-x86_64-unknown-linux-gnu /opt/rust-1.81.0;',installers['rust-1.81.0'])
        self.assertIn('rm -rf /opt/bun-linux-x64;',installers['bun-1.3.14'])
        self.assertIn('rm -rf /opt/elixir-1.18.2;',installers['elixir-1.18.2-otp-27'])

    def test_extension_compiled_toolchain_is_not_reused_by_version(self):
        spec = importlib.util.spec_from_file_location('native_extensions',build.HERE/'extensions.py')
        extensions = importlib.util.module_from_spec(spec)
        with patch.object(Path,'read_text',return_value='{}'):
            spec.loader.exec_module(extensions)
        with patch.object(extensions,'PINS',{'pg_graphql':{'repo':'example/graphql','commit':'a'*40}}), patch.object(Path,'mkdir'), patch.object(Path,'write_text'), patch.object(Path,'iterdir',return_value=[]), patch.object(Path,'exists',return_value=False), patch.object(extensions.subprocess,'check_output',return_value='fixture'), patch.object(extensions,'run') as run:
            extensions.main()
        calls = [call.args for call in run.call_args_list]
        cleanup = ('rm','-rf','/opt/pgrx-cli-1.81.0','/opt/pgrx-1.81.0')
        cargo = next(call for call in calls if call[:2]==('cargo','install'))
        self.assertLess(calls.index(cleanup),calls.index(cargo))

    def test_candidate_requires_matching_provenance(self):
        name = 'supabase-storage'
        recipe = build.RECIPES[name]
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            candidate = out/(name+'-'+recipe['version']+'-ubuntu24.04-amd64.tar.gz.candidate')
            fingerprint = build.build_input_fingerprint(name)
            for commit, recorded, accepted in ((recipe['commit'],fingerprint,True),('0'*40,fingerprint,False),(recipe['commit'],None,False),(recipe['commit'],'0'*64,False)):
                manifest = recipe | {'name':name,'commit':commit}
                if recorded is not None:
                    manifest['buildInputFingerprint'] = recorded
                payload = json.dumps(manifest).encode()
                with tarfile.open(candidate,'w:gz') as archive:
                    member = tarfile.TarInfo('./manifest.json')
                    member.size = len(payload)
                    archive.addfile(member,io.BytesIO(payload))
                with patch.object(build,'OUT',out), patch.object(build,'qualify') as qualify, patch.object(build,'ensure_builder') as ensure:
                    if accepted:
                        build.build(name)
                        qualify.assert_called_once()
                    else:
                        with self.assertRaisesRegex(RuntimeError,'provenance differs|build inputs differ'):
                            build.build(name)
                        qualify.assert_not_called()
                    ensure.assert_not_called()

    def test_candidate_rejects_changed_build_support_inputs(self):
        name = 'supabase-studio'
        recipe = build.RECIPES[name]
        for filename in ('patches/supabase-studio.patch', 'toolchains.json', 'studio-assets.json',
                         'recipe.sh', 'worker.py', 'extensions.py', 'extensions.json', 'build.py', 'build.sh'):
            with self.subTest(input=filename), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                here = root/'inputs'
                shutil.copytree(build.HERE, here, ignore=shutil.ignore_patterns('__pycache__'))
                out = root/'out'
                out.mkdir()
                with patch.object(build,'HERE',here), patch.object(build,'OUT',out), patch.object(build,'qualify') as qualify, patch.object(build,'ensure_builder') as ensure:
                    fingerprint = build.build_input_fingerprint(name)
                    self.assertEqual(fingerprint,build.build_input_fingerprint(name))
                    candidate = out/(name+'-'+recipe['version']+'-ubuntu24.04-amd64.tar.gz.candidate')
                    payload = json.dumps(recipe | {'name':name,'buildInputFingerprint':fingerprint}).encode()
                    with tarfile.open(candidate,'w:gz') as archive:
                        member = tarfile.TarInfo('./manifest.json')
                        member.size = len(payload)
                        archive.addfile(member,io.BytesIO(payload))
                    source = here/filename
                    if source.suffix == '.json':
                        pins = json.loads(source.read_text())
                        pin = pins[0] if isinstance(pins,list) else next(iter(pins.values()))
                        pin['sha256'] = '0'*64
                        source.write_text(json.dumps(pins))
                    else:
                        source.write_bytes(source.read_bytes()+b'\n# changed build input\n')
                    with self.assertRaisesRegex(RuntimeError,'build inputs differ'):
                        build.build(name)
                    qualify.assert_not_called()
                    ensure.assert_not_called()

    def test_fresh_candidate_records_fingerprint_and_refreshes_running_builder(self):
        name = 'supabase-storage'
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            fingerprint = build.build_input_fingerprint(name)
            guests = [{'name':build.BUILDER,'status':'Running'}]
            with patch.object(build,'OUT',out), patch.object(build,'get_instances',return_value=guests), patch.object(build,'push_build_inputs') as push, patch.object(build,'toolchains',return_value='/bin'), patch.object(build,'guest'), patch.object(build,'wait_for_host'), patch.object(build,'run'), patch.object(build.subprocess,'check_output',return_value='123'), patch.object(build,'qualify') as qualify:
                build.build(name)
            self.assertEqual(json.loads((out/(name+'.manifest.json')).read_text())['buildInputFingerprint'],fingerprint)
            push.assert_called_once()
            qualify.assert_called_once()

    def test_studio_fixture_requires_locked_metadata_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory)
            candidate = out/'studio.tar.gz.candidate'
            candidate.write_bytes(b'studio')
            (out/'meta.tar.gz').write_bytes(b'untrusted metadata fixture')
            lock = out/'lock.json'
            original = json.dumps([{'name':'supabase-postgres-meta','file':'meta.tar.gz','sha256':'0'*64}])
            lock.write_text(original)
            guests = [{'name':build.RUNNER,'status':'Running','config':{'limits.memory':'2GiB'}}]
            with patch.object(build,'OUT',out), patch.object(build,'LOCK',lock), patch.object(build,'get_instances',return_value=guests), patch.object(build,'memory_guard'), patch.object(build,'run') as run:
                with self.assertRaisesRegex(RuntimeError,'postgres-meta fixture differs from lock'):
                    build.qualify('supabase-studio',candidate,0)
            self.assertTrue(candidate.exists())
            self.assertEqual(lock.read_text(),original)
            run.assert_any_call('incus','delete',build.RUNNER,'--force')

    def test_heavy_build_waits_for_available_memory(self):
        readings = ['Mem: 64000 50000 1000 0 0 14335\n','Mem: 64000 40000 1000 0 0 14336\n']
        with patch.object(build.subprocess,'check_output',side_effect=readings) as check, patch.object(build.time,'sleep') as sleep, patch('builtins.print'):
            build.wait_for_host()
        self.assertEqual(check.call_count,2)
        self.assertEqual(check.call_args.args[0],['free','-m'])
        sleep.assert_called_once_with(30)


if __name__=='__main__': unittest.main()
