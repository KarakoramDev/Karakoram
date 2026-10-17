#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
"""Sequential native builds; publication is conditional on a clean-runner smoke."""
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tarfile
import shutil
import tempfile
import time
import urllib.request

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
OUT = ROOT / 'dist/native-artifacts'
LOCK = ROOT / 'internal/engine/nativeartifacts/lock.json'
BUILDER = os.environ.get('NATIVE_ARTIFACT_BUILDER', 'karakoram-artifact-builder')
RUNNER = os.environ.get('NATIVE_ARTIFACT_RUNNER', 'karakoram-artifact-runner')
NETWORK = os.environ.get('NATIVE_ARTIFACT_NETWORK', 'incusbr0')
RECIPES = json.loads((HERE / 'recipes.json').read_text())
TOOLCHAINS = json.loads((HERE / 'toolchains.json').read_text())
STUDIO_ASSETS = json.loads((HERE / 'studio-assets.json').read_text())
Q = shlex.quote
TRANSPORTS = []


def build_input_fingerprint(name):
    # Hash the selected recipe and the complete checked-in build support tree.
    # Conservative invalidation avoids maintaining a second dependency graph.
    inputs = {'recipe': RECIPES[name], 'files': {}}
    paths = [path for pattern in ('*.py', '*.sh', '*.json') for path in HERE.glob(pattern)
             if path.name != 'recipes.json' and not path.name.startswith('test_')]
    paths.extend(path for path in (HERE/'patches').rglob('*') if path.is_file())
    for path in sorted(paths):
        inputs['files'][path.relative_to(HERE).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
    payload = json.dumps(inputs, sort_keys=True, separators=(',', ':')).encode()
    return hashlib.sha256(payload).hexdigest()


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def guest(script):
    run('incus', 'exec', BUILDER, '--', 'bash', '-euc', script)


def download(url, filename, digest, headers=None):
    target = OUT / filename
    if not target.exists() or hashlib.file_digest(target.open('rb'), 'sha256').hexdigest() != digest:
        with urllib.request.urlopen(urllib.request.Request(url,headers=headers or {}), timeout=300) as response, target.open('wb') as output:
            while chunk := response.read(1024 * 1024):
                output.write(chunk)
    actual = hashlib.file_digest(target.open('rb'), 'sha256').hexdigest()
    if actual != digest:
        raise RuntimeError(f'checksum mismatch: {filename}: expected {digest}, got {actual}')
    TRANSPORTS.append({'url':url,'file':filename,'sha256':digest,'size':target.stat().st_size})
    return target


def checked_archive(url, filename, digest, prefix, installer=None):
    local = download(url, filename, digest)
    run('incus', 'file', 'push', str(local), BUILDER + '/root/' + filename)
    verify = f"printf '%s\\n' {Q(digest+'  /root/'+filename)} | sha256sum -c -; "
    if installer:
        guest(verify + installer)
    else:
        guest(verify + f'rm -rf {Q(prefix)}; mkdir -p {Q(prefix)}; tar -xf /root/{Q(filename)} -C {Q(prefix)} --strip-components=1')


def install_toolchain(key, prefix='', installer=None):
    pin = TOOLCHAINS[key]
    checked_archive(pin['url'], pin['file'], pin['sha256'], prefix, installer)


def install_otp(version):
    prefix = '/opt/otp-'+version
    source = '/opt/otp-src-'+version
    configure = f'./configure --prefix={Q(prefix)} --without-javac --without-wx --without-odbc'
    # Only OTP is compiled here. A version/executable is not a cache key:
    # include verified source bytes, build commands and the compiler/system
    # package inputs. Compile with a fixed environment, not inherited flags.
    inputs = json.dumps({
        'sourceSHA256': TOOLCHAINS['otp-'+version]['sha256'],
        'builderSHA256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'configure': configure,
        'environment': 'PATH=/usr/bin:/bin HOME=/root LANG=C LC_ALL=C CONFIG_SITE=/dev/null',
    }, sort_keys=True)
    compile = f'cd {Q(source)}; {configure}; make -j4; make install; test -x {Q(prefix+"/bin/erl")}'
    receipt = prefix+'/.karakoram-build-inputs'
    installer = f'''set -o pipefail
expected=$(mktemp)
trap 'rm -f "$expected"' EXIT
{{
    printf '%s\\n' {Q(inputs)}
    uname -m
    dpkg-query -W -f='${{binary:Package}}=${{Version}}\\n' | LC_ALL=C sort
}} > "$expected"
if ! test -x {Q(prefix+"/bin/erl")} || ! cmp -s "$expected" {Q(receipt)}; then
    rm -rf {Q(source)} {Q(prefix)}
    mkdir -p {Q(source)}
    tar -xf {Q("/root/"+TOOLCHAINS["otp-"+version]["file"])} -C {Q(source)} --strip-components=1
    env -i PATH=/usr/bin:/bin HOME=/root LANG=C LC_ALL=C CONFIG_SITE=/dev/null bash -euc {Q(compile)}
    install -m0644 "$expected" {Q(receipt)}
fi'''
    wait_for_host()
    install_toolchain('otp-'+version, installer=installer)


def toolchains(r):
    paths = []
    if 'node' in r:
        v = r['node']
        install_toolchain('node-'+v, '/opt/node-'+v)
        paths.append('/opt/node-'+v+'/bin')
        # Package-manager tarballs bundle their dependencies; unpack verified
        # bytes directly so bootstrap never asks a registry to resolve code.
        for key, binary, target in (
            [('npm-11.12.1', 'npm', 'npm-cli.js')] if r.get('name') == 'supabase-storage' else
            [('pnpm-11.13.1', 'pnpm', 'pnpm.mjs'), ('turbo-linux-64-2.9.14', 'turbo', 'turbo')] if r.get('name') == 'supabase-studio' else []
        ):
            prefix = '/opt/'+key
            install_toolchain(key, prefix)
            if binary != target:
                guest(f'ln -sf {Q(target)} {Q(prefix+"/bin/"+binary)}')
            paths.insert(0, prefix+'/bin')
    if 'go' in r:
        v = r['go']
        install_toolchain('go-'+v, '/opt/go-'+v)
        paths.append('/opt/go-'+v+'/bin')
    if 'rust' in r:
        v = r['rust']
        filename = TOOLCHAINS['rust-'+v]['file']
        install_toolchain('rust-'+v, installer=f'rm -rf /root/rust-{v}-x86_64-unknown-linux-gnu /opt/rust-{v}; tar -xf /root/{filename} -C /root; /root/rust-{v}-x86_64-unknown-linux-gnu/install.sh --prefix=/opt/rust-{v} --disable-ldconfig')
        paths.append('/opt/rust-'+v+'/bin')
    if r.get('name') == 'supabase-pg17-extensions':
        toolchains({'rust':'1.80.0'})
    if r.get('bun'):
        pin = TOOLCHAINS['bun-'+r['bun']]
        install_toolchain('bun-'+r['bun'], installer=f'rm -rf /opt/bun-linux-x64; unzip -oq /root/{pin["file"]} -d /opt; install -m0755 /opt/bun-linux-x64/bun /usr/local/bin/bun')
    if 'otp' in r:
        v = r['otp']
        install_otp(v)
        paths.insert(0, '/opt/otp-'+v+'/bin')
        ev = r['elixir']
        key = 'elixir-'+ev+'-otp-'+v.split('.')[0]
        pin = TOOLCHAINS[key]
        install_toolchain(key, installer=f'rm -rf /opt/elixir-{ev}; mkdir -p /opt/elixir-{ev}; unzip -oq /root/{pin["file"]} -d /opt/elixir-{ev}')
        paths.insert(0, '/opt/elixir-'+ev+'/bin')
        for key in ('hex-2.5.3', 'rebar3-3.24.0'):
            install_toolchain(key, installer='true')
    if r.get('name') == 'supabase-studio':
        for pin in STUDIO_ASSETS:
            local = download(pin['url'],pin['file'],pin['sha256'],headers={'User-Agent':'Deno/2.0.0','Accept':'application/typescript'})
            run('incus','file','push',str(local),BUILDER+'/root/'+pin['file'])
            guest("printf '%s\\n' "+Q(pin['sha256']+'  /root/'+pin['file'])+' | sha256sum -c -')
    return ':'.join(paths)+':/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'


def memory_guard(candidate, gib):
    prefix = os.environ.get('NATIVE_ARTIFACT_MEMORY_PREFIX', BUILDER.rsplit('-',1)[0])
    total = gib
    for instance in get_instances():
        if instance['name'] == candidate or instance['status'] != 'Running' or not instance['name'].startswith(prefix):
            continue
        limit = instance['config'].get('limits.memory','')
        if limit.endswith('GiB'): total += float(limit[:-3])
        elif limit.endswith('MiB'): total += float(limit[:-3])/1024
        else: raise RuntimeError('cannot budget instance memory: '+instance['name'])
    if total > 28:
        raise RuntimeError(f'configured guest memory would be {total:g} GiB, exceeding the 28 GiB budget')

def wait_for_host():
    while True:
        memory = subprocess.check_output(['free','-m'],text=True)
        available = int(next(line.split()[-1] for line in memory.splitlines() if line.startswith('Mem:')))
        if available >= 14 * 1024:
            return
        print('Waiting for at least 14 GiB host available memory before a heavy build step.',flush=True)
        time.sleep(30)



def ensure_builder():
    if shutil.disk_usage(ROOT).free < 120 * 1024**3:
        raise RuntimeError('less than 120 GiB disk available; refusing native build')
    wait_for_host()
    memory_guard(BUILDER, 6)
    instances = get_instances()
    if BUILDER not in {x['name'] for x in instances}:
        run('incus', 'init', 'images:ubuntu/24.04', BUILDER, '--network', NETWORK, '-c', 'limits.memory=6GiB', '-c', 'limits.cpu=8')
    else:
        instance = next(x for x in instances if x['name'] == BUILDER)
        if instance['config'].get('limits.memory') != '6GiB':
            raise RuntimeError('existing builder does not have the expected 6 GiB limit')
    instance = next(x for x in get_instances() if x['name'] == BUILDER)
    if instance['status'] != 'Running': run('incus', 'start', BUILDER)
    guest('export DEBIAN_FRONTEND=noninteractive; apt-get update; apt-get install -y ca-certificates curl git gnupg lsb-release xz-utils unzip build-essential python3 pkg-config cmake clang libclang-dev llvm-dev libssl-dev libncurses-dev zlib1g-dev libreadline-dev libxml2-dev libxslt1-dev libffi-dev libblas-dev liblapack-dev libopenblas-dev libvips-dev locales; locale-gen en_US.UTF-8')


def push_build_inputs():
    run('incus', 'file', 'push', str(HERE/'recipe.sh'), BUILDER+'/root/native-recipe.sh')
    for filename in ('extensions.py','extensions.json','worker.py'):
        run('incus','file','push',str(HERE/filename),BUILDER+'/root/'+filename)
    for patch in (HERE/'patches').glob('*.patch'):
        run('incus','file','push',str(patch),BUILDER+'/root/'+patch.name)


def get_instances():
    return json.loads(subprocess.check_output(['incus','list','--format','json']))


def build(name):
    r = RECIPES[name] | {'name':name}; started = time.monotonic()
    TRANSPORTS.clear()
    fingerprint = build_input_fingerprint(name)
    filename = name+'-'+r['version']+'-ubuntu24.04-amd64.tar.gz'
    candidate = OUT/(filename+'.candidate')
    if candidate.exists():
        metadata = './usr/share/postgresql/17/karakoram-native-artifacts/manifest.json' if name=='supabase-pg17-extensions' else './manifest.json'
        with tarfile.open(candidate) as archive:
            manifest = json.load(archive.extractfile(metadata))
        if any(manifest.get(key) != value for key,value in r.items()):
            raise RuntimeError('retained candidate provenance differs from recipe: '+name)
        if manifest.get('buildInputFingerprint') != fingerprint:
            raise RuntimeError('retained candidate build inputs differ from current inputs: '+name)
        return qualify(name, candidate, started)
    if not any(x['name']==BUILDER and x['status']=='Running' for x in get_instances()):
        ensure_builder()
    push_build_inputs()
    envpath = toolchains(r)
    source = '/build/'+name
    guest(f'mkdir -p /build; rm -rf {Q(source)}; git init {Q(source)}; cd {Q(source)}; git remote add origin https://github.com/{r["repo"]}.git; git fetch --depth=1 origin {r["commit"]}; git checkout --detach FETCH_HEAD; test "$(git rev-parse HEAD)" = {r["commit"]}; git submodule update --init --recursive')
    epoch = subprocess.check_output(['incus','exec',BUILDER,'--','git','-C',source,'show','-s','--format=%ct','HEAD'], text=True).strip()
    filename = name+'-'+r['version']+'-ubuntu24.04-amd64.tar.gz'
    manifest = r | {'name':name, 'build':'native-v1', 'platform':'ubuntu24.04-amd64', 'sourceDateEpoch':int(epoch), 'toolchainTransports':TRANSPORTS, 'buildInputFingerprint':fingerprint}
    local_manifest = OUT / (name+'.manifest.json'); local_manifest.write_text(json.dumps(manifest,indent=2)+'\n')
    run('incus','file','push',str(local_manifest),BUILDER+'/root/native-manifest.json')
    metadata = 'usr/share/postgresql/17/karakoram-native-artifacts' if name=='supabase-pg17-extensions' else '.'
    wait_for_host()
    guest(f'export PATH={Q(envpath)} LANG=en_US.UTF-8 NODE_OPTIONS=--max-old-space-size=4096 CARGO_BUILD_JOBS=4 MAKEFLAGS=-j4 ERL_FLAGS="+S 2:2 +SDcpu 1 +SDio 1"; bash /root/native-recipe.sh {Q(name)} {Q(source)} {Q(r.get("node",""))}; cp /root/native-manifest.json /stage/{name}/{metadata}/manifest.json; tar --sort=name --mtime=@{epoch} --owner=0 --group=0 --numeric-owner -C /stage/{name} -czf /root/{filename} .')
    candidate = OUT/(filename+'.candidate')
    run('incus','file','pull',BUILDER+'/root/'+filename,str(candidate))
    return qualify(name, candidate, started)


def qualify(name, candidate, started):
    r = RECIPES[name]
    filename = candidate.name.removesuffix('.candidate')
    builder = next((x for x in get_instances() if x['name']==BUILDER), None)
    if builder and builder['status']=='Running':
        run('incus','stop',BUILDER,'--timeout','120')
    try:
        memory_guard(RUNNER, 2)
        run('incus','init','images:ubuntu/24.04',RUNNER,'--network',NETWORK,'-c','limits.memory=2GiB','-c','limits.cpu=2')
        run('incus','start',RUNNER)
        run('incus','file','push',str(candidate),RUNNER+'/root/artifact.tar.gz')
        run('incus','file','push',str(HERE/'smoke.py'),RUNNER+'/root/smoke.py')
        if name == 'supabase-studio':
            meta = next(a for a in json.loads(LOCK.read_text()) if a['name']=='supabase-postgres-meta')
            archive = OUT/meta['file']
            with archive.open('rb') as metadata_bytes:
                if hashlib.file_digest(metadata_bytes,'sha256').hexdigest() != meta['sha256']:
                    raise RuntimeError('Studio postgres-meta fixture differs from lock')
            run('incus','file','push',str(archive),RUNNER+'/root/meta.tar.gz')
        smoke_env = {key:os.environ[key] for key in ('S3_ENDPOINT','S3_ACCESS_KEY','S3_SECRET_KEY','S3_REGION','S3_BUCKET','NATIVE_ARTIFACT_SMOKE_PREFIX') if key in os.environ}
        if name == 'supabase-pg17-extensions':
            extension_pins = json.loads((HERE/'extensions.json').read_text())
            smoke_env['extensionFlags'] = {extension:pin['flags'] for extension,pin in extension_pins.items()}
        with tempfile.NamedTemporaryFile(mode='w') as config:
            json.dump(smoke_env, config); config.flush()
            run('incus','file','push',config.name,RUNNER+'/root/smoke-env.json','--mode','0600')
        try:
            run('incus','exec',RUNNER,'--','bash','-euc','apt-get update; apt-get install -y python3 curl '+ ' '.join(map(Q,r['runtimeDeps']))+'; mkdir -p /opt/artifact; tar -xf /root/artifact.tar.gz -C /opt/artifact; python3 /root/smoke.py '+Q(name)+' '+Q(r['health']))
        finally:
            subprocess.run(['incus','file','pull',RUNNER+'/tmp/native-smoke.log',str(OUT/(name+'.smoke.log'))],check=False)
    finally:
        if RUNNER in {x['name'] for x in get_instances()}:
            run('incus','delete',RUNNER,'--force')
    candidate.replace(OUT/filename)
    artifact = {'name':name,'version':r['version'],'file':filename,'sha256':hashlib.file_digest((OUT/filename).open('rb'),'sha256').hexdigest(),'size':(OUT/filename).stat().st_size,'upstreamURL':'','license':r['license'],'source':{'repo':'https://github.com/'+r['repo'],'commit':r['commit']},'build':'native-v1','runtimeDeps':r['runtimeDeps']}
    if name=='supabase-pg17-extensions':
        with tarfile.open(OUT/filename) as archive:
            qualified = json.load(archive.extractfile('./usr/share/postgresql/17/karakoram-native-artifacts/extensions.json'))
        artifact['extensions'] = sorted(extension for extension, result in qualified.items() if result.get('build')=='passed')
        for extension in artifact['extensions']:
            if any(qualified[extension][key] != extension_pins[extension][key] for key in ('repo','commit','version','license')):
                raise RuntimeError('retained extension provenance differs from source pin: '+extension)
        artifact['extensionFlags'] = {extension:extension_pins[extension]['flags'] for extension in artifact['extensions']}
    entries = json.loads(LOCK.read_text()); entries = [a for a in entries if a['name'] != name] + [artifact]
    LOCK.write_text(json.dumps(entries,indent=2)+'\n')
    return artifact | {'seconds':round(time.monotonic()-started,2),'smoke':'passed','runtimeDeps':r['runtimeDeps']}


def main():
    entries = json.loads(LOCK.read_text())
    official = {a['name']:a for a in entries if a['upstreamURL']}
    requested = sys.argv[1:]
    if not requested or any(name not in ['all',*RECIPES,*official] for name in requested) or ('all' in requested and len(requested)!=1):
        raise SystemExit('usage: build.sh <all|artifact [artifact ...]>')
    OUT.mkdir(parents=True,exist_ok=True)
    for name in official if requested==['all'] else (name for name in requested if name in official):
        artifact = official[name]
        file = download(artifact['upstreamURL'],artifact['file'],artifact['sha256'])
        if file.stat().st_size != artifact['size']:
            raise SystemExit('official artifact size differs from lock: '+name)
        print(name+': official bytes verified',flush=True)
    if all(name in official for name in requested):
        (OUT/'SHA256SUMS').write_text(''.join(a['sha256']+'  '+a['file']+'\n' for a in entries if (OUT/a['file']).exists()))
        return
    report_file = OUT/'build-report.json'
    report = json.loads(report_file.read_text()) if report_file.exists() else {}
    order = ['supabase-storage','supabase-postgres-meta','supabase-realtime','supabase-studio','supabase-pg17-extensions','supabase-imgproxy','supabase-supavisor','supabase-logflare','supabase-edge-runtime']
    selected = order if requested==['all'] else [name for name in requested if name in RECIPES]
    try:
        for name in selected:
            log = OUT/(name+'.build.log')
            started = time.monotonic()
            with log.open('w') as output:
                saved_out, saved_err = os.dup(1),os.dup(2)
                try:
                    sys.stdout.flush(); sys.stderr.flush(); os.dup2(output.fileno(),1); os.dup2(output.fileno(),2)
                    report[name] = build(name)
                except Exception as error:
                    report[name] = {'failure':str(error),'seconds':round(time.monotonic()-started,2),'log':log.name}
                finally:
                    sys.stdout.flush(); sys.stderr.flush(); os.dup2(saved_out,1); os.dup2(saved_err,2); os.close(saved_out); os.close(saved_err)
            print(name+': '+('FAILED: '+report[name]['failure'] if 'failure' in report[name] else 'published'),flush=True)
            (OUT/'build-report.json').write_text(json.dumps(report,indent=2)+'\n')
    finally:
        if BUILDER in {x['name'] for x in get_instances()}: run('incus','delete',BUILDER,'--force')
        if RUNNER in {x['name'] for x in get_instances()}: run('incus','delete',RUNNER,'--force')
    entries = json.loads(LOCK.read_text())
    (OUT/'SHA256SUMS').write_text(''.join(a['sha256']+'  '+a['file']+'\n' for a in entries if (OUT/a['file']).exists()))
    if any('failure' in report[name] for name in selected): raise SystemExit(1)


if __name__ == '__main__': main()
