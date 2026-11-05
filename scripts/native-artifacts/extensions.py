#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
"""Build pinned extensions against Percona PG17; retain per-extension failures."""
import json
import os
import re
import shutil
import tarfile
from pathlib import Path
import subprocess

STAGE = Path('/stage/supabase-pg17-extensions')
METADATA = STAGE/'usr/share/postgresql/17/karakoram-native-artifacts'
PG_CONFIG = '/usr/lib/postgresql/17/bin/pg_config'
PINS = json.loads(Path(__file__).with_name('extensions.json').read_text())


def pgrx_upgrade_sql(sql):
    """Refresh functions without dropping user dependencies, grants or triggers."""
    sql = re.sub(r'\bCREATE\s+(?!OR\s+REPLACE\b)FUNCTION\b',
                 'CREATE OR REPLACE FUNCTION', sql, flags=re.IGNORECASE)
    # Existing GraphQL event triggers retain their identity and enabled state.
    sql = re.sub(r'create event trigger graphql_watch_(?:ddl|drop)\s+'
                 r'on (?:ddl_command_end|sql_drop)\s+'
                 r'execute procedure graphql\.increment_schema_version\(\);',
                 '', sql, flags=re.IGNORECASE)
    return sql


def install_pgrx_upgrade(name, pin, output):
    sql_dir = output/'usr/share/postgresql/17/extension'
    current = sql_dir/(name+'--'+pin['version']+'.sql')
    upgrade = sql_dir/(name+'--'+pin['previousVersion']+'--'+pin['version']+'.sql')
    upgrade.write_text(pgrx_upgrade_sql(current.read_text()))


def install_previous_sql(name, output):
    sql_dir = output/'usr/share/postgresql/17/extension'
    with tarfile.open('/root/supabase-pg17-extensions-17.11.1-1-ubuntu24.04-amd64.tar.gz') as baseline:
        for member in baseline.getmembers():
            filename = Path(member.name).name
            if member.isfile() and filename.startswith(name+'--') and filename.endswith('.sql') and filename.count('--') == 1:
                destination = sql_dir/filename
                if not destination.exists():
                    destination.write_bytes(baseline.extractfile(member).read())


def build_sodium():
    source = Path('/build/libsodium-1.0.22')
    shutil.rmtree(source, ignore_errors=True)
    source.mkdir(parents=True)
    run('tar', '-xf', '/root/libsodium-1.0.22.tar.gz', '-C', str(source), '--strip-components=1')
    run('./configure', '--prefix=/opt/libsodium-1.0.22', '--disable-shared',
        '--enable-static', '--with-pic', cwd=source,
        env=os.environ | {'CFLAGS':'-O2 -fPIC -fvisibility=hidden'})
    run('make', '-j4', cwd=source)
    run('make', 'check', '-j2', cwd=source)
    run('make', 'install', cwd=source)
    licenses = METADATA/'LICENSES/libsodium'
    licenses.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source/'LICENSE', licenses/'LICENSE')


def run(*args, **kwargs): return subprocess.run(args,check=True,**kwargs)


def main():
    run('bash','-euc',"curl -fsSL https://repo.percona.com/apt/percona-release_latest.noble_all.deb -o /tmp/percona-release.deb; printf '%s\\n' '10a419065480e4cd4ad6d1036e742832203e79ff537461115361ec2f7ff62e36  /tmp/percona-release.deb' | sha256sum -c -; dpkg -i /tmp/percona-release.deb; percona-release setup -y ppg-17; apt-get install -y postgresql-common; printf 'create_main_cluster = false\\n' > /etc/postgresql-common/createcluster.conf; printf '#!/bin/sh\\nexit 101\\n' > /usr/sbin/policy-rc.d; chmod 0755 /usr/sbin/policy-rc.d; apt-get install -y percona-postgresql-17=2:17.11-1.noble percona-postgresql-server-dev-17=2:17.11-1.noble libcurl4-openssl-dev libsodium-dev libkrb5-dev flex bison")
    METADATA.mkdir(parents=True,exist_ok=True)
    environment = {
        'pgConfig':PG_CONFIG,
        'postgresVersion':subprocess.check_output([PG_CONFIG,'--version'],text=True).strip(),
        'perconaPackages':subprocess.check_output(['dpkg-query','-W','-f=${Package}=${Version}\\n','percona-postgresql-17','percona-postgresql-server-dev-17'],text=True).splitlines(),
    }
    (METADATA/'build-environment.json').write_text(json.dumps(environment,indent=2)+'\n')
    build_sodium()
    report = {}
    for name,pin in PINS.items():
        source = Path('/build/extensions')/name
        shutil.rmtree(source, ignore_errors=True)
        source.mkdir(parents=True,exist_ok=True)
        output = Path('/tmp/extension-'+name)
        shutil.rmtree(output, ignore_errors=True)
        output.mkdir(exist_ok=True)
        try:
            run('git','init',str(source))
            run('git','-C',str(source),'fetch','--depth=1','https://github.com/'+pin['repo']+'.git',pin['commit'])
            run('git','-C',str(source),'checkout','--detach','FETCH_HEAD')
            if name in ('pg_graphql','pg_jsonschema'):
                rust = '1.98.0'
                pgrx = '0.19.2' if name == 'pg_graphql' else '0.16.1'
                env = os.environ | {'PATH':'/opt/rust-'+rust+'/bin:'+os.environ['PATH'],'PGRX_PG_CONFIG_PATH':PG_CONFIG,'PGRX_HOME':'/opt/pgrx-'+rust,'CARGO_BUILD_JOBS':'2','SOURCE_DATE_EPOCH':'0','RUSTFLAGS':'--remap-path-prefix=/build=/usr/src'}
                # cargo install otherwise reuses a same-version executable even
                # after its Rust compiler inputs have changed.
                run('rm','-rf','/opt/pgrx-cli-'+rust,env['PGRX_HOME'])
                run('cargo','install','--locked','cargo-pgrx','--version',pgrx,'--root','/opt/pgrx-cli-'+rust,env=env)
                cargo = '/opt/pgrx-cli-'+rust+'/bin/cargo-pgrx'
                run(cargo,'pgrx','init','--pg17',PG_CONFIG,env=env)
                run(cargo,'pgrx','package','--pg-config',PG_CONFIG,'--no-default-features','--features','pg17','--out-dir',str(output),cwd=source,env=env)
                install_pgrx_upgrade(name, pin, output)
            else:
                flags = ['PG_CONFIG='+PG_CONFIG]
                if name == 'pgsodium':
                    flags += ['PG_CPPFLAGS=-O0 -I/opt/libsodium-1.0.22/include',
                              'SHLIB_LINK=/opt/libsodium-1.0.22/lib/libsodium.a -Wl,--exclude-libs,ALL']
                run('make','-j4',*flags,cwd=source)
                run('make','install',*flags,'DESTDIR='+str(output),cwd=source)
                if name == 'pgsodium':
                    library = output/'usr/lib/postgresql/17/lib/pgsodium.so'
                    symbols = subprocess.check_output(['nm', '-D', '--defined-only', str(library)], text=True)
                    if re.search(r'\bsodium_', symbols):
                        raise RuntimeError('pgsodium exports private libsodium symbols')
            if 'previousVersion' in pin:
                install_previous_sql(name, output)
            for root in ('usr/lib/postgresql/17','usr/share/postgresql/17'):
                if (output/root).exists():
                    (STAGE/root).mkdir(parents=True,exist_ok=True)
                    run('cp','-a',str(output/root)+'/.',str(STAGE/root)+'/')
            licenses = METADATA/'LICENSES'/name; licenses.mkdir(parents=True,exist_ok=True)
            for file in source.iterdir():
                if file.is_file() and file.name.lower().startswith(('license','copying')): run('cp',str(file),str(licenses))
            report[name] = pin | {'build':'passed'}
        except subprocess.CalledProcessError as error:
            report[name] = pin | {'failure':str(error)}
    METADATA.mkdir(parents=True,exist_ok=True)
    (METADATA/'extensions.json').write_text(json.dumps(report,indent=2)+'\n')
    if any(entry.get('build') != 'passed' for entry in report.values()):
        raise SystemExit('all pinned extensions must build successfully')


if __name__=='__main__': main()
