#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
"""Build pinned extensions against Percona PG17; retain per-extension failures."""
import json
import os
from pathlib import Path
import subprocess

STAGE = Path('/stage/supabase-pg17-extensions')
METADATA = STAGE/'usr/share/postgresql/17/karakoram-native-artifacts'
PG_CONFIG = '/usr/lib/postgresql/17/bin/pg_config'
PINS = json.loads(Path('/root/extensions.json').read_text())


def run(*args, **kwargs): return subprocess.run(args,check=True,**kwargs)


def main():
    run('bash','-euc',"curl -fsSL https://repo.percona.com/apt/percona-release_latest.noble_all.deb -o /tmp/percona-release.deb; printf '%s\\n' '10a419065480e4cd4ad6d1036e742832203e79ff537461115361ec2f7ff62e36  /tmp/percona-release.deb' | sha256sum -c -; dpkg -i /tmp/percona-release.deb; percona-release setup -y ppg-17; apt-get install -y postgresql-common; printf 'create_main_cluster = false\\n' > /etc/postgresql-common/createcluster.conf; printf '#!/bin/sh\\nexit 101\\n' > /usr/sbin/policy-rc.d; chmod 0755 /usr/sbin/policy-rc.d; apt-get install -y percona-postgresql-17=2:17.11-1.noble percona-postgresql-server-dev-17 libcurl4-openssl-dev libsodium-dev libkrb5-dev flex bison")
    METADATA.mkdir(parents=True,exist_ok=True)
    environment = {
        'pgConfig':PG_CONFIG,
        'postgresVersion':subprocess.check_output([PG_CONFIG,'--version'],text=True).strip(),
        'perconaPackages':subprocess.check_output(['dpkg-query','-W','-f=${Package}=${Version}\\n','percona-postgresql-17','percona-postgresql-server-dev-17'],text=True).splitlines(),
    }
    (METADATA/'build-environment.json').write_text(json.dumps(environment,indent=2)+'\n')
    report = {}
    for name,pin in PINS.items():
        source = Path('/build/extensions')/name
        source.mkdir(parents=True,exist_ok=True)
        output = Path('/tmp/extension-'+name)
        output.mkdir(exist_ok=True)
        try:
            run('git','init',str(source))
            run('git','-C',str(source),'fetch','--depth=1','https://github.com/'+pin['repo']+'.git',pin['commit'])
            run('git','-C',str(source),'checkout','--detach','FETCH_HEAD')
            if name in ('pg_graphql','pg_jsonschema'):
                rust,pgrx = ('1.81.0','0.12.9') if name=='pg_graphql' else ('1.80.0','0.12.6')
                env = os.environ | {'PATH':'/opt/rust-'+rust+'/bin:'+os.environ['PATH'],'PGRX_PG_CONFIG_PATH':PG_CONFIG,'PGRX_HOME':'/opt/pgrx-'+rust,'CARGO_BUILD_JOBS':'2'}
                # cargo install otherwise reuses a same-version executable even
                # after its Rust compiler inputs have changed.
                run('rm','-rf','/opt/pgrx-cli-'+rust,env['PGRX_HOME'])
                run('cargo','install','--locked','cargo-pgrx','--version',pgrx,'--root','/opt/pgrx-cli-'+rust,env=env)
                cargo = '/opt/pgrx-cli-'+rust+'/bin/cargo-pgrx'
                run(cargo,'pgrx','init','--pg17',PG_CONFIG,env=env)
                run(cargo,'pgrx','package','--pg-config',PG_CONFIG,'--out-dir',str(output),cwd=source,env=env)
            else:
                run('make','-j4','PG_CONFIG='+PG_CONFIG,cwd=source)
                run('make','install','PG_CONFIG='+PG_CONFIG,'DESTDIR='+str(output),cwd=source)
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
    if not any(entry.get('build')=='passed' for entry in report.values()):
        raise SystemExit('no extension built successfully')


if __name__=='__main__': main()
