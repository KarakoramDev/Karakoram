#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
"""Build one unmodified extension against its exact Percona server/header ABI."""
import json
import os
from pathlib import Path
import shutil
import subprocess


def run(*args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def metadata_path(recipe):
    major = recipe['pgMajor']
    if major not in (16, 17, 18):
        raise ValueError('unsupported PostgreSQL major')
    return f'usr/share/postgresql/{major}/karakoram-native-artifacts/{recipe["name"]}'


def packages(recipe):
    return [f'percona-postgresql-{recipe["pgMajor"]}={recipe["pgVersion"]}',
            f'percona-postgresql-server-dev-{recipe["pgMajor"]}={recipe["pgVersion"]}']


def build_commands(recipe, output):
    pg_config = f'/usr/lib/postgresql/{recipe["pgMajor"]}/bin/pg_config'
    if recipe['buildSystem'] == 'cmake':
        return [('./bootstrap', '-DAPACHE_ONLY=1', '-DREGRESS_CHECKS=OFF',
                 '-DSEND_TELEMETRY_DEFAULT=OFF', '-DWARNINGS_AS_ERRORS=OFF',
                 '-DCMAKE_BUILD_TYPE=Release', '-DPG_CONFIG='+pg_config),
                ('cmake', '--build', 'build', '-j4'),
                ('cmake', '--install', 'build')]
    if recipe['buildSystem'] == 'autoconf':
        # PG18 server headers define HAVE_LIBCURL; let configure link the
        # helpers that unmodified Citus consequently compiles on that major.
        curl = '--with-libcurl' if recipe['pgMajor'] == 18 else '--without-libcurl'
        return [('./configure', 'PG_CONFIG='+pg_config, curl),
                ('make', '-j4'), ('make', 'install', 'DESTDIR='+str(output))]
    raise ValueError('unsupported extension build system')


def main():
    recipe = json.loads(Path('/root/native-manifest.json').read_text())
    major = recipe['pgMajor']
    stage = Path('/stage')/recipe['name']
    metadata = stage/metadata_path(recipe)
    run('bash', '-euc', "curl -fsSL https://repo.percona.com/apt/percona-release_latest.noble_all.deb -o /tmp/percona-release.deb; printf '%s\\n' '10a419065480e4cd4ad6d1036e742832203e79ff537461115361ec2f7ff62e36  /tmp/percona-release.deb' | sha256sum -c -; dpkg -i /tmp/percona-release.deb; percona-release setup -y ppg-"+str(major)+"; apt-get install -y postgresql-common; printf 'create_main_cluster = false\\n' > /etc/postgresql-common/createcluster.conf; printf '#!/bin/sh\\nexit 101\\n' > /usr/sbin/policy-rc.d; chmod 0755 /usr/sbin/policy-rc.d")
    run('apt-get', 'install', '-y', *packages(recipe), 'flex', 'bison',
        'libssl-dev', 'libkrb5-dev', 'liblz4-dev', 'libzstd-dev')
    if recipe['buildSystem'] == 'autoconf':
        run('apt-get', 'install', '-y', 'autoconf', 'automake', 'libtool')
    if recipe['buildSystem'] == 'autoconf' and major == 18:
        # Percona PG18's postgres.h defines HAVE_LIBCURL itself, so the
        # unmodified Citus build requires matching curl headers and linkage.
        run('apt-get', 'install', '-y', 'libcurl4-openssl-dev')
    output = Path('/tmp')/(recipe['name']+'-install')
    shutil.rmtree(output, ignore_errors=True)
    output.mkdir()
    for command in build_commands(recipe, output):
        run(*command, env=os.environ | {'DESTDIR':str(output)})
    for root in (f'usr/lib/postgresql/{major}', f'usr/share/postgresql/{major}'):
        shutil.copytree(output/root, stage/root, dirs_exist_ok=True)
    if recipe['buildSystem'] == 'cmake' and any(stage.rglob('*tsl*.so')):
        raise RuntimeError('Apache-only artifact contains a TSL library')
    licenses = metadata/'LICENSES'
    licenses.mkdir(parents=True)
    source = Path.cwd()
    for file in source.iterdir():
        if file.is_file() and file.name.lower().startswith(('license', 'copying', 'notice')):
            shutil.copyfile(file, licenses/file.name)
    if recipe['buildSystem'] == 'autoconf':
        if 'GNU AFFERO GENERAL PUBLIC LICENSE' not in (licenses/'LICENSE').read_text():
            raise RuntimeError('Citus AGPL license text missing')
        (metadata/'SOURCE-OFFER.txt').write_text(
            'This artifact contains unmodified Citus '+recipe['version']+'.\n'
            'Corresponding source (including upstream build scripts):\n'
            'https://github.com/'+recipe['repo']+'/tree/'+recipe['commit']+'\n'
            'Download: https://github.com/'+recipe['repo']+'/archive/'+recipe['commit']+'.tar.gz\n'
            'Build configuration: see manifest.json and build-environment.json beside this file.\n'
            'Distributors must maintain equivalent source access alongside binary downloads.\n')
    environment = {
        'pgConfig':f'/usr/lib/postgresql/{major}/bin/pg_config',
        'postgresVersion':subprocess.check_output([f'/usr/lib/postgresql/{major}/bin/pg_config', '--version'], text=True).strip(),
        'perconaPackages':subprocess.check_output(['dpkg-query', '-W', '-f=${Package}=${Version}\n', f'percona-postgresql-{major}', f'percona-postgresql-server-dev-{major}'], text=True).splitlines(),
        'commands':build_commands(recipe, output),
    }
    (metadata/'build-environment.json').write_text(json.dumps(environment, indent=2)+'\n')
    report = {extension: {key:recipe[key] for key in ('repo', 'commit', 'version', 'license')} | {'build':'passed'} for extension in recipe['extensions']}
    (metadata/'extensions.json').write_text(json.dumps(report, indent=2)+'\n')


if __name__ == '__main__':
    main()
