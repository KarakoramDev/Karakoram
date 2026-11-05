#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
"""Run the real packaged entry point and require an HTTP health response."""
import hashlib
import os
import json
import base64
import hmac
import http.server
import functools
import socket
import threading
from pathlib import Path
import secrets
import subprocess
import sys
import time
import urllib.request
import urllib.parse
import struct
import zlib

def jwt(role, secret):
    encode = lambda value: base64.urlsafe_b64encode(json.dumps(value,separators=(',',':')).encode()).rstrip(b'=')
    message = encode({'alg':'HS256','typ':'JWT'})+b'.'+encode({'role':role,'iss':'supabase','exp':int(time.time())+3600})
    return (message+b'.'+base64.urlsafe_b64encode(hmac.new(secret.encode(),message,hashlib.sha256).digest()).rstrip(b'=')).decode()

def realtime_admin_smoke(env):
    for role in ('anon', 'service_role'):
        request = urllib.request.Request(
            'http://127.0.0.1:4000/api/tenants/realtime-dev',
            headers={'Authorization': 'Bearer '+jwt(role, env['JWT_SECRET'])})
        try:
            urllib.request.urlopen(request, timeout=5).close()
        except urllib.error.HTTPError as error:
            with error:
                assert error.code == 403, 'Unexpected tenant credential rejection status'
        else:
            raise AssertionError('Tenant credential authenticated the administration API')
    print('NATIVE_REALTIME_ADMIN_ISOLATION_OK')
    # Exercise the patched application code without creating extra tenants.
    validation = """
    for id <- ["native-private", "native:other"] do
      changeset = Realtime.Api.Tenant.changeset(%Realtime.Api.Tenant{}, %{external_id: id})
      unless Keyword.has_key?(changeset.errors, :external_id),
        do: raise("Unsafe tenant identifier accepted")
    end
    IO.puts("NATIVE_REALTIME_TENANT_IDENTIFIER_REJECTED")
    tenant = Realtime.Tenants.get_tenant_by_external_id("realtime-dev")
    unless Realtime.Tenants.Migrations.run_migrations(tenant) in [:ok, :noop],
      do: raise("Tenant migrations failed")
    IO.puts("NATIVE_REALTIME_TENANT_MIGRATIONS_OK")
    """
    subprocess.run(['/opt/artifact/app/bin/realtime', 'rpc', validation],
                   env=env, check=True, timeout=40)

def realtime_traffic_smoke(pg, env):
    import websocket
    sql = "CREATE TABLE public.native_realtime(id integer PRIMARY KEY, body text); ALTER PUBLICATION supabase_realtime ADD TABLE public.native_realtime; GRANT SELECT ON public.native_realtime TO anon, authenticated, service_role;"
    psql = ['runuser', '-u', 'postgres', '--', pg+'psql', '-v', 'ON_ERROR_STOP=1', '-d', 'smoke']
    subprocess.run(psql, input=sql, text=True, check=True)
    token = jwt('service_role', env['JWT_SECRET'])
    ws = websocket.create_connection(
        'ws://127.0.0.1:4000/socket/websocket?vsn=1.0.0&apikey='+token,
        host='realtime-dev.supabase-realtime', timeout=60)
    topic = 'realtime:public:native_realtime'
    def send(event, ref, payload):
        ws.send(json.dumps({'topic': topic, 'event': event, 'ref': ref, 'payload': payload}))
    try:
        send('phx_join', '1', {'access_token': token, 'config': {
            'broadcast': {'ack': True, 'self': True},
            'postgres_changes': [{'event': 'INSERT', 'schema': 'public', 'table': 'native_realtime'}]}})
        subscribed = False
        deadline = time.monotonic()+90
        while time.monotonic() < deadline:
            message = json.loads(ws.recv())
            if message['event'] == 'phx_reply' and message.get('ref') == '1':
                assert message['payload']['status'] == 'ok', message
                assert message['payload']['response']['postgres_changes'], message
                print('NATIVE_REALTIME_WEBSOCKET_JOIN_OK', flush=True)
            if message['event'] == 'system':
                assert message['payload']['status'] == 'ok', message
                subscribed = True
                break
        assert subscribed, 'PostgreSQL subscription was not established'
        send('broadcast', '2', {'type': 'broadcast', 'event': 'native-broadcast', 'payload': {'probe': 'native-broadcast'}})
        subprocess.run(psql, input="INSERT INTO public.native_realtime VALUES(1,'native-insert')", text=True, check=True)
        broadcast = insert = False
        while time.monotonic() < deadline and not (broadcast and insert):
            message = json.loads(ws.recv())
            if message['event'] == 'broadcast':
                assert message['payload']['payload']['probe'] == 'native-broadcast', message
                broadcast = True
            if message['event'] == 'postgres_changes':
                record = message['payload']['data']['record']
                assert record == {'id': 1, 'body': 'native-insert'}, message
                insert = True
        assert broadcast and insert, 'Missing real broadcast or PostgreSQL INSERT'
        print('NATIVE_REALTIME_BROADCAST_POSTGRES_CHANGES_OK', flush=True)
    finally:
        ws.close()


def supavisor_sql_smoke(pg, env):
    # Preserve the transaction IPv4 proof and additionally exercise both SQL
    # listeners in inet6 mode. HTTP health alone cannot prove the Ranch patch.
    endpoints = [('127.0.0.1', env.get('PROXY_PORT_TRANSACTION', '6543'))]
    if env.get('ADDR_TYPE') == 'inet6':
        endpoints += [('::1', env.get('PROXY_PORT_TRANSACTION', '6543')),
                      ('::1', env.get('PROXY_PORT_SESSION', '5432'))]
    for host, port in endpoints:
        authority = f'[{host}]' if ':' in host else host
        proof = subprocess.check_output(
            [pg+'psql', '-At', f'postgres://native_pool.native-smoke@{authority}:{port}/smoke?sslmode=disable',
             '-c', 'SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()'],
            env=env | {'PGPASSWORD': env['NATIVE_POOL_PASSWORD']}, text=True, timeout=30)
        assert proof.strip() == 't', f'Pooler upstream session is not TLS on {authority}:{port}'
    print('NATIVE_POOLER_TENANT_TLS_OK')
    if env.get('ADDR_TYPE') == 'inet6':
        print('NATIVE_POOLER_IPV6_SQL_OK')


name, health = sys.argv[1:]
if name == 'mongodb-mcp-server':
    import mongodb_mcp
    import tempfile
    import shutil
    try:
        with tempfile.TemporaryDirectory() as temporary:
            mongod = mongodb_mcp.fixture(temporary)
            mongodb_mcp.qualify('/opt/artifact', mongod, '/tmp')
    finally:
        log = Path('/tmp/mongodb-mcp-server.smoke.log')
        if log.exists():
            shutil.copy2(log, '/tmp/native-smoke.log')
    raise SystemExit(0)
env = os.environ | {'NODE_ENV':'production','HOSTNAME':'127.0.0.1','ERL_FLAGS':'+S 2:2 +SDcpu 1 +SDio 1','LANG':'C.UTF-8'}
fixture = json.loads(Path('/root/smoke-env.json').read_text())
env['PATH'] = '/opt/artifact/bin:'+env.get('PATH','')
extension_recipe = fixture.get('pgExtension')
major = extension_recipe['pgMajor'] if extension_recipe else 17
if name not in ('supabase-imgproxy','supabase-edge-runtime'):
    url = 'https://repo.percona.com/apt/percona-release_latest.noble_all.deb'
    with urllib.request.urlopen(url, timeout=120) as response:
        package = response.read()
    if hashlib.sha256(package).hexdigest() != '10a419065480e4cd4ad6d1036e742832203e79ff537461115361ec2f7ff62e36':
        raise SystemExit('Percona bootstrap checksum changed')
    Path('/tmp/percona-release.deb').write_bytes(package)
    subprocess.run(['apt-get','install','-y','gnupg','lsb-release'],check=True)
    subprocess.run(['dpkg','-i','/tmp/percona-release.deb'],check=True)
    subprocess.run(['percona-release','setup','-y','ppg-'+str(major)],check=True)
    subprocess.run(['apt-get','install','-y','postgresql-common'],check=True)
    Path('/etc/postgresql-common/createcluster.conf').write_text('create_main_cluster = false\n')
    Path('/usr/sbin/policy-rc.d').write_text('#!/bin/sh\nexit 101\n'); Path('/usr/sbin/policy-rc.d').chmod(0o755)
    server_version = extension_recipe['pgVersion'] if extension_recipe else '2:17.11-1.noble'
    server_packages = [f'percona-postgresql-{major}={server_version}']
    if not extension_recipe:
        server_packages.append('percona-postgresql-contrib')
    if name.startswith('pgvector-pg'):
        # Exercise the real APT-to-bundle transition, retaining database objects.
        server_packages.append(f'percona-postgresql-{major}-pgvector=1:0.8.6-1.noble')
    if name == 'supabase-realtime':
        server_packages.append('percona-postgresql-17-wal2json=1:2.6-4.noble')
    subprocess.run(['apt-get','install','-y',*server_packages],check=True)
    if os.environ.get('NATIVE_ARTIFACT_ROOTLESS') == '1':
        # Package downloads need egress; the actual database probe does not.
        # Enter a private network before binding any fixture ports.
        import ctypes
        subprocess.run(['apt-get','install','-y','iproute2'],check=True)
        if ctypes.CDLL(None, use_errno=True).unshare(0x40000000) != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        subprocess.run(['ip','link','set','lo','up'],check=True)
    preload = ''
    if name == 'supabase-pg17-extensions':
        subprocess.run(['cp','-a','/opt/artifact/usr/.','/usr/'],check=True)
        libraries = [lib for lib,flags in fixture['extensionFlags'].items() if flags['preload'] and Path('/usr/lib/postgresql/17/lib/'+lib+'.so').exists()]
        if libraries:
            preload = ' -c shared_preload_libraries='+','.join(libraries)
    elif extension_recipe:
        if not name.startswith('pgvector-pg'):
            subprocess.run(['cp','-a','/opt/artifact/usr/.','/usr/'],check=True)
        preload = ' -c shared_preload_libraries='+','.join(extension for extension, flags in extension_recipe['extensions'].items() if flags['preload'])
        if name.startswith('citus-'):
            preload += ' -c max_worker_processes=16 -c max_wal_senders=16 -c max_replication_slots=16'
    if name == 'supabase-realtime':
        preload += ' -c output_plugin_libraries=pgoutput,test_decoding,wal2json'
    subprocess.run(['install','-d','-o','postgres','-g','postgres','/var/lib/postgresql/smoke'],check=True)
    pg = f'/usr/lib/postgresql/{major}/bin/'
    subprocess.run(['runuser','-u','postgres','--',pg+'initdb','-D','/var/lib/postgresql/smoke','--auth=trust'],check=True,stdout=subprocess.DEVNULL)
    if name in ('supabase-supavisor','supabase-logflare'):
        certificate = '/var/lib/postgresql/smoke/server.crt'
        key = '/var/lib/postgresql/smoke/server.key'
        ca = '/var/lib/postgresql/smoke/ca.crt'
        ca_key = '/var/lib/postgresql/smoke/ca.key'
        csr = '/var/lib/postgresql/smoke/server.csr'
        extensions_file = '/var/lib/postgresql/smoke/server.ext'
        Path(extensions_file).write_text('[leaf]\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectAltName=DNS:database.internal\n')
        commands = [
            ['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=native-smoke-ca','-addext','basicConstraints=critical,CA:TRUE','-addext','keyUsage=critical,keyCertSign,cRLSign','-keyout',ca_key,'-out',ca],
            ['openssl','req','-newkey','rsa:2048','-nodes','-subj','/CN=database.internal','-keyout',key,'-out',csr],
            ['openssl','x509','-req','-in',csr,'-CA',ca,'-CAkey',ca_key,'-CAcreateserial','-days','1','-extfile',extensions_file,'-extensions','leaf','-out',certificate],
        ]
        for command in commands:
            subprocess.run(command,check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        subprocess.run(['chown','postgres:postgres',certificate,key],check=True)
        Path(key).chmod(0o600)
        with Path('/var/lib/postgresql/smoke/postgresql.conf').open('a') as config:
            config.write("\nssl=on\nssl_cert_file='server.crt'\nssl_key_file='server.key'\n")
        Path('/var/lib/postgresql/smoke/pg_hba.conf').write_text('local all all trust\nhostssl all all 127.0.0.1/32 trust\nhostnossl all all 127.0.0.1/32 reject\n')
        env['KARAKORAM_DB_CA_FILE'] = ca
        if name == 'supabase-supavisor':
            wrong_ca = '/var/lib/postgresql/smoke/wrong-ca.crt'
            subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','1','-subj','/CN=untrusted-ca','-addext','basicConstraints=critical,CA:TRUE','-keyout','/var/lib/postgresql/smoke/wrong-ca.key','-out',wrong_ca],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            env['NATIVE_WRONG_CA_FILE'] = wrong_ca
    start = subprocess.run(['runuser','-u','postgres','--',pg+'pg_ctl','-D','/var/lib/postgresql/smoke','-o','-h 127.0.0.1 -p 5432 -c wal_level=logical -c max_replication_slots=10'+preload,'-l','/var/lib/postgresql/smoke.log','-w','start'])
    if start.returncode:
        failure = Path('/var/lib/postgresql/smoke.log').read_text()
        Path('/tmp/native-smoke.log').write_text(failure)
        print(failure,file=sys.stderr)
        raise SystemExit(start.returncode)
    subprocess.run(['runuser','-u','postgres','--',pg+'psql','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE smoke;'],check=True)
    env |= {'PG_META_DB_HOST':'127.0.0.1','PG_META_DB_PORT':'5432','PG_META_DB_NAME':'smoke','PG_META_DB_USER':'postgres','PG_META_PORT':'8080','DATABASE_URL':'postgres://postgres@127.0.0.1:5432/smoke','DATABASE_HOST':'127.0.0.1','DATABASE_PORT':'5432','DATABASE_NAME':'smoke','DATABASE_USERNAME':'postgres','DB_HOST':'127.0.0.1','DB_PORT':'5432','DB_NAME':'smoke','DB_USER':'postgres','DB_PASSWORD':'','SECRET_KEY_BASE':secrets.token_hex(64),'JWT_SECRET':secrets.token_hex(32),'API_JWT_SECRET':secrets.token_hex(32),'DB_ENC_KEY':secrets.token_hex(8),'ENCRYPTION_KEY':secrets.token_hex(16),'VAULT_ENC_KEY':secrets.token_hex(16)}
if extension_recipe:
    logs = []
    workers = []

    def sql(statement, port=5432):
        result = subprocess.run([pg+'psql','-h','127.0.0.1','-p',str(port),'-U','postgres','-d','smoke','-v','ON_ERROR_STOP=1','-At','-c',statement],capture_output=True,text=True,timeout=180)
        logs.append({'port':port,'sql':statement,'stdout':result.stdout,'stderr':result.stderr})
        result.check_returncode()
        return result.stdout.strip()

    try:
        if name.startswith('timescaledb-'):
            if list(Path(f'/usr/lib/postgresql/{major}').rglob('*tsl*.so')):
                raise RuntimeError('Apache-only artifact contains a TSL library')
            sql('CREATE EXTENSION timescaledb;')
            if sql('SHOW timescaledb.license;') != 'apache':
                raise RuntimeError('TimescaleDB license is not apache')
            sql("CREATE TABLE metrics(ts timestamptz NOT NULL, value integer); SELECT create_hypertable('metrics','ts'); INSERT INTO metrics VALUES ('2026-01-01',42);")
            if sql('SELECT sum(value) FROM metrics;') != '42':
                raise RuntimeError('hypertable query returned wrong result')
        elif name.startswith('pgvector-pg'):
            sql("CREATE EXTENSION vector VERSION '0.8.6';")
            for method in ('ivfflat', 'hnsw'):
                options = ' WITH (lists=1)' if method == 'ivfflat' else ''
                sql(f"CREATE TABLE vectors_{method}(id integer PRIMARY KEY, embedding vector(3)); INSERT INTO vectors_{method} SELECT i, ARRAY[i::real,0,0]::vector FROM generate_series(1,100) i;")
                sql(f'CREATE INDEX vectors_{method}_idx ON vectors_{method} USING {method} (embedding vector_l2_ops){options};')
            # Use exactly the publisher embedded by the agent, twice to prove
            # idempotence. The old server remains live during atomic publication.
            subprocess.run(['ln', '-s', '/opt/artifact', '/root/unpacked'], check=True)
            for _ in range(2):
                subprocess.run(['python3', '/root/pgvector_publish.py', '/'], cwd='/root', check=True)
            subprocess.run(['runuser', '-u', 'postgres', '--', pg+'pg_ctl', '-D', '/var/lib/postgresql/smoke', '-m', 'fast', '-w', 'restart'], check=True)
            sql('ALTER EXTENSION vector UPDATE;')
            if sql("SELECT extversion FROM pg_catalog.pg_extension WHERE extname='vector';") != '0.8.7':
                raise RuntimeError('vector SQL upgrade did not reach 0.8.7')
            if sql("SELECT count(*) FROM pg_index WHERE indexrelid IN ('vectors_ivfflat_idx'::regclass,'vectors_hnsw_idx'::regclass) AND indisvalid;") != '2':
                raise RuntimeError('pre-upgrade vector indexes became invalid')
            for method in ('ivfflat', 'hnsw'):
                index = 'vectors_'+method+'_idx'
                for sample in (1, 23, 51, 97):
                    query = f"SELECT id FROM vectors_{method} ORDER BY embedding <-> '[{sample}.1,0,0]' LIMIT 5;"
                    exact = sql('SET enable_indexscan=off; SET enable_bitmapscan=off; '+query).splitlines()[-5:]
                    plan = sql('SET enable_seqscan=off; EXPLAIN '+query)
                    if index not in plan:
                        raise RuntimeError('vector query did not use '+index)
                    indexed = sql('SET enable_seqscan=off; SET hnsw.ef_search=100; '+query).splitlines()[-5:]
                    if indexed != exact:
                        raise RuntimeError('vector indexed query differs from exact search')
            sql('REINDEX INDEX vectors_ivfflat_idx;')
            sql('DROP EXTENSION vector CASCADE; CREATE EXTENSION vector;')
            if sql("SELECT extversion FROM pg_catalog.pg_extension WHERE extname='vector';") != '0.8.7':
                raise RuntimeError('fresh vector creation did not use 0.8.7')
        else:
            sql('CREATE EXTENSION citus;')
            sql("SELECT citus_set_coordinator_host('127.0.0.1',5432);")
            for port in (5433, 5434):
                data = f'/var/lib/postgresql/worker-{port}'
                subprocess.run(['install','-d','-o','postgres','-g','postgres',data],check=True)
                subprocess.run(['runuser','-u','postgres','--',pg+'initdb','-D',data,'--auth=trust'],check=True,stdout=subprocess.DEVNULL)
                subprocess.run(['runuser','-u','postgres','--',pg+'pg_ctl','-D',data,'-o',f'-h 127.0.0.1 -p {port} -c wal_level=logical'+preload,'-l',data+'.log','-w','start'],check=True)
                workers.append(data)
                subprocess.run([pg+'createdb','-h','127.0.0.1','-p',str(port),'-U','postgres','smoke'],check=True)
                sql('CREATE EXTENSION citus;',port)
            sql("SELECT citus_add_node('127.0.0.1',5433); SET citus.shard_count=8; CREATE TABLE distributed_probe(id integer PRIMARY KEY, value integer); SELECT create_distributed_table('distributed_probe','id'); INSERT INTO distributed_probe SELECT i,i FROM generate_series(1,100) i;")
            sql("SELECT citus_add_node('127.0.0.1',5434);")
            sql("SELECT citus_rebalance_start(shard_transfer_mode := 'block_writes');")
            sql('SELECT citus_rebalance_wait();')
            if sql('SELECT sum(value) FROM distributed_probe;') != '5050':
                raise RuntimeError('distributed query returned wrong result')
            if sql("SELECT count(DISTINCT nodename || ':' || nodeport) FROM citus_shards WHERE table_name='distributed_probe'::regclass;") != '2':
                raise RuntimeError('rebalance did not place shards on both workers')
            sql("SELECT * FROM citus_create_restore_point('native_qualification');")
    finally:
        Path('/tmp/native-smoke.log').write_text(json.dumps(logs,indent=2)+'\n')
        for data in reversed(workers+['/var/lib/postgresql/smoke']):
            subprocess.run(['runuser','-u','postgres','--',pg+'pg_ctl','-D',data,'-m','immediate','-w','stop'],check=False)
    raise SystemExit(0)
if name == 'supabase-pg17-extensions':
    extensions = json.loads(Path('/opt/artifact/usr/share/postgresql/17/karakoram-native-artifacts/extensions.json').read_text())
    results = {}

    def extension_sql(database, statement):
        result = subprocess.run(['runuser','-u','postgres','--',pg+'psql','-v','ON_ERROR_STOP=1','-At','-d',database,'-c',statement],capture_output=True,text=True)
        results.setdefault(database, []).append({'sql':statement,'passed':result.returncode == 0,'stdout':result.stdout,'stderr':result.stderr})
        result.check_returncode()
        return result.stdout.strip()

    try:
        extension_sql('postgres', 'CREATE DATABASE smoke_fresh;')
        extension_sql('postgres', 'CREATE ROLE native_extension_probe;')
        for database in ('smoke_fresh', 'smoke'):
            for extension, entry in extensions.items():
                if entry.get('build') != 'passed':
                    raise RuntimeError('required extension did not build: '+extension)
                version = entry.get('previousVersion') if database == 'smoke' else None
                selection = " VERSION '"+version+"'" if version else ''
                extension_sql(database, 'CREATE EXTENSION "'+extension+'"'+selection+' CASCADE;')
                if version:
                    if extension == 'pg_graphql':
                        extension_sql(database, """CREATE VIEW public.graphql_upgrade_dependency AS SELECT graphql.resolve('{ __typename }') AS result;
REVOKE EXECUTE ON FUNCTION graphql.resolve(text,jsonb,text,jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION graphql.resolve(text,jsonb,text,jsonb) TO native_extension_probe;
ALTER EVENT TRIGGER graphql_watch_drop DISABLE;""")
                    elif extension == 'pg_jsonschema':
                        extension_sql(database, """CREATE VIEW public.jsonschema_upgrade_dependency AS SELECT public.json_matches_schema('{"type":"object"}', '{}') AS result;
REVOKE EXECUTE ON FUNCTION public.json_matches_schema(json,json) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.json_matches_schema(json,json) TO native_extension_probe;""")
                    # Separate commands: enum additions in upstream upgrades
                    # must commit before a subsequent upgrade uses new labels.
                    extension_sql(database, 'ALTER EXTENSION "'+extension+'" UPDATE;')
                    if extension == 'pg_graphql':
                        preserved = extension_sql(database, """SELECT has_function_privilege('native_extension_probe', 'graphql.resolve(text,jsonb,text,jsonb)', 'EXECUTE');
SELECT evtenabled = 'D' FROM pg_event_trigger WHERE evtname='graphql_watch_drop';
SELECT result->'errors' IS NULL FROM public.graphql_upgrade_dependency;""")
                        if preserved.splitlines() != ['t', 't', 't']:
                            raise RuntimeError('GraphQL upgrade lost grants, triggers or dependencies')
                    elif extension == 'pg_jsonschema':
                        preserved = extension_sql(database, """SELECT has_function_privilege('native_extension_probe', 'public.json_matches_schema(json,json)', 'EXECUTE');
SELECT result FROM public.jsonschema_upgrade_dependency;""")
                        if preserved.splitlines() != ['t', 't']:
                            raise RuntimeError('JSON Schema upgrade lost grants or dependencies')
                actual = extension_sql(database, "SELECT extversion FROM pg_extension WHERE extname='"+extension+"';")
                if actual != entry.get('sqlVersion', entry['version']):
                    raise RuntimeError('extension default mismatch: '+extension)
                print(('UPDATE_EXTENSION_OK ' if version else 'CREATE_EXTENSION_OK ')+extension)
            proof = extension_sql(database, """
WITH key AS (SELECT pgsodium.crypto_ipcrypt_keygen() AS k)
SELECT pgsodium.crypto_ipcrypt_decrypt(
 pgsodium.crypto_ipcrypt_encrypt('192.0.2.42'::inet, k), k) = '192.0.2.42'::inet FROM key;
SELECT encode(pgsodium.crypto_hash_sha256('native-smoke'::bytea), 'hex')
 = encode(sha256('native-smoke'::bytea), 'hex');
SELECT position('pgsodium' IN current_setting('shared_preload_libraries')) = 0;
""")
            if proof.splitlines() != ['t', 't', 't']:
                raise RuntimeError('crypto-only sodium proof failed')
            print('NATIVE_PGSODIUM_CRYPTO_OK '+database)
    finally:
        Path('/tmp/native-smoke.log').write_text(json.dumps(results,indent=2)+'\n')
    raise SystemExit(0)
if name in ('supabase-storage','supabase-realtime'):
    sql = ''
    if name == 'supabase-realtime':
        sql = fixture['realtime']['bootstrap']
    else:
        sql = 'CREATE ROLE supabase_admin LOGIN SUPERUSER;'
        for filename in ('00000000000000-initial-schema.sql','00000000000001-auth-schema.sql'):
            url = 'https://raw.githubusercontent.com/supabase/postgres/d156ba65c14694c12cc5e782bc15b9b8ed2d1376/migrations/db/init-scripts/'+filename
            with urllib.request.urlopen(url,timeout=30) as response:
                sql += response.read().decode()
    if name == 'supabase-storage':
        sql += 'CREATE ROLE supabase_storage_admin LOGIN NOINHERIT; GRANT anon, authenticated, service_role TO supabase_storage_admin; CREATE SCHEMA storage AUTHORIZATION supabase_storage_admin; GRANT USAGE ON SCHEMA auth TO supabase_storage_admin; GRANT SELECT ON auth.users TO supabase_storage_admin;'
        # Match the product storage bootstrap's schema/default-privilege block.
        sql += '''
REVOKE ALL ON SCHEMA storage FROM PUBLIC;
GRANT USAGE ON SCHEMA storage TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_storage_admin IN SCHEMA storage
  GRANT SELECT, INSERT, UPDATE, DELETE, REFERENCES ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_storage_admin IN SCHEMA storage GRANT USAGE, SELECT ON SEQUENCES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_storage_admin IN SCHEMA storage REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_storage_admin IN SCHEMA storage GRANT EXECUTE ON FUNCTIONS TO anon, authenticated, service_role;
'''
    else:
        sql += 'ALTER ROLE supabase_admin SUPERUSER;'
    subprocess.run(['runuser','-u','postgres','--',pg+'psql','-v','ON_ERROR_STOP=1','-d','smoke'],input=sql,text=True,check=True,stdout=subprocess.DEVNULL)
if name == 'supabase-storage':
    secret = env['JWT_SECRET']
    env |= {'PORT':'5000','STORAGE_BACKEND':'s3','TENANT_ID':fixture['NATIVE_ARTIFACT_SMOKE_PREFIX'].strip('/'),'REGION':fixture.get('S3_REGION','us-east-1'),'GLOBAL_S3_BUCKET':fixture['S3_BUCKET'],'GLOBAL_S3_ENDPOINT':fixture['S3_ENDPOINT'],'GLOBAL_S3_FORCE_PATH_STYLE':'true','AWS_ACCESS_KEY_ID':fixture['S3_ACCESS_KEY'],'AWS_SECRET_ACCESS_KEY':fixture['S3_SECRET_KEY'],'AWS_DEFAULT_REGION':fixture.get('S3_REGION','us-east-1'),'AUTH_JWT_SECRET':secret,'ANON_KEY':jwt('anon',secret),'SERVICE_KEY':jwt('service_role',secret),'PGRST_JWT_SECRET':secret}
    env['DATABASE_URL']='postgres://supabase_storage_admin@127.0.0.1:5432/smoke'
    env |= fixture['storageEnv']
    env |= {'S3_PROTOCOL_ACCESS_KEY_ID':secrets.token_hex(16),'S3_PROTOCOL_ACCESS_KEY_SECRET':secrets.token_hex(32)}
if name == 'supabase-edge-runtime':
    # Runtime /tmp is worker-private, not the runner's host filesystem.
    function = Path('/var/lib/native-functions/health')
    function.mkdir(parents=True)
    (function.parent/'deno.jsonc').write_text('{"imports":{}}\n')
    (function/'index.ts').write_text('Deno.serve(() => new Response(\"native runtime healthy\"));\n')
    imported = function.parent/'imports'
    imported.mkdir()
    (imported/'index.ts').write_text('import isNumber from "npm:is-number@7.0.0";\nDeno.serve(() => new Response(isNumber(17) ? "native npm import healthy" : "import failed"));\n')
    env |= {'FUNCTIONS_ROOT':str(function.parent),'VERIFY_JWT':'false'}
if name == 'supabase-realtime':
    subprocess.run(['apt-get', 'install', '-y', 'python3-websocket'], check=True)
    env.update(fixture['realtime']['environment'])
    env['JWT_SECRET'] = fixture['realtime']['tenantJWTSecret']
    env['KARAKORAM_REALTIME_TENANT_JWT_SECRET'] = env['JWT_SECRET']
    Path(env['HOME']).mkdir(parents=True, exist_ok=True)
    version = json.loads(Path('/opt/artifact/manifest.json').read_text())['version']
    native_root = Path('/opt/karakoram')/(name+'-'+version)
    native_root.parent.mkdir(parents=True,exist_ok=True)
    native_root.symlink_to('/opt/artifact')
    lint = Path('/tmp/pgdelta-lint')
    lint.mkdir()
    (lint/'schema.sql').write_text('CREATE TABLE public.native_probe(id integer PRIMARY KEY);\n')
    subprocess.run(['/opt/artifact/bin/pgdelta','schema','lint','--dir',str(lint)],env=env,check=True)
if name == 'supabase-supavisor':
    env |= {'PORT':'4000','PROXY_PORT_SESSION':'5433','PROXY_PORT_TRANSACTION':'6543','REGION':'local','NODE_IP':'127.0.0.1','CLUSTER_POSTGRES':'true','VAULT_ENC_KEY':secrets.token_urlsafe(24)}
    env.setdefault('ADDR_TYPE', 'inet6')
    env['NATIVE_POOL_PASSWORD'] = secrets.token_hex(16)
    subprocess.run(['runuser','-u','postgres','--',pg+'psql','-v','ON_ERROR_STOP=1','-d','smoke'],input="CREATE ROLE native_pool LOGIN PASSWORD '"+env['NATIVE_POOL_PASSWORD']+"';",text=True,check=True,stdout=subprocess.DEVNULL)
if name == 'supabase-logflare':
    subprocess.run(['runuser','-u','postgres','--',pg+'psql','-v','ON_ERROR_STOP=1','-d','smoke','-c','CREATE SCHEMA _analytics;'],check=True,stdout=subprocess.DEVNULL)
    env |= {'PHX_SERVER':'true','PHX_HTTP_PORT':'4000','PHX_HTTP_IP':'127.0.0.1','PHX_SECRET_KEY_BASE':env['SECRET_KEY_BASE'],'DB_DATABASE':'smoke','DB_HOSTNAME':'127.0.0.1','DB_USERNAME':'postgres','DB_SCHEMA':'public','DB_SSL':'false','LOGFLARE_SINGLE_TENANT':'true','LOGFLARE_SUPABASE_MODE':'true','POSTGRES_BACKEND_URL':'postgres://postgres@127.0.0.1:5432/smoke','POSTGRES_BACKEND_SCHEMA':'_analytics','LOGFLARE_PUBLIC_ACCESS_TOKEN':secrets.token_hex(24),'LOGFLARE_PRIVATE_ACCESS_TOKEN':secrets.token_hex(24)}
image_source = None
if name == 'supabase-imgproxy':
    env |= json.loads(Path('/root/smoke-env.json').read_text())['imgproxyEnv']
    source_root = Path('/tmp/imgproxy-source')
    source_root.mkdir()
    def chunk(kind, data):
        return struct.pack('>I',len(data))+kind+data+struct.pack('>I',zlib.crc32(kind+data))
    png = b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('>IIBBBBB',2,2,8,2,0,0,0))+chunk(b'IDAT',zlib.compress(b'\x00'+b'\xff\x00\x00'*2+b'\x00'+b'\x00\xff\x00'*2))+chunk(b'IEND',b'')
    (source_root/'probe.png').write_bytes(png)
    (source_root/'probe.svg').write_text('<svg xmlns="http://www.w3.org/2000/svg" width="2" height="2"><a href="javascript:alert(1)"><rect width="2" height="2" fill="red"/></a></svg>')
    # Storage's S3 backend supplies an HTTP(S) source URL, not a shared local file.
    # Exercise a private address without weakening the loopback/link-local guards.
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as route:
        route.connect(('192.0.2.1', 9))
        image_source_host = route.getsockname()[0]
    image_source = http.server.ThreadingHTTPServer(('0.0.0.0', 0), functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(source_root)))
    threading.Thread(target=image_source.serve_forever, daemon=True).start()
meta_process = None
image_process = None
if name == 'supabase-studio':
    env |= {'PORT':'3000','NEXT_TELEMETRY_DISABLED':'1','STUDIO_PG_META_URL':'http://127.0.0.1:8080','POSTGRES_HOST':'127.0.0.1','POSTGRES_PORT':'5432','POSTGRES_DB':'smoke','POSTGRES_PASSWORD':'','POSTGRES_USER_READ_WRITE':'postgres','SUPABASE_URL':'http://127.0.0.1:8000','SUPABASE_PUBLIC_URL':'http://127.0.0.1:8000','AUTH_JWT_SECRET':secrets.token_hex(32),'PG_META_CRYPTO_KEY':secrets.token_hex(32)}
    env['CRYPTO_KEY'] = env['PG_META_CRYPTO_KEY']
log = open('/tmp/native-smoke.log','w')
if name == 'supabase-storage':
    subprocess.run(['apt-get','install','-y','libvips42t64','fonts-dejavu-core'],check=True)
    Path('/opt/imgproxy').mkdir()
    subprocess.run(['tar','-xf','/root/imgproxy.tar.gz','-C','/opt/imgproxy'],check=True)
    image_env = os.environ | {'LANG':'C.UTF-8'} | fixture['imgproxyEnv']
    image_process = subprocess.Popen(['/opt/imgproxy/bin/start'],env=image_env,stdout=log,stderr=log)
    print('NATIVE_STORAGE_IMGPROXY_FIXTURE_SHA256 '+fixture['imgproxySHA256'],flush=True)
if name == 'supabase-studio':
    parser = 'require("./node_modules/.pnpm/libpg-query@17.6.0/node_modules/libpg-query").parse("SELECT 1").then(tree => { if (!tree.stmts?.[0]?.stmt?.SelectStmt) throw new Error("SQL parser result differs"); console.log("NATIVE_STUDIO_SQL_PARSER_OK") })'
    subprocess.run(['/opt/artifact/node/bin/node','-e',parser],cwd='/opt/artifact/app',env=env,stdout=log,stderr=log,check=True,timeout=30)
    subprocess.run(['mkdir','-p','/opt/meta'],check=True)
    subprocess.run(['tar','-xf','/root/meta.tar.gz','-C','/opt/meta'],check=True)
    meta_process = subprocess.Popen(['/opt/meta/bin/start'],env=env,stdout=log,stderr=log)
for command in {
    'supabase-realtime':[['/opt/artifact/app/bin/migrate'],['/opt/artifact/app/bin/realtime','eval',fixture.get('realtime', {}).get('seed', '')]],
    'supabase-supavisor':[['/opt/artifact/app/bin/migrate']],
    'supabase-logflare':[['/opt/artifact/app/bin/logflare','eval','Logflare.Release.migrate']],
}.get(name,[]):
    subprocess.run(command,env=env,stdout=log,stderr=log,check=True,cwd='/opt/artifact/app')
if name == 'supabase-supavisor':
    tenant = """
    {:ok, _} = Application.ensure_all_started(:supavisor)
    {:ok, ca} = Supavisor.Helpers.cert_to_bin(File.read!(System.fetch_env!("KARAKORAM_DB_CA_FILE")))
    params = %{
      "external_id" => "native-smoke", "db_host" => "127.0.0.1",
      "db_port" => 5432, "db_database" => "smoke", "require_user" => true,
      "upstream_ssl" => true, "upstream_verify" => "peer",
      "upstream_tls_ca" => ca,
      "default_parameter_status" => %{"server_version" => "17.11"},
      "users" => [%{"db_user" => "native_pool", "db_password" => System.fetch_env!("NATIVE_POOL_PASSWORD"),
                   "mode_type" => "transaction", "pool_size" => 2, "is_manager" => true}]
    }
    {:ok, _} = Supavisor.Tenants.create_tenant(params)
    """
    subprocess.run(['/opt/artifact/app/bin/supavisor','eval',tenant],env=env,stdout=log,stderr=log,check=True,cwd='/opt/artifact/app')
process = subprocess.Popen(['/opt/artifact/bin/start'],env=env,stdout=log,stderr=log)
try:
    deadline = time.monotonic()+120
    last_failure = ''
    while time.monotonic()<deadline:
        if process.poll() is not None:
            raise RuntimeError(f'native process exited with status {process.returncode}; see /tmp/native-smoke.log')
        if meta_process is not None and meta_process.poll() is not None:
            raise RuntimeError(f'postgres-meta fixture exited with status {meta_process.returncode}; see /tmp/native-smoke.log')
        healthy = False
        try:
            if meta_process is not None:
                # Studio can become ready before its independent meta fixture.
                # Keep both processes alive and retry readiness within this deadline.
                with urllib.request.urlopen('http://127.0.0.1:8080/health',timeout=2) as meta:
                    if meta.status != 200:
                        raise OSError('postgres-meta fixture health did not return HTTP 200')
            headers = {'Authorization':'Bearer '+jwt('service_role',env['API_JWT_SECRET'])} if name=='supabase-realtime' else {}
            request = urllib.request.Request(health,headers=headers)
            with urllib.request.urlopen(request,timeout=2) as response:
                if response.status == (204 if name == 'supabase-supavisor' else 200):
                    if name == 'supabase-realtime' and not json.load(response).get('data', {}).get('healthy'):
                        raise OSError('Realtime tenant health payload is unhealthy')
                    healthy = True
                    print('NATIVE_SMOKE_OK '+name)
                    if name == 'supabase-realtime':
                        realtime_admin_smoke(env)
                        realtime_traffic_smoke(pg, env)
                    if name == 'supabase-edge-runtime':
                        assert response.read() == b'native runtime healthy', 'Hello function response differs'
                        with urllib.request.urlopen('http://127.0.0.1:9000/imports',timeout=60) as imported:
                            assert imported.status == 200
                            assert imported.read() == b'native npm import healthy'
                        print('NATIVE_EDGE_NPM_IMPORT_OK')
                        process.terminate()
                        process.wait(timeout=20)
                        env |= {'VERIFY_JWT':'true','JWT_SECRET':secrets.token_hex(32)}
                        process = subprocess.Popen(['/opt/artifact/bin/start'],env=env,stdout=log,stderr=log)
                        for _ in range(120):
                            try:
                                urllib.request.urlopen(health,timeout=2)
                            except urllib.error.HTTPError as denied:
                                assert denied.code == 401, 'JWT dispatcher did not reject missing bearer'
                                break
                            except OSError:
                                time.sleep(1)
                        else:
                            raise RuntimeError('JWT-enabled dispatcher did not become ready')
                        for route, body in (('health',b'native runtime healthy'),('imports',b'native npm import healthy')):
                            request = urllib.request.Request('http://127.0.0.1:9000/'+route,headers={'Authorization':'Bearer '+jwt('anon',env['JWT_SECRET'])})
                            with urllib.request.urlopen(request,timeout=60) as invoked:
                                assert invoked.status == 200 and invoked.read() == body
                        print('NATIVE_EDGE_JWT_ON_OFF_DISPATCHER_OK',flush=True)
                    if name == 'supabase-storage':
                        headers = {'Authorization':'Bearer '+env['SERVICE_KEY'],'Content-Type':'application/json'}
                        request = urllib.request.Request('http://127.0.0.1:5000/bucket',data=b'{"id":"native-smoke","name":"native-smoke","public":false}',headers=headers,method='POST')
                        with urllib.request.urlopen(request,timeout=20) as created:
                            assert created.status == 200
                        headers['Content-Type'] = 'text/plain'
                        request = urllib.request.Request('http://127.0.0.1:5000/object/native-smoke/probe.txt',data=b'native S3 storage healthy',headers=headers,method='POST')
                        with urllib.request.urlopen(request,timeout=20) as uploaded:
                            assert uploaded.status == 200
                        request = urllib.request.Request('http://127.0.0.1:5000/object/native-smoke/probe.txt',headers=headers)
                        with urllib.request.urlopen(request,timeout=20) as downloaded:
                            assert downloaded.read() == b'native S3 storage healthy'
                        import storage_smoke
                        storage_smoke.qualify(env)
                    if name in ('supabase-supavisor','supabase-logflare'):
                        proof = subprocess.check_output(['runuser','-u','postgres','--',pg+'psql','-At','-d','smoke','-c',"SELECT count(*) FROM pg_stat_ssl s JOIN pg_stat_activity a USING(pid) WHERE a.datname='smoke' AND s.ssl"],text=True)
                        assert int(proof.strip()) > 0, 'No live TLS database connection'
                        print('NATIVE_TLS_HANDSHAKE_OK '+name)
                    if name == 'supabase-supavisor':
                        supavisor_sql_smoke(pg, env)
                        rejected_ca = """
                        config = Supavisor.Repo.config()
                        :verify_peer = config[:ssl_opts][:verify]
                        opts = Keyword.put(config[:ssl_opts], :cacertfile, System.fetch_env!("NATIVE_WRONG_CA_FILE"))
                        {:ok, socket} = :gen_tcp.connect(~c"127.0.0.1", 5432, [:binary, active: false], 5000)
                        :ok = :gen_tcp.send(socket, <<8::32, 80877103::32>>)
                        {:ok, "S"} = :gen_tcp.recv(socket, 1, 5000)
                        {:error, reason} = :ssl.connect(socket, opts, 5000)
                        :gen_tcp.close(socket)
                        unless String.contains?(inspect(reason), "unknown_ca"), do: raise("Expected unknown_ca TLS rejection")
                        IO.puts("NATIVE_POOLER_WRONG_CA_REJECTED")
                        """
                        subprocess.run(['/opt/artifact/app/bin/supavisor','rpc',rejected_ca],env=env,stdout=log,stderr=log,check=True,timeout=40)
                    if name == 'supabase-logflare':
                        backend = """
                        repo = Logflare.Backends.Adaptor.PostgresAdaptor.SharedRepo
                        backend = %Logflare.Backends.Backend{id: -173, token: "native-smoke", type: :postgres,
                          config: %{url: System.fetch_env!("POSTGRES_BACKEND_URL"), schema: "_analytics", pool_size: 1, socket_options: [:inet]}}
                        %{rows: [[true]]} = repo.with_repo(backend, fn ->
                          Ecto.Adapters.SQL.query!(repo.get_dynamic_repo(), "SELECT ssl FROM pg_stat_ssl WHERE pid=pg_backend_pid()", [])
                        end)
                        IO.puts("NATIVE_ANALYTICS_BACKEND_TLS_OK")
                        """
                        subprocess.run(['/opt/artifact/app/bin/logflare','rpc',backend],env=env,stdout=log,stderr=log,check=True,timeout=40)
                    if name == 'supabase-studio':
                        with urllib.request.urlopen('http://127.0.0.1:3000/',timeout=30) as home:
                            assert home.status == 200
                            assert b'<html' in home.read().lower()
                        with urllib.request.urlopen('http://127.0.0.1:8080/tables',timeout=10) as tables:
                            assert tables.status == 200
                            assert isinstance(json.load(tables),list)
                        print('NATIVE_STUDIO_HOME_PROFILE_META_OK')
                    if name == 'supabase-postgres-meta':
                        with urllib.request.urlopen('http://127.0.0.1:8080/tables',timeout=10) as tables:
                            assert tables.status == 200
                            assert isinstance(json.load(tables),list)
                    if name == 'supabase-imgproxy':
                        def transform(source_url, options, headers=None):
                            source = urllib.parse.quote(source_url, safe='')
                            request = urllib.request.Request('http://127.0.0.1:5001/public/'+options+'/plain/'+source, headers=headers or {})
                            return urllib.request.urlopen(request, timeout=10)
                        origin = f'http://{image_source_host}:{image_source.server_port}'
                        with transform(origin+'/probe.png?probe=storage', 'width:1/height:1/resizing_type:fit/format:png') as transformed:
                            assert transformed.status == 200
                            assert 'Last-Modified' not in transformed.headers
                            output = transformed.read()
                            assert output.startswith(b'\x89PNG')
                            assert struct.unpack('>II',output[16:24]) == (1,1)
                        with transform(origin+'/probe.png', 'width:1/height:1/resizing_type:fill', headers={'Accept':'image/webp'}) as transformed:
                            output = transformed.read()
                            assert transformed.headers['Content-Type'] == 'image/webp'
                            assert output[:4] == b'RIFF' and output[8:12] == b'WEBP'
                        with transform(origin+'/probe.svg', 'width:1/height:1/resizing_type:fit/format:svg') as transformed:
                            output = transformed.read().lower()
                            assert b'<svg' in output and b'javascript:' not in output
                        try:
                            transform(f'http://127.0.0.1:{image_source.server_port}/probe.png', 'width:1/format:png')
                        except urllib.error.HTTPError as rejected:
                            assert 400 <= rejected.code < 500
                        else:
                            raise AssertionError('loopback source guard was bypassed')
                        print('NATIVE_IMGPROXY_HTTP_PNG_WEBP_SVG_SOURCE_GUARD_OK')
                    break
        except (OSError,urllib.error.URLError) as error:
            if healthy:
                raise
            last_failure = str(error)
            if isinstance(error,urllib.error.HTTPError):
                last_failure += ': '+error.read(4096).decode(errors='replace')
        time.sleep(1)
    else:
        raise RuntimeError('health endpoint did not return HTTP 200 within 120 seconds: '+health+'; last failure: '+last_failure)
finally:
    process.terminate()
    try: process.wait(timeout=20)
    except subprocess.TimeoutExpired: process.kill(); process.wait()
    if image_source is not None:
        image_source.shutdown()
        image_source.server_close()
    if meta_process is not None:
        meta_process.terminate()
        try: meta_process.wait(timeout=20)
        except subprocess.TimeoutExpired: meta_process.kill(); meta_process.wait()
    if image_process is not None:
        image_process.terminate()
        try: image_process.wait(timeout=20)
        except subprocess.TimeoutExpired: image_process.kill(); image_process.wait()
