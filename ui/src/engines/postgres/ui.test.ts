// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import { readFileSync } from 'node:fs';
import { register } from 'node:module';
import { createElement, type ReactElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { createMemoryRouter, Outlet, RouterProvider } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Schemas } from '../../api/client';
import { validateFunction } from '../supabase/function-validation.ts';

// Source imports must wait for the TSX/alias loader; static imports run before registration.
register('../../../test/tsx-loader.mjs', import.meta.url);
const { postgresUI } = await import('./index.tsx');
const { supabaseUI } = await import('../supabase/index.tsx');
const { OptionsSection, ResourcesSection, TopologySection } = await import('./WizardSections.tsx');
const { SupabaseOptions } = await import('../supabase/WizardSection.tsx');
const { ConnectionHelp } = await import('./ConnectionHelp.tsx');
const { SupabaseCredentialsReveal } = await import('../supabase/CredentialsReveal.tsx');
const { FunctionsTab } = await import('../supabase/FunctionsTab.tsx');
const { PostgresAccessTab } = await import('./AccessTab.tsx');
const { OverviewTab } = await import('../../pages/clusters/detail/OverviewTab.tsx');
const { UpgradesTab } = await import('../../pages/clusters/detail/UpgradesTab.tsx');
const { ToastProvider } = await import('../../components/ui/Toast.tsx');
const { engines, tabAvailable } = await import('../index.ts');
const { applyPreset, defaultComponent } = await import('./spec.ts');
const { validateWizard } = await import('../../pages/clusters/wizard/validation.ts');
engines.postgres = postgresUI;
engines.supabase = supabaseUI;

const spec: Schemas['ClusterSpec'] = { provider: 'postgres', version: '17.11.1', topology: { type: 'ha' }, components: { engine: { ...defaultComponent(undefined, 'engine'), replicas: 3 }, extra: { proxy: defaultComponent(undefined, 'proxy') } }, tls: { mode: 'requireTLS', ca: 'selfSigned' } };
const topology: Schemas['ProviderTopology'] = { type: 'ha', displayName: 'HA', defaults: spec, componentSchemas: {}, limits: {} };
const provider: Schemas['Provider'] = { name: 'postgres', displayName: 'PostgreSQL', topologies: [topology], versions: [{ version: spec.version, major: '17', status: 'supported', default: true }, { version: '18.6.1', major: '18', status: 'supported', default: false }], backupTypes: ['full', 'diff', 'incr'], capabilities: ['postgresAccess', 'backups', 'restores', 'pitr'], presets: [], upgradePaths: [] };
const connection: Schemas['ConnectionSummary'] = { hosts: ['dbpx:5432'], publicHosts: ['db.example.org:5432'], uri: 'postgresql://dbpx:5432/postgres', endpoints: [{ name: 'supabase-api', url: 'https://api.example.org', public: true }, { name: 'studio', url: 'https://studio.example.org', public: true }] };
const cluster: Schemas['Cluster'] = { namespace: 'default', name: 'db', generation: 1, createdAt: '', updatedAt: '', spec, status: { observedGeneration: 1, phase: 'Ready', members: [], conditions: [], connection } };

function render(component: ReactElement, value = cluster, allowed = true, catalogOverride?: Schemas['Provider']): string {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  client.setQueryData(['backup-storages', 'default'], { items: [{ name: 'objects', namespace: 'default' }] });
  client.setQueryData(['cluster', 'default', 'db', 'postgres-access', 'roles'], { items: [] });
  client.setQueryData(['cluster', 'default', 'db', 'postgres-access', 'databases'], { items: [] });
  client.setQueryData(['cluster', 'default', 'db', 'supabase-functions'], { items: ['hello'] });
  const catalog = catalogOverride ?? { ...provider, name: value.spec.provider, engineOptions: { extensions: { type: 'array', items: { enum: ['pgvector', 'pg_cron'] } } } };
  client.setQueryData(['providers'], { items: [catalog] });
  const router = createMemoryRouter([{ path: '/', element: createElement(Outlet, { context: { cluster: value, provider: catalog, namespace: 'default', name: 'db', base: '/clusters/default/db', can: () => allowed } }), children: [{ index: true, element: component }] }]);
  try { return renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ToastProvider, null, createElement(RouterProvider, { router })))); }
  finally { router.dispose(); client.clear(); }
}

test('PostgreSQL wizard renders catalog major, HA mode, proxy and distinct stateless resources', () => {
  const props = { provider, spec, onChange: () => {} };
  const options = renderToStaticMarkup(createElement(OptionsSection, props));
  assert.match(options, /PostgreSQL major/);
  assert.match(options, /value="17"/); assert.match(options, /value="18"/);
  assert.match(options, /Synchronous replication/); assert.match(options, /strict/);
  assert.match(options, /Proxy read\/write split/); assert.match(options, /5433/);
  const single = { ...spec, topology: { type: 'single' as const } };
  assert.doesNotMatch(renderToStaticMarkup(createElement(OptionsSection, { ...props, spec: single })), /Synchronous replication/);
  const counts = renderToStaticMarkup(createElement(TopologySection, props));
  assert.match(counts, /value="3"/); assert.match(counts, /value="5"/);
  assert.match(counts, /Proxy replicas/);
  const resources = renderToStaticMarkup(createElement(ResourcesSection, props));
  assert.equal((resources.match(/Min disk \(GiB\)/g) ?? []).length, 1, 'only data members get volume sizing');
  assert.match(resources, /proxy/);
  assert.deepEqual(validateWizard('newdb', 'default', spec, [], provider), { basics: [], topology: [], resources: [], backups: [], access: [] });
});

test('PostgreSQL profile selection follows per-major provider availability', () => {
  const catalog = { ...provider, engineOptions: { profiles: { items: [
    { id: 'postgis', majors: { '17': { available: true, extensions: [{ name: 'postgis' }, { name: 'postgis_topology' }] } } },
    { id: 'timescaledb', majors: { '17': { available: false, reason: 'native artifact timescaledb-pg17 is unavailable', extensions: [{ name: 'timescaledb' }] } } },
  ] } } };
  const markup = renderToStaticMarkup(createElement(OptionsSection, { provider: catalog, spec: { ...spec, engineOptions: { profiles: ['postgis'] } }, onChange: () => {} }));
  assert.match(markup, /PostgreSQL profiles/);
  assert.match(markup, /postgis_topology/);
  assert.match(markup, /timescaledb-pg17 is unavailable/);
  assert.match(markup, /disabled/);
  assert.match(markup, /checked/);
  assert.doesNotMatch(renderToStaticMarkup(createElement(OptionsSection, { provider: catalog, spec: { ...spec, provider: 'supabase' }, onChange: () => {} })), /PostgreSQL profiles/);
});

test('descriptors register access/components/functions tabs without Mongo-only tabs', () => {
  assert.deepEqual(postgresUI.detailTabs?.map((t) => t.id), ['postgres', 'bindings']);
  assert.deepEqual(supabaseUI.detailTabs?.map((t) => t.id), ['postgres', 'components', 'functions']);
  assert.equal(tabAvailable(provider, 'postgres'), true);
  assert.equal(tabAvailable(provider, 'users'), false);
  assert.equal(tabAvailable({ ...provider, name: 'supabase' }, 'functions'), true);
  assert.equal(tabAvailable(provider, 'functions'), false);
});

test('Supabase options expose all fields, registered storage and catalog extension allowlist', () => {
  const sb = applyPreset({ ...spec, provider: 'supabase' }, topology, 'minimal');
  const catalog = { ...provider, name: 'supabase', engineOptions: { extensions: { items: { enum: ['pgvector', 'pg_cron'] } } } };
  const markup = render(createElement(SupabaseOptions, { namespace: 'default', provider: catalog, spec: sb, onChange: () => {} }), { ...cluster, spec: sb });
  for (const label of ['Site URL', 'Additional redirect URLs', 'Disable signup', 'Automatically confirm email', 'JWT expiry', 'SMTP host', 'SMTP port', 'SMTP user', 'Admin email', 'Sender name', 'Exposed schemas', 'Maximum rows', 'Registered backend storage', 'objects', 'Bucket', 'File size limit', 'Image transformation', 'Public gateway HTTPS', 'Studio organization', 'Studio project', 'pgvector', 'pg_cron']) assert.ok(markup.includes(label), label);
  assert.doesNotMatch(markup, /name="password"/);
  assert.match(markup, /Gateway CORS origins/);
});

test('connection help uses libpq verification and credentials stay opt-in', () => {
  const markup = renderToStaticMarkup(createElement(ConnectionHelp, { connection, tlsMode: 'requireTLS' }));
  assert.match(markup, /psql/); assert.match(markup, /postgresql:\/\/USER:PASSWORD@dbpx:5432/);
  assert.match(markup, /sslmode=verify-full/); assert.match(markup, /sslrootcert=ca.pem/);
  const disabled = renderToStaticMarkup(createElement(ConnectionHelp, { connection, tlsMode: 'disabled' }));
  assert.match(disabled, /sslmode=disable/); assert.doesNotMatch(disabled, /sslrootcert/);
  const denied = renderToStaticMarkup(createElement(SupabaseCredentialsReveal, { namespace: 'default', name: 'db', allowed: false }));
  assert.match(denied, /cluster-credentials read permission/); assert.doesNotMatch(denied, /<button/);
  const allowed = renderToStaticMarkup(createElement(SupabaseCredentialsReveal, { namespace: 'default', name: 'db', allowed: true }));
  assert.match(allowed, /Reveal Supabase keys/); assert.doesNotMatch(allowed, /serviceRoleKey|jwtSecret/);
});

test('Supabase overview shows native component roles, readiness and API/Studio links', () => {
  const members = ['primary', 'replica', 'proxy', 'gateway', 'studio'].map((role) => ({ name: `db-${role}`, role, health: true, set: '', state: 'ready', host: 'private' } as Schemas['Member']));
  const markup = render(createElement(OverviewTab), { ...cluster, spec: { ...spec, provider: 'supabase' }, status: { ...cluster.status, members } });
  for (const role of ['primary', 'replica', 'proxy', 'gateway', 'studio']) assert.ok(markup.includes(role));
  assert.match(markup, /Open studio/); assert.match(markup, /Open supabase-api/);
  assert.match(markup, /db.example.org/); assert.match(markup, /Reveal Supabase keys/);
});

test('native access and function forms render supported controls and write-only SMTP', () => {
  const access = render(createElement(PostgresAccessTab));
  for (const label of ['Create role', 'Create database', 'Grant privileges', 'Connection limit', 'Member of', 'Password (empty generates one)']) assert.ok(access.includes(label), label);
  assert.doesNotMatch(access, /generatedPassword/);
  const functions = render(createElement(FunctionsTab), { ...cluster, spec: { ...spec, provider: 'supabase' } });
  assert.match(functions, /hello/); assert.match(functions, /Deploy function/);
  assert.match(functions, /Set SMTP password/); assert.match(functions, /Write-only secret/);
  assert.match(functions, /type="password"/);
  assert.match(functions, /<button[^>]*disabled=""[^>]*>Deploy function<\/button>/);
  assert.match(functions, /Function name must be 1–63 lowercase/);
  assert.deepEqual(validateFunction('hello', { 'index.ts': 'Deno.serve(() => new Response("hello"))' }), []);
  assert.ok(validateFunction('../bad', { '../index.ts': 'x' }).length);
  assert.ok(validateFunction('valid', { 'index.ts': 'x'.repeat(1024 * 1024 + 1) }).length);
});

test('database extension input preserves raw separators and parses only on submission', () => {
  const source = readFileSync(new URL('./AccessTab.tsx', import.meta.url), 'utf8');
  assert.match(source, /const \[extensionsText, setExtensionsText\] = useState\(''\)/);
  assert.match(source, /label="Extensions \(optional, comma separated\)" value=\{extensionsText\} onChange=\{\(e\) => setExtensionsText\(e.target.value\)\}/);
  assert.match(source, /const extensions = extensionsText\.split\(','\)\.map\(\(v\) => v\.trim\(\)\)\.filter\(Boolean\); await unwrap\(api\.POST/);
  assert.match(source, /body: \{ \.\.\.database, extensions \}/);
  assert.match(source, /useSubmissionKey\(JSON\.stringify\(\{ role, database, extensionsText,/);
});

test('upgrade table labels both adjacent PostgreSQL major transitions as offline and minors as rolling', () => {
  for (const [current, target] of [['16.13.1', '17.11.1'], ['17.11.1', '18.6.1']]) {
    const minor = `${current!.split('.')[0]}.99.1`;
    const catalog: Schemas['Provider'] = {
      ...provider,
      versions: [current!, minor, target!].map((version) => ({ version, major: version.split('.')[0]!, status: 'supported', default: false })),
      upgradePaths: [minor, target!].map((to) => ({ from: current!, to, requiresBackup: true })),
    };
    const markup = render(createElement(UpgradesTab), { ...cluster, spec: { ...spec, version: current! } }, true, catalog);
    assert.match(markup, /Major \(OFFLINE maintenance\)/);
    assert.match(markup, /Minor \(rolling\)/);
    assert.doesNotMatch(markup, /Major \(rolling\)/);
    assert.match(markup, /successful backup required/);
    const upgrading = render(createElement(UpgradesTab), { ...cluster, spec: { ...spec, version: target! }, status: { ...cluster.status, phase: 'Upgrading', members: [{ name: 'db-primary', role: 'primary', version: current!, health: true, set: '', state: 'stopped' }] } }, true, catalog);
    assert.match(upgrading, /OFFLINE maintenance: all data members are stopped for pg_upgrade --link/);
    assert.match(upgrading, /All database access is unavailable during pg_upgrade and replica rebuild/);
  }
});

test('mixed data-member majors retain offline progress guidance independently of catalog selection', () => {
  for (const providerName of ['postgres', 'supabase']) {
    for (const [current, target] of [['16.13.1', '17.11.1'], ['17.11.1', '18.6.1']]) {
      for (const oldMember of [
        { role: 'replica', set: '', state: 'rebuilding' },
        { role: 'unknown', set: 'postgres', state: 'starting' },
      ] as const) {
        const members: Schemas['Member'][] = [
          { name: 'db-primary', role: 'primary', version: target!, health: true, set: 'postgres', state: 'primary' },
          { name: 'db-replica', ...oldMember, version: current!, health: false },
        ];
        for (const ordered of [members, [...members].reverse()]) {
          const markup = render(createElement(UpgradesTab), { ...cluster, spec: { ...spec, provider: providerName, version: target! }, status: { ...cluster.status, phase: 'Upgrading', members: ordered } });
          const progress = markup.match(/<strong>Upgrade in progress<\/strong><div>(.*?)<\/div>/)?.[1];
          assert.ok(progress, 'upgrade progress callout must render');
          assert.match(progress, /OFFLINE maintenance: all data members are stopped for pg_upgrade --link/);
          assert.match(progress, /All database access is unavailable during pg_upgrade and replica rebuild/);
          assert.doesNotMatch(progress, /replicas restart first|briefly interrupts writes/);
        }
      }
    }
  }
});

test('mixed minor data versions and optional-service versions do not imply offline major maintenance', () => {
  const members: Schemas['Member'][] = [
    { name: 'db-primary', role: 'primary', version: '17.12.1', health: true, set: 'postgres', state: 'primary' },
    { name: 'db-replica', role: 'replica', version: '17.11.1', health: true, set: 'postgres', state: 'replica' },
    { name: 'db-proxy', role: 'proxy', version: '16.13.1', health: true, set: 'proxy', state: 'ready' },
    { name: 'db-rest', role: 'rest', version: '12.0.0', health: true, set: 'rest', state: 'ready' },
  ];
  const markup = render(createElement(UpgradesTab), { ...cluster, spec: { ...spec, provider: 'supabase', version: '17.12.1' }, status: { ...cluster.status, phase: 'Upgrading', members } });
  const progress = markup.match(/<strong>Upgrade in progress<\/strong><div>(.*?)<\/div>/)?.[1];
  assert.ok(progress, 'upgrade progress callout must render');
  assert.match(progress, /replicas restart first.*switchover/);
  assert.doesNotMatch(progress, /OFFLINE|pg_upgrade/);
});
