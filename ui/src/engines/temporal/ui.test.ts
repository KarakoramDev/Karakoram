// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import test from 'node:test';
import assert from 'node:assert/strict';
import { register } from 'node:module';
import { createElement, type ReactElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { createMemoryRouter, Outlet, RouterProvider } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Schemas } from '../../api/client';

register('../../../test/tsx-loader.mjs', import.meta.url);
// Static imports run before register; source imports must wait for the TSX/alias loader.
const { OptionsSection, ResourcesSection, TopologySection } = await import('./WizardSections.tsx');
const { TopologyEditor } = await import('./TopologyEditor.tsx');
const { NamespacesTab, deletable } = await import('./NamespacesTab.tsx');
const { CertificatesTab, issueCertificate } = await import('./CertificatesTab.tsx');
const { WebUITab } = await import('./WebUITab.tsx');
const { temporalUI } = await import('./index.tsx');
const { DEFAULT_RESOURCES, namespaceErrors, readOptions, renameNamespace, withReplicas, writeOptions } = await import('./spec.ts');
const { engines, tabAvailable } = await import('../index.ts');
const { engineRules, engineConsequences } = await import('../../pages/clusters/wizard/engine.ts');
const { firstInvalidStep } = await import('../../pages/clusters/wizard/state.ts');
const { validateWizard } = await import('../../pages/clusters/wizard/validation.ts');
const { cleanSpec } = await import('../../pages/clusters/ClusterWizard.tsx');
const { configurationReviewRows } = await import('../../pages/clusters/wizard/review.ts');
const { maintenanceCopy } = await import('../../pages/clusters/detail/detail-state.ts');
const { RestoresTab } = await import('../../pages/clusters/detail/RestoresTab.tsx');
const { ToastProvider } = await import('../../components/ui/Toast.tsx');
const { ChoiceGroup } = await import('../../components/ui/Field.tsx');
const { AuthProvider } = await import('../../auth/session.tsx');
engines.temporal = temporalUI;

const resources = { ...DEFAULT_RESOURCES };
const engine: Schemas['Component'] = { replicas: 0, resources: { cpuMode: 'SHARED', minCpu: 0, maxCpu: 0, minRam: 0, maxRam: 0, minDisk: 0, maxDisk: 0 } };
const spec: Schemas['ClusterSpec'] = {
  provider: 'temporal', version: '1.32.1', topology: { type: 'split' },
  components: { engine, extra: { history: { replicas: 3, resources }, matching: { replicas: 2, resources }, frontend: { replicas: 2, resources }, worker: { replicas: 1, resources }, ui: { replicas: 1, resources } } },
  engineOptions: { persistence: { postgres: { cluster: 'appdb' } }, historyShards: 512, frontend: { requireClientAuth: true }, namespaces: [{ name: 'orders', retention: '168h' }], ui: { publicHttps: false } },
};
const combinedDefaults: Schemas['ClusterSpec'] = { ...spec, topology: { type: 'combined' }, components: { engine, extra: { server: { replicas: 1, resources }, ui: { replicas: 1, resources } } }, engineOptions: {} };
const splitTopology: Schemas['ProviderTopology'] = { type: 'split', displayName: 'split', defaults: { ...spec, engineOptions: {} }, componentSchemas: {}, limits: { history: { minReplicas: 0, maxReplicas: 9 }, matching: { minReplicas: 0, maxReplicas: 5 }, frontend: { minReplicas: 0, maxReplicas: 5 }, worker: { minReplicas: 0, maxReplicas: 3 }, ui: { minReplicas: 0, maxReplicas: 3 } } };
const combinedTopology: Schemas['ProviderTopology'] = { type: 'combined', displayName: 'combined', defaults: combinedDefaults, componentSchemas: {}, limits: { server: { minReplicas: 0, maxReplicas: 5 }, ui: { minReplicas: 0, maxReplicas: 3 } } };
const provider: Schemas['Provider'] = { name: 'temporal', displayName: 'Temporal', topologies: [combinedTopology, splitTopology], versions: [{ version: '1.32.1', major: '1.32', status: 'supported', default: true }], presets: [], upgradePaths: [], backupTypes: ['full', 'diff', 'incr'], capabilities: ['tls', 'backups', 'restores', 'temporalAccess'], operations: ['restart', 'rotate-tls', 'backup', 'delete-backup', 'restore'] };
const connection: Schemas['ConnectionSummary'] = { hosts: ['wf-frontend-1:7233'], uri: 'temporal://wf-frontend-1:7233', endpoints: [{ name: 'frontend', url: 'temporal://wf-frontend-1:7233', public: false }] };
const cluster: Schemas['Cluster'] = { name: 'wf', namespace: 'default', generation: 1, createdAt: '2026-01-01T00:00:00Z', updatedAt: '2026-01-01T00:00:00Z', spec, status: { phase: 'Ready', observedGeneration: 1, members: [], connection, conditions: [{ type: 'PostgreSQLBinding', status: 'True', reason: 'Bound', message: '{"sourceNamespace":"default","sourceCluster":"appdb","shared":true,"consumerCount":2}', lastTransitionTime: '2026-01-01T00:00:00Z' }] } };
const postgresCluster: Schemas['Cluster'] = { ...cluster, name: 'appdb', spec: { provider: 'postgres', version: '17.4', topology: { type: 'single' }, components: { engine: { replicas: 1, resources } } } };
const oldPostgres: Schemas['Cluster'] = { ...postgresCluster, name: 'legacy', spec: { ...postgresCluster.spec, version: '11.2' } };
const mongoCluster: Schemas['Cluster'] = { ...cluster, name: 'docs', spec: { provider: 'psmdb', version: '8.0', topology: { type: 'replicaSet' }, components: { engine: { replicas: 3, resources } } } };

test('Temporal create omits the unused engine response slot from requests', () => {
  for (const defaults of [spec, combinedDefaults]) {
    const body: Schemas['CreateClusterRequest'] = { name: 'wf', spec: cleanSpec(defaults, 'default', false) };
    assert.equal(Object.hasOwn(body.spec.components ?? {}, 'engine'), false);
    assert.deepEqual(body.spec.components?.extra, defaults.components.extra);
    assert.equal(defaults.components.engine.replicas, 0);
  }
  assert.deepEqual(cleanSpec(postgresCluster.spec, 'default', false).components?.engine, postgresCluster.spec.components.engine);
});

function render(element: ReactElement, setup?: (client: QueryClient) => void, path = '/') {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  client.setQueryData(['clusters', 'default'], { items: [postgresCluster, oldPostgres, mongoCluster, cluster] });
  setup?.(client);
  const router = createMemoryRouter([{ path: '/', element: createElement(Outlet, { context: { cluster, provider, namespace: 'default', name: 'wf', base: '/clusters/default/wf', can: () => true } }), children: [{ index: true, element }] }], { initialEntries: [path] });
  try {
    return renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(AuthProvider, null, createElement(ToastProvider, null, createElement(RouterProvider, { router })))));
  } finally { router.dispose(); client.clear(); }
}

test('descriptor registers Temporal tabs, summaries and immutable-aware editors', () => {
  assert.deepEqual(temporalUI.detailTabs?.map((t) => [t.id, t.label, t.capability]), [['namespaces', 'Namespaces', 'temporalAccess'], ['certificates', 'Client certificates', 'temporalAccess'], ['web-ui', 'Web UI', 'temporalAccess']]);
  assert.equal(temporalUI.topologySummary?.(cluster), 'Split · 3 history, 2 matching, 2 frontend, 1 worker · Web UI');
  assert.equal(tabAvailable(provider, 'namespaces'), true);
  assert.equal(tabAvailable(provider, 'users'), false);
  assert.equal(tabAvailable(provider, 'restores'), true);
  assert.ok(temporalUI.topologyEditor && temporalUI.wizardSection && temporalUI.topologySection && temporalUI.resourcesSection);
  assert.match(maintenanceCopy('temporal', 0), /history, matching, frontend, worker, server, Web UI/);
});

test('Basics explains both Temporal topology choices and the review lists Temporal rows', () => {
  const rules = engineRules('temporal');
  const options = Object.entries(rules.topology).map(([value, choice]) => ({ value, ...choice }));
  const choices = renderToStaticMarkup(createElement(ChoiceGroup, { name: 'topology', legend: 'Topology', value: 'combined', onChange: () => {}, options }));
  assert.match(choices, /Combined server/);
  assert.match(choices, /Split services/);
  assert.equal(rules.resourceMinimums.disk, 1);
  const rows = Object.fromEntries(configurationReviewRows(spec));
  assert.equal(rows['PostgreSQL source'], 'appdb');
  assert.equal(rows.Namespaces, 'default (72h), orders (168h)');
  assert.ok(engineConsequences(spec).some((c) => c.includes('client certificate')));
  assert.ok(engineConsequences(writeOptions(spec, { ...readOptions(spec), ui: { publicHttps: true, oidc: { issuer: 'https://login.example', clientId: 'x' } } })).some((c) => c.includes('OIDC login')));
});

test('options section lists only PostgreSQL sources of the namespace, explains defaults and immutability', () => {
  const markup = render(createElement(OptionsSection, { namespace: 'default', provider, spec, onChange: () => {} }));
  assert.match(markup, /appdb · PostgreSQL 17\.4/);
  assert.match(markup, /legacy · PostgreSQL 11\.2/);
  assert.doesNotMatch(markup, /value="docs"/);
  assert.doesNotMatch(markup, /value="wf"/);
  assert.match(markup, /major 12 or later/);
  assert.match(markup, /PostgreSQL bindings are unavailable/);
  assert.match(markup, /placeholder="temporal_&lt;cluster&gt;"/);
  assert.match(markup, /placeholder="temporal_&lt;cluster&gt;_visibility"/);
  assert.match(markup, /Immutable after creation: shards bound/);
  assert.match(markup, /default<\/span> · 72h \(3 days\) · always ensured/);
  assert.match(markup, /value="orders"/);
  assert.match(markup, /class="field-hint"[^>]*>7 days</);
  assert.equal((markup.match(/Temporal options/g) ?? []).length, 1);
  assert.doesNotMatch(render(createElement(OptionsSection, { namespace: 'default', provider, spec, legend: false, onChange: () => {} })), /<legend class="field-label"/);
  assert.equal(temporalUI.engineConfigUnsupported, true);
  assert.doesNotMatch(markup, /OIDC issuer/);
  assert.doesNotMatch(markup, /Frontend accepts unauthenticated clients/);
  const legacy = render(createElement(OptionsSection, { namespace: 'default', provider, spec: writeOptions(spec, { ...readOptions(spec), persistence: { postgres: { cluster: 'legacy' } } }), onChange: () => {} }));
  assert.match(legacy, /older than 12; the binding will be refused/);
  const open = writeOptions(spec, { ...readOptions(spec), frontend: { requireClientAuth: false }, ui: { publicHttps: true, oidc: { issuer: 'https://login.example', clientId: 'temporal-ui' } } });
  const openMarkup = render(createElement(OptionsSection, { namespace: 'default', provider, spec: open, onChange: () => {} }));
  assert.match(openMarkup, /Frontend accepts unauthenticated clients/);
  assert.match(openMarkup, /OIDC issuer/);
  assert.match(openMarkup, /value="https:\/\/login.example"/);
  assert.match(openMarkup, /Client secret is set after creation/);
  assert.match(openMarkup, /write-only/);
  const persisted = render(createElement(OptionsSection, { namespace: 'default', provider, spec, persisted: spec, clusterName: 'order-flows', onChange: () => {} }));
  assert.match(persisted, /placeholder="temporal_order_flows"/);
  assert.match(persisted, /Immutable after creation\./);
  assert.match(persisted, /Set or replace it on the Web UI tab|OIDC login/);
});

test('topology section offers bounded instance pills per role and an optional Web UI', () => {
  const markup = renderToStaticMarkup(createElement(TopologySection, { provider, spec, onChange: () => {} }));
  for (const role of ['History instances', 'Matching instances', 'Frontend instances', 'Worker instances', 'Web UI instances']) assert.match(markup, new RegExp(`aria-label="${role}"`));
  assert.equal((markup.match(/aria-pressed="true"/g) ?? []).length, 5);
  assert.match(markup, /aria-label="History instances">(?:<button[^>]*>\d<\/button>){9}<\/div>/);
  const combined = renderToStaticMarkup(createElement(TopologySection, { provider, spec: combinedDefaults, onChange: () => {} }));
  assert.match(combined, /aria-label="Server instances">(?:<button[^>]*>\d<\/button>){5}<\/div>/);
  assert.doesNotMatch(combined, /History instances/);
  const noUI = renderToStaticMarkup(createElement(TopologySection, { provider, spec: withReplicas(spec, splitTopology, 'ui', 0), onChange: () => {} }));
  assert.doesNotMatch(noUI, /Web UI instances/);
  const resourcesMarkup = renderToStaticMarkup(createElement(ResourcesSection, { provider, spec, onChange: () => {} }));
  assert.match(resourcesMarkup, /History · 3 × resources/);
  assert.match(resourcesMarkup, /Web UI · 1 × resources/);
  assert.match(resourcesMarkup, /id="temporal-history-minDisk"/);
});

test('wizard validation accepts a complete split spec and blocks a missing source on Basics', () => {
  assert.equal(firstInvalidStep(validateWizard('wf', 'default', spec, [], provider)), undefined);
  const missing = writeOptions(spec, { ...readOptions(spec), persistence: { postgres: { cluster: '' } } });
  assert.equal(firstInvalidStep(validateWizard('wf', 'default', missing, [], provider)), 'basics');
  const noFrontend = withReplicas(spec, splitTopology, 'frontend', 0);
  assert.equal(firstInvalidStep(validateWizard('wf', 'default', noFrontend, [], provider)), 'topology');
  const publicFrontend: Schemas['ClusterSpec'] = { ...spec, exposure: { public: true, ipType: 'IPV6', allowlist: ['0.0.0.0/0'] } };
  assert.ok(validateWizard('wf', 'default', publicFrontend, [], provider, true).basics.some((e) => e.includes('frontend is private')));
});

test('detail editor locks immutable blocks and omits TLS and exposure editors', () => {
  const markup = render(createElement(TopologyEditor, { cluster, provider, editable: true, can: () => true, busy: false, submit: async () => cluster }));
  assert.match(markup, /<h3 class="card-title">Split services<\/h3>/);
  assert.match(markup, /fixed after creation/);
  assert.match(markup, /Temporal options/);
  assert.doesNotMatch(markup, /TLS mode/);
  assert.doesNotMatch(markup, /Network exposure/);
  assert.doesNotMatch(markup, /Engine configuration overrides/);
  assert.match(markup, /Review &amp; apply/);
  const readOnly = render(createElement(TopologyEditor, { cluster, provider: undefined, editable: false, can: () => false, busy: false, submit: async () => cluster }));
  assert.doesNotMatch(readOnly, /Review &amp; apply/);
});

test('namespaces tab shows the binding, lists namespaces and offers deletion only for unmanaged ones', () => {
  const items: Schemas['TemporalNamespace'][] = [
    { name: 'default', retention: '72h', state: 'Registered', managed: true },
    { name: 'orders', retention: '168h', state: 'Registered', managed: true, description: 'Order workflows' },
    { name: 'scratch', retention: '24h', state: 'Registered', managed: false },
    { name: 'temporal-system', retention: '168h', state: 'Registered', managed: false },
  ];
  assert.deepEqual(items.map(deletable), [false, false, true, false]);
  const markup = render(createElement(NamespacesTab), (client) => client.setQueryData(['cluster', 'default', 'wf', 'temporal-namespaces'], { items }));
  assert.match(markup, /default\/appdb/);
  assert.match(markup, /Shared/);
  assert.match(markup, /2 consumers/);
  assert.match(markup, /Order workflows/);
  assert.equal((markup.match(/>Delete…</g) ?? []).length, 1);
  assert.equal((markup.match(/>Managed<\/span>/g) ?? []).length, 2);
  assert.equal((markup.match(/>Unmanaged<\/span>/g) ?? []).length, 2);
  const waiting = { ...cluster, status: { ...cluster.status, conditions: [{ type: 'PostgreSQLBinding', status: 'False' as const, reason: 'SourceUnavailable', message: 'Waiting for bound PostgreSQL source', lastTransitionTime: '2026-01-01T00:00:00Z' }] } };
  const router = createMemoryRouter([{ path: '/', element: createElement(Outlet, { context: { cluster: waiting, provider, namespace: 'default', name: 'wf', base: '/clusters/default/wf', can: () => true } }), children: [{ index: true, element: createElement(NamespacesTab) }] }]);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  try {
    const pending = renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ToastProvider, null, createElement(RouterProvider, { router }))));
    assert.match(pending, /Waiting for the bound PostgreSQL source/);
  } finally { router.dispose(); client.clear(); }
});

test('certificates tab flags certificates of a superseded CA and never renders key material', () => {
  const items: Schemas['TemporalClientCertificate'][] = [
    { serial: '101', commonName: 'orders', fingerprintSha256: 'aa'.repeat(32), issuedAt: '2026-01-01T00:00:00Z', notAfter: '2099-01-01T00:00:00Z', caFingerprintSha256: 'ca-old', currentCa: true },
    { serial: '102', commonName: 'billing', fingerprintSha256: 'bb'.repeat(32), issuedAt: '2026-03-01T00:00:00Z', notAfter: '2099-01-01T00:00:00Z', caFingerprintSha256: 'ca-new', currentCa: false },
    { serial: '103', commonName: 'stale', fingerprintSha256: 'cc'.repeat(32), issuedAt: '2026-02-01T00:00:00Z', notAfter: '2026-02-02T00:00:00Z', caFingerprintSha256: 'ca-new', currentCa: false },
  ];
  const markup = render(createElement(CertificatesTab), (client) => client.setQueryData(['cluster', 'default', 'wf', 'temporal-client-certificates'], { items }));
  assert.match(markup, /Issue certificate…/);
  assert.match(markup, /Mutual TLS on the frontend/);
  assert.equal((markup.match(/Invalid after CA rotation/g) ?? []).length, 1);
  assert.equal((markup.match(/>Current CA</g) ?? []).length, 1);
  assert.equal((markup.match(/>Expired</g) ?? []).length, 1);
  assert.ok(markup.indexOf('billing') < markup.indexOf('orders'), 'newest first');
  assert.doesNotMatch(markup, /PRIVATE KEY|privateKeyPem/);
  const rotated = render(createElement(CertificatesTab), (client) => client.setQueryData(['cluster', 'default', 'wf', 'temporal-client-certificates'], { items: [{ ...items[1]!, currentCa: false }] }));
  assert.match(rotated, /Invalid after CA rotation/);
  assert.doesNotMatch(rotated, />Current CA<|most recently issued certificate/);
  const optional = { ...cluster, spec: writeOptions(spec, { ...readOptions(spec), frontend: { requireClientAuth: false } }) };
  const router = createMemoryRouter([{ path: '/', element: createElement(Outlet, { context: { cluster: optional, provider, namespace: 'default', name: 'wf', base: '/clusters/default/wf', can: (_action: string, resource?: string) => resource !== 'cluster-credentials' } }), children: [{ index: true, element: createElement(CertificatesTab) }] }]);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  try {
    const limited = renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ToastProvider, null, createElement(RouterProvider, { router }))));
    assert.match(limited, /Client certificates are optional/);
    assert.match(limited, /requires cluster-credentials read permission/);
    assert.doesNotMatch(limited, /Issue certificate…/);
  } finally { router.dispose(); client.clear(); }
});

test('issuance hands the bundle to the dialog and leaves no key material in the mutation result', async () => {
  const bundle: Schemas['TemporalIssuedClientCertificate'] = { serial: '7', commonName: 'svc', fingerprintSha256: 'aa', issuedAt: '2026-01-01T00:00:00Z', notAfter: '2027-01-01T00:00:00Z', caFingerprintSha256: 'ca', currentCa: true, certificatePem: 'CERT', privateKeyPem: 'SECRET-KEY', caPem: 'CA', frontendAddress: 'wf:7233', serverName: 'wf' };
  const received: Schemas['TemporalIssuedClientCertificate'][] = [];
  const result = await issueCertificate(async () => bundle, (issued) => received.push(issued));
  assert.deepEqual(result, { commonName: 'svc', serial: '7' });
  assert.doesNotMatch(JSON.stringify(result), /SECRET-KEY|CERT|privateKeyPem|certificatePem|caPem/);
  assert.deepEqual(received, [bundle]);
  await assert.rejects(issueCertificate(async () => { throw new Error('denied'); }, () => assert.fail('receive must not run on failure')), /denied/);
});

test('a declaration renamed to default snaps to 72h, keeps an editable name and can be removed', () => {
  const fresh = { name: '', retention: '168h' };
  assert.deepEqual(renameNamespace(fresh, 'default'), { name: 'default', retention: '72h' });
  assert.deepEqual(renameNamespace({ name: 'default', retention: '72h', description: 'd' }, 'defaults'), { name: 'defaults', retention: '72h', description: 'd' });
  assert.deepEqual(namespaceErrors([renameNamespace(fresh, 'default')]), []);
  const declared = writeOptions(spec, { ...readOptions(spec), namespaces: [{ name: 'default', retention: '72h' }, { name: 'orders', retention: '168h' }] });
  const markup = render(createElement(OptionsSection, { namespace: 'default', provider, spec: declared, onChange: () => {} }));
  assert.equal((markup.match(/>Remove</g) ?? []).length, 2, 'every declaration, including default, can be removed');
  const inputOf = (value: string) => markup.match(new RegExp(`<input[^>]*value="${value}"[^>]*>`))?.[0] ?? '';
  assert.ok(inputOf('default'), 'default name input rendered');
  assert.doesNotMatch(inputOf('default'), /readonly/i);
  assert.match(inputOf('72h'), /readonly/i);
  assert.doesNotMatch(inputOf('168h'), /readonly/i);
  assert.match(markup, /Fixed for the default namespace/);
});

test('Web UI tab offers a write-only client secret form without echoing values', () => {
  const markup = render(createElement(WebUITab));
  assert.match(markup, /Private to the project network/);
  assert.match(markup, /type="password"/);
  assert.match(markup, /Store client secret/);
  assert.match(markup, /never returned by the API/);
  assert.match(markup, /OIDC login applies only to a public Web UI/);
  const publicUI = { ...cluster, spec: writeOptions(spec, { ...readOptions(spec), ui: { publicHttps: true, oidc: { issuer: 'https://login.example', clientId: 'temporal-ui' } } }), status: { ...cluster.status, connection: { ...connection, endpoints: [...connection.endpoints!, { name: 'ui', url: 'https://wf-ui.example', public: true }] } } };
  const router = createMemoryRouter([{ path: '/', element: createElement(Outlet, { context: { cluster: publicUI, provider, namespace: 'default', name: 'wf', base: '/clusters/default/wf', can: () => true } }), children: [{ index: true, element: createElement(WebUITab) }] }]);
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  try {
    const open = renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ToastProvider, null, createElement(RouterProvider, { router }))));
    assert.match(open, /Public HTTPS/);
    assert.match(open, /https:\/\/wf-ui.example/);
    assert.match(open, /temporal-ui/);
    assert.doesNotMatch(open, /OIDC login applies only/);
  } finally { router.dispose(); client.clear(); }
});

test('in-place restore of a Temporal cluster requires the shared-source acknowledgement', () => {
  const markup = render(createElement(RestoresTab), (client) => client.setQueryData(['cluster', 'default', 'wf', 'restores'], { items: [] }), '/?backup=nightly-1');
  assert.match(markup, /rolls back every consumer/);
  assert.match(markup, /bound PostgreSQL source restores/);
  assert.match(markup, /Restore and overwrite data/);
  assert.match(markup, /<button[^>]*disabled[^>]*>Restore and overwrite data/);
});
