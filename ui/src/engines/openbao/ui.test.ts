// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import { register } from 'node:module';
import { Children, createElement, isValidElement, type ReactElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { createMemoryRouter, Outlet, RouterProvider } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Schemas } from '../../api/client';
import type { Action, Resource } from '../../auth/permissions';

// Source imports must wait for the TSX/alias loader; static imports run before registration.
register('../../../test/tsx-loader.mjs', import.meta.url);
const { openbaoUI } = await import('./index.ts');
const { default: OpenBaoTab, SealOverview, UnsealKeysPanel, DatabaseConnections, ConnectionDialog } = await import('./OpenBaoTab.tsx');
const { default: OpenBaoOptionsSection } = await import('./WizardSection.tsx');
const { TopologyEditor } = await import('./TopologyEditor.tsx');
const { default: RestoreSharesSection } = await import('./RestoreShares.tsx');
const { ToastProvider } = await import('../../components/ui/Toast.tsx');
const { AuthProvider } = await import('../../auth/session.tsx');
const { engines } = await import('../index.ts');
engines.openbao = openbaoUI;

const resources: Schemas['Resources'] = { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 1, maxRam: 2, minDisk: 5, maxDisk: 20 };
const spec: Schemas['ClusterSpec'] = { provider: 'openbao', version: '2.7.1', topology: { type: 'cluster' }, components: { engine: { replicas: 3, resources } }, tls: { mode: 'requireTLS', ca: 'selfSigned' }, engineOptions: { unseal: { shares: 5, threshold: 3 }, seal: { type: 'shamir' }, storage: { type: 'raft' }, logLevel: 'info' } };
const topology: Schemas['ProviderTopology'] = { type: 'cluster', displayName: 'Cluster', defaults: spec, componentSchemas: {}, limits: { engine: { allowedReplicas: [1, 3, 5] } } };
const provider: Schemas['Provider'] = { name: 'openbao', displayName: 'OpenBao', topologies: [topology], versions: [{ version: '2.7.1', major: '2', status: 'supported', default: true }], backupTypes: [], capabilities: ['monitoring', 'tls', 'publicExposure'], presets: [], upgradePaths: [] };
const member = (name: string, role: Schemas['Member']['role'], state: string, health = true): Schemas['Member'] => ({ name, role, state, set: 'openbao', health, version: '2.7.1' });
const cluster: Schemas['Cluster'] = {
  namespace: 'default', name: 'bao', generation: 1, createdAt: '', updatedAt: '', spec,
  status: { observedGeneration: 1, phase: 'Ready', members: [member('baom0', 'primary', 'RUNNING'), member('baom1', 'unknown', 'SEALED', false), member('baom2', 'secondary', 'RUNNING')], conditions: [
    { type: 'Sealed', status: 'True', reason: 'MemberSealed', message: 'baom1 is sealed and will be unsealed automatically.', lastTransitionTime: '' },
    { type: 'RaftVoter/baom0', status: 'True', reason: 'HealthyVoter', message: 'healthy voter', lastTransitionTime: '' },
    { type: 'RaftVoter/baom1', status: 'False', reason: 'UnhealthyVoter', message: 'sealed voter', lastTransitionTime: '' },
  ] },
};
const connection: Schemas['OpenBaoDatabaseConnection'] = { name: 'orders', targetCluster: 'pg', targetProvider: 'postgres', mountPath: 'database', phase: 'ready', createdAt: '2026-10-21T00:00:00Z', roles: [{ name: 'reader', access: 'readonly', credentialsPath: 'database/creds/orders_reader', defaultTtlSeconds: 3600, maxTtlSeconds: 86400 }] };

function render(component: ReactElement, can: (action: Action, resource?: Resource) => boolean, setup?: (client: QueryClient) => void, value = cluster): { markup: string; client: QueryClient } {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  setup?.(client);
  const router = createMemoryRouter([{ path: '/', element: createElement(Outlet, { context: { cluster: value, provider, namespace: 'default', name: 'bao', base: '/clusters/default/bao', can } }), children: [{ index: true, element: component }] }]);
  try { return { markup: renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(AuthProvider, null, createElement(ToastProvider, null, createElement(RouterProvider, { router }))))), client }; }
  finally { router.dispose(); }
}

test('descriptor registers the OpenBao tab, wizard section and locked topology editor', () => {
  assert.deepEqual(openbaoUI.detailTabs?.map((tab) => tab.id), ['openbao']);
  assert.equal(openbaoUI.detailTabs?.[0]?.capability, undefined);
  assert.equal(engines.rabbitmq?.detailTabs?.some((tab) => tab.id === 'openbao') ?? false, false);
  assert.ok(openbaoUI.wizardSection && openbaoUI.topologyEditor && openbaoUI.changeReview && openbaoUI.validateSpec);
  assert.equal(openbaoUI.publicPort, 8200);
});

test('seal overview explains conditions, member seal states and Raft voter observations', () => {
  const html = renderToStaticMarkup(createElement(SealOverview, { cluster }));
  for (const text of ['Members are sealed', 'baom1 is sealed and will be unsealed automatically.', 'unseals sealed members automatically', 'Shamir (Karakoram-held shares)', '3 of 5 required', 'Integrated Raft storage', 'Active (unsealed)', 'Standby (unsealed)', 'Healthy voter', 'Unhealthy voter', 'baom0']) assert.ok(html.includes(text), text);
  assert.match(html, /Active member<\/dt><dd>baom0/);
  const lost = { ...cluster, status: { ...cluster.status, conditions: [{ type: 'InitializationOutcomeLost', status: 'True' as const, reason: 'ReceiptMissing', message: 'baom0 is initialized without a receipt.', lastTransitionTime: '' }, { type: 'QuorumBlocked', status: 'True' as const, reason: 'MajorityRequired', message: 'removing baom2 would leave 1 of 2 voters.', lastTransitionTime: '' }] } };
  const lostHtml = renderToStaticMarkup(createElement(SealOverview, { cluster: lost }));
  assert.match(lostHtml, /Initialization outcome lost[\s\S]*Nothing can unseal this cluster[\s\S]*Raft membership change refused[\s\S]*peers\.json/);
  assert.match(lostHtml, /role="alert"/);
});

test('viewers see neither the reveal control nor connection management; administrators get the audited reveal flow', () => {
  const viewer = render(createElement(OpenBaoTab), () => false, (client) => client.setQueryData(['cluster', 'default', 'bao', 'openbao-connections'], { items: [connection] }));
  assert.doesNotMatch(viewer.markup, /Reveal unseal keys/);
  assert.match(viewer.markup, /requires the <code>openbao-unseal-keys<\/code> reveal permission/);
  assert.doesNotMatch(viewer.markup, /Add connection/);
  assert.doesNotMatch(viewer.markup, /Delete…/);
  assert.match(viewer.markup, /do not have permission to read this cluster&#x27;s database connections/);

  const checks: [Action, Resource | undefined][] = [];
  const admin = render(createElement(OpenBaoTab), (action, resource) => { checks.push([action, resource]); return true; }, (client) => client.setQueryData(['cluster', 'default', 'bao', 'openbao-connections'], { items: [connection] }));
  assert.ok(checks.some(([action, resource]) => action === 'reveal' && resource === 'openbao-unseal-keys'));
  assert.match(admin.markup, /Reveal unseal keys…/);
  assert.match(admin.markup, /Add connection/);
  assert.doesNotMatch(admin.markup, /Delete…/, 'no database-users:delete on the target without a loaded session');
  assert.match(admin.markup, /database\/creds\/orders_reader/);
  assert.match(admin.markup, /TTL 1h, max 1d/);
  assert.match(admin.markup, /Type <code>bao<\/code> to confirm/);
  // Nothing revealed is ever rendered or cached before an explicit, confirmed request.
  assert.doesNotMatch(admin.markup, /initialRootToken|Initial root token|Unseal share 1/);
  assert.equal(admin.client.getQueryCache().getAll().some((query) => JSON.stringify(query.queryKey).includes('unseal')), false);
  const denied = renderToStaticMarkup(createElement(UnsealKeysPanel, { namespace: 'default', name: 'bao', allowed: false }));
  assert.doesNotMatch(denied, /<button/);
});

test('reveal state belongs to a keyed namespace/name child and permission loss removes it', () => {
  const original = UnsealKeysPanel({ namespace: 'prod', name: 'bao', allowed: true });
  assert.equal(original.key, UnsealKeysPanel({ namespace: 'prod', name: 'bao', allowed: true }).key);
  assert.notEqual(original.key, UnsealKeysPanel({ namespace: 'other', name: 'bao', allowed: true }).key);
  assert.notEqual(original.key, UnsealKeysPanel({ namespace: 'prod', name: 'other', allowed: true }).key);
  assert.notEqual(original.type, UnsealKeysPanel({ namespace: 'prod', name: 'bao', allowed: false }).type);
  // React discards the entire reducer, confirmation and pending-request owner
  // when this child key changes, rather than waiting for a post-render reset.
  assert.deepEqual(original.props, { namespace: 'prod', name: 'bao' });
});

test('database connections table and dialog respect target permissions and provider scopes', () => {
  const targetCan = (_action: 'create' | 'delete', target: string) => target === 'pg';
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  client.setQueryData(['cluster', 'default', 'bao', 'openbao-connections'], { items: [connection, { ...connection, name: 'events', targetCluster: 'mq', targetProvider: 'rabbitmq', mountPath: 'rabbitmq-events', phase: 'failed', message: 'target unreachable', roles: [{ name: 'consumer', access: 'readwrite', database: 'events', credentialsPath: 'rabbitmq-events/creds/events-consumer' }] }] });
  client.setQueryData(['clusters', 'default'], { items: [{ ...cluster, name: 'pg', spec: { ...spec, provider: 'postgres' } }, { ...cluster, name: 'mq', spec: { ...spec, provider: 'rabbitmq' } }] });
  const router = createMemoryRouter([{ path: '/', element: createElement(DatabaseConnections, { namespace: 'default', name: 'bao', can: () => true, targetCan }) }]);
  const html = renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ToastProvider, null, createElement(RouterProvider, { router }))));
  router.dispose();
  assert.match(html, /<code>orders<\/code>[\s\S]*PostgreSQL[\s\S]*Delete…/);
  assert.match(html, /<code>events<\/code>[\s\S]*RabbitMQ[\s\S]*target unreachable/);
  assert.equal(html.match(/Delete…/g)?.length, 1, 'delete only where database-users:delete holds on the target');
  assert.match(html, /rabbitmq-events\/creds\/events-consumer/);

  const dialog = renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ToastProvider, null, createElement(ConnectionDialog, { namespace: 'default', cluster: 'bao', targets: [{ name: 'mongo', spec: { ...spec, provider: 'psmdb' } }], loading: false, onClose: () => {}, onSaved: () => {} }))));
  assert.match(dialog, /Add database connection/);
  assert.match(dialog, /mongo — MongoDB/);
  assert.match(dialog, /Role 1 name/);
  assert.match(dialog, /Default TTL \(seconds\)/);
  assert.doesNotMatch(dialog, /Virtual host/);
});

test('wizard section renders defaults, conditional seal fields, write-only secrets and a locked editor', () => {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  client.setQueryData(['clusters', 'default'], { items: [{ ...cluster, name: 'pg', spec: { ...spec, provider: 'postgres' } }] });
  const section = (engineOptions: Schemas['ClusterSpec']['engineOptions'], locked = false, available = false) => renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(OpenBaoOptionsSection, { provider: available ? { ...provider, capabilities: [...(provider.capabilities ?? []), 'postgresql-storage'] } : provider, namespace: 'default', spec: { ...spec, engineOptions }, onChange: () => {}, locked })));
  const html = section(undefined);
  assert.match(html, /name="shares"|value="5"/);
  assert.match(html, /value="3"/);
  assert.match(html, /Shamir/);
  assert.doesNotMatch(html, /Transit seal|AWS KMS seal/);
  assert.match(html, /info — default/);
  assert.match(html, /<input(?=[^>]*value="postgresql")(?=[^>]*disabled="")[^>]*>/);
  assert.match(html, /PostgreSQL storage is unavailable/);
  const available = section(undefined, false, true);
  assert.doesNotMatch(available, /<input(?=[^>]*value="postgresql")(?=[^>]*disabled="")[^>]*>/);
  assert.doesNotMatch(available, /PostgreSQL storage is unavailable/);
  const transit = section({ seal: { type: 'transit' } });
  assert.match(transit, /Transit seal/);
  assert.match(transit, /type="password"/);
  assert.match(transit, /never shown again/);
  assert.match(transit, /Recovery shares/);
  const aws = section({ seal: { type: 'awskms' }, storage: { type: 'postgresql' } });
  assert.match(aws, /AWS KMS seal/);
  assert.match(aws, /Secret access key/);
  assert.match(aws, /PostgreSQL cluster/);
  assert.match(aws, /<option value="pg">pg<\/option>/);
  const locked = section({ seal: { type: 'transit', transit: { address: 'https://vault.example.net', keyName: 'k' } } }, true);
  assert.match(locked, /Fixed at initialization/);
  assert.match(locked, /<fieldset[^>]*disabled=""/);
  assert.doesNotMatch(locked, /type="password"/);
  assert.match(locked, /Only the log level can be changed here/);
});

test('topology editor keeps initialization options read-only and offers 1/3/5 members', () => {
  const submitted: Record<string, unknown>[] = [];
  const { markup } = render(createElement(TopologyEditor, { cluster, provider, editable: true, can: () => true, busy: false, submit: async (patch) => { submitted.push(patch); return cluster; } }), () => true);
  assert.match(markup, /Raft members/);
  assert.match(markup, /<option value="1">1<\/option><option value="3" selected="">3<\/option><option value="5">5<\/option>/);
  assert.match(markup, /Fixed at initialization/);
  assert.match(markup, /Review &amp; apply/);
  assert.match(markup, /API exposure/);
  assert.doesNotMatch(markup, /Engine configuration overrides/);
  assert.deepEqual(submitted, []);
});

test('restore shares section is opt-in, explains when shares are needed and renders the pasted-material form', () => {
  assert.ok(openbaoUI.restoreSection);
  assert.equal(renderToStaticMarkup(createElement(RestoreSharesSection, { source: undefined, inPlace: false, value: {}, onChange: () => {} })), '');
  const off = renderToStaticMarkup(createElement(RestoreSharesSection, { source: { namespace: 'default', cluster: 'old' }, inPlace: false, value: {}, onChange: () => {} }));
  assert.match(off, /Supply the source cluster&#x27;s unseal shares from an offline copy/);
  assert.match(off, /Karakoram copies the stored unseal material of a live source automatically/);
  assert.doesNotMatch(off, /<textarea/);
  const on = renderToStaticMarkup(createElement(RestoreSharesSection, { source: { namespace: 'default', cluster: 'old' }, inPlace: true, value: { openbaoUnsealShares: { shares: [], threshold: 0 } }, onChange: () => {}, showErrors: true }));
  assert.match(on, /restored with its own stored unseal material/);
  assert.match(on, /<textarea/);
  assert.match(on, /Unseal shares \(one per line\)/);
  assert.match(on, /Paste at least one unseal share/);
  assert.match(on, /Shamir \(Karakoram-held shares\)/);
});

test('discarding restore material removes its state owner and re-enabling starts empty', () => {
  const source = { namespace: 'prod', cluster: 'old' };
  const value = { openbaoUnsealShares: { sealType: 'shamir' as const, shares: ['old-secret'], threshold: 1 } };
  let changed: unknown;
  const onChange = (next: unknown) => { changed = next; };
  const children = (extras: Parameters<typeof RestoreSharesSection>[0]['value']) => Children.toArray(RestoreSharesSection({ source, inPlace: false, value: extras, onChange })!.props.children).filter((child): child is ReactElement => isValidElement(child));
  const enabled = children(value);
  assert.equal(enabled.length, 3, 'enabled editor is a separate state owner');
  const toggle = enabled[0]!;
  toggle.props.onChange({ target: { checked: false } });
  assert.deepEqual(changed, {});
  const cleared = children({});
  assert.equal(cleared.length, 2, 'parent clearing material unmounts the draft even if the outer section stays mounted');
  cleared[0]!.props.onChange({ target: { checked: true } });
  assert.deepEqual(changed, { openbaoUnsealShares: { sealType: 'shamir', shares: [], threshold: 0 } });
  const elsewhere = RestoreSharesSection({ source: { ...source, cluster: 'other' }, inPlace: false, value, onChange })!;
  const elsewhereEditor = Children.toArray(elsewhere.props.children).filter((child): child is ReactElement => isValidElement(child))[2]!;
  assert.notEqual(enabled[2]!.key, elsewhereEditor.key);
});
