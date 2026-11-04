// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import test from 'node:test';
import assert from 'node:assert/strict';
import type { Schemas } from '../../api/client';
import {
  DEFAULT_RESOURCES,
  bindingStatus,
  certificateFiles,
  certificateState,
  cliExample,
  configurationReviewRows,
  defaultDatabase,
  defaultVisibilityDatabase,
  disruptions,
  durationHours,
  formatRetention,
  limitsFor,
  namespaceErrors,
  readOptions,
  resourceRows,
  shortFingerprint,
  specPatch,
  specSummary,
  ttlError,
  validateComponents,
  validateSpec,
  wireOptions,
  withReplicas,
  writeOptions,
  type ClientCertificate,
  type IssuedClientCertificate,
} from './spec.ts';
import { applyMergePatch } from '../changes.ts';

const resources = { ...DEFAULT_RESOURCES };
const engine: Schemas['Component'] = { replicas: 0, resources: { cpuMode: 'SHARED', minCpu: 0, maxCpu: 0, minRam: 0, maxRam: 0, minDisk: 0, maxDisk: 0 } };
const combined: Schemas['ClusterSpec'] = {
  provider: 'temporal', version: '1.32.1', topology: { type: 'combined' },
  components: { engine, extra: { server: { replicas: 1, resources }, ui: { replicas: 1, resources } } },
  engineOptions: { persistence: { postgres: { cluster: 'appdb' } }, historyShards: 512, frontend: { requireClientAuth: true }, ui: { publicHttps: false } },
};
const split: Schemas['ClusterSpec'] = {
  ...combined, topology: { type: 'split' },
  components: { engine, extra: { history: { replicas: 3, resources }, matching: { replicas: 2, resources }, frontend: { replicas: 2, resources }, worker: { replicas: 1, resources }, ui: { replicas: 1, resources } } },
};
const topology: Schemas['ProviderTopology'] = {
  type: 'split', displayName: 'split', defaults: split, componentSchemas: {},
  limits: { history: { minReplicas: 0, maxReplicas: 9 }, matching: { minReplicas: 0, maxReplicas: 5 }, frontend: { minReplicas: 0, maxReplicas: 5 }, worker: { minReplicas: 0, maxReplicas: 3 }, ui: { minReplicas: 0, maxReplicas: 3 } },
};
const noResourceErrors = () => [];

test('options read tolerantly with provider defaults and round-trip through the wire shape', () => {
  const empty = readOptions({ engineOptions: undefined });
  assert.deepEqual(empty, { persistence: { postgres: { cluster: '' } }, historyShards: 512, frontend: { requireClientAuth: true }, namespaces: [], ui: { publicHttps: false } });
  const full = readOptions({ engineOptions: { persistence: { postgres: { cluster: 'pg', namespace: 'infra', database: 'wf', visibilityDatabase: 'wf_vis' } }, historyShards: 1024, frontend: { requireClientAuth: false }, namespaces: [{ name: 'orders', retention: '168h', description: 'Orders' }], ui: { publicHttps: true, oidc: { issuer: 'https://login.example', clientId: 'temporal' } } } });
  assert.equal(full.persistence.postgres.namespace, 'infra');
  assert.equal(full.frontend.requireClientAuth, false);
  assert.deepEqual(full.namespaces, [{ name: 'orders', retention: '168h', description: 'Orders' }]);
  assert.deepEqual(full.ui.oidc, { issuer: 'https://login.example', clientId: 'temporal' });
  assert.deepEqual(readOptions(writeOptions(combined, full)), full);
  assert.deepEqual(wireOptions(empty), { persistence: { postgres: { cluster: '' } }, historyShards: 512, frontend: { requireClientAuth: true }, ui: { publicHttps: false } }, 'empty namespaces and unset OIDC are omitted');
});

test('database defaults replace hyphens and durations follow Go syntax', () => {
  assert.equal(defaultDatabase('order-flows'), 'temporal_order_flows');
  assert.equal(defaultVisibilityDatabase('order-flows'), 'temporal_order_flows_visibility');
  assert.equal(durationHours('72h'), 72);
  assert.equal(durationHours('24h30m'), 24.5);
  assert.equal(durationHours('1.5h'), 1.5);
  assert.equal(durationHours('90m'), 1.5);
  assert.equal(durationHours('7d'), undefined);
  assert.equal(durationHours('h'), undefined);
  assert.equal(durationHours(''), undefined);
  assert.equal(durationHours('72h extra'), undefined);
  assert.equal(formatRetention('168h'), '168h (7 days)');
  assert.equal(formatRetention('24h'), '24h (1 day)');
  assert.equal(formatRetention('36h'), '36h (36 hours)');
  assert.equal(formatRetention('24h30m'), '24h30m (24.5 hours)');
  assert.equal(formatRetention('bogus'), 'bogus');
});

test('namespace declarations need valid names, unique entries and at least 24h retention', () => {
  assert.deepEqual(namespaceErrors([{ name: 'orders', retention: '168h' }]), []);
  assert.deepEqual(namespaceErrors([{ name: 'default', retention: '72h', description: 'd' }]), []);
  const errors = namespaceErrors([{ name: 'orders', retention: '12h' }, { name: 'orders', retention: '168h' }, { name: '-bad', retention: 'soon' }, { name: 'default', retention: '48h' }]);
  assert.ok(errors.some((e) => e.includes('at least 24h')));
  assert.ok(errors.some((e) => e.includes('unique')));
  assert.ok(errors.some((e) => e.startsWith('Namespace -bad:')));
  assert.ok(errors.some((e) => e.includes('default namespace keeps its 72h')));
});

test('spec validation mirrors the provider: source cluster, shards, OIDC pairing and private frontend', () => {
  assert.deepEqual(validateSpec(combined), []);
  const missingSource = validateSpec({ ...combined, engineOptions: { ...combined.engineOptions, persistence: { postgres: { cluster: '' } } } });
  assert.ok(missingSource.some((e) => e.includes('PostgreSQL cluster')));
  const shards = validateSpec({ ...combined, engineOptions: { ...combined.engineOptions, historyShards: 0 } });
  assert.ok(shards.some((e) => e.includes('History shards')));
  assert.ok(validateSpec({ ...combined, engineOptions: { ...combined.engineOptions, historyShards: 16385 } }).some((e) => e.includes('History shards')));
  const oidcOnly = validateSpec({ ...combined, engineOptions: { ...combined.engineOptions, ui: { publicHttps: false, oidc: { issuer: 'https://login.example', clientId: 'x' } } } });
  assert.ok(oidcOnly.some((e) => e.includes('needs public HTTPS')));
  const publicOnly = validateSpec({ ...combined, engineOptions: { ...combined.engineOptions, ui: { publicHttps: true } } });
  assert.ok(publicOnly.some((e) => e.includes('requires OIDC')));
  const badIssuer = validateSpec({ ...combined, engineOptions: { ...combined.engineOptions, ui: { publicHttps: true, oidc: { issuer: 'http://login.example', clientId: '' } } } });
  assert.ok(badIssuer.some((e) => e.includes('https://')));
  assert.ok(badIssuer.some((e) => e.includes('client ID')));
  const noUI = validateSpec({ ...withReplicas(combined, undefined, 'ui', 0), engineOptions: { ...combined.engineOptions, ui: { publicHttps: true, oidc: { issuer: 'https://login.example', clientId: 'x' } } } });
  assert.ok(noUI.some((e) => e.includes('at least one Web UI instance')));
  assert.ok(validateSpec({ ...combined, exposure: { public: true, ipType: 'IPV6', allowlist: [] } }).some((e) => e.includes('frontend is private')));
  assert.ok(validateSpec({ ...combined, engineConfig: 'x: 1' }).some((e) => e.includes('engine configuration')));
  assert.ok(validateSpec({ ...combined, tls: { mode: 'disabled', ca: 'selfSigned' } }).some((e) => e.includes('requireTLS')));
  assert.ok(validateSpec({ ...combined, engineOptions: { ...combined.engineOptions, persistence: { postgres: { cluster: 'appdb', database: 'Bad-Name' } } } }).some((e) => e.startsWith('Persistence database')));
  assert.ok(validateSpec({ ...combined, engineOptions: { ...combined.engineOptions, persistence: { postgres: { cluster: 'appdb', database: 'same', visibilityDatabase: 'same' } } } }).some((e) => e.includes('must differ')));
});

test('persistence, history shards and topology are immutable once persisted', () => {
  assert.deepEqual(validateSpec(combined, combined), []);
  assert.ok(validateSpec({ ...combined, engineOptions: { ...combined.engineOptions, historyShards: 1024 } }, combined).some((e) => e.includes('History shards are immutable')));
  assert.ok(validateSpec({ ...combined, engineOptions: { ...combined.engineOptions, persistence: { postgres: { cluster: 'other' } } } }, combined).some((e) => e.includes('persistence is immutable')));
  assert.ok(validateSpec({ ...split, engineOptions: combined.engineOptions }, combined).some((e) => e.includes('topology type is immutable')));
});

test('component validation enforces the topology component set and replica bounds', () => {
  assert.deepEqual(validateComponents(split, topology, noResourceErrors), { topology: [], resources: [] });
  assert.deepEqual(validateComponents(combined, undefined, noResourceErrors), { topology: [], resources: [] });
  const tooMany = validateComponents(withReplicas(split, topology, 'history', 10), topology, noResourceErrors);
  assert.deepEqual(tooMany.topology, ['History: At most 9 instances allowed.']);
  const none = validateComponents(withReplicas(split, topology, 'frontend', 0), topology, noResourceErrors);
  assert.deepEqual(none.topology, ['Frontend: At least 1 instance required.']);
  const leftover = validateComponents({ ...combined, components: { ...combined.components, extra: { ...combined.components.extra, history: { replicas: 1, resources } } } }, undefined, noResourceErrors);
  assert.deepEqual(leftover.topology, ['History is not part of the combined topology.']);
  const uiOff = validateComponents(withReplicas(combined, undefined, 'ui', 0), undefined, noResourceErrors);
  assert.deepEqual(uiOff, { topology: [], resources: [] });
  const engineUsed = validateComponents({ ...combined, components: { ...combined.components, engine: { replicas: 1, resources } } }, undefined, noResourceErrors);
  assert.ok(engineUsed.topology[0]!.includes('components.extra'));
  const checked = validateComponents(combined, undefined, (r, noDisk, name) => [`${name}:${noDisk}:${r.minDisk}`]);
  assert.deepEqual(checked.resources, ['Server: server:false:1', 'Web UI: ui:false:1']);
  assert.deepEqual(limitsFor(topology, 'history'), { minReplicas: 1, maxReplicas: 9 });
  assert.deepEqual(limitsFor(topology, 'ui'), { minReplicas: 0, maxReplicas: 3 });
  assert.deepEqual(limitsFor(undefined, 'server'), { minReplicas: 1, maxReplicas: 5 });
});

test('summaries and review rows describe both topologies', () => {
  assert.equal(specSummary(combined), 'Combined · 1 server · Web UI');
  assert.equal(specSummary(split), 'Split · 3 history, 2 matching, 2 frontend, 1 worker · Web UI');
  assert.equal(specSummary(withReplicas(withReplicas(combined, undefined, 'ui', 0), undefined, 'server', 3)), 'Combined · 3 server · no Web UI');
  const rows = Object.fromEntries(configurationReviewRows(combined, 'order-flows'));
  assert.equal(rows['PostgreSQL source'], 'appdb');
  assert.equal(rows.Databases, 'temporal_order_flows · temporal_order_flows_visibility');
  assert.equal(rows['History shards'], '512 (immutable)');
  assert.equal(rows['Client certificates'], 'required (mTLS)');
  assert.equal(rows.Namespaces, 'default (72h)');
  assert.equal(rows['Web UI'], 'private to the project network');
  assert.deepEqual(resourceRows(split).map(([label]) => label), ['History resources', 'Matching resources', 'Frontend resources', 'Worker resources', 'Web UI resources']);
  assert.match(resourceRows(combined)[0]![1], /^1 × 1–1 vCPU shared, 1–1 GiB RAM, 1–1 GiB runtime disk$/);
});

test('merge patch carries only changed components, whole options with nulls, and explains consequences', () => {
  assert.deepEqual(specPatch(split, split), {});
  const scaled = withReplicas(split, topology, 'history', 5);
  assert.deepEqual(specPatch(split, scaled), { components: { extra: { history: { replicas: 5 } } } });
  const declared = writeOptions(split, { ...readOptions(split), namespaces: [{ name: 'orders', retention: '168h' }], ui: { publicHttps: true, oidc: { issuer: 'https://login.example', clientId: 'temporal' } } });
  const patch = specPatch(split, declared);
  assert.deepEqual(patch.components, undefined);
  assert.deepEqual(patch.engineOptions, { persistence: { postgres: { cluster: 'appdb' } }, historyShards: 512, frontend: { requireClientAuth: true }, namespaces: [{ name: 'orders', retention: '168h' }], ui: { publicHttps: true, oidc: { issuer: 'https://login.example', clientId: 'temporal' } } });
  const reverted = specPatch(declared, split);
  assert.deepEqual(reverted.engineOptions, { persistence: { postgres: { cluster: 'appdb' } }, historyShards: 512, frontend: { requireClientAuth: true }, namespaces: null, ui: { publicHttps: false, oidc: null } });
  assert.deepEqual(readOptions(applyMergePatch(declared, reverted) as Schemas['ClusterSpec']), readOptions(split));
  assert.deepEqual(specPatch(split, { ...split, monitoring: { enabled: false } }), { monitoring: { enabled: false } });

  const changes = disruptions(split, declared);
  assert.ok(changes.some((c) => c.includes('orders will be registered')));
  assert.ok(changes.some((c) => c.includes('public HTTPS route')));
  const back = disruptions(declared, split);
  assert.ok(back.some((c) => c.includes('no longer managed')));
  assert.ok(back.some((c) => c.includes('public Web UI route is removed')));
  assert.ok(disruptions(split, scaled).some((c) => c.includes('History scales from 3 to 5')));
  const auth = writeOptions(split, { ...readOptions(split), frontend: { requireClientAuth: false } });
  assert.ok(disruptions(split, auth).some((c) => c.includes('without a client certificate')));
  const resized = { ...split, components: { ...split.components, extra: { ...split.components.extra, worker: { replicas: 1, resources: { ...resources, maxRam: 2 } } } } };
  assert.ok(disruptions(split, resized).some((c) => c.includes('Worker resources change') && c.includes('single instance')));
  assert.ok(disruptions(split, withReplicas(split, topology, 'ui', 0)).some((c) => c.includes('Web UI is removed')));
});

test('binding condition JSON becomes a persistence summary', () => {
  assert.equal(bindingStatus([]), undefined);
  const at = '2026-01-01T00:00:00Z';
  assert.deepEqual(bindingStatus([{ type: 'PostgreSQLBinding', status: 'False', reason: 'SourceUnavailable', message: 'Waiting for bound PostgreSQL source', lastTransitionTime: at }]), { waiting: true, message: 'Waiting for bound PostgreSQL source' });
  assert.deepEqual(bindingStatus([{ type: 'PostgreSQLBinding', status: 'True', reason: 'Bound', message: '{"sourceNamespace":"default","sourceCluster":"appdb","shared":true,"consumerCount":2}', lastTransitionTime: at }]), { waiting: false, sourceNamespace: 'default', sourceCluster: 'appdb', shared: true, consumerCount: 2 });
  assert.deepEqual(bindingStatus([{ type: 'PostgreSQLBinding', status: 'True', reason: 'Bound', message: 'not json', lastTransitionTime: at }]), { waiting: false, sourceNamespace: undefined, sourceCluster: undefined, shared: undefined, consumerCount: undefined });
});

test('client certificates flag expiry and a superseded issuing CA from the journal', () => {
  const now = Date.parse('2026-06-01T00:00:00Z');
  const base: ClientCertificate = { serial: '1', commonName: 'orders', fingerprintSha256: 'aa'.repeat(32), issuedAt: '2026-01-01T00:00:00Z', notAfter: '2027-01-01T00:00:00Z', caFingerprintSha256: 'ca-old', currentCa: false };
  const newer: ClientCertificate = { ...base, serial: '2', issuedAt: '2026-05-01T00:00:00Z', caFingerprintSha256: 'ca-new', currentCa: true };
  const expired: ClientCertificate = { ...base, serial: '3', notAfter: '2026-02-01T00:00:00Z' };
  assert.equal(certificateState(base, now), 'superseded-ca');
  assert.equal(certificateState(newer, now), 'valid');
  assert.equal(certificateState(expired, now), 'expired');
  // Rotation with no new issuance still invalidates the newest journal entry.
  assert.equal(certificateState({ ...newer, currentCa: false }, now), 'superseded-ca');
  assert.equal(certificateState({ ...base, currentCa: true }, now), 'valid');
  assert.equal(shortFingerprint('sha256:' + 'ab'.repeat(32)), 'abababababababababab…');
  assert.equal(shortFingerprint('short'), 'short');
  assert.equal(ttlError(720), undefined);
  assert.equal(ttlError(0)?.includes('1 to 9528'), true);
  assert.equal(ttlError(9529)?.includes('397 days'), true);
  const issued: IssuedClientCertificate = { ...newer, certificatePem: 'CERT', privateKeyPem: 'KEY', caPem: 'CA', frontendAddress: 'wf-frontend-1:7233', serverName: 'wf-frontend' };
  assert.deepEqual(certificateFiles(issued), [{ name: 'orders.crt', content: 'CERT' }, { name: 'orders.key', content: 'KEY' }, { name: 'temporal-ca.crt', content: 'CA' }]);
  assert.deepEqual(certificateFiles({ ...issued, commonName: 'svc one/two' }).map((f) => f.name), ['svc-one-two.crt', 'svc-one-two.key', 'temporal-ca.crt']);
  assert.equal(cliExample(issued), 'temporal --address wf-frontend-1:7233 --tls-server-name wf-frontend --tls-cert-path orders.crt --tls-key-path orders.key --tls-ca-path temporal-ca.crt workflow list');
});
