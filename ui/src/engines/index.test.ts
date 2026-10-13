// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Schemas } from '../api/client';
import type { EngineUI } from './types';
import { defaultLogProcess, defaultProvider, detailProvider, engines, engineTopologySummary, hasCapability, monitoringPanels, providerProcesses, publicExposureMetadata, replicaLimits, supportsOperation, tabAvailable, validateReplicas } from './index.ts';

const resources: Schemas['Resources'] = { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 1, maxRam: 2, minDisk: 2, maxDisk: 20 };
const spec: Schemas['ClusterSpec'] = { provider: 'unregistered', version: '1.0', topology: { type: 'cluster' }, components: { engine: { replicas: 3, resources }, extra: { worker: { replicas: 2, resources }, disabled: { replicas: 0, resources } } } };

test('default provider selects psmdb explicitly rather than catalog order', () => {
  assert.equal(defaultProvider([{ name: 'elasticsearch' }, { name: 'psmdb' }])?.name, 'psmdb');
  assert.equal(defaultProvider([{ name: 'other' }])?.name, 'other');
  assert.equal(defaultProvider([]), undefined);
});

test('older provider metadata retains Mongo capabilities, operations and processes', () => {
  for (const capability of ['databaseUsers', 'systemUsers', 'backups', 'restores', 'fcv', 'balancer', 'pitr']) assert.equal(hasCapability({}, capability), true);
  assert.equal(supportsOperation({}, 'restart'), true);
  assert.equal(supportsOperation({}, 'rotate-system-user'), true);
  assert.equal(supportsOperation({}, 'unknown'), false);
  assert.deepEqual(providerProcesses({}), ['mongod', 'mongos', 'pbm-agent', 'agent']);
  assert.equal(tabAvailable(undefined, 'users'), true);
});

test('explicit capabilities and operations gate generic tabs and durable actions', () => {
  const provider = { capabilities: ['systemUsers', 'backups'], operations: ['backup'], processes: ['broker', 'agent'] };
  assert.equal(tabAvailable(provider, 'users'), true);
  assert.equal(tabAvailable(provider, 'backups'), true);
  assert.equal(tabAvailable(provider, 'restores'), false);
  assert.equal(tabAvailable({ capabilities: [] }, 'users'), false);
  assert.equal(tabAvailable(provider, 'topology'), true);
  for (const capability of ['fcv', 'balancer', 'pitr', 'databaseUsers']) assert.equal(hasCapability(provider, capability), false);
  assert.equal(supportsOperation(provider, 'backup'), true);
  assert.equal(supportsOperation(provider, 'restart'), false);
  assert.equal(supportsOperation({ capabilities: [] }, 'restart'), false);
  assert.deepEqual(providerProcesses(provider), ['broker', 'agent']);
  assert.deepEqual(providerProcesses({ capabilities: [], processes: [] }), ['agent']);
});

test('engine descriptors supply shared form metadata and validate the complete spec', () => {
  const descriptor: EngineUI = {
    label: 'Test',
    publicPort: 1234,
    publicPortLabel: 'Client',
    engineConfigLabel: 'Test configuration',
    engineConfigPlaceholder: 'setting = value',
    validateSpec: (draft) => draft.engineOptions?.enabled === true ? [] : ['Enable the engine option.'],
  };
  engines.test = descriptor;
  try {
    assert.equal(engines.test.publicPort, 1234);
    assert.equal(engines.test.publicPortLabel, 'Client');
    assert.equal(engines.test.engineConfigLabel, 'Test configuration');
    assert.equal(engines.test.engineConfigPlaceholder, 'setting = value');
    assert.deepEqual(engines.test.validateSpec?.({ ...spec, provider: 'test' }), ['Enable the engine option.']);
    assert.deepEqual(engines.test.validateSpec?.({ ...spec, provider: 'test', engineOptions: { enabled: true } }), []);
  } finally {
    delete engines.test;
  }
});

test('exposure metadata supports static ports and reacts to the current TLS spec', () => {
  assert.deepEqual(publicExposureMetadata(engines.psmdb, spec), { publicPort: 27017, publicPortLabel: undefined });
  assert.deepEqual(publicExposureMetadata(undefined, spec), { publicPort: undefined, publicPortLabel: undefined });
  const descriptor: EngineUI = {
    label: 'Test',
    publicPort: (draft) => draft.tls?.mode === 'disabled' ? 1234 : 1235,
    publicPortLabel: (draft) => draft.tls?.mode === 'disabled' ? 'Plaintext' : 'Secure',
  };
  assert.deepEqual(publicExposureMetadata(descriptor, spec), { publicPort: 1235, publicPortLabel: 'Secure' });
  assert.deepEqual(publicExposureMetadata(descriptor, { ...spec, tls: { mode: 'disabled', ca: 'selfSigned' } }), { publicPort: 1234, publicPortLabel: 'Plaintext' });
});

test('detail catalog fallback preserves Mongo access without granting other engines unsupported capabilities', () => {
  const mongo = detailProvider('psmdb', undefined);
  assert.equal(mongo, undefined);
  for (const tab of ['users', 'backups', 'restores']) assert.equal(tabAvailable(mongo, tab), true);
  assert.equal(supportsOperation(mongo, 'restart'), true);

  const generic = detailProvider('other', undefined);
  assert.equal(generic?.name, 'other');
  for (const tab of ['users', 'backups', 'restores']) assert.equal(tabAvailable(generic, tab), false);
  for (const capability of ['fcv', 'pitr', 'balancer']) assert.equal(hasCapability(generic, capability), false);
  assert.equal(supportsOperation(generic, 'restart'), false);
  assert.equal(tabAvailable(generic, 'topology'), true);
  assert.deepEqual(providerProcesses(generic), ['agent']);

  const described: Schemas['Provider'] = { ...generic!, capabilities: ['backups'], processes: ['broker'] };
  assert.equal(detailProvider('other', described), described);
  assert.equal(tabAvailable(detailProvider('other', described), 'backups'), true);
});

test('Mongo log defaults follow member roles while other engines use their first described process', () => {
  const mongo = detailProvider('psmdb', undefined);
  assert.equal(defaultLogProcess('psmdb', 'router', mongo), 'mongos');
  for (const role of ['primary', 'secondary', 'config-primary', 'unknown', undefined] as const) assert.equal(defaultLogProcess('psmdb', role, mongo), 'mongod');
  const generic = { capabilities: [], processes: ['broker', 'helper'] };
  assert.equal(defaultLogProcess('other', 'router', generic), 'broker');
  assert.equal(defaultLogProcess('other', 'secondary', generic), 'broker');
  assert.equal(defaultLogProcess('other', undefined, detailProvider('other', undefined)), 'agent');
});

test('Mongo panels come from the registry while every engine gets host panels', () => {
  assert.equal(engines.psmdb?.label, 'Percona Server for MongoDB');
  assert.equal(engines.psmdb?.publicPort, 27017);
  assert.equal(engines.psmdb?.detailTabs, undefined);
  assert.deepEqual(monitoringPanels('psmdb').map((panel) => panel.title), ['Operations', 'Connections', 'Replication lag', 'Cache used', 'CPU', 'Memory used', 'Disk used']);
  assert.deepEqual(monitoringPanels('unregistered').map((panel) => panel.title), ['CPU', 'Memory used', 'Disk used']);
});

test('non-Mongo summaries use the extension or count all configured nodes', () => {
  const cluster: Schemas['Cluster'] = { name: 'example', namespace: 'default', generation: 1, createdAt: '', updatedAt: '', spec, status: { phase: 'Ready', observedGeneration: 1, members: [], conditions: [] } };
  assert.equal(engineTopologySummary(cluster), '5 nodes');
  engines.test = { label: 'Test', topologySummary: () => 'Custom topology' };
  try {
    assert.equal(engineTopologySummary({ ...cluster, spec: { ...cluster.spec, provider: 'test' } }), 'Custom topology');
    assert.equal(engineTopologySummary({ ...cluster, spec: { ...cluster.spec, provider: 'psmdb' } }), undefined);
  } finally {
    delete engines.test;
  }
});

test('replica validation follows provider limits instead of Mongo voting sizes', () => {
  const topology: Schemas['ProviderTopology'] = { type: 'cluster', displayName: 'Cluster', defaults: spec, componentSchemas: {}, limits: { engine: { minReplicas: 2, maxReplicas: 6 }, worker: { allowedReplicas: [0, 2, 4] } } };
  const limits = replicaLimits(topology, 'engine');
  assert.deepEqual(validateReplicas(2, limits), []);
  assert.equal(validateReplicas(1, limits).length, 1);
  assert.equal(validateReplicas(7, limits).length, 1);
  assert.equal(validateReplicas(2.5, limits).length, 1);
  assert.deepEqual(validateReplicas(0, replicaLimits(topology, 'worker')), []);
  assert.equal(validateReplicas(3, replicaLimits(topology, 'worker')).length, 1);
  assert.deepEqual(replicaLimits(undefined, 'engine'), {});
});
