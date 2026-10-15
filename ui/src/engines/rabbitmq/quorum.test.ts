// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import test from 'node:test';
import assert from 'node:assert/strict';
import { register } from 'node:module';
import { createElement } from 'react';
import { renderToString } from 'react-dom/server';
import { MemoryRouter } from 'react-router-dom';
import type { Schemas } from '../../api/client';

register('../../../test/tsx-loader.mjs', import.meta.url);
// These imports must follow loader registration: Node cannot statically load TSX.
const { QuorumCallout } = await import('./QuorumCallout.tsx');
const { QuorumRecoveryPanel } = await import('./QuorumRecovery.tsx');

const cluster: Schemas['Cluster'] = {
  name: 'orders', namespace: 'default', generation: 1, createdAt: '', updatedAt: '',
  spec: { provider: 'rabbitmq', version: '4.3.6', topology: { type: 'cluster' }, components: { engine: { replicas: 3, resources: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 0.5, maxRam: 2, minDisk: 5, maxDisk: 20 } } } },
  status: { phase: 'Updating', observedGeneration: 1, members: [{ name: 'ordersm2', role: 'broker', set: 'rabbitmq', state: 'LEAVING_BLOCKED', health: false }], conditions: [
    { type: 'QuorumCriticalQueues', status: 'True', reason: 'RollingRestartBlocked', message: 'Queue orders in vhost / has only rabbit@ordersm0.', lastTransitionTime: '' },
    { type: 'QuorumReplicaRecovery', status: 'True', reason: 'ManagementAPI', message: `curl --user admin --cacert ca.pem -X POST --data '{"node":"rabbit@ordersm1"}' 'https://ordersm0:15671/api/queues/quorum/%2F/orders/replicas/add'`, lastTransitionTime: '' },
  ] },
};
const base = '/namespaces/default/clusters/orders';

test('overview quorum callout names the queue and links to explicit recovery', () => {
  const html = renderToString(createElement(MemoryRouter, null, createElement(QuorumCallout, { cluster, base })));
  assert.match(html, /Rolling restart blocked by queue quorum/);
  assert.match(html, /Queue orders in vhost/);
  assert.match(html, /href="\/namespaces\/default\/clusters\/orders\/queue-recovery"/);
  assert.match(html, /Grow queue replicas safely/);
  const healthy = { ...cluster, status: { ...cluster.status, conditions: [] } };
  assert.equal(renderToString(createElement(MemoryRouter, null, createElement(QuorumCallout, { cluster: healthy, base }))), '');
});

test('recovery panel exposes real native commands, safety guidance and blocked leaving broker', () => {
  const html = renderToString(createElement(MemoryRouter, null, createElement(QuorumRecoveryPanel, { cluster, base })));
  for (const text of ['Targeted Management API commands', 'Copy recovery procedure', 'curl --user admin --cacert ca.pem', 'replicas/add', '%2F/orders', 'rabbit@ordersm1', 'online majority', 'voting membership', 'bypass Karakoram RBAC', 'Removal blocked: ordersm2', 'still present and leaving, not healthy']) assert.ok(html.includes(text), text);
  assert.match(html, /href="\/namespaces\/default\/clusters\/orders"/);
  assert.doesNotMatch(html, /rabbitmq-queues add_member/);
});

test('no blocker inventory does not imply broker scale-up grows queues', () => {
  const healthy = { ...cluster, status: { ...cluster.status, conditions: [], members: [] } };
  const html = renderToString(createElement(MemoryRouter, null, createElement(QuorumRecoveryPanel, { cluster: healthy, base })));
  assert.match(html, /Adding brokers does not grow existing quorum queue groups/);
  assert.doesNotMatch(html, /Targeted Management API commands|Removal blocked/);
});
