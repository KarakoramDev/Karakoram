// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { register } from 'node:module';
import type { Schemas } from '../../api/client';
import { rabbitmqUI } from './index.ts';

register('../../../test/tsx-loader.mjs', import.meta.url);
// Static imports run before register; registry/TSX imports must wait for the loader.
await import('../register.ts');
const { engines } = await import('../index.ts');
const { elasticsearchUI } = await import('../elasticsearch/index.tsx');

const cluster: Schemas['Cluster'] = {
  name: 'orders', namespace: 'default', generation: 1, createdAt: '', updatedAt: '',
  spec: { provider: 'rabbitmq', version: '4.3.6', topology: { type: 'cluster' }, components: { engine: { replicas: 3, resources: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 0.5, maxRam: 2, minDisk: 5, maxDisk: 20 } } } },
  status: { phase: 'Ready', observedGeneration: 1, members: [], conditions: [] },
};

test('registration attaches all built-in engines to the shared registry', () => {
  assert.deepEqual(Object.keys(engines).sort(), ['elasticsearch', 'psmdb', 'rabbitmq']);
  assert.equal(engines.rabbitmq, rabbitmqUI);
  assert.equal(engines.elasticsearch, elasticsearchUI);
});

test('RabbitMQ extension exposes its wizard, access tab and topology summary', () => {
  assert.equal(engines.rabbitmq, rabbitmqUI);
  assert.equal(rabbitmqUI.label, 'RabbitMQ');
  assert.ok(rabbitmqUI.wizardSection);
  assert.deepEqual(rabbitmqUI.detailTabs?.map((tab) => [tab.id, tab.label]), [['access', 'Access'], ['queue-recovery', 'Queue recovery']]);
  assert.equal(rabbitmqUI.topologySummary?.(cluster), '3-node RabbitMQ cluster');
  assert.equal(rabbitmqUI.topologySummary?.({ ...cluster, spec: { ...cluster.spec, components: { engine: { ...cluster.spec.components.engine, replicas: 1 } } } }), '1-node RabbitMQ cluster');
  assert.equal(rabbitmqUI.engineConfigLabel, 'rabbitmq.conf overrides');
  assert.deepEqual(rabbitmqUI.validateSpec?.(cluster.spec), []);
});

test('monitoring uses every RabbitMQ catalog series without duplicates', () => {
  const names = rabbitmqUI.monitoringPanels?.flatMap((panel) => panel.names) ?? [];
  assert.equal(names.length, 17);
  assert.equal(new Set(names).size, 17);
  assert.deepEqual([...names].sort(), [
    'rabbitmq.up', 'rabbitmq.uptimeSeconds', 'rabbitmq.connections', 'rabbitmq.channels',
    'rabbitmq.consumers', 'rabbitmq.queues', 'rabbitmq.messagesReady', 'rabbitmq.messagesUnacked',
    'rabbitmq.published', 'rabbitmq.delivered', 'rabbitmq.memoryAlarm', 'rabbitmq.diskAlarm',
    'rabbitmq.residentMemoryBytes', 'rabbitmq.memoryLimitBytes', 'rabbitmq.diskFreeBytes',
    'rabbitmq.diskFreeLimitBytes', 'rabbitmq.clusterMembersRunning',
  ].sort());
  assert.equal(rabbitmqUI.monitoringPanels?.find((panel) => panel.title === 'Publish & delivery rate')?.unit, 'messages/s');
  const alarms = rabbitmqUI.monitoringPanels?.find((panel) => panel.title === 'Memory & disk alarms');
  assert.equal(alarms?.format?.(0), 'Clear');
  assert.equal(alarms?.format?.(1), 'Active');
});

test('public exposure selects AMQPS with TLS and AMQP only when disabled', () => {
  assert.equal(typeof rabbitmqUI.publicPort, 'function');
  assert.equal(typeof rabbitmqUI.publicPortLabel, 'function');
  if (typeof rabbitmqUI.publicPort !== 'function' || typeof rabbitmqUI.publicPortLabel !== 'function') return;
  for (const mode of ['requireTLS', 'preferTLS'] as const) {
    const spec = { ...cluster.spec, tls: { mode, ca: 'selfSigned' as const } };
    assert.equal(rabbitmqUI.publicPort(spec), 5671);
    assert.equal(rabbitmqUI.publicPortLabel(spec), 'AMQPS');
  }
  const spec = { ...cluster.spec, tls: { mode: 'disabled' as const, ca: 'selfSigned' as const } };
  assert.equal(rabbitmqUI.publicPort(spec), 5672);
  assert.equal(rabbitmqUI.publicPortLabel(spec), 'AMQP');
});
