// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Schemas } from '../../api/client';
import { buildEngineOptions, managementIsPublic, parseDefinition, parseOptionalLimit, parsePriority, parseUserTags, quorumFailureTolerance, selectedPluginGroups, validateConfigOverrides, validateRabbitMQSpec } from './helpers.ts';

const spec: Schemas['ClusterSpec'] = {
  provider: 'rabbitmq', version: '4.3.6', topology: { type: 'cluster' },
  components: { engine: { replicas: 3, resources: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 0.5, maxRam: 2, minDisk: 5, maxDisk: 20 } } },
};

test('engine options include protocol and management plugins in canonical order', () => {
  assert.deepEqual(buildEngineOptions(['stream', 'shovel', 'shovel'], true), {
    plugins: ['rabbitmq_shovel', 'rabbitmq_shovel_management', 'rabbitmq_stream', 'rabbitmq_stream_management'],
    managementUI: { public: true },
  });
  assert.deepEqual(buildEngineOptions([], false), { plugins: [], managementUI: { public: false } });
  assert.deepEqual(buildEngineOptions(['federation'], false).plugins, ['rabbitmq_federation', 'rabbitmq_federation_management']);
});

test('wizard reads optional plugin groups and public management safely', () => {
  assert.deepEqual(selectedPluginGroups(undefined), []);
  assert.deepEqual(selectedPluginGroups({ plugins: ['rabbitmq_stream_management', 'unknown'] }), ['stream']);
  assert.equal(managementIsPublic(undefined), false);
  assert.equal(managementIsPublic({ managementUI: null }), false);
  assert.equal(managementIsPublic({ managementUI: { public: true } }), true);
  assert.equal(managementIsPublic({ managementUI: { public: 'true' } }), false);
});

test('quorum failure tolerance follows majority formula for supported counts', () => {
  assert.deepEqual([1, 3, 5, 7].map(quorumFailureTolerance), [0, 1, 2, 3]);
});

test('RabbitMQ validation accepts all supported node counts and constructed options', () => {
  for (const replicas of [1, 3, 5, 7]) assert.deepEqual(validateRabbitMQSpec({ ...spec, components: { engine: { ...spec.components.engine, replicas } }, engineOptions: { ...buildEngineOptions(['stream', 'federation', 'shovel'], false) } }), []);
  for (const replicas of [0, 2, 4, 8, 2.5]) assert.match(validateRabbitMQSpec({ ...spec, components: { engine: { ...spec.components.engine, replicas } } })[0]!, /node count/);
});

test('RabbitMQ validation rejects unknown options and malformed plugin or UI settings', () => {
  for (const engineOptions of [{ unexpected: true }, { plugins: 'rabbitmq_shovel' }, { plugins: ['unknown'] }, { plugins: ['rabbitmq_shovel', 'rabbitmq_shovel'] }, { plugins: [42] }, { managementUI: [] }, { managementUI: { public: true, port: 80 } }, { managementUI: { public: 'true' } }]) assert.ok(validateRabbitMQSpec({ ...spec, engineOptions }).length > 0);
});

test('rabbitmq.conf overrides allow comments and settings but reject managed keys', () => {
  assert.deepEqual(validateConfigOverrides('# comment\r\n\nconsumer_timeout = 1800000\ncluster_name = orders'), []);
  assert.match(validateConfigOverrides('consumer_timeout: 100')[0]!, /Line 1.*key = value/);
  for (const key of ['listeners.ssl.default', 'ssl_options.cacertfile', 'management.tcp.port', 'management.ssl.port', 'prometheus.tcp.port', 'cluster_formation.peer_discovery_backend', 'vm_memory_high_watermark.absolute', 'total_memory_available_override_value', 'disk_free_limit.absolute', 'log.console', 'default_user', 'default_pass', 'loopback_users.guest', 'distribution.listener.port_range.min', 'default_queue_type', 'quorum_queue.initial_cluster_size', 'management.load_definitions', 'load_definitions', 'definitions.import_backend', 'auth_backends.1', 'ssl_cert_login_from']) assert.match(validateConfigOverrides(`${key} = 1`)[0]!, /managed/);
  assert.match(validateRabbitMQSpec({ ...spec, engineConfig: 'listeners.tcp.default = 5672' })[0]!, /managed/);
});

test('policy definitions must be JSON objects', () => {
  assert.deepEqual(parseDefinition('{"message-ttl":60000}'), { 'message-ttl': 60000 });
  for (const value of ['null', '[]', '"text"', '42', '{']) assert.throws(() => parseDefinition(value));
});

test('limits and priority validate numeric input without truncation', () => {
  assert.equal(parseOptionalLimit('', 'Queues'), undefined);
  assert.equal(parseOptionalLimit('-1', 'Queues'), -1);
  assert.equal(parseOptionalLimit('0', 'Queues'), 0);
  assert.equal(parseOptionalLimit('100', 'Connections'), 100);
  for (const value of ['-2', '1.5', 'abc', 'Infinity', '9007199254740992']) assert.throws(() => parseOptionalLimit(value, 'Queues'));
  assert.equal(parsePriority('0'), 0);
  assert.equal(parsePriority('10'), 10);
  for (const value of ['', '-1', '1.5', 'abc']) assert.throws(() => parsePriority(value));
});

test('user tags validate against the management API enum and remove duplicates', () => {
  assert.deepEqual(parseUserTags(' management, monitoring\nmanagement '), ['management', 'monitoring']);
  assert.deepEqual(parseUserTags(''), []);
  assert.throws(() => parseUserTags('administrator,unknown'), /Unsupported user tags: unknown/);
});
