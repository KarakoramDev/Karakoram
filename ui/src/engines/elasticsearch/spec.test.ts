// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { test } from 'node:test';
import assert from 'node:assert/strict';
import type { Schemas } from '../../api/client';
import {
  CLUSTER_SIZES,
  DEFAULT_RESOURCES,
  activeComponents,
  defaultComponent,
  disruptions,
  formatIndexPrivileges,
  heapMB,
  limitsFor,
  memberKind,
  parseIndexPrivileges,
  parseRoleDescriptors,
  readOptions,
  replicasOf,
  specPatch,
  specSummary,
  validateComponents,
  validatePipelines,
  validateSpec,
  withReplicas,
  writeOptions,
} from './spec.ts';

const resources: Schemas['Resources'] = { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 2, maxRam: 2, minDisk: 5, maxDisk: 20 };
const base: Schemas['ClusterSpec'] = { provider: 'elasticsearch', version: '9.5.5', topology: { type: 'cluster' }, components: { engine: { replicas: 3, resources } } };
const dedicated: Schemas['ClusterSpec'] = {
  ...base,
  topology: { type: 'dedicatedMasters' },
  components: { engine: { replicas: 4, resources }, extra: { master: { replicas: 3, resources: DEFAULT_RESOURCES.master }, kibana: { replicas: 2, resources: DEFAULT_RESOURCES.kibana }, logstash: { replicas: 0, resources: DEFAULT_RESOURCES.logstash } } },
};
const topology: Schemas['ProviderTopology'] = {
  type: 'dedicatedMasters',
  displayName: 'Dedicated masters',
  defaults: dedicated,
  componentSchemas: {},
  limits: { engine: { minReplicas: 1, maxReplicas: 20 }, master: { allowedReplicas: [3, 5] }, kibana: { minReplicas: 0, maxReplicas: 3 } },
};
const noResourceErrors = () => [];

test('options round-trip through engineOptions with server-compatible keys', () => {
  const empty = readOptions(base);
  assert.deepEqual(empty, { kibana: { public: false, settings: undefined }, apm: { public: false, apiKeys: true }, logstash: { pipelines: [], persistentQueue: false }, elasticsearch: { settings: undefined } });
  const spec = writeOptions(base, { ...empty, kibana: { public: true }, apm: { public: true, apiKeys: false }, logstash: { persistentQueue: true, pipelines: [{ id: 'main', config: 'input {}' }] } });
  assert.deepEqual(spec.engineOptions, { kibana: { public: true }, apm: { public: true, apiKeys: false }, logstash: { persistentQueue: true, pipelines: [{ id: 'main', config: 'input {}' }] }, elasticsearch: {} });
  assert.deepEqual(readOptions(spec).logstash.pipelines, [{ id: 'main', config: 'input {}' }]);
  assert.equal(readOptions({ engineOptions: { apm: { apiKeys: 'yes' }, logstash: { pipelines: 'nope' } } }).apm.apiKeys, true);
  assert.deepEqual(readOptions({ engineOptions: 'garbage' as unknown as Record<string, unknown> }).logstash.pipelines, []);
});

test('pipeline validation mirrors the server rules', () => {
  assert.deepEqual(validatePipelines([{ id: 'main', config: 'input {}' }]), []);
  assert.equal(validatePipelines([{ id: 'Main', config: 'x' }]).length, 1);
  assert.equal(validatePipelines([{ id: '1abc', config: 'x' }]).length, 1);
  assert.equal(validatePipelines([{ id: 'a'.repeat(33), config: 'x' }]).length, 1);
  assert.equal(validatePipelines([{ id: 'a_b-c', config: 'x' }]).length, 0);
  assert.equal(validatePipelines([{ id: 'dup', config: 'x' }, { id: 'dup', config: 'y' }]).length, 1);
  assert.equal(validatePipelines([{ id: 'empty', config: '  \n' }]).length, 1);
  assert.equal(validatePipelines([{ id: 'big', config: 'x'.repeat(64 * 1024 + 1) }]).length, 1);
  const many = Array.from({ length: 11 }, (_, i) => ({ id: `p${i}`, config: 'x' }));
  assert.match(validatePipelines(many)[0] ?? '', /at most 10/);
});

test('spec validation ties options to enabled components and rejects engineConfig', () => {
  assert.deepEqual(validateSpec(base), []);
  assert.equal(validateSpec({ ...base, engineConfig: 'http.port: 1' }).length, 1);
  const publicKibana = writeOptions(base, { ...readOptions(base), kibana: { public: true } });
  assert.match(validateSpec(publicKibana)[0] ?? '', /Kibana/);
  const withKibana = withReplicas(publicKibana, undefined, 'kibana', 1);
  assert.deepEqual(validateSpec(withKibana), []);
  const orphanPipelines = writeOptions(base, { ...readOptions(base), logstash: { persistentQueue: false, pipelines: [{ id: 'main', config: 'x' }] } });
  assert.match(validateSpec(orphanPipelines)[0] ?? '', /Logstash is disabled/);
});

test('component helpers materialize defaults from the catalog topology', () => {
  assert.equal(replicasOf(base, 'kibana'), 0);
  const enabled = withReplicas(base, topology, 'kibana', 2);
  assert.equal(replicasOf(enabled, 'kibana'), 2);
  assert.deepEqual(enabled.components.extra?.kibana?.resources, DEFAULT_RESOURCES.kibana);
  assert.deepEqual(defaultComponent(undefined, 'apm'), { replicas: 0, resources: DEFAULT_RESOURCES.apm });
  assert.deepEqual(activeComponents(base), ['engine']);
  assert.deepEqual(activeComponents(dedicated), ['engine', 'master', 'kibana']);
  assert.deepEqual(limitsFor(topology, 'master'), { allowedReplicas: [3, 5], minReplicas: undefined, maxReplicas: undefined });
  assert.deepEqual(limitsFor(undefined, 'engine'), { minReplicas: 1, maxReplicas: CLUSTER_SIZES[CLUSTER_SIZES.length - 1] });
  assert.deepEqual(limitsFor(topology, 'logstash'), { minReplicas: 0, maxReplicas: 5 });
});

test('component validation covers replicas, RAM floors and injected resource checks', () => {
  assert.deepEqual(validateComponents(dedicated, topology, noResourceErrors), { topology: [], resources: [] });
  const tooManyMasters = withReplicas(dedicated, topology, 'master', 4);
  assert.match(validateComponents(tooManyMasters, topology, noResourceErrors).topology[0] ?? '', /Dedicated masters/);
  const noMasters: Schemas['ClusterSpec'] = { ...dedicated, components: { engine: dedicated.components.engine } };
  assert.equal(validateComponents(noMasters, topology, noResourceErrors).topology.length, 1);
  const tooManyNodes = withReplicas(base, undefined, 'engine', 10);
  assert.equal(validateComponents(tooManyNodes, undefined, noResourceErrors).topology.length, 1);
  const smallRam = withReplicas({ ...base, components: { engine: { replicas: 1, resources: { ...resources, minRam: 1, maxRam: 1 } } } }, undefined, 'apm', 1);
  const result = validateComponents(smallRam, undefined, (_r, stateless) => (stateless ? ['stateless'] : ['stateful']));
  assert.deepEqual(result.resources, ['Elasticsearch nodes: stateful', 'Elasticsearch nodes: minimum RAM must be at least 1.5 GiB.', 'APM Server: stateless']);
  // Disabled components are not validated for resources.
  assert.deepEqual(validateComponents(dedicated, topology, () => ['x']).resources, ['Dedicated masters: x', 'Elasticsearch nodes: x', 'Kibana: x']);
});

test('summaries describe nodes by role and enabled stack components', () => {
  assert.equal(specSummary(base), '3 nodes');
  assert.equal(specSummary(withReplicas(base, undefined, 'engine', 1)), '1 node');
  assert.equal(specSummary(dedicated), '3 masters + 4 data nodes · Kibana ×2');
  assert.equal(specSummary(withReplicas(withReplicas(base, undefined, 'logstash', 1), undefined, 'apm', 3)), '3 nodes · Logstash, APM Server ×3');
  assert.equal(memberKind({ role: 'master,data' }), 'master,data');
  assert.equal(memberKind({ role: 'kibana' }), 'kibana');
  assert.equal(memberKind({ role: 'primary' }), undefined);
});

test('heap sizing follows the agent rules', () => {
  assert.equal(heapMB('engine', 2), 768);
  assert.equal(heapMB('engine', 1), 512);
  assert.equal(heapMB('engine', 8), 4096);
  assert.equal(heapMB('engine', 100), 31744);
  assert.equal(heapMB('master', 1.5), 576);
  assert.equal(heapMB('kibana', 1.5), 921);
  assert.equal(heapMB('kibana', 0.5), 512);
  assert.equal(heapMB('logstash', 1.5), 768);
  assert.equal(heapMB('logstash', 0.5), 384);
  assert.equal(heapMB('apm', 1), undefined);
  assert.equal(heapMB('engine', Number.NaN), undefined);
});

test('index privileges parse and format symmetrically', () => {
  const text = 'logs-*,metrics-* read,view_index_metadata\n.secret all restricted';
  const parsed = parseIndexPrivileges(text);
  assert.deepEqual(parsed, [
    { names: ['logs-*', 'metrics-*'], privileges: ['read', 'view_index_metadata'], allowRestrictedIndices: false },
    { names: ['.secret'], privileges: ['all'], allowRestrictedIndices: true },
  ]);
  assert.equal(formatIndexPrivileges(parsed!), text);
  assert.deepEqual(parseIndexPrivileges('\n  \n'), []);
  assert.equal(parseIndexPrivileges('logs-*'), null);
  assert.equal(parseIndexPrivileges('logs-* read extra'), null);
  assert.equal(parseIndexPrivileges('logs-* read restricted more'), null);
});

test('API key role descriptors accept both snake and camel case restricted flags', () => {
  assert.deepEqual(parseRoleDescriptors('  '), {});
  assert.match(parseRoleDescriptors('{').error ?? '', /valid JSON/);
  assert.match(parseRoleDescriptors('[]').error ?? '', /object/);
  const parsed = parseRoleDescriptors('{"r":{"cluster":["monitor"],"indices":[{"names":["logs-*"],"privileges":["read"],"allow_restricted_indices":true}],"applications":[{"application":"kibana-.kibana","privileges":["read"],"resources":["*"]}]},"empty":{}}');
  assert.deepEqual(parsed.value, {
    r: { cluster: ['monitor'], indices: [{ names: ['logs-*'], privileges: ['read'], allowRestrictedIndices: true }], applications: [{ application: 'kibana-.kibana', privileges: ['read'], resources: ['*'] }] },
    empty: { cluster: [], indices: [], applications: [] },
  });
});

test('merge patch carries only changed components, whole engine options with nulls, TLS, exposure and monitoring', () => {
  assert.deepEqual(specPatch(dedicated, dedicated), {});
  const scaled = withReplicas(withReplicas(dedicated, topology, 'engine', 6), topology, 'master', 5);
  assert.deepEqual(specPatch(dedicated, scaled), { components: { engine: { replicas: 6 }, extra: { master: { replicas: 5 } } } });
  const enabledApm = withReplicas(dedicated, topology, 'apm', 1);
  assert.deepEqual(specPatch(dedicated, enabledApm), { components: { extra: { apm: { replicas: 1, resources: DEFAULT_RESOURCES.apm } } } });
  const disabledKibana = withReplicas(dedicated, topology, 'kibana', 0);
  assert.deepEqual(specPatch(dedicated, disabledKibana), { components: { extra: { kibana: { replicas: 0 } } } });
  const moreRam = { ...dedicated, components: { ...dedicated.components, engine: { replicas: 4, resources: { ...resources, minRam: 4, maxRam: 4 } } } };
  assert.deepEqual(specPatch(dedicated, moreRam), { components: { engine: { resources: { ...resources, minRam: 4, maxRam: 4 } } } });

  const withPipeline = writeOptions(dedicated, { ...readOptions(dedicated), logstash: { persistentQueue: true, pipelines: [{ id: 'main', config: 'x' }] } });
  assert.deepEqual(specPatch(dedicated, withPipeline).engineOptions, { kibana: { public: false, settings: null }, apm: { public: false, apiKeys: true }, logstash: { persistentQueue: true, pipelines: [{ id: 'main', config: 'x' }] }, elasticsearch: { settings: null } });
  const removedPipeline = writeOptions(withPipeline, { ...readOptions(withPipeline), logstash: { persistentQueue: true, pipelines: [] } });
  assert.deepEqual(specPatch(withPipeline, removedPipeline).engineOptions, { kibana: { public: false, settings: null }, apm: { public: false, apiKeys: true }, logstash: { persistentQueue: true, pipelines: null }, elasticsearch: { settings: null } });

  const current = { ...dedicated, tls: { mode: 'requireTLS' as const, ca: 'selfSigned' as const }, exposure: { public: false, ipType: 'IPV6' as const, allowlist: [] } };
  const exposed = { ...current, exposure: { public: true, ipType: 'IPV4' as const, allowlist: ['203.0.113.0/24'] }, monitoring: { enabled: false } };
  assert.deepEqual(specPatch(current, exposed), { exposure: { public: true, ipType: 'IPV4', allowlist: ['203.0.113.0/24'] }, monitoring: { enabled: false } });
  const userCA = { ...current, tls: { mode: 'requireTLS' as const, ca: 'userProvided' as const, caCertificate: 'CERT', caPrivateKey: 'KEY', certificate: ' ' } };
  assert.deepEqual(specPatch(current, userCA), { tls: { ca: 'userProvided', caCertificate: 'CERT', caPrivateKey: 'KEY' } });
  // Topology type and version changes are never emitted.
  assert.deepEqual(specPatch(current, { ...current, version: '9.9.9', topology: { type: 'cluster' } }), {});
});

test('disruptions explain scaling, restarts and component lifecycle', () => {
  assert.deepEqual(disruptions(dedicated, dedicated), []);
  const shrink = withReplicas(dedicated, topology, 'engine', 2);
  assert.match(disruptions(dedicated, shrink)[0] ?? '', /4 → 2: nodes are drained and removed one at a time/);
  const grow = withReplicas(dedicated, topology, 'master', 5);
  assert.match(disruptions(dedicated, grow)[0] ?? '', /Dedicated masters 3 → 5/);
  const moreRam = { ...dedicated, components: { ...dedicated.components, engine: { replicas: 4, resources: { ...resources, minRam: 4, maxRam: 4 } } } };
  assert.match(disruptions(dedicated, moreRam)[0] ?? '', /rolling restart/);
  assert.match(disruptions(dedicated, withReplicas(dedicated, topology, 'apm', 1))[0] ?? '', /APM Server enabled/);
  assert.match(disruptions(dedicated, withReplicas(dedicated, topology, 'kibana', 0))[0] ?? '', /Kibana disabled/);
  assert.match(disruptions(dedicated, withReplicas(dedicated, topology, 'kibana', 3))[0] ?? '', /Kibana 2 → 3 containers/);
  const publicKibana = writeOptions(dedicated, { ...readOptions(dedicated), kibana: { public: true } });
  assert.deepEqual(disruptions(dedicated, publicKibana), ['Kibana options change → Kibana restarts.']);
});
