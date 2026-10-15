// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import test from 'node:test';
import { register } from 'node:module';
import { readFileSync } from 'node:fs';
import { createElement, type ReactElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { createMemoryRouter, Outlet, RouterProvider } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Schemas } from '../../api/client';
import createClient from 'openapi-fetch';
import type { paths } from '../../api/schema';

register('../../../test/tsx-loader.mjs', import.meta.url);
// Static imports run before register; source imports must wait for the TSX/alias loader.
const { OptionsSection, ResourcesSection } = await import('./WizardSections.tsx');
const { ConnectionHelp } = await import('./ConnectionHelp.tsx');
const { elasticsearchUI, elasticsearchMonitoringPanels } = await import('./index.tsx');
const { kibanaReady, DEFAULT_RESOURCES, MIN_RAM, readOptions, withReplicas, writeOptions } = await import('./spec.ts');
const { elasticResourceRows } = await import('./resource-summary.ts');
const { engines } = await import('../index.ts');
const { engineRules } = await import('../../pages/clusters/wizard/engine.ts');
const { normalizeWizardSpec, submitWizard, firstInvalidStep } = await import('../../pages/clusters/wizard/state.ts');
const { validateWizard } = await import('../../pages/clusters/wizard/validation.ts');
const { resourceMinimums, resourceErrors } = await import('../../pages/clusters/wizard/resources.ts');
const { totalFootprint } = await import('../../pages/clusters/wizard/review.ts');
const { OverviewTab } = await import('../../pages/clusters/detail/OverviewTab.tsx');
const { BackupsTab } = await import('../../pages/clusters/detail/BackupsTab.tsx');
const { TopologyTab } = await import('../../pages/clusters/detail/TopologyTab.tsx');
const { MonitoringTab } = await import('../../pages/clusters/detail/MonitoringTab.tsx');
const { ToastProvider } = await import('../../components/ui/Toast.tsx');
const { KV } = await import('../../components/ui/Primitives.tsx');
const { ChoiceGroup } = await import('../../components/ui/Field.tsx');
engines.elasticsearch = elasticsearchUI;

const spec: Schemas['ClusterSpec'] = {
  provider: 'elasticsearch', version: '9.5.5', topology: { type: 'dedicatedMasters' },
  components: { engine: { replicas: 1, resources: DEFAULT_RESOURCES.engine }, extra: { master: { replicas: 3, resources: DEFAULT_RESOURCES.master } } },
  tls: { mode: 'requireTLS', ca: 'selfSigned' }, exposure: { public: false, ipType: 'IPV6', allowlist: [] },
};
const topology: Schemas['ProviderTopology'] = { type: 'dedicatedMasters', displayName: 'Dedicated masters', defaults: spec, componentSchemas: {}, limits: {} };
const provider: Schemas['Provider'] = { name: 'elasticsearch', displayName: 'Elasticsearch', topologies: [topology], versions: [{ version: spec.version, major: '9', status: 'supported', default: true }], presets: [], upgradePaths: [], backupTypes: ['snapshot'], capabilities: ['backups', 'restores', 'tls', 'monitoring'], operations: ['backup', 'delete-backup'] };
const connection: Schemas['ConnectionSummary'] = { hosts: ['search:9200'], uri: 'https://search:9200', endpoints: [{ name: 'elasticsearch', url: 'https://search:9200', public: false }, { name: 'kibana', url: 'https://dashboards.example', public: true }] };
const cluster: Schemas['Cluster'] = { name: 'search', namespace: 'default', generation: 1, createdAt: '2026-01-01T00:00:00Z', updatedAt: '2026-01-01T00:00:00Z', spec, status: { phase: 'Ready', observedGeneration: 1, members: [], conditions: [], connection } };

function renderDetail(component: ReactElement, value = cluster, setup?: (client: QueryClient) => void) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } });
  client.setQueryData(['backup-storages', 'default'], { items: [] });
  setup?.(client);
  const router = createMemoryRouter([{ path: '/', element: createElement(Outlet, { context: { cluster: value, provider, namespace: 'default', name: 'search', base: '/clusters/default/search', can: () => true } }), children: [{ index: true, element: component }] }]);
  try {
    return renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ToastProvider, null, createElement(RouterProvider, { router }))));
  } finally { router.dispose(); client.clear(); }
}

test('Basics explains both Elasticsearch topology choices', () => {
  const rules = engineRules('elasticsearch');
  const options = Object.entries(rules.topology).map(([value, choice]) => ({ value, ...choice }));
  const markup = renderToStaticMarkup(createElement(ChoiceGroup, { name: 'topology', legend: 'Topology', value: 'cluster', onChange: () => {}, options }));
  assert.match(markup, /Combined master and data nodes/);
  assert.match(markup, /Dedicated masters \+ data nodes/);
  assert.match(markup, /master-only nodes.*data nodes/);
});

test('public APM is offered, serializes enabled state and clears when APM is disabled', () => {
  const enabled = withReplicas(spec, topology, 'apm', 1);
  const markup = renderToStaticMarkup(createElement(OptionsSection, { provider, spec: enabled, onChange: () => {} }));
  assert.match(markup, /Public HTTPS access to APM Server/);
  const published = writeOptions(enabled, { ...readOptions(enabled), apm: { public: true, apiKeys: true } });
  assert.equal(readOptions(published).apm.public, true);
  assert.equal(readOptions(withReplicas(published, topology, 'apm', 0)).apm.public, false);
});

test('clone disabled stack options normalize into a valid request and Create emits one POST', async () => {
  const enabled = withReplicas(withReplicas(withReplicas(spec, topology, 'kibana', 1), topology, 'apm', 1), topology, 'logstash', 1);
  const inherited = writeOptions(enabled, { ...readOptions(enabled), kibana: { public: true }, apm: { public: true, apiKeys: true }, logstash: { persistentQueue: true, pipelines: [{ id: 'main', config: 'input {}' }] } });
  const seeded = structuredClone(inherited);
  for (const name of ['kibana', 'apm', 'logstash'] as const) seeded.components.extra![name]!.replicas = 0;
  const clone = normalizeWizardSpec({ ...seeded, dataSource: { cluster: 'source', backup: 'snapshot' } });
  assert.equal(readOptions(clone).kibana.public, false);
  assert.equal(readOptions(clone).apm.public, false);
  assert.deepEqual(readOptions(clone).logstash, { persistentQueue: false, pipelines: [] });
  assert.equal(readOptions(inherited).kibana.public, true, 'source must not be mutated');
  const validation = validateWizard('newsearch', 'default', clone, [], provider);
  assert.equal(firstInvalidStep(validation), undefined);
  const lock = { current: false };
  const requests: Request[] = [];
  const client = createClient<paths>({ baseUrl: 'https://control.example/api/v1', fetch: async (request) => {
    requests.push(request);
    return Response.json(cluster, { status: 201 });
  } });
  const pending: Promise<unknown>[] = [];
  const create = () => { pending.push(client.POST('/clusters', { body: { name: 'newsearch', spec: clone } })); };
  const onInvalid = () => { assert.fail('Valid clone must not select a blocking step'); };
  submitWizard(validation, lock, onInvalid, create);
  submitWizard(validation, lock, onInvalid, create);
  await Promise.all(pending);
  assert.equal(requests.length, 1);
  assert.equal(requests[0]!.method, 'POST');
  assert.equal(new URL(requests[0]!.url).pathname, '/api/v1/clusters');
  const body: Schemas['CreateClusterRequest'] = await requests[0]!.json();
  assert.equal(readOptions(body.spec).kibana.public, false);
  const invalid = { ...clone, engineOptions: { logstash: { pipelines: [{ id: 'Bad ID', config: '' }] } }, components: { ...clone.components, extra: { ...clone.components.extra, logstash: { replicas: 1, resources: DEFAULT_RESOURCES.logstash } } } };
  assert.equal(firstInvalidStep(validateWizard('newsearch', 'default', invalid, [], provider)), 'basics');
  const blockedSteps: string[] = [];
  submitWizard(validateWizard('newsearch', 'default', invalid, [], provider), { current: false }, (step) => blockedSteps.push(step), create);
  assert.deepEqual(blockedSteps, ['basics']);
  assert.equal(requests.length, 1, 'blocked options must not send another request');
  const markup = renderToStaticMarkup(createElement(OptionsSection, { provider, spec: invalid, onChange: () => {} }));
  assert.match(markup, /field-error/);
  assert.match(markup, /Pipeline|ID/);
});

test('Review and Overview resource summaries include three masters separately from one data node', () => {
  const rows = elasticResourceRows(spec);
  const markup = renderToStaticMarkup(createElement(KV, { items: rows }));
  assert.match(markup, /Data node resources/);
  assert.match(markup, /1 × 1–2 vCPU shared · 2 GiB RAM · 5–20 GiB disk/);
  assert.match(markup, /Dedicated master resources/);
  assert.match(markup, /3 × 1–2 vCPU shared · 1.5 GiB RAM · 5–20 GiB disk/);
  const footprint = totalFootprint(spec);
  assert.equal(footprint.members, 4);
  assert.equal(footprint.minRam, 6.5);
  assert.equal(footprint.minDisk, 20);
  assert.match(renderDetail(createElement(OverviewTab)), /Dedicated master resources/);
});

test('each Elasticsearch resource editor shows inline RAM errors just below its server floor', () => {
  for (const name of ['engine', 'master', 'kibana', 'logstash', 'apm'] as const) {
    const minimums = resourceMinimums('elasticsearch', topology, name);
    assert.equal(minimums.ram, MIN_RAM[name]);
    const resources = { ...DEFAULT_RESOURCES[name], minRam: MIN_RAM[name] - 0.01 };
    assert.match(resourceErrors(resources, undefined, false, minimums).minRam!, /RAM must be at least/);
    const draft = name === 'engine' ? { ...spec, components: { ...spec.components, engine: { replicas: 1, resources } } } : { ...spec, components: { ...spec.components, extra: { ...spec.components.extra, [name]: { replicas: name === 'master' ? 3 : 1, resources } } } };
    const markup = renderToStaticMarkup(createElement(ResourcesSection, { provider, spec: draft, onChange: () => {} }));
    assert.match(markup, new RegExp(`id="es-${name}-minRam"[^>]*aria-invalid="true"|aria-invalid="true"[^>]*id="es-${name}-minRam"`));
    assert.ok(markup.includes(`RAM must be at least ${MIN_RAM[name]} GiB.`));
    assert.equal(resourceErrors({ ...resources, minRam: MIN_RAM[name] }, undefined, false, minimums).minRam, undefined);
  }
});

test('Elasticsearch detail uses native options without an unused generic override editor', () => {
  const markup = renderDetail(createElement(TopologyTab));
  assert.doesNotMatch(markup, /Engine configuration overrides/);
  assert.match(markup, /Elastic Stack options/);
  assert.match(markup, /Public HTTPS access to APM Server/);
  const wizard = readFileSync(new URL('../../pages/clusters/ClusterWizard.tsx', import.meta.url), 'utf8');
  assert.match(wizard, /provider\.name !== 'elasticsearch' \? <TextAreaField/);
  const inherited = renderToStaticMarkup(createElement(OptionsSection, { provider, spec: { ...spec, engineConfig: 'inherited: true' }, onChange: () => {} }));
  assert.match(inherited, /Clear inherited overrides/);
});

test('connection walkthrough selects HTTPS, directs CA download and explains private project access', () => {
  const markup = renderToStaticMarkup(createElement(ConnectionHelp, { connection }));
  assert.match(markup, /How to connect \(Elasticsearch\)/);
  assert.match(markup, /Reveal credentials/);
  assert.match(markup, /download <code>ca.pem<\/code>/);
  assert.match(markup, /curl --cacert ca.pem -u elastic &quot;https:\/\/search:9200\/_cluster\/health&quot;/);
  assert.match(markup, /same Zerops project/);
  assert.doesNotMatch(markup, /curl[^<]*dashboards/);
});

test('Kibana routes are starting until all Kibana members are joined and healthy', () => {
  assert.equal(kibanaReady([]), false);
  assert.equal(kibanaReady([{ role: 'kibana', state: 'joined', health: false }]), false);
  assert.equal(kibanaReady([{ role: 'kibana', state: 'absent', health: true }]), false);
  assert.equal(kibanaReady([{ role: 'kibana', state: 'joined', health: true }]), true);
  assert.equal(kibanaReady([{ role: 'kibana', state: 'joined', health: true }, { role: 'kibana', state: 'absent', health: false }]), false);
  const starting = renderDetail(createElement(OverviewTab));
  assert.match(starting, /Kibana is starting — retry shortly/);
  assert.doesNotMatch(starting, /https:\/\/dashboards.example/);
  const ready = renderDetail(createElement(OverviewTab), { ...cluster, status: { ...cluster.status, members: [{ name: 'kibana', set: '', role: 'kibana', state: 'joined', health: true }] } });
  assert.match(ready, /https:\/\/dashboards.example/);
  assert.doesNotMatch(ready, /Kibana is starting/);
});

test('suspended overview describes Kibana as suspended rather than starting', () => {
  const markup = renderDetail(createElement(OverviewTab), { ...cluster, status: { ...cluster.status, phase: 'Suspended', members: [{ name: 'kibana', set: '', role: 'kibana', state: 'suspended', health: false }] } });
  assert.match(markup, /Kibana is suspended\. Resume the cluster/);
  assert.doesNotMatch(markup, /Kibana is starting|https:\/\/dashboards.example/);
});

test('health panels retain native categories and label fractional samples as averages', () => {
  const health = elasticsearchMonitoringPanels.find((panel) => panel.names.includes('elasticsearch.cluster.status'))!;
  const kibana = elasticsearchMonitoringPanels.find((panel) => panel.names.includes('kibana.status.level'))!;
  assert.deepEqual([0, 1, 2].map(health.format!), ['green', 'yellow', 'red']);
  assert.deepEqual([0, 1, 2].map(kibana.format!), ['available', 'degraded', 'unavailable']);
  for (const panel of [health, kibana]) {
    assert.equal(panel.format!(0.5), '0.5 (average)');
    assert.match(panel.description!, /fractional averages.*not additional health states/);
  }
  const markup = renderDetail(createElement(MonitoringTab), cluster, (client) => {
    client.setQueryData(['cluster', 'default', 'search', 'metrics', '1h'], { series: [], start: '2026-01-01T00:00:00Z', end: '2026-01-01T01:00:00Z', step: '30s' });
  });
  assert.match(markup, /Elasticsearch cluster health/);
  assert.match(markup, /Kibana status/);
  assert.match(markup, /0 = green/);
  assert.match(markup, /0 = available/);
  assert.match(markup, /fractional averages/);
});

test('snapshot details display the native reference with copy control and safe deletion guidance', () => {
  const markup = renderDetail(createElement(BackupsTab), cluster, (client) => {
    client.setQueryData(['cluster', 'default', 'search', 'backups'], { items: [{ name: 'snapshot', provider: 'elasticsearch', type: 'snapshot', phase: 'Succeeded', storage: 'archive', location: 'repo/snapshot', trigger: 'manual', size: 0 }] });
  });
  assert.match(markup, /Repository\/snapshot reference/);
  assert.match(markup, /repo\/snapshot/);
  assert.match(markup, /Copy/);
  assert.match(markup, /not a deletable object prefix/);
});
