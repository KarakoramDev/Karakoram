// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { resourceMinimums, validateResources } from '@/pages/clusters/wizard/resources';
import type { EngineUI, MonitoringPanel } from '../types';
import { ElasticAccessTab } from './ElasticAccessTab';
import { TopologyEditor } from './TopologyEditor';
import { OptionsSection, ResourcesSection, TopologySection } from './WizardSections';
import { PUBLIC_PORT, elasticsearchChangeReview, specSummary, validateComponents, validateSpec } from './spec';

const CLUSTER_STATUS = ['green', 'yellow', 'red'];
const KIBANA_STATUS = ['available', 'degraded', 'unavailable'];
const STATUS_AVERAGES = 'Downsampled historical values can be fractional averages of categorical samples, not additional health states.';

export function formatHealth(value: number, labels: string[]): string {
  return Number.isInteger(value) ? labels[value] ?? String(value) : `${Number(value.toFixed(2))} (average)`;
}

/** Series names come from the provider's metrics catalog (internal/engine/elastic/metrics_catalog.go). */
export const elasticsearchMonitoringPanels: MonitoringPanel[] = [
  { title: 'Elasticsearch cluster health', names: ['elasticsearch.cluster.status'], yMax: 2, format: (v) => formatHealth(v, CLUSTER_STATUS), description: `0 = green (all shards assigned); 1 = yellow (primaries available, replicas missing); 2 = red (a primary unavailable). ${STATUS_AVERAGES}` },
  { title: 'Kibana status', names: ['kibana.status.level'], yMax: 2, format: (v) => formatHealth(v, KIBANA_STATUS), description: `0 = available; 1 = degraded; 2 = unavailable. ${STATUS_AVERAGES}` },
  { title: 'JVM heap', names: ['elasticsearch.jvm.heap.usedBytes', 'elasticsearch.jvm.heap.maxBytes'], unit: 'bytes' },
  { title: 'Documents', names: ['elasticsearch.docs'], unit: 'count' },
  { title: 'Store size', names: ['elasticsearch.store.bytes'], unit: 'bytes' },
  { title: 'Indexing rate', names: ['elasticsearch.indexing.total'], unit: 'docs/s' },
  { title: 'Search rate', names: ['elasticsearch.search.query.total'], unit: 'queries/s' },
  { title: 'Unassigned shards', names: ['elasticsearch.shards.unassigned'], unit: 'count' },
  { title: 'Cluster nodes', names: ['elasticsearch.cluster.nodes'], unit: 'count' },
  { title: 'Stack components up', names: ['kibana.up', 'logstash.up', 'apm.up'], unit: 'count', yMax: 1 },
  { title: 'Logstash events', names: ['logstash.events.in', 'logstash.events.out'], unit: 'events/s' },
  { title: 'APM events accepted', names: ['apm.events.accepted'], unit: 'events/s' },
];

export const elasticsearchUI: EngineUI = {
  label: 'Elasticsearch',
  description: 'Elasticsearch clusters with optional Kibana, Logstash and APM Server.',
  publicPort: PUBLIC_PORT,
  publicPortLabel: 'HTTPS',
  validateSpec,
  wizardSection: OptionsSection,
  topologySection: TopologySection,
  resourcesSection: ResourcesSection,
  validateComponents: (spec, topology) => validateComponents(spec, topology, (resources, stateless, name) => validateResources(resources, undefined, stateless, resourceMinimums(spec.provider, topology, name))),
  specSummary,
  changeReview: elasticsearchChangeReview,
  topologyEditor: TopologyEditor,
  detailTabs: [{ id: 'elastic', label: 'Elastic access', element: <ElasticAccessTab />, capability: 'elasticAccess' }],
  monitoringPanels: elasticsearchMonitoringPanels,
  topologySummary: (cluster) => specSummary(cluster.spec),
};
