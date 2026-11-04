// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import assert from 'node:assert/strict';
import { register } from 'node:module';
import test from 'node:test';
import { createElement, type ReactElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { createMemoryRouter, Outlet, RouterProvider } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import type { Schemas } from '@/api/client';
import type { ClusterContext } from './ClusterDetail';
import { eligibleDrillBackups, memberMoveRestriction } from './ops-state.ts';

register('../../../../test/tsx-loader.mjs', import.meta.url);
// TSX must load after registering the custom Node loader; static imports run before registration.
const { AgentRollout } = await import('./AgentRollout.tsx');
const { BackupDrills } = await import('./BackupDrills.tsx');
const { MaintenanceBadge, MaintenanceCard, MaintenanceOverride } = await import('./Maintenance.tsx');
const { OverviewTab } = await import('./OverviewTab.tsx');
const { TopologyTab } = await import('./TopologyTab.tsx');
const { ToastProvider } = await import('../../../components/ui/Toast.tsx');

const cluster: Schemas['Cluster'] = {
  name: 'orders', namespace: 'production', generation: 1,
  createdAt: '2026-10-01T00:00:00Z', updatedAt: '2026-10-01T00:00:00Z',
  spec: {
    provider: 'psmdb', version: '8.0', topology: { type: 'replicaSet' },
    components: { engine: { replicas: 3, resources: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 2, maxRam: 2, minDisk: 5, maxDisk: 20 } } },
    maintenance: { timeZone: 'UTC', windows: [{ schedule: '0 2 * * 0', duration: '2h' }] },
    backup: { drill: { enabled: true, schedule: '0 4 * * 0', timeout: '6h' } },
  },
  status: {
    phase: 'Ready', observedGeneration: 1, conditions: [],
    maintenance: { open: false, nextWindowStart: '2026-10-25T02:00:00Z' },
    agents: { targetBuild: 'new-build', targetVersion: '1.2', outdated: ['ordersm1'], unknown: ['ordersm2'] },
    members: [
      { name: 'ordersm0', set: 'rs0', role: 'primary', state: 'PRIMARY', health: true, agent: { connected: true, version: '1.2', build: 'new-build', commit: 'new-commit' } },
      { name: 'ordersm1', set: 'rs0', role: 'secondary', state: 'SECONDARY', health: true, agent: { connected: true, version: '1.2', build: 'old-build', commit: 'old-commit' } },
      { name: 'ordersm2', set: 'rs0', role: 'secondary', state: 'SECONDARY', health: true, agent: { connected: true } },
    ],
    placement: { source: 'backend', guaranteedAntiAffinity: true, warning: '', groups: [{ name: 'a', members: ['ordersm0'] }, { name: 'b', members: ['ordersm1'] }, { name: 'c', members: ['ordersm2'] }] },
  },
};
const provider: Schemas['Provider'] = { name: 'psmdb', displayName: 'MongoDB', capabilities: ['backups', 'restores'], operations: ['upgrade-agents', 'replace-member', 'restore'], backupTypes: ['logical'], presets: [], topologies: [], upgradePaths: [], versions: [] };
const report: Schemas['BackupDrill'] = {
  id: 'report-1', cluster: 'orders', namespace: 'production', backup: 'nightly-1', backupType: 'logical', trigger: 'manual', phase: 'Failed', createdAt: '2026-10-21T01:00:00Z',
  targetCluster: 'dr0123456789', startedAt: '2026-10-21T01:00:01Z', readyAt: '2026-10-21T01:02:00Z', completedAt: '2026-10-21T01:03:00Z', restoreDurationSeconds: 119, totalDurationSeconds: 180, dataAgeSeconds: 3600,
  checks: [{ name: 'restore inventory', passed: true, message: 'Restore confirmed' }, { name: 'cleanup', passed: false, message: 'Volume requires attention' }],
  limitations: 'Recovery inventory does not validate application-level data invariants.', error: 'Cleanup could not finish', cleanup: { phase: 'Failed', orphanedVolumes: ['orphaned-data'] },
};

function context(can: ClusterContext['can'] = () => true, value = cluster): ClusterContext {
  return { cluster: value, provider, namespace: value.namespace, name: value.name, base: `/clusters/${value.namespace}/${value.name}`, can };
}

function render(element: ReactElement, ctx = context(), options: { url?: string; reports?: boolean; detail?: boolean; owningReport?: Schemas['BackupDrill'] } = {}): string {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  if (options.reports) client.setQueryData(['cluster', ctx.namespace, ctx.name, 'backup-drills'], { pages: [{ items: [report], nextCursor: 'older' }], pageParams: [undefined] });
  if (options.detail) client.setQueryData(['cluster', ctx.namespace, ctx.name, 'backup-drill', report.id], report);
  if (options.owningReport && ctx.cluster.managedBy) client.setQueryData(['cluster', ctx.namespace, ctx.cluster.managedBy.cluster, 'backup-drill', ctx.cluster.managedBy.id], options.owningReport);
  const router = createMemoryRouter([{ path: '/', element: createElement(Outlet, { context: ctx }), children: [{ index: true, element }] }], { initialEntries: [options.url || '/'] });
  try {
    return renderToStaticMarkup(createElement(QueryClientProvider, { client }, createElement(ToastProvider, null, createElement(RouterProvider, { router }))));
  } finally { router.dispose(); client.clear(); }
}

test('agent rollout explains the current artifact and gates the confirmed action by permission and capability', () => {
  const ctx = context();
  const markup = render(createElement(AgentRollout, { context: ctx }), ctx);
  assert.match(markup, /Agent updates available/);
  assert.match(markup, /1 outdated/);
  assert.match(markup, /1 unknown/);
  assert.match(markup, /new-build/);
  assert.match(markup, /Upgrade agents…/);
  assert.match(markup, /Include members already on the current build/);
  assert.match(markup, /Ignore maintenance window for this action/);
  const readonly = context(() => false);
  assert.doesNotMatch(render(createElement(AgentRollout, { context: readonly }), readonly), /Upgrade agents…/);
  const unsupported = { ...ctx, provider: { ...provider, operations: [] } };
  assert.doesNotMatch(render(createElement(AgentRollout, { context: unsupported }), unsupported), /Upgrade agents…/);
});

test('member table renders identity, rollout status and authoritative placement without losing engine details', () => {
  const markup = render(createElement(OverviewTab));
  assert.match(markup, /Current/);
  assert.match(markup, /Outdated/);
  assert.match(markup, /Unknown/);
  assert.match(markup, /old-build/);
  assert.match(markup, /old-commit/);
  assert.match(markup, /Members are separated/);
  assert.match(markup, /backend enforces placement/);
  assert.match(markup, /<th>Set<\/th>/);
  assert.match(markup, /rs0/);
  assert.doesNotMatch(markup, /Zerops does not guarantee/);
});

test('maintenance badge and editor preserve the schedule and enforce read-only access', () => {
  assert.match(render(createElement(MaintenanceBadge, { cluster })), /Maintenance: waiting/);
  assert.match(render(createElement(MaintenanceBadge, { cluster: { ...cluster, spec: { ...cluster.spec, maintenance: undefined } } })), /always open/);
  const markup = render(createElement(MaintenanceCard));
  assert.match(markup, /0 2 \* \* 0/);
  assert.match(markup, /2h/);
  assert.match(markup, /Edit windows…/);
  assert.doesNotMatch(render(createElement(MaintenanceCard), context(() => false)), /Edit windows…/);
  assert.match(render(createElement(MaintenanceOverride, { cluster, checked: true, onChange: () => {} })), /may start immediately/);
});

test('drill list supports history pagination and deep-linked detailed recovery and cleanup evidence', () => {
  const markup = render(createElement(BackupDrills, { backups: [] }), context(), { reports: true, detail: true, url: '/?drill=report-1' });
  for (const text of ['report-1', 'nightly-1', 'Run drill…', 'Drill policy…', 'Load more reports', 'Drill report', 'Restore confirmed', 'Volume requires attention', 'orphaned-data', 'Verification limitations', 'Backup data age at start', 'Cleanup could not finish']) assert.ok(markup.includes(text), text);
  assert.match(markup, /href="\/clusters\/production\/dr0123456789"/);
});

test('drill report caches do not leak across namespaces or read permissions', () => {
  const readonly = render(createElement(BackupDrills, { backups: [] }), context(() => false), { reports: true, detail: true, url: '/?drill=report-1' });
  assert.match(readonly, /need restore read permission/);
  for (const text of ['report-1', 'nightly-1', 'Run drill…', 'Drill policy…', 'Load more reports', 'orphaned-data']) assert.ok(!readonly.includes(text), text);
  const otherNamespace = context(() => true, { ...cluster, namespace: 'staging' });
  const markup = render(createElement(BackupDrills, { backups: [] }), otherNamespace);
  assert.match(markup, /Loading drills/);
  assert.doesNotMatch(markup, /nightly-1/);
});

test('managed drill targets cannot schedule nested drills or run another drill', () => {
  const target = { ...cluster, managedBy: { kind: 'backup-drill' as const, cluster: 'source', id: 'original-drill' } };
  const markup = render(createElement(BackupDrills, { backups: [] }), context(() => true, target));
  assert.doesNotMatch(markup, /Run drill…/);
  assert.doesNotMatch(markup, /Drill policy…/);
});

test('RabbitMQ recovery drills disclose the definitions-only limitation before running', () => {
  const rabbit = { ...cluster, spec: { ...cluster.spec, provider: 'rabbitmq' } };
  assert.match(render(createElement(BackupDrills, { backups: [] }), context(() => true, rabbit)), /definitions only, not messages/);
});

test('topology relocation explains backend guarantees and preserves the engine editor', () => {
  const markup = render(createElement(TopologyTab));
  assert.match(markup, /Member placement &amp; replacement/);
  assert.match(markup, /Avoid the current placement group/);
  assert.match(markup, /fails before removing the original member/);
  assert.match(markup, /Maintenance override for topology changes/);
  assert.match(markup, /Member resources/);
  const bestEffort = { ...cluster, status: { ...cluster.status, placement: { ...cluster.status.placement!, source: 'best-effort' as const, guaranteedAntiAffinity: false } } };
  assert.match(render(createElement(TopologyTab), context(() => true, bestEffort)), /cannot guarantee a different host/);
});

test('manual drills offer only successful backups and always support automatic eligible selection', () => {
  const backups: Schemas['Backup'][] = [
    { name: 'successful-choice', cluster: 'orders', namespace: 'production', storage: 'archive', type: 'logical', phase: 'Succeeded', trigger: 'manual' },
    { name: 'failed-choice', cluster: 'orders', namespace: 'production', storage: 'archive', type: 'logical', phase: 'Failed', trigger: 'manual' },
  ];
  const markup = render(createElement(BackupDrills, { backups }));
  assert.match(markup, /Latest successful eligible backup/);
  assert.match(markup, /successful-choice/);
  assert.doesNotMatch(markup, /failed-choice/);
});

test('moves reject native singleton/component targets and unobserved authoritative placement before confirmation', () => {
  const data: Schemas['Member'] = { name: 'data0', set: 'postgres', role: 'replica', state: 'replica', health: true, placementGroup: 'node-a' };
  const pg: Schemas['Cluster'] = { ...cluster, spec: { ...cluster.spec, provider: 'postgres', topology: { type: 'single' } }, status: { ...cluster.status, members: [data] } };
  assert.match(memberMoveRestriction(pg, data) ?? '', /HA cluster/);
  assert.match(render(createElement(TopologyTab), context(() => true, pg)), /Move unavailable: Member moves require a PostgreSQL HA cluster/);
  const ha: Schemas['Cluster'] = { ...pg, spec: { ...pg.spec, topology: { type: 'ha' } } };
  assert.equal(memberMoveRestriction(ha, data), undefined);
  assert.match(memberMoveRestriction(ha, { ...data, role: 'proxy', set: 'proxy' }) ?? '', /Only PostgreSQL data/);
  const supabase: Schemas['Cluster'] = { ...ha, spec: { ...ha.spec, provider: 'supabase' } };
  assert.match(memberMoveRestriction(supabase, { ...data, role: 'studio', set: 'studio' }) ?? '', /component services/);
  const node: Schemas['Member'] = { ...data, set: '', role: 'master,data', state: 'joined' };
  const component: Schemas['Member'] = { ...node, name: 'ui0', role: 'kibana' };
  const es: Schemas['Cluster'] = { ...cluster, spec: { ...cluster.spec, provider: 'elasticsearch' }, status: { ...cluster.status, members: [node, component] } };
  assert.match(memberMoveRestriction(es, node) ?? '', /Single-node/);
  assert.match(memberMoveRestriction(es, component) ?? '', /Component services/);
  const multinode: Schemas['Cluster'] = { ...es, status: { ...es.status, members: [node, { ...node, name: 'data1', role: 'master' }, component] } };
  assert.equal(memberMoveRestriction(multinode, node), undefined);
  assert.match(memberMoveRestriction(multinode, { ...node, state: 'draining' }) ?? '', /already being removed/);
  for (const placementGroup of ['', 'unknown', 'UNAVAILABLE', 'pending', 'best-effort:unknown']) assert.match(memberMoveRestriction(ha, { ...data, placementGroup }) ?? '', /authoritative backend placement/);
  const bestEffort: Schemas['Cluster'] = { ...ha, status: { ...ha.status, placement: { groups: [], source: 'best-effort', guaranteedAntiAffinity: false, warning: '' } } };
  assert.equal(memberMoveRestriction(bestEffort, { ...data, placementGroup: 'unknown' }), undefined);
});

test('explicit drill backup choices obey the same configured schedule filter as automatic selection', () => {
  const backups: Schemas['Backup'][] = [
    { name: 'daily-success', cluster: 'orders', namespace: 'production', storage: 'archive', type: 'logical', phase: 'Succeeded', trigger: 'schedule', schedule: 'daily' },
    { name: 'weekly-success', cluster: 'orders', namespace: 'production', storage: 'archive', type: 'logical', phase: 'Succeeded', trigger: 'schedule', schedule: 'weekly' },
    { name: 'manual-success', cluster: 'orders', namespace: 'production', storage: 'archive', type: 'logical', phase: 'Succeeded', trigger: 'manual' },
    { name: 'daily-failed', cluster: 'orders', namespace: 'production', storage: 'archive', type: 'logical', phase: 'Failed', trigger: 'schedule', schedule: 'daily' },
  ];
  assert.deepEqual(eligibleDrillBackups(backups, 'daily').map((b) => b.name), ['daily-success']);
  assert.deepEqual(eligibleDrillBackups(backups).map((b) => b.name), ['daily-success', 'weekly-success', 'manual-success']);
  const filtered: Schemas['Cluster'] = { ...cluster, spec: { ...cluster.spec, backup: { drill: { enabled: true, schedule: '0 4 * * 0', timeout: '6h', backupSchedule: 'daily' } } } };
  const markup = render(createElement(BackupDrills, { backups }), context(() => true, filtered));
  assert.match(markup, /daily-success/);
  for (const excluded of ['weekly-success', 'manual-success', 'daily-failed']) assert.ok(!markup.includes(excluded), excluded);
  assert.match(markup, /Both explicit and automatic selection are restricted/);
});

test('managed target maintenance stays read-only without a terminal owning report and permits terminal repair', () => {
  const target: Schemas['Cluster'] = { ...cluster, name: 'dr0123456789', managedBy: { kind: 'backup-drill', cluster: report.cluster, id: report.id } };
  const missing = render(createElement(MaintenanceCard), context(() => true, target));
  assert.match(missing, /missing or inaccessible reports cannot release ownership/);
  assert.doesNotMatch(missing, /Edit windows…/);
  const nonterminal: Schemas['BackupDrill']['phase'][] = ['Pending', 'Provisioning', 'Restoring', 'Verifying', 'CleaningUp'];
  for (const phase of nonterminal) {
    const markup = render(createElement(MaintenanceCard), context(() => true, target), { owningReport: { ...report, phase } });
    assert.match(markup, /Maintenance policy is read-only/);
    assert.doesNotMatch(markup, /Edit windows…/);
  }
  const terminal: Schemas['BackupDrill']['phase'][] = ['Succeeded', 'Failed'];
  for (const phase of terminal) {
    const markup = render(createElement(MaintenanceCard), context(() => true, target), { owningReport: { ...report, phase } });
    assert.match(markup, /Edit windows…/);
    assert.doesNotMatch(markup, /Maintenance policy is read-only/);
    assert.doesNotMatch(render(createElement(MaintenanceCard), context(() => false, target), { owningReport: { ...report, phase } }), /Edit windows…/);
  }
});
