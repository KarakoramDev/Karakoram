// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { colocatedGroups, knownHost } from './detail-state.ts';

export function agentBuildState(agent: Schemas['AgentStatus'] | undefined, targetBuild?: string): 'Current' | 'Outdated' | 'Unknown' {
  if (!agent?.build || !targetBuild) return 'Unknown';
  return agent.build === targetBuild ? 'Current' : 'Outdated';
}

export function placementCopy(placement?: Schemas['Placement']): { title: string; message: string } {
  if (colocatedGroups(placement?.groups).length) return { title: 'Members share a placement group', message: placement?.warning || 'A single host failure may affect multiple members. Move a member to reduce co-location.' };
  if (placement?.guaranteedAntiAffinity) return { title: 'Members are separated', message: 'The backend enforces placement and all authoritative member groups are distinct.' };
  if (placement?.source === 'backend') return { title: 'Backend placement is not yet confirmed', message: placement.warning || 'The backend supports placement constraints, but separation has not been confirmed for every member.' };
  if (!placement || placement.source === 'unavailable' || !placement.groups.length || placement.groups.some((g) => !knownHost(g.name))) return { title: 'Placement unavailable', message: placement?.warning || 'Placement information is not available for every member. Host-failure tolerance cannot be confirmed.' };
  return { title: 'Placement is best effort', message: placement.warning || 'Observed host groups do not guarantee anti-affinity. Moving a member requests a fresh placement, but cannot guarantee a different host.' };
}

export function plannedPlacementCopy(capabilities?: readonly string[]): { title: string; message: string } {
  if (!capabilities) return { title: 'Placement depends on the backend', message: 'Placement capability is not available to this account. Host groups and any anti-affinity guarantee are reported on the cluster overview after provisioning.' };
  if (capabilities.includes('placement')) return { title: 'Backend-enforced placement', message: 'The selected backend supports placement constraints. Host-failure tolerance is confirmed only after all authoritative member groups are reported and distinct.' };
  return { title: 'Placement is best effort', message: 'The selected backend does not guarantee host anti-affinity. More members improve process resilience, but cannot promise tolerance to a host failure. Co-location is reported on the cluster overview.' };
}

/** Mirror observable native replacement admission before offering a move. */
export function memberMoveRestriction(cluster: Schemas['Cluster'], member?: Schemas['Member']): string | undefined {
  if (!member) return 'Member is no longer observed.';
  const provider = cluster.spec.provider;
  if (['postgres', 'postgresql', 'supabase'].includes(provider)) {
    if (cluster.spec.topology.type !== 'ha') return 'Member moves require a PostgreSQL HA cluster.';
    if (member.set !== 'postgres' && !['primary', 'replica', 'standalone'].includes(member.role)) return 'Only PostgreSQL data members can be moved; component services cannot be replaced.';
  }
  if (['elasticsearch', 'elastic'].includes(provider)) {
    const nativeRoles = ['master', 'data', 'master,data'];
    if (!nativeRoles.includes(member.role)) return 'Component services cannot be replaced; use restart.';
    if (!cluster.status.members.some((m) => m.name !== member.name && nativeRoles.includes(m.role))) return 'Single-node clusters cannot replace members; use snapshot restore.';
    if (['draining', 'removing'].includes(member.state)) return 'Member is already being removed.';
  }
  if (provider === 'psmdb' && member.role === 'router') return 'Replacement requires a data/config member.';
  if (provider === 'rabbitmq' && ['LEAVING', 'LEAVING_BLOCKED'].includes(member.state)) return 'Replacement requires a ready member.';
  const group = member.placementGroup?.toLowerCase() ?? '';
  if (cluster.status.placement?.source === 'backend' && ['', 'unknown', 'unavailable', 'pending', 'best-effort:unknown'].includes(group)) return 'Wait for the current authoritative backend placement group to be observed before moving this member.';
  return undefined;
}

export function eligibleDrillBackups(backups: Schemas['Backup'][], backupSchedule?: string): Schemas['Backup'][] {
  return backups.filter((backup) => backup.phase === 'Succeeded' && (!backupSchedule || backup.schedule === backupSchedule));
}

/** Missing or nonterminal reports never release controller ownership. */
export function managedDrillTargetLocked(cluster: Schemas['Cluster'], report?: Schemas['BackupDrill']): boolean {
  const owner = cluster.managedBy;
  if (!owner || owner.kind !== 'backup-drill') return false;
  return !report || report.id !== owner.id || report.cluster !== owner.cluster || report.namespace !== cluster.namespace || !['Succeeded', 'Failed'].includes(report.phase);
}

/** Parse nonnegative Go durations for the window/timeout form bounds. */
export function durationSeconds(value: string): number | undefined {
  if (!/^\+?(?:(?:\d+(?:\.\d*)?|\.\d+)(?:ns|us|µs|μs|ms|s|m|h))+$/.test(value)) return undefined;
  let total = 0;
  const units: Record<string, number> = { h: 3600, m: 60, s: 1, ms: 0.001, us: 0.000001, 'µs': 0.000001, 'μs': 0.000001, ns: 0.000000001 };
  for (const match of value.matchAll(/(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|μs|ms|s|m|h)/g)) {
    const unit = match[2];
    const multiplier = unit === undefined ? undefined : units[unit];
    if (multiplier === undefined) return undefined;
    total += Number(match[1]) * multiplier;
  }
  return total;
}

export function maintenanceErrors(policy: Schemas['MaintenancePolicy']): string[] {
  const errors: string[] = [];
  try { new Intl.DateTimeFormat('en', { timeZone: policy.timeZone }); } catch { errors.push('Enter a valid IANA time zone.'); }
  if ((policy.windows?.length ?? 0) > 16) errors.push('At most 16 windows are allowed.');
  for (const window of policy.windows ?? []) {
    if (window.schedule.trim().split(/\s+/).length !== 5) errors.push('Each start schedule needs a five-field cron expression.');
    const duration = durationSeconds(window.duration);
    if (duration === undefined || duration < 900 || duration > 604800) errors.push('Window duration must be between 15m and 168h.');
  }
  return errors;
}

export function drillPolicyErrors(policy: Schemas['BackupDrillPolicy']): string[] {
  const errors: string[] = [];
  if (policy.schedule.trim().split(/\s+/).length !== 5) errors.push('Enter a five-field cron schedule.');
  const duration = durationSeconds(policy.timeout);
  if (duration === undefined || duration < 1800 || duration > 172800) errors.push('Drill timeout must be between 30m and 48h.');
  return errors;
}
