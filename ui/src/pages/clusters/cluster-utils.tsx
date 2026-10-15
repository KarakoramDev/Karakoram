// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import type { Schemas } from '@/api/client';
import { Badge, type Tone } from '@/components/ui/Primitives';
import { memberHealthCounts, rememberMemberRoles, type MemberRoles } from './member-status';
import { engineTopologySummary } from '@/engines';

type Phase = Schemas['ClusterStatus']['phase'];
type Cluster = Schemas['Cluster'];

const PHASE_TONE: Record<Phase, Tone> = {
  Creating: 'progress',
  Initializing: 'progress',
  Ready: 'ok',
  Updating: 'progress',
  Scaling: 'progress',
  Upgrading: 'progress',
  Restoring: 'progress',
  Degraded: 'warn',
  Error: 'danger',
  Deleting: 'danger',
  Suspended: 'neutral',
};

export function PhaseBadge({ phase, message }: { phase: Phase; message?: string }) {
  return (
    <Badge tone={PHASE_TONE[phase]} title={message}>
      {phase}
    </Badge>
  );
}

const OP_TONE: Record<Schemas['Operation']['phase'], Tone> = { Pending: 'neutral', Running: 'progress', Succeeded: 'ok', Failed: 'danger', Cancelled: 'neutral' };

export function OpPhaseBadge({ phase }: { phase: Schemas['Operation']['phase'] }) {
  return <Badge tone={OP_TONE[phase]}>{phase}</Badge>;
}

const BACKUP_TONE: Record<Schemas['Backup']['phase'], Tone> = { Pending: 'neutral', Running: 'progress', Succeeded: 'ok', Failed: 'danger', Deleting: 'warn' };

export function BackupPhaseBadge({ phase }: { phase: Schemas['Backup']['phase'] }) {
  return <Badge tone={BACKUP_TONE[phase]}>{phase}</Badge>;
}

export function topologySummary(c: Cluster): string {
  const summary = engineTopologySummary(c);
  if (summary !== undefined) return summary;
  const { topology, components } = c.spec;
  if (topology.type === 'sharded') {
    return `Sharded · ${topology.shards ?? 0} shards × ${components.engine.replicas} · ${components.configServer?.replicas ?? 0} cfg · ${components.router?.replicas ?? 0} routers`;
  }
  return `Replica set · ${components.engine.replicas} members`;
}

/** Role history belongs to one cluster and is discarded when a member disappears. */
export function useMemberRoles(members: Schemas['Member'][], clusterKey: string): MemberRoles {
  const [history, setHistory] = useState(() => ({ members, clusterKey, roles: rememberMemberRoles(members) }));
  if (history.members !== members || history.clusterKey !== clusterKey) {
    const roles = rememberMemberRoles(members, history.clusterKey === clusterKey ? history.roles : undefined);
    setHistory({ members, clusterKey, roles });
    return roles;
  }
  return history.roles;
}

const ROLE_TONE: Record<Schemas['Member']['role'], Tone> = {
  primary: 'accent', secondary: 'neutral', arbiter: 'neutral', 'config-primary': 'accent',
  'config-secondary': 'neutral', router: 'info', unknown: 'warn',
};

export function MemberRole({ member, roles }: { member: Schemas['Member']; roles?: MemberRoles }) {
  const role = member.role !== 'unknown' ? member.role : roles?.get(member.name) ?? 'unknown';
  const leaving = member.state === 'LEAVING';
  return <Badge tone={leaving ? 'neutral' : ROLE_TONE[role]} plain>{leaving && role === 'unknown' ? 'Leaving' : role}</Badge>;
}

export function MemberHealthBadge({ member, suspended = false }: { member: Schemas['Member']; suspended?: boolean }) {
  if (suspended) return <Badge tone="neutral">historical</Badge>;
  if (member.state === 'LEAVING') return <Badge tone="progress">Leaving</Badge>;
  return <Badge tone={member.health ? 'ok' : 'danger'}>{member.health ? 'healthy' : 'unhealthy'}</Badge>;
}

export function HealthSummary({ cluster }: { cluster: Cluster }) {
  const { healthy, total, leaving } = memberHealthCounts(cluster.status.members ?? []);
  if (total === 0 && leaving === 0) return <span className="faint">—</span>;
  const tone: Tone = healthy === total ? 'ok' : healthy === 0 ? 'danger' : 'warn';
  return (
    <span className="row wrap" style={{ gap: 4 }}>
      {total > 0 ? <Badge tone={tone}>{healthy}/{total} healthy</Badge> : null}
      {leaving > 0 ? <Badge tone="progress">{leaving} Leaving</Badge> : null}
    </span>
  );
}

export function BackupSummary({ cluster }: { cluster: Cluster }) {
  const b = cluster.status.backup;
  const spec = cluster.spec.backup;
  const scheduled = (spec?.schedules ?? []).some((s) => s.enabled);
  const pitr = spec?.pitr?.enabled && b?.pitrWindow?.available;
  if (!spec?.storage) return <Badge plain>Not configured</Badge>;
  return (
    <span className="row wrap" style={{ gap: 4 }}>
      <Badge plain tone={scheduled ? 'info' : 'neutral'}>
        {scheduled ? 'Scheduled' : 'Manual'}
      </Badge>
      {spec.pitr?.enabled ? (
        <Badge plain tone={pitr ? 'ok' : 'warn'}>
          PITR
        </Badge>
      ) : null}
    </span>
  );
}

export function resourcesSummary(r: Schemas['Resources'], noDisk = false): string {
  const cpu = r.minCpu === r.maxCpu ? `${r.maxCpu}` : `${r.minCpu}–${r.maxCpu}`;
  const ram = r.minRam === r.maxRam ? `${r.maxRam}` : `${r.minRam}–${r.maxRam}`;
  const disk = noDisk ? '' : ` · ${r.minDisk}–${r.maxDisk} GiB disk`;
  return `${cpu} vCPU ${r.cpuMode === 'SHARED' ? 'shared' : 'dedicated'} · ${ram} GiB RAM${disk}`;
}
