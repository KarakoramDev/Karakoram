// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { Badge, type Tone } from '@/components/ui/Primitives';

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

const OP_TONE: Record<Schemas['Operation']['phase'], Tone> = { Pending: 'neutral', Running: 'progress', Succeeded: 'ok', Failed: 'danger' };

export function OpPhaseBadge({ phase }: { phase: Schemas['Operation']['phase'] }) {
  return <Badge tone={OP_TONE[phase]}>{phase}</Badge>;
}

const BACKUP_TONE: Record<Schemas['Backup']['phase'], Tone> = { Pending: 'neutral', Running: 'progress', Succeeded: 'ok', Failed: 'danger', Deleting: 'warn' };

export function BackupPhaseBadge({ phase }: { phase: Schemas['Backup']['phase'] }) {
  return <Badge tone={BACKUP_TONE[phase]}>{phase}</Badge>;
}

export function topologySummary(c: Cluster): string {
  const { topology, components } = c.spec;
  if (topology.type === 'sharded') {
    return `Sharded · ${topology.shards ?? 0} shards × ${components.engine.replicas} · ${components.configServer?.replicas ?? 0} cfg · ${components.router?.replicas ?? 0} routers`;
  }
  return `Replica set · ${components.engine.replicas} members`;
}

export function memberHealth(c: Cluster): { healthy: number; total: number } {
  const members = c.status.members ?? [];
  return { healthy: members.filter((m) => m.health).length, total: members.length };
}

export function HealthSummary({ cluster }: { cluster: Cluster }) {
  const { healthy, total } = memberHealth(cluster);
  if (total === 0) return <span className="faint">—</span>;
  const tone: Tone = healthy === total ? 'ok' : healthy === 0 ? 'danger' : 'warn';
  return (
    <Badge tone={tone}>
      {healthy}/{total} healthy
    </Badge>
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
