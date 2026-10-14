// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

export function knownHost(name: string): boolean {
  return Boolean(name.trim()) && !['unknown', 'unavailable', 'pending'].includes(name.trim().toLowerCase());
}

export function colocatedGroups<T extends { name: string; members: string[] }>(groups: T[] = []): T[] {
  return groups.filter((g) => knownHost(g.name) && g.members.length > 1);
}

export function observedVersion(status: { version?: string; members: { version?: string; role?: string }[] }): string | undefined {
  const versions = [...new Set(status.members.filter((m) => m.role !== 'router').map((m) => m.version).filter((v): v is string => Boolean(v)))];
  return versions.length ? versions.join(', ') : undefined;
}

export function maintenanceCopy(provider: string, replicas: number): string {
  if (replicas === 1) return 'Downtime: the only member becomes unavailable during upgrade or restart. Applications cannot use the database until it returns.';
  const neutral = 'Members restart one at a time; the cluster stays available if a majority/quorum remains.';
  return provider === 'psmdb' ? `${neutral} Secondaries restart first; a primary failover can briefly pause writes.` : neutral;
}

export const FCV_GUIDANCE = 'After a major upgrade, FCV keeps compatibility with the previous major until raised. Raising it enables new on-disk features. Binary downgrades are not supported, even before FCV is raised. Confirm backups are current first.';

export function fcvObservation(fcv: string | undefined, binary: string | undefined, major: string): string {
  if (!fcv) return 'not yet observed';
  if (!binary) return 'binary version not yet observed';
  return fcv === major ? 'aligned with observed binary version' : `behind binary major ${major}`;
}

export function generationState(phase: string, observed: number, desired: number): string {
  return phase === 'Suspended' ? 'runtime suspended' : observed < desired ? 'reconciling changes' : 'in sync';
}

export function latestSuccessfulBackup<T extends { phase: string; completedAt?: string; startedAt?: string }>(backups: T[]): T | undefined {
  return backups.filter((b) => b.phase === 'Succeeded').reduce<T | undefined>((latest, b) => {
    const timestamp = (value: T) => Date.parse(value.completedAt || value.startedAt || '') || 0;
    return !latest || timestamp(b) > timestamp(latest) ? b : latest;
  }, undefined);
}

export function passwordValid(password: string): boolean {
  return !password || Array.from(password).length >= 8;
}

export function assignedUsers(users: { username: string; database: string; roles: { role: string; db: string }[] }[], role: { name: string; database: string }): string[] {
  return users.filter((u) => u.roles.some((r) => r.role === role.name && r.db === role.database)).map((u) => `${u.username}@${u.database}`);
}

export function clusterEmptyCopy(canCreate: boolean): { title: string; message: string } {
  return canCreate ? { title: 'No clusters yet', message: 'Create a database cluster to get started.' } : { title: 'No accessible clusters', message: 'No clusters are accessible to your account in this scope. Ask an administrator for access; you do not have permission to create clusters.' };
}

/** Describe only the fields included in an RFC 7396 spec patch. */
export function scalingReview(spec: { components: Record<string, { replicas: number; resources: object } | undefined> }, patch: Record<string, unknown>): string[] {
  const changes: string[] = [];
  const components = patch.components as Record<string, { replicas?: number; resources?: Record<string, unknown> }> | undefined;
  for (const [name, component] of Object.entries(components ?? {})) {
    const current = spec.components[name];
    if (component.replicas !== undefined) {
      const action = component.replicas > (current?.replicas ?? 0) ? 'add members with initial sync and voting promotion' : 'remove members while preserving a voting majority';
      changes.push(name === 'router' ? `Routers: ${current?.replicas ?? 0} → ${component.replicas}; update router capacity.` : `${name} members: ${current?.replicas ?? 0} → ${component.replicas}; ${action}.`);
    }
    if (component.resources) {
      const previous = (current?.resources ?? {}) as Record<string, unknown>;
      const fields = Object.keys(component.resources).filter((key) => component.resources![key] !== previous[key]);
      if (fields.length) changes.push(fields.every((key) => ['cpuMode', 'minCpu', 'maxCpu', 'startCpu'].includes(key))
        ? `${name}: CPU limits apply without data movement. Platform resource updates are applied one member at a time with disruption safety checks; a platform restart may occur.`
        : `${name}: resource changes are applied one member at a time with rolling restart precautions.${fields.some((key) => key === 'minDisk' || key === 'maxDisk') ? ' Storage bounds are updated on the existing data volume.' : ''}`);
    }
  }
  if (patch.topology) changes.push('Add shard replica sets and join them to the cluster; existing shards are retained.');
  if (patch.tls) changes.push('TLS settings/certificates change; a rolling restart may be required.');
  if (patch.exposure) changes.push('Network exposure changes; public routes and allowlists are updated.');
  if (patch.engineConfig !== undefined) changes.push('Engine configuration changes; rolling restart.');
  if (patch.monitoring) changes.push('Monitoring collection settings change.');
  return changes;
}

export function psmdbConnect(uri: string): { shell: string; driver: string } {
  const driver = `${uri}${uri.includes('?') ? '&' : '?'}tlsCAFile=ca.pem`;
  return { shell: `mongosh '${uri.replaceAll("'", "'\\''")}' --tlsCAFile ca.pem`, driver };
}

export function credentialDatabaseLabel(provider: string, credentials: { database: string }[]): string | undefined {
  if (!credentials.some((c) => Boolean(c.database))) return undefined;
  const labels: Record<string, string> = { psmdb: 'Auth database', rabbitmq: 'Virtual host' };
  return labels[provider] ?? 'Context';
}

export function certificateDownload(certificate: string): { filename: string; href: string } {
  return { filename: 'ca.pem', href: `data:application/x-pem-file;charset=utf-8,${encodeURIComponent(certificate)}` };
}

export function requireUpgradeBackup(requiresBackup: boolean, hasSuccessfulBackup: boolean): void {
  if (requiresBackup && !hasSuccessfulBackup) throw new Error('This upgrade path requires a successful backup. Create one before starting the upgrade.');
}
