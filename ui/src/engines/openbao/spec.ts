// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '../../api/client';
import type { EngineChangeReview } from '../types';

type Spec = Schemas['ClusterSpec'];

export const PROVIDER = 'openbao';
export const API_PORT = 8200;
export const MEMBER_COUNTS = [1, 3, 5] as const;
export const SEAL_TYPES = ['shamir', 'transit', 'awskms'] as const;
export const STORAGE_TYPES = ['raft', 'postgresql'] as const;
export const LOG_LEVELS = ['trace', 'debug', 'info', 'warn', 'error'] as const;
export const DEFAULT_SHARES = 5;
export const DEFAULT_THRESHOLD = 3;
export const MAX_SHARES = 10;
export const DEFAULT_TRANSIT_MOUNT = 'transit/';
export type SealType = (typeof SEAL_TYPES)[number];
export type StorageType = (typeof STORAGE_TYPES)[number];
export type LogLevel = (typeof LOG_LEVELS)[number];

/** Typed view of `spec.engineOptions` (internal/engine/openbao/options.go). Write-only secrets are present only in drafts. */
export interface OpenBaoOptions {
  unseal: { shares: number; threshold: number };
  seal: {
    type: SealType;
    transit: { address: string; mountPath: string; keyName: string; caPEM: string; tlsServerName: string; token: string };
    awskms: { region: string; kmsKeyId: string; endpoint: string; accessKeyId: string; secretAccessKey: string };
  };
  storage: { type: StorageType; postgresql: { cluster: string } };
  logLevel: LogLevel;
}

export const IMMUTABLE_EXPLANATION = 'Unseal shares, the seal and the storage backend are fixed when the cluster is initialized; OpenBao does not migrate seals or storage in place. Create a new cluster to change them.';

function record(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function text(value: unknown, fallback = ''): string {
  return typeof value === 'string' ? value : fallback;
}

function count(value: unknown, fallback: number): number {
  return typeof value === 'number' && Number.isFinite(value) ? value : fallback;
}

function oneOf<T extends string>(value: unknown, choices: readonly T[], fallback: T): T {
  return choices.includes(value as T) ? (value as T) : fallback;
}

/** Read engine options tolerantly; absent parts take the server defaults. */
export function readOptions(spec: Pick<Spec, 'engineOptions'>): OpenBaoOptions {
  const raw = record(spec.engineOptions);
  const unseal = record(raw.unseal);
  const seal = record(raw.seal);
  const transit = record(seal.transit);
  const awskms = record(seal.awskms);
  const storage = record(raw.storage);
  const postgresql = record(storage.postgresql);
  return {
    unseal: { shares: count(unseal.shares, DEFAULT_SHARES), threshold: count(unseal.threshold, DEFAULT_THRESHOLD) },
    seal: {
      type: oneOf(seal.type, SEAL_TYPES, 'shamir'),
      transit: { address: text(transit.address), mountPath: text(transit.mountPath, DEFAULT_TRANSIT_MOUNT), keyName: text(transit.keyName), caPEM: text(transit.caPEM), tlsServerName: text(transit.tlsServerName), token: text(transit.token) },
      awskms: { region: text(awskms.region), kmsKeyId: text(awskms.kmsKeyId), endpoint: text(awskms.endpoint), accessKeyId: text(awskms.accessKeyId), secretAccessKey: text(awskms.secretAccessKey) },
    },
    storage: { type: oneOf(storage.type, STORAGE_TYPES, 'raft'), postgresql: { cluster: text(postgresql.cluster) } },
    logLevel: oneOf(raw.logLevel, LOG_LEVELS, 'info'),
  };
}

function compact(values: Record<string, string>): Record<string, string> {
  return Object.fromEntries(Object.entries(values).filter(([, value]) => value !== ''));
}

/** Serialize options into the wire shape the server decodes with unknown fields rejected; only the selected seal and storage blocks are sent. */
export function writeOptions(spec: Spec, o: OpenBaoOptions): Spec {
  const seal: Record<string, unknown> = { type: o.seal.type };
  if (o.seal.type === 'transit') seal.transit = compact({ ...o.seal.transit, mountPath: o.seal.transit.mountPath || DEFAULT_TRANSIT_MOUNT });
  if (o.seal.type === 'awskms') seal.awskms = compact(o.seal.awskms);
  const storage: Record<string, unknown> = { type: o.storage.type };
  if (o.storage.type === 'postgresql') storage.postgresql = { cluster: o.storage.postgresql.cluster };
  return { ...spec, engineOptions: { unseal: { ...o.unseal }, seal, storage, logLevel: o.logLevel } };
}

const HTTPS_URL = /^https:\/\/\S+$/;

/**
 * Mirrors parseOptions/validateSealAndStorage in internal/engine/openbao. Write-only
 * credentials are required only on create: persisted specs never echo them.
 */
export function validateOptions(o: OpenBaoOptions, create = true): string[] {
  const errors: string[] = [];
  const { shares, threshold } = o.unseal;
  if (!Number.isInteger(shares) || shares < 1 || shares > MAX_SHARES) errors.push(`Unseal shares must be a whole number between 1 and ${MAX_SHARES}.`);
  if (!Number.isInteger(threshold) || threshold < 1) errors.push('Unseal threshold must be a whole number of at least 1.');
  else if (threshold > shares) errors.push('Unseal threshold cannot exceed the number of shares.');
  else if (shares >= 2 && threshold < 2) errors.push('With more than one share the threshold must be at least 2.');
  if (o.seal.type === 'transit') {
    const t = o.seal.transit;
    if (!HTTPS_URL.test(t.address)) errors.push('Transit address must be an https:// URL.');
    if (!t.keyName.trim()) errors.push('Transit key name is required.');
    if (t.mountPath && !t.mountPath.endsWith('/')) errors.push('Transit mount path must end with a slash, for example transit/.');
    if (create && !t.token) errors.push('A transit token is required to configure auto-unseal.');
  }
  if (o.seal.type === 'awskms') {
    const a = o.seal.awskms;
    if (!a.region.trim()) errors.push('AWS KMS region is required.');
    if (!a.kmsKeyId.trim()) errors.push('AWS KMS key ID is required.');
    if (a.endpoint && !HTTPS_URL.test(a.endpoint)) errors.push('AWS KMS endpoint must be an https:// URL when set.');
    if (create && (!a.accessKeyId || !a.secretAccessKey)) errors.push('AWS access key ID and secret access key are both required to configure auto-unseal.');
  }
  if (o.storage.type === 'postgresql' && !o.storage.postgresql.cluster) errors.push('Choose the PostgreSQL cluster that stores OpenBao data.');
  if (!LOG_LEVELS.includes(o.logLevel)) errors.push('Choose a log level.');
  return errors;
}

/** Create-form validation of the complete spec (the wizard's Basics step). */
export function validateSpec(spec: Spec): string[] {
  const errors = validateOptions(readOptions(spec), true);
  if (spec.tls?.mode && spec.tls.mode !== 'requireTLS') errors.push('OpenBao always requires TLS; choose Require TLS.');
  if (spec.engineConfig?.trim()) errors.push('OpenBao does not accept engine configuration overrides; use engine options.');
  return errors;
}

/** Only the log level may change after creation; the merge patch never touches immutable blocks. */
export function optionsPatch(current: Spec, draft: Spec): Record<string, unknown> {
  const before = readOptions(current);
  const after = readOptions(draft);
  return before.logLevel === after.logLevel ? {} : { engineOptions: { logLevel: after.logLevel } };
}

/** Merge patch of everything the editor may change; initialization-time options are never included. */
export function buildPatch(current: Spec, draft: Spec): Record<string, unknown> {
  const patch: Record<string, unknown> = {};
  const engine: Record<string, unknown> = {};
  if (draft.components.engine.replicas !== current.components.engine.replicas) engine.replicas = draft.components.engine.replicas;
  if (JSON.stringify(draft.components.engine.resources) !== JSON.stringify(current.components.engine.resources)) engine.resources = draft.components.engine.resources;
  if (Object.keys(engine).length) patch.components = { engine };
  Object.assign(patch, optionsPatch(current, draft));
  for (const key of ['tls', 'exposure', 'monitoring'] as const) if (JSON.stringify(current[key]) !== JSON.stringify(draft[key])) patch[key] = draft[key];
  return patch;
}

export const openbaoChangeReview: EngineChangeReview = {
  buildPatch,
  disruptions: (current, draft) => {
    const notes: string[] = [];
    const before = current.components.engine.replicas;
    const after = draft.components.engine.replicas;
    if (after > before) notes.push(`Raft grows from ${before} to ${after} members: each new member is deployed, unsealed and admitted as a voter one at a time.`);
    if (after < before) notes.push(`Raft shrinks from ${before} to ${after} members, highest index first: a leader steps down before removal, and a removal that would break the live majority is refused (QuorumBlocked) until the other members are healthy.`);
    if (JSON.stringify(current.components.engine.resources) !== JSON.stringify(draft.components.engine.resources) || readOptions(current).logLevel !== readOptions(draft).logLevel) {
      notes.push(after === 1 ? 'Downtime: the only member restarts and is re-unsealed before serving again.' : 'Members restart one at a time, standbys first and the active node last after stepping down; each is re-unsealed before the next.');
    }
    if (JSON.stringify(current.tls) !== JSON.stringify(draft.tls)) notes.push('Listener certificates are rewritten and reloaded in place (SIGHUP); clients pinning the CA keep working unless the CA source changes.');
    if (JSON.stringify(current.exposure) !== JSON.stringify(draft.exposure)) notes.push(draft.exposure?.public ? `Every member's API becomes reachable on public port ${API_PORT} for the allowlisted addresses.` : 'Public API routes are removed; only project-private access remains.');
    return notes;
  },
};

export function sealLabel(type: SealType): string {
  return type === 'shamir' ? 'Shamir (Karakoram-held shares)' : type === 'transit' ? 'Transit auto-unseal' : 'AWS KMS auto-unseal';
}

export function storageLabel(o: OpenBaoOptions): string {
  return o.storage.type === 'raft' ? 'Integrated Raft storage' : `PostgreSQL storage (${o.storage.postgresql.cluster || 'cluster not chosen'})`;
}

export function specSummary(spec: Spec): string {
  const o = readOptions(spec);
  const n = spec.components.engine.replicas;
  return `${n}-member OpenBao cluster · ${o.seal.type === 'shamir' ? `${o.unseal.threshold} of ${o.unseal.shares} shares` : sealLabel(o.seal.type)} · ${o.storage.type === 'raft' ? 'Raft' : 'PostgreSQL'} storage`;
}

export function failureTolerance(members: number): number {
  return Math.max(0, Math.floor((members - 1) / 2));
}

/** Clusters a PostgreSQL storage binding may point at: PostgreSQL in the same namespace. */
export function storageClusters<T extends { name: string; namespace: string; spec: { provider: string } }>(clusters: T[], namespace: string): T[] {
  return clusters.filter((c) => c.namespace === namespace && c.spec.provider === 'postgres');
}

// ---- Status -----------------------------------------------------------------

export const CONDITION_SEALED = 'Sealed';
export const CONDITION_INIT_LOST = 'InitializationOutcomeLost';
export const CONDITION_QUORUM_BLOCKED = 'QuorumBlocked';
/** Per-member voter observation: condition type `RaftVoter/<member>`, reason HealthyVoter, UnhealthyVoter, NotVoter or ObservationUnavailable. */
export const CONDITION_RAFT_VOTER_PREFIX = 'RaftVoter/';

export type MemberSealState = 'active' | 'standby' | 'sealed' | 'uninitialized' | 'pending' | 'leaving' | 'unknown';

export interface MemberSeal {
  name: string;
  state: MemberSealState;
  version?: string;
  health: boolean;
  /** Raft voter observation, undefined for PostgreSQL storage or before the first observation. */
  voter?: { status: Schemas['Condition']['status']; reason: string; message: string };
}

export const VOTER_LABEL: Record<string, string> = { HealthyVoter: 'Healthy voter', UnhealthyVoter: 'Unhealthy voter', NotVoter: 'Not a voter', ObservationUnavailable: 'No observation' };

/** Member edge status: role primary/secondary while unsealed, state UNINITIALIZED/SEALED/RUNNING/PENDING/LEAVING otherwise. */
export function memberSealState(member: Pick<Schemas['Member'], 'role' | 'state'>): MemberSealState {
  const state = member.state.toUpperCase();
  if (state === 'SEALED') return 'sealed';
  if (state === 'UNINITIALIZED') return 'uninitialized';
  if (state === 'PENDING') return 'pending';
  if (state.startsWith('LEAVING')) return 'leaving';
  if (member.role === 'primary') return 'active';
  if (member.role === 'secondary') return 'standby';
  return 'unknown';
}

export const MEMBER_STATE_LABEL: Record<MemberSealState, string> = {
  active: 'Active (unsealed)',
  standby: 'Standby (unsealed)',
  sealed: 'Sealed',
  uninitialized: 'Not initialized',
  pending: 'Pending',
  leaving: 'Leaving',
  unknown: 'Unknown',
};

export interface SealOverview {
  members: MemberSeal[];
  active: string | undefined;
  sealed: string[];
  uninitialized: string[];
  conditions: { sealed?: Schemas['Condition']; initializationLost?: Schemas['Condition']; quorumBlocked?: Schemas['Condition'] };
}

function active(conditions: Schemas['Condition'][], type: string): Schemas['Condition'] | undefined {
  return conditions.find((c) => c.type === type && c.status === 'True');
}

export function sealOverview(status: Pick<Schemas['ClusterStatus'], 'members' | 'conditions'>): SealOverview {
  const members = status.members.map((m) => {
    const voter = status.conditions.find((c) => c.type === CONDITION_RAFT_VOTER_PREFIX + m.name);
    return { name: m.name, state: memberSealState(m), version: m.version, health: m.health, ...(voter ? { voter: { status: voter.status, reason: voter.reason, message: voter.message } } : {}) };
  });
  return {
    members,
    active: members.find((m) => m.state === 'active')?.name,
    sealed: members.filter((m) => m.state === 'sealed').map((m) => m.name),
    uninitialized: members.filter((m) => m.state === 'uninitialized').map((m) => m.name),
    conditions: {
      sealed: active(status.conditions, CONDITION_SEALED),
      initializationLost: active(status.conditions, CONDITION_INIT_LOST),
      quorumBlocked: active(status.conditions, CONDITION_QUORUM_BLOCKED),
    },
  };
}

/** Operator guidance for each cluster condition OpenBao publishes. */
export const CONDITION_GUIDANCE: Record<'sealed' | 'initializationLost' | 'quorumBlocked', { title: string; tone: 'warn' | 'danger'; guidance: string }> = {
  sealed: {
    title: 'Members are sealed',
    tone: 'warn',
    guidance: 'Karakoram unseals sealed members automatically with its stored shares on every sync pass (about every 30 seconds when converged). With an auto-unseal seal, the members unseal themselves once the transit server or AWS KMS is reachable. If members stay sealed, check the member logs under Events & logs and that the control plane can reach the member API on port 8200.',
  },
  initializationLost: {
    title: 'Initialization outcome lost',
    tone: 'danger',
    guidance: 'The first member reports that it is initialized but its encrypted initialization receipt is gone, so Karakoram never received the unseal shares or the root token. Nothing can unseal this cluster. Delete it and create a new cluster; restore a Raft snapshot into the new cluster if you have one. Keep revealed unseal keys of your other clusters offline so this cannot happen to them.',
  },
  quorumBlocked: {
    title: 'Raft membership change refused',
    tone: 'warn',
    guidance: 'Removing a member now would leave fewer live Raft voters than a majority of the remaining configuration, so the scale-down or replacement is paused instead of risking the data. Bring the unhealthy members back (sealed members are re-unsealed automatically) and the change resumes. If a majority of members is permanently lost, follow the documented manual peers.json recovery before retrying.',
  },
};

// ---- Database connections ---------------------------------------------------

export const CONNECTION_NAME = /^[a-z][a-z0-9-]{0,30}$/;
export const CONNECTION_TARGET_PROVIDERS = ['postgres', 'psmdb', 'rabbitmq'] as const;
export const ACCESS_LEVELS = ['readonly', 'readwrite'] as const;
export const DEFAULT_TTL_SECONDS = 3600;
export const DEFAULT_MAX_TTL_SECONDS = 86400;
export type AccessLevel = (typeof ACCESS_LEVELS)[number];

export const TARGET_PROVIDER_LABEL: Record<string, string> = { postgres: 'PostgreSQL', psmdb: 'MongoDB', rabbitmq: 'RabbitMQ' };

/** Clusters a database connection may target: PostgreSQL, MongoDB replica sets and RabbitMQ in the same namespace. */
export function connectionTargets<T extends { name: string; namespace: string; spec: { provider: string; topology: { type: string } } }>(clusters: T[], namespace: string): T[] {
  return clusters.filter((c) => c.namespace === namespace && (c.spec.provider === 'postgres' || c.spec.provider === 'rabbitmq' || (c.spec.provider === 'psmdb' && c.spec.topology.type === 'replicaSet')));
}

/** Per-target meaning of the optional role scope, or undefined when the engine has none. */
export function databaseScope(targetProvider: string): { label: string; hint: string } | undefined {
  if (targetProvider === 'psmdb') return { label: 'Database (optional)', hint: 'Scopes the role to one MongoDB database; empty grants it on every database.' };
  if (targetProvider === 'rabbitmq') return { label: 'Virtual host', hint: 'RabbitMQ vhost the issued user may access; empty means /.' };
  return undefined;
}

export interface RoleDraft {
  name: string;
  access: AccessLevel;
  database: string;
  defaultTtl: string;
  maxTtl: string;
}

export function emptyRole(): RoleDraft {
  return { name: '', access: 'readonly', database: '', defaultTtl: '', maxTtl: '' };
}

function ttl(value: string, label: string, errors: string[]): number | undefined {
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  const seconds = Number(trimmed);
  if (!Number.isInteger(seconds) || seconds < 1) errors.push(`${label} must be a whole number of seconds (at least 1).`);
  return seconds;
}

/** Build the create request from the dialog draft; errors mirror the server's shape rules. */
export function buildConnectionWrite(name: string, targetCluster: string, targetProvider: string | undefined, roles: RoleDraft[]): { body: Schemas['OpenBaoDatabaseConnectionWrite']; errors: string[] } {
  const errors: string[] = [];
  const trimmedName = name.trim();
  if (!CONNECTION_NAME.test(trimmedName)) errors.push('Connection name must start with a letter and use only lowercase letters, digits and dashes (at most 31 characters).');
  if (!targetCluster) errors.push('Choose a target cluster.');
  if (roles.length === 0) errors.push('Add at least one role.');
  const seen = new Set<string>();
  const scoped = targetProvider === undefined || databaseScope(targetProvider) !== undefined;
  const written = roles.map((role, index) => {
    const roleName = role.name.trim();
    const label = `Role ${index + 1}`;
    if (!CONNECTION_NAME.test(roleName)) errors.push(`${label}: name must start with a letter and use only lowercase letters, digits and dashes (at most 31 characters).`);
    else if (seen.has(roleName)) errors.push(`${label}: role names must be unique per connection.`);
    seen.add(roleName);
    if (!ACCESS_LEVELS.includes(role.access)) errors.push(`${label}: choose readonly or readwrite access.`);
    const defaultTtlSeconds = ttl(role.defaultTtl, `${label}: default TTL`, errors) ?? DEFAULT_TTL_SECONDS;
    const maxTtlSeconds = ttl(role.maxTtl, `${label}: maximum TTL`, errors) ?? DEFAULT_MAX_TTL_SECONDS;
    if (maxTtlSeconds < defaultTtlSeconds) errors.push(`${label}: maximum TTL must be at least the default TTL.`);
    const database = role.database.trim();
    return { name: roleName, access: role.access, ...(scoped && database ? { database } : {}), defaultTtlSeconds, maxTtlSeconds };
  });
  return { body: { name: trimmedName, targetCluster, roles: written }, errors };
}

export function formatTtl(seconds: number | undefined, fallback: number): string {
  const value = seconds ?? fallback;
  if (value % 86400 === 0) return `${value / 86400}d`;
  if (value % 3600 === 0) return `${value / 3600}h`;
  if (value % 60 === 0) return `${value / 60}m`;
  return `${value}s`;
}

// ---- Restore from a deleted cluster's snapshot ------------------------------

export type RestoreShares = Schemas['OpenBaoRestoreUnsealShares'];

/** Draft of the offline copy an operator pastes: one share per line, exactly as revealed. */
export interface RestoreSharesDraft {
  sealType: SealType;
  threshold: string;
  shares: string;
}

export function emptyRestoreShares(): RestoreSharesDraft {
  return { sealType: 'shamir', threshold: '', shares: '' };
}

/** Build the write-only request field from the pasted draft; validity is judged by validateRestore. */
export function buildRestoreShares(draft: RestoreSharesDraft): RestoreShares {
  const shares = draft.shares.split(/\s+/).filter(Boolean);
  const recovery = draft.sealType !== 'shamir';
  return { shares, threshold: Number(draft.threshold.trim()), sealType: draft.sealType, ...(recovery ? { recovery } : {}) };
}

/** Material pasted for a restore must be complete; whether it is required at all (source deleted) is the server's call. */
export function validateRestore(extras: Pick<Schemas['CreateRestoreRequest'], 'openbaoUnsealShares'>): string[] {
  if (!extras.openbaoUnsealShares) return [];
  const { shares, threshold } = extras.openbaoUnsealShares;
  const errors: string[] = [];
  if (shares.length === 0) errors.push('Paste at least one unseal share from the offline copy.');
  else if (shares.length > MAX_SHARES) errors.push(`Supply at most ${MAX_SHARES} distinct unseal shares.`);
  else if (new Set(shares).size !== shares.length) errors.push('Each supplied unseal share must be distinct.');
  else if (shares.some((share) => !/^[A-Za-z0-9+/=]+$/.test(share))) errors.push('Shares must be the base64 values exactly as revealed, one per line.');
  if (!Number.isInteger(threshold) || threshold < 1) errors.push('Unseal threshold must be a whole number of at least 1.');
  else if (threshold > shares.length) errors.push(`The threshold (${threshold}) needs at least that many shares; ${shares.length} pasted.`);
  return errors;
}
