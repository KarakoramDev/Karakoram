// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Pure helpers for the Temporal create-form and detail pages. They mirror the
// server rules in internal/engine/temporal (provider.go, options.go, tspec) so
// the wizard can report problems before the request is sent. No React here:
// the file is covered by node --test.

import type { Schemas } from '../../api/client';
import { replicaLimits, type ReplicaLimits } from '../index.ts';
import type { ComponentValidation, EngineChangeReview } from '../types';

type ClusterSpec = Schemas['ClusterSpec'];
type Component = Schemas['Component'];
type Resources = Schemas['Resources'];
type Topology = Schemas['ProviderTopology'];
type Condition = Schemas['Cluster']['status']['conditions'][number];

export const PROVIDER = 'temporal';
/** Private gRPC frontend port; never exposed through the generic exposure block. */
export const FRONTEND_PORT = 7233;

/** Components in roll order (tspec.RollOrder); all live under `components.extra`. */
export const COMPONENTS = ['history', 'matching', 'frontend', 'worker', 'server', 'ui'] as const;
export type ComponentName = (typeof COMPONENTS)[number];
export const TOPOLOGIES = ['combined', 'split'] as const;
export type TopologyType = (typeof TOPOLOGIES)[number];

export const COMPONENT_LABEL: Record<ComponentName, string> = { history: 'History', matching: 'Matching', frontend: 'Frontend', worker: 'Worker', server: 'Server', ui: 'Web UI' };
export const COMPONENT_DESCRIPTION: Record<ComponentName, string> = {
  history: 'Owns workflow state and the history shards; the busiest service.',
  matching: 'Dispatches tasks to workers through task queues.',
  frontend: 'gRPC entry point for SDK clients and the Web UI.',
  worker: 'Temporal’s internal system worker, not your application workers.',
  server: 'History, matching, frontend and worker roles in one process.',
  ui: 'Temporal Web UI (ui-server); zero disables it.',
};
/** Replica ceilings from the provider component set (provider.go componentSet). */
export const MAX_REPLICAS: Record<ComponentName, number> = { history: 9, matching: 5, frontend: 5, worker: 3, server: 5, ui: 3 };
export const DEFAULT_RESOURCES: Resources = { cpuMode: 'SHARED', minCpu: 1, maxCpu: 1, minRam: 1, maxRam: 1, minDisk: 1, maxDisk: 1 };

export const DEFAULT_HISTORY_SHARDS = 512;
export const MAX_HISTORY_SHARDS = 16384;
export const MIN_RETENTION_HOURS = 24;
export const DEFAULT_RETENTION = '72h';
export const MAX_DESCRIPTION = 1024;
/** Temporal namespace names (tspec safeNamespace). */
export const NAMESPACE_RE = /^[A-Za-z0-9][A-Za-z0-9._-]{1,254}$/;
/** PostgreSQL database names (tspec safeDatabase). */
export const DATABASE_RE = /^[a-z_][a-z0-9_]{0,62}$/;
/** Minimum major version of the bound PostgreSQL source (pgbinding MinMajor). */
export const MIN_POSTGRES_MAJOR = 12;
/** Client certificate TTL bounds in hours (default 30 days, maximum 397 days). */
export const DEFAULT_TTL_HOURS = 720;
export const MAX_TTL_HOURS = 9528;
export const DOCS_URL = 'https://karakoram.dev/docs/temporal/';

export interface NamespaceDeclaration {
  name: string;
  retention: string;
  description?: string;
}

export interface OIDCOptions {
  issuer: string;
  clientId: string;
}

/** Typed view of `spec.engineOptions` for this provider (internal/engine/temporal/options.go). */
export interface TemporalOptions {
  persistence: { postgres: { cluster: string; namespace?: string; database?: string; visibilityDatabase?: string } };
  historyShards: number;
  frontend: { requireClientAuth: boolean };
  namespaces: NamespaceDeclaration[];
  ui: { publicHttps: boolean; oidc?: OIDCOptions };
}

function record(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function text(value: unknown): string {
  return typeof value === 'string' ? value : '';
}

/** Read engine options tolerantly; unknown or malformed parts fall back to the provider defaults. */
export function readOptions(spec: Pick<ClusterSpec, 'engineOptions'>): TemporalOptions {
  const raw = record(spec.engineOptions);
  const postgres = record(record(raw.persistence).postgres);
  const ui = record(raw.ui);
  const oidc = ui.oidc === undefined || ui.oidc === null ? undefined : record(ui.oidc);
  const namespaces = Array.isArray(raw.namespaces)
    ? raw.namespaces.map((n) => {
        const r = record(n);
        return { name: text(r.name), retention: text(r.retention), ...(text(r.description) ? { description: text(r.description) } : {}) };
      })
    : [];
  return {
    persistence: {
      postgres: {
        cluster: text(postgres.cluster),
        ...(text(postgres.namespace) ? { namespace: text(postgres.namespace) } : {}),
        ...(text(postgres.database) ? { database: text(postgres.database) } : {}),
        ...(text(postgres.visibilityDatabase) ? { visibilityDatabase: text(postgres.visibilityDatabase) } : {}),
      },
    },
    historyShards: typeof raw.historyShards === 'number' ? raw.historyShards : DEFAULT_HISTORY_SHARDS,
    frontend: { requireClientAuth: record(raw.frontend).requireClientAuth !== false },
    namespaces,
    ui: { publicHttps: ui.publicHttps === true, ...(oidc ? { oidc: { issuer: text(oidc.issuer), clientId: text(oidc.clientId) } } : {}) },
  };
}

/** Wire shape of the options (the server rejects unknown fields and inline secrets); optional keys are omitted when unset. */
export type WireOptions = {
  persistence: { postgres: { cluster: string; namespace?: string; database?: string; visibilityDatabase?: string } };
  historyShards: number;
  frontend: { requireClientAuth: boolean };
  namespaces?: NamespaceDeclaration[];
  ui: { publicHttps: boolean; oidc?: OIDCOptions };
};

export function wireOptions(o: TemporalOptions): WireOptions {
  const pg = o.persistence.postgres;
  return {
    persistence: {
      postgres: {
        cluster: pg.cluster,
        ...(pg.namespace ? { namespace: pg.namespace } : {}),
        ...(pg.database ? { database: pg.database } : {}),
        ...(pg.visibilityDatabase ? { visibilityDatabase: pg.visibilityDatabase } : {}),
      },
    },
    historyShards: o.historyShards,
    frontend: { requireClientAuth: o.frontend.requireClientAuth },
    ...(o.namespaces.length ? { namespaces: o.namespaces.map((n) => ({ name: n.name, retention: n.retention, ...(n.description ? { description: n.description } : {}) })) } : {}),
    ui: { publicHttps: o.ui.publicHttps, ...(o.ui.oidc ? { oidc: { issuer: o.ui.oidc.issuer, clientId: o.ui.oidc.clientId } } : {}) },
  };
}

export function writeOptions(spec: ClusterSpec, o: TemporalOptions): ClusterSpec {
  return { ...spec, engineOptions: wireOptions(o) };
}

/** Database names the provider derives when none are given (options.go optionsFor). */
export function defaultDatabase(clusterName: string): string {
  return `temporal_${clusterName.replaceAll('-', '_')}`;
}

export function defaultVisibilityDatabase(clusterName: string): string {
  return `${defaultDatabase(clusterName)}_visibility`;
}

const DURATION_UNIT_HOURS: Record<string, number> = { h: 1, m: 1 / 60, s: 1 / 3600, ms: 1 / 3_600_000, us: 1 / 3_600_000_000, 'µs': 1 / 3_600_000_000, 'μs': 1 / 3_600_000_000, ns: 1 / 3_600_000_000_000 };

/** Hours expressed by a Go duration such as `72h`, `168h` or `24h30m`; undefined when it does not parse. */
export function durationHours(value: string): number | undefined {
  const input = value.trim();
  if (!/^\d/.test(input)) return undefined;
  const part = /(\d+(?:\.\d*)?|\.\d+)(ns|us|µs|μs|ms|s|m|h)/gy;
  let hours = 0;
  let consumed = 0;
  for (let match = part.exec(input); match; match = part.exec(input)) {
    hours += Number(match[1]) * DURATION_UNIT_HOURS[match[2]!]!;
    consumed = part.lastIndex;
  }
  return consumed === input.length && consumed > 0 ? hours : undefined;
}

/** Plain length of a retention duration: `168h` → `7 days`, `36h` → `36 hours`; undefined when it does not parse. */
export function retentionLabel(value: string): string | undefined {
  const hours = durationHours(value);
  if (hours === undefined) return undefined;
  if (hours % 24 === 0) return `${hours / 24} day${hours === 24 ? '' : 's'}`;
  return `${Number(hours.toFixed(2))} hours`;
}

/** Retention with its plain length, e.g. `168h (7 days)`; unparsable values pass through. */
export function formatRetention(value: string): string {
  const label = retentionLabel(value);
  return label ? `${value} (${label})` : value;
}

export function retentionError(value: string): string | undefined {
  const hours = durationHours(value);
  if (hours === undefined) return 'Use a Go duration such as 72h or 168h.';
  if (hours < MIN_RETENTION_HOURS) return `Retention must be at least ${MIN_RETENTION_HOURS}h.`;
  return undefined;
}

/** Rename a declaration; `default` always carries the provider-fixed 72h retention so the row never becomes unfixable. */
export function renameNamespace(declaration: NamespaceDeclaration, name: string): NamespaceDeclaration {
  return { ...declaration, name, ...(name === 'default' ? { retention: DEFAULT_RETENTION } : {}) };
}

export function namespaceNameError(name: string, all: string[], index: number): string | undefined {
  if (!NAMESPACE_RE.test(name)) return '2–255 characters: letters, digits, dots, underscores or hyphens, starting with a letter or digit.';
  if (name === 'default' && all.indexOf(name) === index) return undefined;
  if (all.indexOf(name) !== index) return 'Namespace names must be unique.';
  return undefined;
}

/** Declaration errors matching options.go validateOptions and tspec Namespace.Validate. */
export function namespaceErrors(namespaces: NamespaceDeclaration[]): string[] {
  const errors: string[] = [];
  const names = namespaces.map((n) => n.name);
  namespaces.forEach((n, i) => {
    const label = n.name ? `Namespace ${n.name}` : `Namespace #${i + 1}`;
    const nameError = namespaceNameError(n.name, names, i);
    if (nameError) errors.push(`${label}: ${nameError}`);
    const retention = retentionError(n.retention);
    if (retention) errors.push(`${label}: ${retention}`);
    if (n.name === 'default' && n.retention !== DEFAULT_RETENTION) errors.push('The default namespace keeps its 72h retention.');
    if ((n.description ?? '').length > MAX_DESCRIPTION) errors.push(`${label}: description must be at most ${MAX_DESCRIPTION} characters.`);
  });
  return errors;
}

export function oidcErrors(o: TemporalOptions): string[] {
  const errors: string[] = [];
  const oidc = o.ui.oidc;
  if (oidc && !o.ui.publicHttps) errors.push('OIDC login needs public HTTPS for the Web UI (the callback origin must be HTTPS).');
  if (oidc) {
    let valid = false;
    try {
      const url = new URL(oidc.issuer);
      valid = url.protocol === 'https:' && url.host !== '' && url.username === '' && url.password === '';
    } catch {
      valid = false;
    }
    if (!valid) errors.push('OIDC issuer must be an https:// URL without credentials.');
    if (!oidc.clientId.trim()) errors.push('OIDC client ID is required.');
  }
  if (o.ui.publicHttps && !oidc) errors.push('A public Web UI requires OIDC login; configure the issuer and client ID.');
  return errors;
}

export function historyShardsError(value: number): string | undefined {
  if (!Number.isInteger(value) || value < 1 || value > MAX_HISTORY_SHARDS) return `History shards must be a whole number from 1 to ${MAX_HISTORY_SHARDS}.`;
  return undefined;
}

export function postgresMajor(version: string): number {
  return Number(version.split('.')[0]);
}

export function componentOf(spec: ClusterSpec, name: ComponentName): Component | undefined {
  return spec.components.extra?.[name];
}

export function replicasOf(spec: ClusterSpec, name: ComponentName): number {
  return componentOf(spec, name)?.replicas ?? 0;
}

export function withComponent(spec: ClusterSpec, name: ComponentName, component: Component): ClusterSpec {
  return { ...spec, components: { ...spec.components, extra: { ...spec.components.extra, [name]: component } } };
}

/** Server-role components of a topology; `ui` is available in both. */
export function serverComponents(topology: string): ComponentName[] {
  return topology === 'split' ? ['history', 'matching', 'frontend', 'worker'] : ['server'];
}

export function activeComponents(spec: Pick<ClusterSpec, 'topology'>): ComponentName[] {
  return [...serverComponents(spec.topology.type), 'ui'];
}

/** Catalog default for a component in the chosen topology, or the built-in fallback. */
export function defaultComponent(topology: Topology | undefined, name: ComponentName): Component {
  const c = topology?.defaults.components.extra?.[name];
  return c ? structuredClone(c) : { replicas: 1, resources: { ...DEFAULT_RESOURCES } };
}

/** Set replicas, materializing the component from catalog defaults when it is absent. */
export function withReplicas(spec: ClusterSpec, topology: Topology | undefined, name: ComponentName, replicas: number): ClusterSpec {
  const current = componentOf(spec, name) ?? defaultComponent(topology, name);
  return withComponent(spec, name, { ...current, replicas });
}

/** Replica bounds: the catalog ceiling, with server roles required (≥ 1) and the UI optional (≥ 0). */
export function limitsFor(topology: Topology | undefined, name: ComponentName): ReplicaLimits {
  const limits = replicaLimits(topology, name);
  const min = name === 'ui' ? 0 : 1;
  return { minReplicas: Math.max(min, limits.minReplicas ?? min), maxReplicas: limits.maxReplicas ?? MAX_REPLICAS[name] };
}

export function replicaError(replicas: number, limits: ReplicaLimits): string | undefined {
  if (!Number.isInteger(replicas)) return 'Instance count must be a whole number.';
  if (limits.minReplicas !== undefined && replicas < limits.minReplicas) return limits.minReplicas === 0 ? undefined : `At least ${limits.minReplicas} instance${limits.minReplicas === 1 ? '' : 's'} required.`;
  if (limits.maxReplicas !== undefined && replicas > limits.maxReplicas) return `At most ${limits.maxReplicas} instances allowed.`;
  return undefined;
}

/** Basics-step errors: persistence, shards, namespaces, Web UI and the unsupported generic blocks. */
export function validateSpec(spec: ClusterSpec, persisted?: ClusterSpec): string[] {
  const errors: string[] = [];
  const o = readOptions(spec);
  if (spec.engineConfig?.trim()) errors.push('Temporal is configured through engine options; clear the engine configuration overrides.');
  if (spec.exposure?.public) errors.push('The Temporal frontend is private; only the Web UI can be public, through the public HTTPS option.');
  if (spec.tls && spec.tls.mode !== 'requireTLS') errors.push('Temporal always requires TLS on the frontend; keep the requireTLS mode.');
  const pg = o.persistence.postgres;
  if (!pg.cluster) errors.push('Choose the PostgreSQL cluster that stores Temporal persistence and visibility.');
  for (const [label, value] of [['Persistence database', pg.database], ['Visibility database', pg.visibilityDatabase]] as const) {
    if (value && !DATABASE_RE.test(value)) errors.push(`${label}: use up to 63 lowercase letters, digits or underscores, not starting with a digit.`);
  }
  if (pg.database && pg.database === pg.visibilityDatabase) errors.push('Persistence and visibility databases must differ.');
  const shards = historyShardsError(o.historyShards);
  if (shards) errors.push(shards);
  errors.push(...namespaceErrors(o.namespaces), ...oidcErrors(o));
  if (o.ui.publicHttps && replicasOf(spec, 'ui') === 0) errors.push('A public Web UI needs at least one Web UI instance under Topology.');
  if (persisted) {
    const before = readOptions(persisted);
    if (JSON.stringify(before.persistence) !== JSON.stringify(o.persistence)) errors.push('PostgreSQL persistence is immutable after creation.');
    if (before.historyShards !== o.historyShards) errors.push('History shards are immutable after creation.');
    if (persisted.topology.type !== spec.topology.type) errors.push('The topology type is immutable after creation.');
  }
  return errors;
}

/** Resource checks for one component. Components are stateless, yet `noDisk` stays false: the server requires a 1 GiB+ runtime disk. */
export type ResourcesCheck = (resources: Resources, noDisk: boolean, name: ComponentName) => string[];

/** Topology- and Resources-step errors for every component of the chosen topology. */
export function validateComponents(spec: ClusterSpec, topology: Topology | undefined, checkResources: ResourcesCheck): ComponentValidation {
  const t: string[] = [];
  const r: string[] = [];
  if (!TOPOLOGIES.includes(spec.topology.type as TopologyType)) t.push('Choose the combined or split topology.');
  if (spec.components.engine.replicas !== 0) t.push('Temporal runs every role under components.extra; the generic engine component must stay at zero replicas.');
  const active = activeComponents(spec);
  for (const name of COMPONENTS) {
    const component = componentOf(spec, name);
    const label = COMPONENT_LABEL[name];
    if (!active.includes(name)) {
      if (component && component.replicas !== 0) t.push(`${label} is not part of the ${spec.topology.type} topology.`);
      continue;
    }
    if (!component) {
      if (name !== 'ui') t.push(`${label}: at least 1 instance required.`);
      continue;
    }
    const error = replicaError(component.replicas, limitsFor(topology, name));
    if (error) t.push(`${label}: ${error}`);
    if (component.replicas === 0) continue;
    r.push(...checkResources(component.resources, false, name).map((e) => `${label}: ${e}`));
  }
  return { topology: t, resources: r };
}

/** Short topology label, e.g. "Split · 3 history, 2 matching, 2 frontend, 1 worker · Web UI". */
export function specSummary(spec: ClusterSpec): string {
  const roles = serverComponents(spec.topology.type).map((name) => `${replicasOf(spec, name)} ${name}`).join(', ');
  const ui = replicasOf(spec, 'ui');
  return `${spec.topology.type === 'split' ? 'Split' : 'Combined'} · ${roles} · ${ui === 0 ? 'no Web UI' : ui === 1 ? 'Web UI' : `Web UI ×${ui}`}`;
}

export function configurationReviewRows(spec: ClusterSpec, clusterName = ''): [string, string][] {
  const o = readOptions(spec);
  const pg = o.persistence.postgres;
  const source = pg.cluster ? `${pg.namespace ? `${pg.namespace}/` : ''}${pg.cluster}` : 'not chosen';
  const placeholder = clusterName || '<cluster>';
  return [
    ['PostgreSQL source', source],
    ['Databases', `${pg.database || defaultDatabase(placeholder)} · ${pg.visibilityDatabase || defaultVisibilityDatabase(placeholder)}`],
    ['History shards', `${o.historyShards} (immutable)`],
    ['Client certificates', o.frontend.requireClientAuth ? 'required (mTLS)' : 'not required (TLS only)'],
    ['Namespaces', ['default (72h)', ...o.namespaces.filter((n) => n.name !== 'default').map((n) => `${n.name} (${n.retention})`)].join(', ')],
    ['Web UI', replicasOf(spec, 'ui') === 0 ? 'disabled' : o.ui.publicHttps ? `public HTTPS with OIDC login (${o.ui.oidc?.issuer ?? 'issuer missing'})` : 'private to the project network'],
  ];
}

export function resourceRows(spec: ClusterSpec): [string, string][] {
  return activeComponents(spec)
    .map((name): [string, string] | undefined => {
      const c = componentOf(spec, name);
      if (!c || c.replicas === 0) return undefined;
      const r = c.resources;
      return [`${COMPONENT_LABEL[name]} resources`, `${c.replicas} × ${r.minCpu}–${r.maxCpu} vCPU ${r.cpuMode.toLowerCase()}, ${r.minRam}–${r.maxRam} GiB RAM, ${r.minDisk}–${r.maxDisk} GiB runtime disk`];
    })
    .filter((row): row is [string, string] => row !== undefined);
}

function same(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

/**
 * RFC 7396 merge patch turning the persisted spec into the draft: components (per name,
 * only the replicas/resources that changed), engineOptions as a whole with nulls for
 * dropped optional keys, and monitoring. Topology, TLS, exposure and version are never patched here.
 */
export function specPatch(current: ClusterSpec, draft: ClusterSpec): Record<string, unknown> {
  const patch: Record<string, unknown> = {};
  const extra: Record<string, unknown> = {};
  for (const name of COMPONENTS) {
    const before = componentOf(current, name);
    const after = componentOf(draft, name);
    if (!after || same(before, after)) continue;
    const change: Record<string, unknown> = {};
    if (before?.replicas !== after.replicas) change.replicas = after.replicas;
    if (!same(before?.resources, after.resources)) change.resources = after.resources;
    if (Object.keys(change).length) extra[name] = before ? change : after;
  }
  if (Object.keys(extra).length) patch.components = { extra };
  const before = readOptions(current);
  const after = readOptions(draft);
  if (!same(before, after)) {
    const wire = wireOptions(after);
    // Merge-patch semantics: optional keys that disappeared must be nulled, not omitted.
    patch.engineOptions = { ...wire, namespaces: wire.namespaces ?? null, ui: { ...wire.ui, oidc: wire.ui.oidc ?? null } };
  }
  if ((draft.monitoring?.enabled ?? true) !== (current.monitoring?.enabled ?? true)) patch.monitoring = { enabled: draft.monitoring?.enabled ?? true };
  return patch;
}

/** Human-readable consequences of applying `draft` over `current`, for the confirmation dialog. */
export function disruptions(current: ClusterSpec, draft: ClusterSpec): string[] {
  const out: string[] = [];
  for (const name of COMPONENTS) {
    const before = componentOf(current, name);
    const after = componentOf(draft, name);
    if (!after) continue;
    const label = COMPONENT_LABEL[name];
    const was = before?.replicas ?? 0;
    if (was !== after.replicas) {
      if (after.replicas === 0) out.push(`${label} is removed; the Web UI becomes unavailable.`);
      else if (was === 0) out.push(`${label} starts with ${after.replicas} instance${after.replicas === 1 ? '' : 's'}.`);
      else out.push(`${label} scales from ${was} to ${after.replicas} instances${name === 'history' ? '; history shards rebalance across the new instances' : ''}.`);
    } else if (before && !same(before.resources, after.resources) && after.replicas > 0) {
      out.push(`${label} resources change; its instances restart one at a time${after.replicas === 1 ? ' (brief unavailability with a single instance)' : ''}.`);
    }
  }
  const before = readOptions(current);
  const after = readOptions(draft);
  if (before.frontend.requireClientAuth !== after.frontend.requireClientAuth) {
    out.push(after.frontend.requireClientAuth ? 'The frontend restarts and rejects clients that do not present a certificate issued by this cluster.' : 'The frontend restarts and accepts any TLS client without a client certificate; rely on network isolation.');
  }
  const added = after.namespaces.filter((n) => !before.namespaces.some((b) => b.name === n.name)).map((n) => n.name);
  const removed = before.namespaces.filter((n) => !after.namespaces.some((a) => a.name === n.name)).map((n) => n.name);
  const changed = after.namespaces.filter((n) => before.namespaces.some((b) => b.name === n.name && (b.retention !== n.retention || (b.description ?? '') !== (n.description ?? '')))).map((n) => n.name);
  if (added.length) out.push(`Namespace${added.length > 1 ? 's' : ''} ${added.join(', ')} will be registered once a frontend is ready.`);
  if (changed.length) out.push(`Retention or description of ${changed.join(', ')} is updated in Temporal.`);
  if (removed.length) out.push(`${removed.join(', ')} ${removed.length > 1 ? 'are' : 'is'} no longer managed; the namespace stays in Temporal until deleted from the Namespaces tab.`);
  if (before.ui.publicHttps !== after.ui.publicHttps) {
    out.push(after.ui.publicHttps ? 'The Web UI gets a public HTTPS route behind OIDC login; set the OIDC client secret on the Web UI tab before users sign in.' : 'The public Web UI route is removed; the UI stays reachable inside the project network.');
  } else if (!same(before.ui.oidc, after.ui.oidc)) {
    out.push('The Web UI restarts with the new OIDC settings; update the client secret on the Web UI tab if the provider changed.');
  }
  return out;
}

export const temporalChangeReview: EngineChangeReview = { buildPatch: specPatch, disruptions };

export interface BindingStatus {
  waiting: boolean;
  message?: string;
  sourceNamespace?: string;
  sourceCluster?: string;
  shared?: boolean;
  consumerCount?: number;
}

/** The PostgreSQLBinding condition carries the bound source as JSON (status.go). */
export function bindingStatus(conditions: Condition[]): BindingStatus | undefined {
  const condition = conditions.find((c) => c.type === 'PostgreSQLBinding');
  if (!condition) return undefined;
  if (condition.status !== 'True') return { waiting: true, message: condition.message };
  let parsed: Record<string, unknown> = {};
  try {
    parsed = record(JSON.parse(condition.message ?? ''));
  } catch {
    parsed = {};
  }
  return {
    waiting: false,
    sourceNamespace: text(parsed.sourceNamespace) || undefined,
    sourceCluster: text(parsed.sourceCluster) || undefined,
    shared: typeof parsed.shared === 'boolean' ? parsed.shared : undefined,
    consumerCount: typeof parsed.consumerCount === 'number' ? parsed.consumerCount : undefined,
  };
}

export type ClientCertificate = Schemas['TemporalClientCertificate'];
export type IssuedClientCertificate = Schemas['TemporalIssuedClientCertificate'];

export type CertificateState = 'expired' | 'superseded-ca' | 'valid';

/** `superseded-ca`: issued by a CA that differs from the current one (after a rotate-tls); `expired`: past notAfter. */
export function certificateState(certificate: ClientCertificate, now: number): CertificateState {
  if (Date.parse(certificate.notAfter) <= now) return 'expired';
  if (!certificate.currentCa) return 'superseded-ca';
  return 'valid';
}

export function shortFingerprint(fingerprint: string): string {
  const hex = fingerprint.replace(/^sha256:/i, '');
  return hex.length > 20 ? `${hex.slice(0, 20)}…` : hex;
}

export function ttlError(hours: number): string | undefined {
  if (!Number.isInteger(hours) || hours < 1 || hours > MAX_TTL_HOURS) return `TTL must be a whole number of hours from 1 to ${MAX_TTL_HOURS} (397 days).`;
  return undefined;
}

export function commonNameError(name: string): string | undefined {
  if (!name.trim()) return 'Common name is required.';
  if (name.length > 64) return 'Common name must be at most 64 characters.';
  return undefined;
}

export interface CertificateFile {
  name: string;
  content: string;
}

/** Files an SDK client needs; the key is only ever held in the dialog that received it. */
export function certificateFiles(issued: IssuedClientCertificate): CertificateFile[] {
  const base = issued.commonName.replace(/[^A-Za-z0-9._-]+/g, '-') || 'client';
  return [
    { name: `${base}.crt`, content: issued.certificatePem },
    { name: `${base}.key`, content: issued.privateKeyPem },
    { name: 'temporal-ca.crt', content: issued.caPem },
  ];
}

export function cliExample(issued: IssuedClientCertificate): string {
  const [cert, key, ca] = certificateFiles(issued);
  return `temporal --address ${issued.frontendAddress} --tls-server-name ${issued.serverName} --tls-cert-path ${cert!.name} --tls-key-path ${key!.name} --tls-ca-path ${ca!.name} workflow list`;
}
