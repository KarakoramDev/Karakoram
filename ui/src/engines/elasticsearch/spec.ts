// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Pure helpers for the Elasticsearch create-form and detail pages. They mirror the
// server rules in internal/engine/elastic (options.go, sizing.go, catalog) so the
// wizard can report problems before the request is sent. No React here: the file
// is covered by node --test.

import type { Schemas } from '../../api/client';
import { replicaLimits, validateReplicas, type ReplicaLimits } from '../index.ts';

type ClusterSpec = Schemas['ClusterSpec'];
type Component = Schemas['Component'];
type Resources = Schemas['Resources'];
type Topology = Schemas['ProviderTopology'];

export const PROVIDER = 'elasticsearch';
/** Elasticsearch HTTPS port exposed publicly per data node. */
export const PUBLIC_PORT = 9200;

/** Spec component names: `engine` holds the combined or data nodes, the rest live under `components.extra`. */
export const COMPONENTS = ['engine', 'master', 'kibana', 'logstash', 'apm'] as const;
export type ComponentName = (typeof COMPONENTS)[number];
/** Stack components that may be disabled with zero replicas. */
export const STACK_COMPONENTS = ['kibana', 'logstash', 'apm'] as const;
export type StackComponent = (typeof STACK_COMPONENTS)[number];

export const COMPONENT_LABEL: Record<ComponentName, string> = { engine: 'Elasticsearch nodes', master: 'Dedicated masters', kibana: 'Kibana', logstash: 'Logstash', apm: 'APM Server' };

/** Recommended node counts for the combined topology (odd counts keep a clear master majority). */
export const CLUSTER_SIZES = [1, 3, 5, 7, 9];
export const MASTER_SIZES = [3, 5];
export const MAX_DATA_NODES = 20;
/** Replica ceilings of the optional stack components. */
export const STACK_MAX: Record<StackComponent, number> = { kibana: 3, logstash: 5, apm: 3 };

export const MAX_PIPELINES = 10;
export const PIPELINE_ID_RE = /^[a-z][a-z0-9_-]{0,31}$/;
const MAX_PIPELINE_CONFIG = 64 * 1024;

/** Minimum RAM (GiB) per component, matching sizing.go ValidateMinimumRAM. */
export const MIN_RAM: Record<ComponentName, number> = { engine: 1.5, master: 1, kibana: 1, logstash: 1, apm: 0.25 };

/** Fallback resources when the catalog topology carries no default for a component (sizing.go DefaultResources). */
export const DEFAULT_RESOURCES: Record<ComponentName, Resources> = {
  engine: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 2, maxRam: 2, minDisk: 5, maxDisk: 20 },
  master: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 1.5, maxRam: 1.5, minDisk: 5, maxDisk: 20 },
  kibana: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 1.5, maxRam: 2, minDisk: 1, maxDisk: 5 },
  logstash: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 1.5, maxRam: 1.5, minDisk: 1, maxDisk: 5 },
  apm: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 0.5, maxRam: 0.5, minDisk: 1, maxDisk: 5 },
};

/** Environment the agent exports to Logstash pipelines. */
export const PIPELINE_ENV = ['KARAKORAM_ES_HOSTS', 'KARAKORAM_ES_HOST', 'KARAKORAM_ES_USER', 'KARAKORAM_ES_PASSWORD', 'KARAKORAM_ES_CA'] as const;

export const DEFAULT_PIPELINE_CONFIG = `input {
  beats { port => 5044 }
  http { port => 8080 }
}
output {
  elasticsearch {
    hosts => "\${KARAKORAM_ES_HOSTS}"
    user => "\${KARAKORAM_ES_USER}"
    password => "\${KARAKORAM_ES_PASSWORD}"
    ssl_enabled => true
    ssl_certificate_authorities => "\${KARAKORAM_ES_CA}"
    data_stream => true
  }
}
`;

export interface Pipeline {
  id: string;
  config: string;
}

/** Typed view of `spec.engineOptions` for this provider (internal/engine/elastic/options.go). */
export interface ElasticOptions {
  kibana: { public: boolean; settings?: Record<string, unknown> };
  apm: { public: boolean; apiKeys: boolean };
  logstash: { pipelines: Pipeline[]; persistentQueue: boolean };
  elasticsearch: { settings?: Record<string, unknown> };
}

function record(value: unknown): Record<string, unknown> {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : {};
}

function settings(value: unknown): Record<string, unknown> | undefined {
  const r = record(value);
  return Object.keys(r).length ? r : undefined;
}

/** Read engine options tolerantly; unknown or malformed parts fall back to defaults. */
export function readOptions(spec: Pick<ClusterSpec, 'engineOptions'>): ElasticOptions {
  const raw = record(spec.engineOptions);
  const kibana = record(raw.kibana);
  const apm = record(raw.apm);
  const logstash = record(raw.logstash);
  const es = record(raw.elasticsearch);
  const pipelines = Array.isArray(logstash.pipelines)
    ? logstash.pipelines.map((p) => {
        const r = record(p);
        return { id: typeof r.id === 'string' ? r.id : '', config: typeof r.config === 'string' ? r.config : '' };
      })
    : [];
  return {
    kibana: { public: kibana.public === true, settings: settings(kibana.settings) },
    apm: { public: apm.public === true, apiKeys: apm.apiKeys !== false },
    logstash: { pipelines, persistentQueue: logstash.persistentQueue === true },
    elasticsearch: { settings: settings(es.settings) },
  };
}

/** Serialize options into the wire shape the server decodes with unknown fields rejected. */
export function writeOptions(spec: ClusterSpec, o: ElasticOptions): ClusterSpec {
  const engineOptions: Record<string, unknown> = {
    kibana: { public: o.kibana.public, ...(o.kibana.settings ? { settings: o.kibana.settings } : {}) },
    apm: { public: o.apm.public, apiKeys: o.apm.apiKeys },
    logstash: { persistentQueue: o.logstash.persistentQueue, ...(o.logstash.pipelines.length ? { pipelines: o.logstash.pipelines } : {}) },
    elasticsearch: o.elasticsearch.settings ? { settings: o.elasticsearch.settings } : {},
  };
  return { ...spec, engineOptions };
}

/** Remove options whose component has been disabled, including inherited clone settings. */
export function normalizeOptions(spec: ClusterSpec): ClusterSpec {
  if (spec.provider !== PROVIDER) return spec;
  const options = readOptions(spec);
  let changed = false;
  if (replicasOf(spec, 'kibana') === 0 && options.kibana.public) {
    options.kibana.public = false;
    changed = true;
  }
  if (replicasOf(spec, 'apm') === 0 && options.apm.public) {
    options.apm.public = false;
    changed = true;
  }
  if (replicasOf(spec, 'logstash') === 0 && (options.logstash.pipelines.length || options.logstash.persistentQueue)) {
    options.logstash = { pipelines: [], persistentQueue: false };
    changed = true;
  }
  return changed ? writeOptions(spec, options) : spec;
}

/** A provisioned public route is not evidence that Kibana has finished starting. */
export function kibanaReady(members: Pick<Schemas['Member'], 'role' | 'state' | 'health'>[]): boolean {
  let found = false;
  for (const member of members) {
    if (member.role !== 'kibana') continue;
    found = true;
    if (!member.health || member.state !== 'joined') return false;
  }
  return found;
}

export function componentOf(spec: ClusterSpec, name: ComponentName): Component | undefined {
  return name === 'engine' ? spec.components.engine : spec.components.extra?.[name];
}

export function replicasOf(spec: ClusterSpec, name: ComponentName): number {
  return componentOf(spec, name)?.replicas ?? 0;
}

export function withComponent(spec: ClusterSpec, name: ComponentName, component: Component): ClusterSpec {
  if (name === 'engine') return { ...spec, components: { ...spec.components, engine: component } };
  return { ...spec, components: { ...spec.components, extra: { ...spec.components.extra, [name]: component } } };
}

/** Catalog default for a component in the chosen topology, or the built-in fallback. */
export function defaultComponent(topology: Topology | undefined, name: ComponentName): Component {
  const d = topology?.defaults.components;
  const c = name === 'engine' ? d?.engine : d?.extra?.[name];
  return c ? structuredClone(c) : { replicas: name === 'engine' ? 1 : name === 'master' ? 3 : 0, resources: { ...DEFAULT_RESOURCES[name] } };
}

/** Set replicas, materializing the component from catalog defaults when it is absent. */
export function withReplicas(spec: ClusterSpec, topology: Topology | undefined, name: ComponentName, replicas: number): ClusterSpec {
  const current = componentOf(spec, name) ?? defaultComponent(topology, name);
  return normalizeOptions(withComponent(spec, name, { ...current, replicas }));
}

/** Components to show editors for: `master` only with dedicated masters, stack components only when enabled. */
export function activeComponents(spec: ClusterSpec): ComponentName[] {
  const out: ComponentName[] = ['engine'];
  if (spec.topology.type === 'dedicatedMasters') out.push('master');
  for (const name of STACK_COMPONENTS) if (replicasOf(spec, name) > 0) out.push(name);
  return out;
}

/** Component replica limits with the design-time bounds as fallback when the catalog is silent. */
export function limitsFor(topology: Topology | undefined, name: ComponentName): ReplicaLimits {
  const limits = replicaLimits(topology, name);
  if (limits.allowedReplicas || limits.minReplicas !== undefined || limits.maxReplicas !== undefined) return limits;
  if (name === 'master') return { allowedReplicas: MASTER_SIZES };
  if (name === 'engine') return { minReplicas: 1, maxReplicas: topology?.type === 'dedicatedMasters' ? MAX_DATA_NODES : CLUSTER_SIZES[CLUSTER_SIZES.length - 1] };
  return { minReplicas: 0, maxReplicas: STACK_MAX[name] };
}

export function validatePipelines(pipelines: Pipeline[]): string[] {
  const errors: string[] = [];
  if (pipelines.length > MAX_PIPELINES) errors.push(`Logstash permits at most ${MAX_PIPELINES} pipelines.`);
  const seen = new Set<string>();
  pipelines.forEach((p, i) => {
    const label = `Pipeline ${i + 1}`;
    if (!PIPELINE_ID_RE.test(p.id)) errors.push(`${label}: ID must be 1–32 lowercase letters, digits, _ or -, starting with a letter.`);
    else if (seen.has(p.id)) errors.push(`${label}: duplicate ID ${p.id}.`);
    seen.add(p.id);
    if (!p.config.trim()) errors.push(`${label}: configuration must not be empty.`);
    else if (p.config.length > MAX_PIPELINE_CONFIG) errors.push(`${label}: configuration exceeds 64 KiB.`);
  });
  return errors;
}

/** Basics-step errors: engine options, TLS mode and the unsupported free-form engineConfig. */
export function validateSpec(spec: ClusterSpec): string[] {
  const errors: string[] = [];
  const o = readOptions(spec);
  if (spec.engineConfig?.trim()) errors.push('Elasticsearch settings belong in Elastic Stack options, not engine configuration overrides. Clear inherited overrides before creating the cluster.');
  if (spec.tls && spec.tls.mode !== 'requireTLS') errors.push('Elasticsearch always requires TLS; choose the requireTLS mode.');
  if (o.kibana.public && replicasOf(spec, 'kibana') === 0) errors.push('Public Kibana access needs at least one Kibana instance.');
  if (o.apm.public && replicasOf(spec, 'apm') === 0) errors.push('Public APM access needs at least one APM Server instance.');
  if (o.logstash.pipelines.length && replicasOf(spec, 'logstash') === 0) errors.push('Logstash pipelines are configured but Logstash is disabled.');
  errors.push(...validatePipelines(o.logstash.pipelines));
  return errors;
}

function same(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

/**
 * RFC 7396 merge patch turning the persisted spec into the draft: components (per name, only the
 * replicas/resources that changed), engineOptions as a whole, TLS, exposure and monitoring.
 * Topology type and version are never patched here.
 */
export function specPatch(current: ClusterSpec, draft: ClusterSpec): Record<string, unknown> {
  const patch: Record<string, unknown> = {};
  const components: Record<string, unknown> = {};
  const extra: Record<string, unknown> = {};
  for (const name of COMPONENTS) {
    const before = componentOf(current, name);
    const after = componentOf(draft, name);
    if (!after || same(before, after)) continue;
    const change: Record<string, unknown> = {};
    if (before?.replicas !== after.replicas) change.replicas = after.replicas;
    if (!same(before?.resources, after.resources)) change.resources = after.resources;
    if (!Object.keys(change).length) continue;
    if (name === 'engine') components.engine = change;
    else extra[name] = before ? change : after;
  }
  if (Object.keys(extra).length) components.extra = extra;
  if (Object.keys(components).length) patch.components = components;

  const before = readOptions(current);
  const after = readOptions(draft);
  if (!same(before, after)) {
    // Merge-patch semantics: optional keys that disappeared must be nulled, not omitted.
    patch.engineOptions = {
      kibana: { public: after.kibana.public, settings: after.kibana.settings ?? null },
      apm: { public: after.apm.public, apiKeys: after.apm.apiKeys },
      logstash: { persistentQueue: after.logstash.persistentQueue, pipelines: after.logstash.pipelines.length ? after.logstash.pipelines : null },
      elasticsearch: { settings: after.elasticsearch.settings ?? null },
    };
  }

  const tls: Record<string, unknown> = {};
  const ca = draft.tls?.ca ?? 'selfSigned';
  if (ca !== (current.tls?.ca ?? 'selfSigned')) tls.ca = ca;
  if ((draft.tls?.mode ?? 'requireTLS') !== (current.tls?.mode ?? 'requireTLS')) tls.mode = draft.tls?.mode;
  if (ca === 'userProvided') {
    for (const key of ['caCertificate', 'caPrivateKey', 'certificate', 'privateKey'] as const) if (draft.tls?.[key]?.trim()) tls[key] = draft.tls[key];
  }
  if (Object.keys(tls).length) patch.tls = tls;

  const exposureBefore = current.exposure ?? { public: false, ipType: 'IPV6', allowlist: [] };
  const exposureAfter = draft.exposure ?? exposureBefore;
  if (exposureAfter.public !== exposureBefore.public || exposureAfter.ipType !== exposureBefore.ipType || !same(exposureAfter.allowlist ?? [], exposureBefore.allowlist ?? [])) {
    patch.exposure = { public: exposureAfter.public, ipType: exposureAfter.ipType, allowlist: exposureAfter.public ? exposureAfter.allowlist ?? [] : [] };
  }
  if ((draft.monitoring?.enabled ?? true) !== (current.monitoring?.enabled ?? true)) patch.monitoring = { enabled: draft.monitoring?.enabled ?? true };
  return patch;
}

/** Human-readable consequences of applying `draft` over `current`, for the confirmation dialog. */
export function disruptions(current: ClusterSpec, draft: ClusterSpec): string[] {
  const out: string[] = [];
  const dedicated = current.topology.type === 'dedicatedMasters';
  for (const name of COMPONENTS) {
    const before = componentOf(current, name);
    const after = componentOf(draft, name);
    if (!after) continue;
    const label = COMPONENT_LABEL[name];
    const from = before?.replicas ?? 0;
    const to = after.replicas;
    if (name === 'engine' || name === 'master') {
      if (to > from) out.push(`${label} ${from} → ${to}: new nodes are provisioned and join; shards rebalance automatically.`);
      if (to < from) out.push(`${label} ${from} → ${to}: nodes are drained and removed one at a time; each waits until it holds no shards${name === 'master' || !dedicated ? ' and has left the voting configuration' : ''}.`);
      if (before && before.resources.minRam !== after.resources.minRam) out.push(`${label} RAM change → heap change → rolling restart of these nodes.`);
    } else {
      if (from === 0 && to > 0) out.push(`${label} enabled: the service is created and wired to the cluster.`);
      else if (from > 0 && to === 0) out.push(`${label} disabled: the service and its containers are deleted.`);
      else if (from !== to) out.push(`${label} ${from} → ${to} containers.`);
      if (before && to > 0 && from > 0 && !same(before.resources, after.resources)) out.push(`${label} resources change → containers restart.`);
    }
  }
  const before = readOptions(current);
  const after = readOptions(draft);
  if (!same(before.kibana, after.kibana)) out.push('Kibana options change → Kibana restarts.');
  if (!same(before.apm, after.apm)) out.push('APM Server options change → APM Server restarts.');
  if (!same(before.logstash, after.logstash)) out.push('Logstash options change → Logstash restarts.');
  if (!same(current.tls, draft.tls)) out.push('TLS change → certificates are re-issued and reloaded.');
  if (!same(current.exposure, draft.exposure)) out.push('Exposure change → public routes added or removed; HTTP certificates re-issued with the public addresses.');
  return out;
}

/** Resource checks for one component; `stateless` components (Kibana, Logstash, APM) have no data volume. */
export type ResourcesCheck = (resources: Resources, stateless: boolean, name: ComponentName) => string[];

/** Topology- and Resources-step errors for every active component. */
export function validateComponents(spec: ClusterSpec, topology: Topology | undefined, checkResources: ResourcesCheck): { topology: string[]; resources: string[] } {
  const t: string[] = [];
  const r: string[] = [];
  const dedicated = spec.topology.type === 'dedicatedMasters';
  for (const name of [...(dedicated ? (['master'] as const) : []), 'engine', ...STACK_COMPONENTS] as ComponentName[]) {
    const component = componentOf(spec, name);
    if (!component) {
      if (name === 'master') t.push('Dedicated masters: choose 3 or 5 master nodes.');
      continue;
    }
    const label = COMPONENT_LABEL[name];
    t.push(...validateReplicas(component.replicas, limitsFor(topology, name)).map((e) => `${label}: ${e}`));
    if (component.replicas === 0) continue;
    const stateless = name === 'kibana' || name === 'logstash' || name === 'apm';
    r.push(...checkResources(component.resources, stateless, name).map((e) => `${label}: ${e}`));
    if (component.resources.minRam < MIN_RAM[name]) r.push(`${label}: minimum RAM must be at least ${MIN_RAM[name]} GiB.`);
  }
  return { topology: t, resources: r };
}

function plural(n: number, noun: string): string {
  return `${n} ${noun}${n === 1 ? '' : 's'}`;
}

/** Short topology label, e.g. "3 masters + 4 data nodes · Kibana, Logstash ×2". */
export function specSummary(spec: ClusterSpec): string {
  const data = replicasOf(spec, 'engine');
  const nodes = spec.topology.type === 'dedicatedMasters' ? `${plural(replicasOf(spec, 'master'), 'master')} + ${plural(data, 'data node')}` : plural(data, 'node');
  const stack = STACK_COMPONENTS.filter((name) => replicasOf(spec, name) > 0).map((name) => (replicasOf(spec, name) > 1 ? `${COMPONENT_LABEL[name]} ×${replicasOf(spec, name)}` : COMPONENT_LABEL[name]));
  return stack.length ? `${nodes} · ${stack.join(', ')}` : nodes;
}

export type MemberKind = 'master' | 'data' | 'master,data' | StackComponent;

/** Role of a member as reported by the provider, or undefined for roles of other engines. */
export function memberKind(member: Pick<Schemas['Member'], 'role'>): MemberKind | undefined {
  const role = member.role;
  if (role === 'master' || role === 'data' || role === 'master,data' || role === 'kibana' || role === 'logstash' || role === 'apm') return role;
  return undefined;
}

export type IndexPrivileges = Schemas['ElasticIndexPrivileges'];

/** Match the API/provider's Unicode character count, not UTF-16 code units. */
export function validUserPassword(password: string, updating: boolean): boolean {
  return (updating && password === '') || Array.from(password).length >= 8;
}

function parseIndexPrivilege(value: unknown): IndexPrivileges | null {
  const entry = record(value);
  const out: IndexPrivileges = { names: strings(entry.names), privileges: strings(entry.privileges), allowRestrictedIndices: entry.allow_restricted_indices === true || entry.allowRestrictedIndices === true };
  if (!out.names.length || !out.privileges.length) return null;
  if (entry.query !== undefined) {
    if (typeof entry.query === 'string') out.query = entry.query;
    else if (entry.query && typeof entry.query === 'object' && !Array.isArray(entry.query)) out.query = JSON.stringify(entry.query);
    else return null;
  }
  if (entry.field_security !== undefined && entry.fieldSecurity !== undefined) return null;
  const fields = entry.field_security ?? entry.fieldSecurity;
  if (fields !== undefined) {
    if (!fields || typeof fields !== 'object' || Array.isArray(fields)) return null;
    const f = record(fields);
    out.fieldSecurity = {};
    for (const key of ['grant', 'except'] as const) {
      if (f[key] !== undefined) {
        if (!Array.isArray(f[key]) || !f[key].every((v: unknown) => typeof v === 'string')) return null;
        out.fieldSecurity[key] = f[key] as string[];
      }
    }
  }
  return out;
}

function list(text: string): string[] {
  return text
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);
}

/**
 * Parse one index privilege entry per line: `<index patterns> <privileges> [restricted]`,
 * both lists comma-separated, e.g. `logs-*,metrics-* read,view_index_metadata`.
 * A JSON array also accepts document/field restrictions without interpreting query JSON.
 * Returns null when a non-empty line is malformed.
 */
export function parseIndexPrivileges(text: string): IndexPrivileges[] | null {
  if (text.trim().startsWith('[')) {
    try {
      const parsed: unknown = JSON.parse(text);
      if (!Array.isArray(parsed)) return null;
      const entries = parsed.map(parseIndexPrivilege);
      return entries.every((e): e is IndexPrivileges => e !== null) ? entries : null;
    } catch {
      return null;
    }
  }
  const out: IndexPrivileges[] = [];
  for (const raw of text.split('\n')) {
    const line = raw.trim();
    if (!line) continue;
    const parts = line.split(/\s+/);
    const [names, privileges, flag, ...rest] = parts;
    if (!names || !privileges || rest.length || (flag !== undefined && flag !== 'restricted')) return null;
    const entry = { names: list(names), privileges: list(privileges), allowRestrictedIndices: flag === 'restricted' };
    if (!entry.names.length || !entry.privileges.length) return null;
    out.push(entry);
  }
  return out;
}

export function formatIndexPrivileges(entries: IndexPrivileges[]): string {
  if (entries.some((e) => e.query !== undefined || e.fieldSecurity !== undefined)) return JSON.stringify(entries, null, 2);
  return entries.map((e) => `${e.names.join(',')} ${e.privileges.join(',')}${e.allowRestrictedIndices ? ' restricted' : ''}`).join('\n');
}

export const ROLE_NAME_RE = /^[A-Za-z0-9_.-]{1,255}$/;
/** Native realm usernames: printable ASCII without leading/trailing spaces, 1–507 characters. */
export const USERNAME_RE = /^[\x21-\x7e][\x20-\x7e]{0,505}[\x21-\x7e]$|^[\x21-\x7e]$/;
/** Elasticsearch time value accepted as API key expiration (e.g. 30d, 12h, 90m). */
export const EXPIRATION_RE = /^[1-9][0-9]*(d|h|m|s|ms|micros|nanos)$/;

/** The API marks built-in roles with `reserved`; custom roles remain visible by default. */
export function visibleElasticRoles(roles: readonly Schemas['ElasticRole'][], showBuiltIn = false): Schemas['ElasticRole'][] {
  return roles.filter((role) => showBuiltIn || !role.reserved);
}

/** Parse required, explicitly scoped API-key role descriptors without dropping restrictions. */
export function parseRoleDescriptors(text: string): { value?: Record<string, Schemas['ElasticRoleDescriptor']>; error?: string } {
  if (!text.trim()) return { error: 'At least one role descriptor is required.' };
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { error: 'Role descriptors must be valid JSON.' };
  }
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return { error: 'Role descriptors must be a JSON object keyed by role name.' };
  if (Object.keys(parsed).length === 0) return { error: 'At least one role descriptor is required.' };
  const value: Record<string, Schemas['ElasticRoleDescriptor']> = Object.create(null);
  for (const [name, descriptor] of Object.entries(parsed as Record<string, unknown>)) {
    const d = record(descriptor);
    const indices: IndexPrivileges[] = [];
    for (const raw of Array.isArray(d.indices) ? d.indices : []) {
      const index = parseIndexPrivilege(raw);
      if (!index) return { error: `Invalid index privileges or security restrictions in descriptor "${name}".` };
      indices.push(index);
    }
    value[name] = {
      cluster: strings(d.cluster),
      indices,
      applications: Array.isArray(d.applications) ? d.applications.map((a) => ({ application: String(record(a).application ?? ''), privileges: strings(record(a).privileges), resources: strings(record(a).resources) })) : [],
    };
  }
  return { value };
}

function strings(value: unknown): string[] {
  return Array.isArray(value) ? value.map(String) : [];
}

/**
 * JVM heap (MiB) the agent derives from minimum RAM, matching sizing.go: Elasticsearch
 * nodes get 40 % (50 % above 4 GiB) floored to 64 MiB and clamped to 512 MiB–31 GiB;
 * Kibana's old-space limit is 60 % (min 512); Logstash gets 50 % (min 384).
 */
export function heapMB(name: ComponentName, minRamGiB: number): number | undefined {
  if (!Number.isFinite(minRamGiB)) return undefined;
  const mb = minRamGiB * 1024;
  switch (name) {
    case 'engine':
    case 'master': {
      const heap = Math.floor((mb * (minRamGiB > 4 ? 0.5 : 0.4)) / 64) * 64;
      return Math.min(31744, Math.max(512, heap));
    }
    case 'kibana':
      return Math.max(512, Math.floor(mb * 0.6));
    case 'logstash':
      return Math.max(384, Math.floor(mb * 0.5));
    case 'apm':
      return undefined;
  }
}
