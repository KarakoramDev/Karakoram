// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '../../api/client.ts';
import type { ComponentValidation, EngineChangeReview } from '../types.ts';

type Spec = Schemas['ClusterSpec'];
export const SUPABASE_COMPONENTS = ['gateway', 'auth', 'rest', 'realtime', 'storage', 'imgproxy', 'meta', 'studio', 'functions', 'pooler', 'analytics', 'vector'] as const;
export function object(value: unknown): Record<string, unknown> { return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}; }
export function isPostgres(provider: string): boolean { return provider === 'postgres' || provider === 'supabase'; }
export function component(spec: Spec, name: string): Schemas['Component'] | undefined { return name === 'engine' ? spec.components.engine : spec.components.extra?.[name]; }
export function defaultComponent(topology: Schemas['ProviderTopology'] | undefined, name: string): Schemas['Component'] {
  return structuredClone((name === 'engine' ? topology?.defaults.components.engine : topology?.defaults.components.extra?.[name]) ?? { replicas: 1, resources: { cpuMode: 'SHARED', minCpu: 1, maxCpu: 2, minRam: 1, maxRam: 2, minDisk: name === 'engine' ? 10 : 0, maxDisk: name === 'engine' ? 20 : 0 } });
}
export function withComponent(spec: Spec, name: string, value: Schemas['Component']): Spec {
  return { ...spec, components: name === 'engine' ? { ...spec.components, engine: value } : { ...spec.components, extra: { ...spec.components.extra, [name]: value } } };
}
const FALLBACK_DEPS: Record<string, string[]> = { studio: ['meta', 'gateway'], imgproxy: ['storage'], vector: ['analytics'] };
export function dependencies(topology: Schemas['ProviderTopology'] | undefined, name: string): string[] {
  const schema = object(topology?.componentSchemas[name]);
  const deps = schema.dependencies ?? schema.requires ?? schema['x-dependencies'];
  return Array.isArray(deps) && deps.every((v) => typeof v === 'string') ? deps : FALLBACK_DEPS[name] ?? [];
}
export function toggleComponent(spec: Spec, topology: Schemas['ProviderTopology'] | undefined, name: string, enabled: boolean): Spec {
  let next = withComponent(spec, name, { ...(component(spec, name) ?? defaultComponent(topology, name)), replicas: enabled ? Math.max(1, component(spec, name)?.replicas ?? 0) : 0 });
  if (enabled) {
    const visit = (key: string, seen: Set<string>) => {
      if (seen.has(key)) return;
      seen.add(key);
      for (const dep of dependencies(topology, key)) {
        next = withComponent(next, dep, { ...(component(next, dep) ?? defaultComponent(topology, dep)), replicas: Math.max(1, component(next, dep)?.replicas ?? 0) });
        visit(dep, seen);
      }
    };
    visit(name, new Set());
  } else {
    let changed = true;
    while (changed) {
      changed = false;
      for (const key of SUPABASE_COMPONENTS) if ((component(next, key)?.replicas ?? 0) > 0 && dependencies(topology, key).some((dep) => !component(next, dep)?.replicas)) {
        next = withComponent(next, key, { ...component(next, key)!, replicas: 0 }); changed = true;
      }
    }
  }
  return next;
}
export function applyPreset(spec: Spec, topology: Schemas['ProviderTopology'] | undefined, preset: string, provider?: Schemas['Provider']): Spec {
  const catalog = provider?.presets.find((p) => p.name === preset)?.spec.components.extra;
  const selected = catalog ? SUPABASE_COMPONENTS.filter((name) => catalog[name]?.replicas) : preset === 'full' ? [...SUPABASE_COMPONENTS] : preset === 'standard' ? ['gateway', 'auth', 'rest', 'realtime', 'storage', 'meta', 'studio'] : ['gateway', 'auth', 'rest'];
  let next = spec;
  for (const name of SUPABASE_COMPONENTS) next = withComponent(next, name, { ...(component(next, name) ?? defaultComponent(topology, name)), replicas: 0 });
  for (const name of selected) next = toggleComponent(next, topology, name, true);
  return next;
}
export function normalizeSpec(spec: Spec): Spec {
  if (!isPostgres(spec.provider)) return spec;
  const options = { ...spec.engineOptions };
  if (spec.topology.type === 'single') delete options.synchronousMode;
  if (spec.provider === 'supabase') {
    if (!component(spec, 'imgproxy')?.replicas && object(options.storage).imageTransformation) options.storage = { ...object(options.storage), imageTransformation: false };
    if (!component(spec, 'gateway')?.replicas && object(options.gateway).publicHttps) options.gateway = { ...object(options.gateway), publicHttps: false };
  }
  return JSON.stringify(options) === JSON.stringify(spec.engineOptions ?? {}) ? spec : { ...spec, engineOptions: options };
}
export function validateSpec(spec: Spec): string[] {
  const errors: string[] = [];
  const o = object(spec.engineOptions);
  if (spec.engineConfig?.trim()) errors.push('Use engine options instead of raw engine configuration.');
  if (o.synchronousMode !== undefined && !['off', 'on', 'strict'].includes(String(o.synchronousMode))) errors.push('Synchronous mode must be off, on or strict.');
  if (spec.topology.type === 'single' && o.synchronousMode && o.synchronousMode !== 'off') errors.push('Synchronous replication requires HA topology.');
  for (const [key, value] of Object.entries(object(o.parameters))) {
    if (!/^[a-z][a-z0-9_]*$/.test(key) || /^(archive_|ssl_|listen_addresses$|port$|data_directory$|hba|shared_preload_libraries$)/.test(key)) errors.push(`PostgreSQL parameter ${key} is managed or invalid.`);
    if (typeof value !== 'string' || !value.trim() || /[\0\r\n]/.test(value) || value.length > 1024) errors.push(`PostgreSQL parameter ${key} needs a single-line value of at most 1024 characters.`);
  }
  if (spec.provider === 'supabase') {
    if (!spec.version.startsWith('17.')) errors.push('Supabase requires PostgreSQL 17.');
    if (o.siteUrl && !validURL(o.siteUrl)) errors.push('Site URL must be an absolute HTTP(S) URL.');
    if (Array.isArray(o.additionalRedirectUrls) && o.additionalRedirectUrls.some((url) => !validURL(url))) errors.push('Redirect URLs must be absolute HTTP(S) URLs without credentials or fragments.');
    const auth = object(o.auth), smtp = object(auth.smtp), rest = object(o.rest), storage = object(o.storage);
    for (const [label, value, min, max] of [['JWT expiry', auth.jwtExpiry, 1, 604800], ['SMTP port', smtp.port, 1, 65535], ['REST max rows', rest.maxRows, 1, Infinity], ['File size limit', storage.fileSizeLimit, 1, Infinity]] as const) if (value !== undefined && (!Number.isInteger(value) || Number(value) < min || Number(value) > max)) errors.push(`${label} must be an integer between ${min} and ${max}.`);
    if (component(spec, 'storage')?.replicas && !storage.backendStorage) errors.push('Storage requires a registered backend storage.');
    if (storage.imageTransformation && !component(spec, 'imgproxy')?.replicas) errors.push('Image transformation requires imgproxy.');
    if (object(o.gateway).publicHttps && !component(spec, 'gateway')?.replicas) errors.push('Public HTTPS requires gateway.');
    const origins = object(o.gateway).corsOrigins;
    if (origins !== undefined && (!Array.isArray(origins) || origins.some((origin) => !validOrigin(origin) && !(origin === '*' && origins.length === 1)))) errors.push('Gateway CORS origins must be exact HTTP(S) origins, or a single wildcard.');
  }
  return errors;
}
function validURL(value: unknown): boolean {
  try {
    const url = new URL(String(value));
    return ['http:', 'https:'].includes(url.protocol) && !url.username && !url.password && !url.hash;
  } catch { return false; }
}
function validOrigin(value: unknown): boolean {
  if (typeof value !== 'string' || !validURL(value)) return false;
  const url = new URL(value);
  return value === `${url.protocol}//${url.host}`;
}
export function validateComponents(spec: Spec, topology?: Schemas['ProviderTopology'], resources?: (r: Schemas['Resources'], stateless: boolean, name: string) => string[]): ComponentValidation {
  const errors: ComponentValidation = { topology: [], resources: [] };
  if (spec.topology.type !== 'single' && spec.topology.type !== 'ha') errors.topology.push('Choose single or HA topology.');
  if (!(spec.topology.type === 'single' ? [1] : [3, 5]).includes(spec.components.engine.replicas)) errors.topology.push('Single requires 1 data member; HA requires 3 or 5.');
  const proxy = component(spec, 'proxy')?.replicas ?? 0;
  if (!Number.isInteger(proxy) || proxy < (spec.topology.type === 'ha' ? 1 : 0)) errors.topology.push('HA requires at least one proxy replica; single permits zero.');
  for (const [name, c] of [['engine', spec.components.engine], ...Object.entries(spec.components.extra ?? {})] as [string, Schemas['Component']][]) {
    if (!Number.isInteger(c.replicas) || c.replicas < 0) errors.topology.push(`${name}: replicas must be a nonnegative integer.`);
    const limits = object(topology?.limits[name]);
    if (typeof limits.maxReplicas === 'number' && c.replicas > limits.maxReplicas) errors.topology.push(`${name}: at most ${limits.maxReplicas} replicas are allowed.`);
    if (c.replicas > 0 && typeof limits.minReplicas === 'number' && c.replicas < limits.minReplicas) errors.topology.push(`${name}: at least ${limits.minReplicas} replicas are required when enabled.`);
    if (Array.isArray(limits.allowedReplicas) && !limits.allowedReplicas.includes(c.replicas)) errors.topology.push(`${name}: replicas must be ${limits.allowedReplicas.join(', ')}.`);
    if (c.replicas && spec.provider === 'supabase') for (const dep of dependencies(topology, name)) if (!component(spec, dep)?.replicas) errors.topology.push(`${name} requires ${dep}.`);
    if (c.replicas && resources) errors.resources.push(...resources(c.resources, name !== 'engine', name).map((e) => `${name}: ${e}`));
  }
  return errors;
}
export function specSummary(spec: Spec): string { return `${spec.topology.type === 'ha' ? 'HA' : 'Single'} · ${spec.components.engine.replicas} data · ${component(spec, 'proxy')?.replicas ?? 0} proxy${spec.provider === 'supabase' ? ` · ${SUPABASE_COMPONENTS.filter((name) => component(spec, name)?.replicas).length} components` : ''}`; }
export function configurationReviewRows(spec: Spec): [string, string][] {
  const o = object(spec.engineOptions);
  const rows: [string, string][] = [['PostgreSQL topology', specSummary(spec)], ['Synchronous replication', String(o.synchronousMode ?? 'off')], ['Parameters', Object.entries(object(o.parameters)).map(([k, v]) => `${k}=${v}`).join(', ') || 'Provider defaults'], ['Proxy routing', `${object(o.proxy).readWriteSplit ? 'Read/write split' : 'Writer only'}${object(o.proxy).readerPort ? '; reader port 5433' : ''}`]];
  for (const [name, c] of Object.entries(spec.components.extra ?? {})) if (c.replicas > 0) {
    const r = c.resources;
    rows.push([`${name} resources`, `${c.replicas} × ${r.minCpu}–${r.maxCpu} vCPU ${r.cpuMode.toLowerCase()}, ${r.minRam}–${r.maxRam} GiB RAM`]);
  }
  if (spec.provider === 'supabase') {
    rows.push(['Components', SUPABASE_COMPONENTS.filter((name) => component(spec, name)?.replicas).map((name) => `${name} × ${component(spec, name)!.replicas}`).join(', ') || 'none']);
    for (const key of ['siteUrl', 'additionalRedirectUrls', 'auth', 'rest', 'storage', 'gateway', 'studio', 'extensions', 'functions']) if (o[key] !== undefined) rows.push([key, JSON.stringify(o[key])]);
  }
  return rows;
}

/** Build the recursive RFC 7396 delta, including nulls for removed option keys. */
export function specPatch(current: Spec, draft: Spec): Record<string, unknown> {
  const diff = (before: Record<string, unknown>, after: Record<string, unknown>): Record<string, unknown> => {
    const patch: Record<string, unknown> = {};
    for (const key of new Set([...Object.keys(before), ...Object.keys(after)])) {
      if (JSON.stringify(before[key]) === JSON.stringify(after[key])) continue;
      const value = after[key] === undefined ? null : after[key] !== null && typeof after[key] === 'object' && !Array.isArray(after[key]) ? diff(object(before[key]), object(after[key])) : after[key];
      Object.defineProperty(patch, key, { value, enumerable: true, configurable: true, writable: true });
    }
    return patch;
  };
  const fields = ['topology', 'components', 'engineOptions', 'version'] as const;
  return diff(Object.fromEntries(fields.map((key) => [key, current[key]])), Object.fromEntries(fields.map((key) => [key, draft[key]])));
}
export function postgresMajorUpgrade(current: string, target: string): boolean {
  return current.split('.')[0] !== target.split('.')[0];
}
export const POSTGRES_OFFLINE_UPGRADE = 'OFFLINE maintenance: all data members are stopped for pg_upgrade --link. All database access is unavailable during pg_upgrade and replica rebuild. A recent successful backup is required before proceeding; link-mode upgrades do not support binary rollback.';

export const postgresChangeReview: EngineChangeReview = {
  buildPatch: specPatch,
  disruptions: (current, draft) => JSON.stringify(current) === JSON.stringify(draft) ? [] : postgresMajorUpgrade(current.version, draft.version)
    ? [POSTGRES_OFFLINE_UPGRADE]
    : ['Data member changes roll replicas before the primary; failover briefly interrupts writes. Component changes redeploy the affected services. Single-member maintenance causes downtime.'],
};
