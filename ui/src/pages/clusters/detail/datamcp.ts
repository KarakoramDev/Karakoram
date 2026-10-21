// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import type { Tone } from '@/components/ui/Primitives';

type DataMcpSpec = Schemas['DataMcpSpec'];
type DataMcpStatus = Schemas['DataMcpStatus'];
type Access = NonNullable<DataMcpSpec['access']>;
export type ScopeKey = keyof NonNullable<DataMcpSpec['scope']>;

export const DATA_MCP_RESOURCE = 'cluster-data';

/** Engines served by the data MCP gateway, by provider id. */
export function supportsDataMcp(provider: string): boolean {
  return provider === 'psmdb' || provider === 'postgres' || provider === 'supabase' || provider === 'elasticsearch' || provider === 'rabbitmq';
}

export interface ScopeField {
  key: ScopeKey;
  label: string;
  placeholder: string;
  hint: string;
}

/** Scope inputs relevant to an engine; other scope keys are never shown or sent. */
export function scopeFields(provider: string): ScopeField[] {
  switch (provider) {
    case 'psmdb':
      return [{ key: 'databases', label: 'Databases', placeholder: 'app', hint: 'Empty allows every non-system database.' }];
    case 'postgres':
    case 'supabase':
      return [
        { key: 'databases', label: 'Databases', placeholder: 'app', hint: 'Empty allows every non-template database.' },
        { key: 'schemas', label: 'Schemas', placeholder: 'public', hint: 'Empty allows every non-system schema in the selected databases.' },
      ];
    case 'elasticsearch':
      return [{ key: 'indexPatterns', label: 'Index patterns', placeholder: 'logs-*', hint: 'Index names or wildcard patterns. Hidden and system indices (names starting with a dot) are never reachable.' }];
    case 'rabbitmq':
      return [{ key: 'vhosts', label: 'Virtual hosts', placeholder: '/', hint: 'Empty allows every virtual host.' }];
    default:
      return [];
  }
}

export interface DataMcpDraft {
  enabled: boolean;
  access: Access;
  scope: Partial<Record<ScopeKey, string[]>>;
  deny: string[];
  confirmWrites: boolean;
  auditQueryText: boolean;
}

export function dataMcpDraft(spec: DataMcpSpec | undefined, provider: string): DataMcpDraft {
  const scope: DataMcpDraft['scope'] = {};
  for (const field of scopeFields(provider)) scope[field.key] = [...(spec?.scope?.[field.key] ?? [])];
  return {
    enabled: spec?.enabled ?? false,
    access: spec?.access ?? 'read',
    scope,
    deny: [...(spec?.tools?.deny ?? [])],
    confirmWrites: spec?.tools?.confirmWrites ?? false,
    auditQueryText: spec?.auditQueryText ?? false,
  };
}

function sameList(a: string[] | undefined, b: string[]): boolean {
  return JSON.stringify(a ?? []) === JSON.stringify(b);
}

/** RFC 7396 merge patch for `spec.dataMcp` containing only what changed; empty lists are sent as `null`. */
export function buildDataMcpPatch(spec: DataMcpSpec | undefined, draft: DataMcpDraft, provider: string): NonNullable<Schemas['DataMcpSpecPatch']> {
  const patch: NonNullable<Schemas['DataMcpSpecPatch']> = {};
  if (draft.enabled !== (spec?.enabled ?? false)) patch.enabled = draft.enabled;
  if (draft.access !== (spec?.access ?? 'read')) patch.access = draft.access;
  const scope: NonNullable<Schemas['DataMcpScopePatch']> = {};
  for (const field of scopeFields(provider)) {
    const value = draft.scope[field.key] ?? [];
    if (!sameList(spec?.scope?.[field.key], value)) scope[field.key] = value.length ? value : null;
  }
  if (Object.keys(scope).length) patch.scope = scope;
  const tools: NonNullable<Schemas['DataMcpToolsPatch']> = {};
  if (!sameList(spec?.tools?.deny, draft.deny)) tools.deny = draft.deny.length ? draft.deny : null;
  if (draft.confirmWrites !== (spec?.tools?.confirmWrites ?? false)) tools.confirmWrites = draft.confirmWrites;
  if (Object.keys(tools).length) patch.tools = tools;
  if (draft.auditQueryText !== (spec?.auditQueryText ?? false)) patch.auditQueryText = draft.auditQueryText;
  return patch;
}

const MAX_ITEMS = 64;
const TOOL_NAME = /^[A-Za-z0-9_.-]{1,64}$/;

/** Mirrors the server-side list limits so saves do not fail validation. */
export function validateDataMcpDraft(draft: DataMcpDraft, provider: string): string[] {
  const errors: string[] = [];
  for (const field of scopeFields(provider)) {
    const value = draft.scope[field.key] ?? [];
    if (value.length > MAX_ITEMS) errors.push(`${field.label}: at most ${MAX_ITEMS} entries.`);
    if (value.some((v) => v.length > 256)) errors.push(`${field.label}: entries must be at most 256 characters.`);
    if (field.key === 'indexPatterns' && value.some((v) => v.startsWith('.'))) errors.push('Index patterns: hidden and system indices (leading dot) cannot be scoped.');
    if (new Set(value).size !== value.length) errors.push(`${field.label}: entries must be unique.`);
  }
  if (draft.deny.length > MAX_ITEMS) errors.push(`Denied tools: at most ${MAX_ITEMS} entries.`);
  if (draft.deny.some((v) => !TOOL_NAME.test(v))) errors.push('Denied tools: use tool names (letters, digits, "_", "-", "."; at most 64 characters).');
  if (new Set(draft.deny).size !== draft.deny.length) errors.push('Denied tools: entries must be unique.');
  return errors;
}

export type DataMcpPhase = DataMcpStatus['phase'];

const PHASE_TONE: Record<DataMcpPhase, Tone> = { Disabled: 'neutral', Provisioning: 'progress', Ready: 'ok', Degraded: 'warn' };

export interface DataMcpView {
  enabled: boolean;
  phase: DataMcpPhase;
  tone: Tone;
  kind?: DataMcpStatus['kind'];
  /** Upstream name with its version, e.g. `mongodb-mcp-server 1.2.3`. */
  upstream?: string;
  /** Access currently provisioned; falls back to the spec until the status reports. */
  access?: Access;
  message?: string;
}

/** Observed data MCP state; a disabled or absent spec always reads as Disabled. */
export function dataMcpView(spec: DataMcpSpec | undefined, status: DataMcpStatus | undefined): DataMcpView {
  const enabled = spec?.enabled ?? false;
  if (!enabled) return { enabled, phase: 'Disabled', tone: PHASE_TONE.Disabled, message: status && status.phase !== 'Disabled' ? status.message : undefined };
  const phase = status?.phase ?? 'Provisioning';
  return {
    enabled,
    phase,
    tone: PHASE_TONE[phase],
    kind: status?.kind,
    upstream: status?.upstream ? `${status.upstream}${status.upstreamVersion ? ` ${status.upstreamVersion}` : ''}` : undefined,
    access: status?.access ?? spec?.access ?? 'read',
    message: status?.message,
  };
}

export function dataMcpEndpoint(origin: string, namespace: string, name: string): string {
  return `${origin}/mcp/clusters/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`;
}

/** `karactl` commands that mint a cluster-scoped token; the write variant only when writes are provisioned. */
export function dataMcpConnectCommands(namespace: string, name: string, access: Access | undefined): string[] {
  const base = `karactl mcp config --cluster ${namespace}/${name}`;
  return access === 'readWrite' ? [base, `${base} --write`] : [base];
}
