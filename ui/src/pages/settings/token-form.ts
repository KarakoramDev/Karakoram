// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';

type Restrictions = Schemas['APITokenRestrictions'];
type RestrictionList = 'namespaces' | 'clusters' | 'permissions';

export interface TokenRestrictionInput {
  readOnly: boolean;
  namespaces: string;
  clusters: string;
  permissions: string;
  allowCredentialReveal: boolean;
}

export interface TokenSubmission {
  name: string;
  expiresAt?: string;
  restrictions?: Restrictions;
  key: string;
}

/** Empty entries and duplicates do not add scope; preserve the user's order. */
export function parseTokenList(input: string): string[] {
  return [...new Set(input.split(/[,\r\n]/).map((value) => value.trim()).filter(Boolean))];
}

export function tokenListError(kind: RestrictionList, values: string[]): string | undefined {
  const namespace = /^[a-z][a-z0-9-]{0,30}[a-z0-9]$/;
  const invalid = values.find((value) => {
    if (kind === 'namespaces') return !namespace.test(value);
    if (kind === 'clusters') {
      const parts = value.split('/');
      return parts.length !== 2 || !parts[0] || !parts[1] || !namespace.test(parts[0]) || !/^[a-z][a-z0-9]{1,11}$/.test(parts[1]);
    }
    return !/^[a-z][a-z0-9-]*:(?:[a-z][a-z0-9-]*|\*)$/.test(value);
  });
  if (!invalid) return undefined;
  if (kind === 'namespaces') return `Invalid namespace "${invalid}". Use 2–32 lowercase letters, digits or hyphens, starting with a letter and ending with a letter or digit.`;
  if (kind === 'clusters') return `Invalid cluster "${invalid}". Use namespace/name; cluster names need 2–12 lowercase letters or digits, starting with a letter.`;
  return `Invalid permission "${invalid}". Use resource:action (for example clusters:read); only the action may be *.`;
}

export function buildTokenRestrictions(input: TokenRestrictionInput): {
  restrictions?: Restrictions;
  errors: Partial<Record<RestrictionList, string>>;
} {
  const namespaces = parseTokenList(input.namespaces);
  const clusters = parseTokenList(input.clusters);
  const permissions = parseTokenList(input.permissions);
  const errors: Partial<Record<RestrictionList, string>> = {};
  for (const kind of ['namespaces', 'clusters', 'permissions'] as const) {
    const error = tokenListError(kind, { namespaces, clusters, permissions }[kind]);
    if (error) errors[kind] = error;
  }
  if (Object.keys(errors).length || (!input.readOnly && !input.allowCredentialReveal && !namespaces.length && !clusters.length && !permissions.length)) return { errors };
  return {
    restrictions: {
      readOnly: input.readOnly,
      allowCredentialReveal: input.allowCredentialReveal,
      ...(namespaces.length ? { namespaces } : {}),
      ...(clusters.length ? { clusters } : {}),
      ...(permissions.length ? { permissions } : {}),
    },
    errors,
  };
}

export function tokenRestrictionSummary(restrictions?: Restrictions): string {
  if (!restrictions) return 'Unrestricted';
  const parts: string[] = [];
  if (restrictions.readOnly) parts.push('Read-only');
  for (const kind of ['namespaces', 'clusters', 'permissions'] as const) {
    const count = restrictions[kind]?.length ?? 0;
    if (count) parts.push(`${count} ${count === 1 ? kind.slice(0, -1) : kind}`);
  }
  parts.push(restrictions.allowCredentialReveal ? 'Credential reveal allowed' : 'No credential reveal');
  return parts.join(' · ');
}

export function tokenNameError(name: string): string | undefined {
  const length = [...name.trim()].length;
  if (length === 0) return 'Name is required and must not be blank.';
  if (length > 128) return 'Name must contain 1–128 characters after trimming whitespace.';
  return undefined;
}

export function tokenExpiry(input: string, now = Date.now()): { expiresAt?: string; error?: string } {
  if (!input) return {};
  const date = new Date(input);
  if (!Number.isFinite(date.getTime())) return { error: 'Expiry must be a valid date and time.' };
  if (date.getTime() <= now) return { error: 'Expiry must be in the future.' };
  return { expiresAt: date.toISOString() };
}

/** Preserve the request body and key together across lost-response retries. */
export function tokenSubmission(uuid: () => string = () => crypto.randomUUID()) {
  let busy = false;
  let fingerprint: string | undefined;
  let key: string | undefined;
  return {
    begin(name: string, expiresAt?: string, restrictions?: Restrictions): TokenSubmission | null {
      if (busy) return null;
      const next = JSON.stringify({ name: name.trim(), expiresAt, restrictions });
      if (fingerprint !== next) { fingerprint = next; key = uuid(); }
      busy = true;
      return { name: name.trim(), expiresAt, ...(restrictions ? { restrictions: structuredClone(restrictions) } : {}), key: key! };
    },
    release() { busy = false; },
  };
}
