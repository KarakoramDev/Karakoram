// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '../../api/client';

export type ApplicationDraft = { application: string; privileges: string; resources: string };
const values = (text: string) => text.split(/[,\n]+/).map((value) => value.trim()).filter(Boolean);

export function applicationDrafts(grants: Schemas['ElasticApplicationPrivileges'][]): ApplicationDraft[] {
  return grants.map((grant) => ({ application: grant.application, privileges: grant.privileges.join(', '), resources: grant.resources.join(', ') }));
}

export function applicationGrants(drafts: ApplicationDraft[]): Schemas['ElasticApplicationPrivileges'][] | null {
  const grants = drafts.map((draft) => ({ application: draft.application.trim(), privileges: values(draft.privileges), resources: values(draft.resources) }));
  return grants.every((grant) => grant.application && grant.privileges.length && grant.resources.length) ? grants : null;
}

export const INVALIDATION_WAIT_MS = 45_000;
export type PendingInvalidation = { id: string; startedAt: number };
type MarkerStorage = Pick<Storage, 'length' | 'key' | 'getItem' | 'setItem' | 'removeItem'>;
const markerPrefix = (namespace: string, cluster: string) => `karakoram.elastic.invalidate:${encodeURIComponent(namespace)}:${encodeURIComponent(cluster)}:`;
const markerKey = (namespace: string, cluster: string, id: string) => markerPrefix(namespace, cluster) + encodeURIComponent(id);

export function pendingInvalidations(storage: MarkerStorage, namespace: string, cluster: string): PendingInvalidation[] {
  const prefix = markerPrefix(namespace, cluster);
  const pending: PendingInvalidation[] = [];
  for (let i = 0; i < storage.length; i++) {
    const key = storage.key(i);
    if (!key?.startsWith(prefix)) continue;
    const startedAt = Number(storage.getItem(key));
    if (Number.isFinite(startedAt)) pending.push({ id: decodeURIComponent(key.slice(prefix.length)), startedAt });
  }
  return pending;
}

export function rememberInvalidation(storage: MarkerStorage, namespace: string, cluster: string, marker: PendingInvalidation) {
  storage.setItem(markerKey(namespace, cluster, marker.id), String(marker.startedAt));
}

export function forgetInvalidation(storage: MarkerStorage, namespace: string, cluster: string, id: string) {
  storage.removeItem(markerKey(namespace, cluster, id));
}

export function invalidationState(marker: PendingInvalidation | undefined, key: Pick<Schemas['ElasticAPIKey'], 'invalidated'> | undefined, now: number): 'checking' | 'invalidated' | 'retry' | 'active' {
  if (key?.invalidated || (marker && !key)) return 'invalidated';
  if (!marker) return 'active';
  return now - marker.startedAt < INVALIDATION_WAIT_MS ? 'checking' : 'retry';
}
