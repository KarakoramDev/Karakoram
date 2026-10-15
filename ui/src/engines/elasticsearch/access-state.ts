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

/** Native roles accept arbitrary applications; warn without rejecting those grants. */
export function applicationWarnings(drafts: ApplicationDraft[]): string[] {
  return drafts.flatMap((draft) => {
    const application = draft.application.trim();
    if (!application) return [];
    const unknown = values(draft.privileges).filter((privilege) => !/^(?:all|read|space_all|space_read|feature_[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)$/.test(privilege));
    if (application === 'kibana-.kibana' && !unknown.length) return [];
    return [`Application "${application}" or its privileges do not match Kibana's registered application kibana-.kibana and known privilege forms (all, read, space_all, space_read, feature_<name>.<privilege>, including minimal_all/minimal_read and sub-feature privileges). Elasticsearch accepts custom grants, but unregistered privileges do not grant Kibana access.`];
  });
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

export function failedInvalidation(storage: MarkerStorage, namespace: string, cluster: string, pending: PendingInvalidation[], id: string, status: number | undefined): PendingInvalidation[] {
  if (status === 404) {
    forgetInvalidation(storage, namespace, cluster, id);
    return pending.filter((marker) => marker.id !== id);
  }
  const marker = { id, startedAt: 0 };
  rememberInvalidation(storage, namespace, cluster, marker);
  return pending.map((item) => item.id === id ? marker : item);
}

export function invalidationState(marker: PendingInvalidation | undefined, key: Pick<Schemas['ElasticAPIKey'], 'invalidated'> | undefined, now: number): 'checking' | 'invalidated' | 'retry' | 'active' {
  if (key?.invalidated || (marker && !key)) return 'invalidated';
  if (!marker) return 'active';
  return now - marker.startedAt < INVALIDATION_WAIT_MS ? 'checking' : 'retry';
}
