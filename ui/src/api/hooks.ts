// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, unwrap, type Schemas } from './client';
import { keys } from './keys';
import { fetchAllNamespaces } from './namespaces';
import { useAuth } from '@/auth/session';
import { DEFAULT_NAMESPACE } from '@/auth/permissions';
import { namespaceQuery } from '@/namespaces/useNamespace';

export type Cluster = Schemas['Cluster'];
export type ClusterSpec = Schemas['ClusterSpec'];
export type Provider = Schemas['Provider'];
export type Namespace = Schemas['Namespace'];

/** Clusters in one namespace, or in every readable namespace when omitted. */
export function useClusters(namespace?: string) {
  return useQuery({
    queryKey: keys.clusterList(namespace),
    queryFn: () => unwrap(api.GET('/clusters', { params: { query: { limit: 200, ...namespaceQuery(namespace) } } })),
    refetchInterval: 30_000,
  });
}

export function useCluster(namespace: string, name: string, enabled = true) {
  return useQuery({
    queryKey: keys.cluster(namespace, name),
    queryFn: () => unwrap(api.GET('/clusters/{cluster}', { params: { path: { cluster: name }, query: { namespace } } })),
    enabled,
    refetchInterval: 15_000,
  });
}

export function useProviders() {
  return useQuery({
    queryKey: keys.providers,
    queryFn: () => unwrap(api.GET('/providers')),
    staleTime: 5 * 60_000,
  });
}

/** Backup storages in one namespace, or in every readable namespace when omitted. */
export function useBackupStorages(namespace?: string) {
  return useQuery({
    queryKey: keys.backupStorageList(namespace),
    queryFn: () => unwrap(api.GET('/backup-storages', { params: { query: { limit: 200, ...namespaceQuery(namespace) } } })),
  });
}

export function useOrphanedVolumes(enabled = true, namespace?: string) {
  return useQuery({
    queryKey: keys.orphanedVolumeList(namespace),
    queryFn: () => unwrap(api.GET('/orphaned-volumes', { params: { query: namespaceQuery(namespace) } })),
    enabled,
    staleTime: 60_000,
    refetchInterval: 60_000,
    retry: false,
  });
}

/**
 * All namespaces the current user may read, following every authorized page.
 * A user without any namespace grant simply gets an empty list.
 */
export function useNamespaces(enabled = true) {
  return useQuery({
    queryKey: keys.namespaces,
    queryFn: () => fetchAllNamespaces((cursor) => unwrap(api.GET('/namespaces', { params: { query: { limit: 200, cursor } } }))),
    enabled,
    staleTime: 30_000,
    refetchInterval: 30_000,
    retry: false,
  });
}

export function useNamespaceDetail(name: string, enabled = true) {
  return useQuery({
    queryKey: keys.namespace(name),
    queryFn: () => unwrap(api.GET('/namespaces/{namespace}', { params: { path: { namespace: name } } })),
    enabled,
    refetchInterval: 15_000,
  });
}

/**
 * Namespace names a user may place objects in. Empty while the list is still
 * loading; without a readable list (no grant, or a pre-namespace installation)
 * the only choice is default.
 */
export function useNamespaceOptions(): string[] {
  const auth = useAuth();
  const enabled = auth.status === 'authenticated' && auth.can('namespaces', 'read');
  const namespaces = useNamespaces(enabled);
  return useMemo(() => {
    if (enabled && namespaces.isPending) return [];
    const names = namespaces.data?.items.filter((n) => n.phase !== 'Deleting').map((n) => n.name) ?? [];
    return names.length ? names : [DEFAULT_NAMESPACE];
  }, [enabled, namespaces.isPending, namespaces.data]);
}

export function useVersion() {
  return useQuery({
    queryKey: keys.version,
    queryFn: () => unwrap(api.GET('/version')),
    staleTime: Infinity,
    retry: false,
  });
}
