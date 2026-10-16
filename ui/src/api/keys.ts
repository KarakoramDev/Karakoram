// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

/**
 * Query keys. Cluster-scoped keys all start with ['cluster', namespace, name]
 * so a single event for that cluster can invalidate every dependent view.
 * Collection keys carry the namespace filter ('' = all authorized namespaces)
 * behind a stable prefix so one invalidation covers every filter.
 */
export const keys = {
  clusters: ['clusters'] as const,
  clusterList: (namespace: string | undefined) => ['clusters', namespace ?? ''] as const,
  cluster: (namespace: string, name: string) => ['cluster', namespace, name] as const,
  clusterSub: (namespace: string, name: string, sub: string) => ['cluster', namespace, name, sub] as const,
  providers: ['providers'] as const,
  backends: ['backends'] as const,
  backupStorages: ['backup-storages'] as const,
  backupStorageList: (namespace: string | undefined) => ['backup-storages', namespace ?? ''] as const,
  orphanedVolumes: ['orphaned-volumes'] as const,
  orphanedVolumeList: (namespace: string | undefined) => ['orphaned-volumes', namespace ?? ''] as const,
  namespaces: ['namespaces'] as const,
  namespace: (name: string) => ['namespaces', name] as const,
  accounts: ['accounts'] as const,
  apiTokens: ['api-tokens'] as const,
  settings: ['settings'] as const,
  rbacPolicy: ['rbac-policy'] as const,
  events: ['events'] as const,
  eventList: (namespace: string | undefined) => ['events', namespace ?? ''] as const,
  operations: ['operations'] as const,
  operationList: (namespace: string | undefined) => ['operations', namespace ?? ''] as const,
  version: ['version'] as const,
};
