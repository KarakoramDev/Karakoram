// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

/**
 * Query keys. Cluster-scoped keys all start with ['cluster', name] so a single
 * event for that cluster can invalidate every dependent view.
 */
export const keys = {
  clusters: ['clusters'] as const,
  cluster: (name: string) => ['cluster', name] as const,
  clusterSub: (name: string, sub: string) => ['cluster', name, sub] as const,
  providers: ['providers'] as const,
  backupStorages: ['backup-storages'] as const,
  orphanedVolumes: ['orphaned-volumes'] as const,
  accounts: ['accounts'] as const,
  apiTokens: ['api-tokens'] as const,
  settings: ['settings'] as const,
  rbacPolicy: ['rbac-policy'] as const,
  events: ['events'] as const,
  operations: ['operations'] as const,
  version: ['version'] as const,
};
