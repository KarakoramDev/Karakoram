// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useQuery } from '@tanstack/react-query';
import { api, unwrap, type Schemas } from './client';
import { keys } from './keys';

export type Cluster = Schemas['Cluster'];
export type ClusterSpec = Schemas['ClusterSpec'];
export type Provider = Schemas['Provider'];

export function useClusters() {
  return useQuery({
    queryKey: keys.clusters,
    queryFn: () => unwrap(api.GET('/clusters', { params: { query: { limit: 200 } } })),
    refetchInterval: 30_000,
  });
}

export function useCluster(name: string) {
  return useQuery({
    queryKey: keys.cluster(name),
    queryFn: () => unwrap(api.GET('/clusters/{cluster}', { params: { path: { cluster: name } } })),
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

export function useBackupStorages() {
  return useQuery({
    queryKey: keys.backupStorages,
    queryFn: () => unwrap(api.GET('/backup-storages', { params: { query: { limit: 200 } } })),
  });
}

export function useOrphanedVolumes(enabled = true) {
  return useQuery({
    queryKey: keys.orphanedVolumes,
    queryFn: () => unwrap(api.GET('/orphaned-volumes')),
    enabled,
    staleTime: 60_000,
    refetchInterval: 60_000,
    retry: false,
  });
}

export function useVersion() {
  return useQuery({
    queryKey: keys.version,
    queryFn: () => unwrap(api.GET('/version')),
    staleTime: Infinity,
    retry: false,
  });
}
