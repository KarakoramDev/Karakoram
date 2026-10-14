// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useQuery } from '@tanstack/react-query';
import { api, unwrap } from '@/api/client';
import { keys } from '@/api/keys';

export function useSourcePitrWindow(namespace: string, cluster: string, enabled = true) {
  return useQuery({
    queryKey: keys.clusterSub(namespace, cluster, 'pitr'),
    queryFn: () => unwrap(api.GET('/clusters/{cluster}/pitr', { params: { path: { cluster }, query: { namespace } } })),
    enabled: Boolean(cluster) && enabled,
  });
}
