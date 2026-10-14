// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { clusterPath } from '../pages/clusters/cluster-path.ts';
import { DEFAULT_NAMESPACE } from '../auth/permissions.ts';

type Operation = Schemas['Operation'];

export function operationPath(id: string): string {
  return `/operations/${encodeURIComponent(id)}`;
}

export function operationTargetLink(op: Pick<Operation, 'namespace' | 'cluster' | 'target'>): string | undefined {
  if (!op.target.name) return undefined;
  const namespace = op.namespace || DEFAULT_NAMESPACE;
  if (op.target.kind === 'cluster') return clusterPath(namespace, op.target.name);
  if (op.target.kind === 'backup' && op.cluster) {
    return `${clusterPath(namespace, op.cluster)}/backups#backup-${encodeURIComponent(op.target.name)}`;
  }
  return undefined;
}

export function isCancellable(op: Pick<Operation, 'phase' | 'cancelRequested'>, canUpdateCluster: boolean): boolean {
  return canUpdateCluster && !op.cancelRequested && (op.phase === 'Pending' || op.phase === 'Running');
}
