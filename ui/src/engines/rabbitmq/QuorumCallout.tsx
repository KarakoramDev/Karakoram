// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { Link } from 'react-router-dom';
import type { Schemas } from '../../api/client';
import { Callout } from '../../components/ui/Primitives';

export function QuorumCallout({ cluster, base }: { cluster: Schemas['Cluster']; base: string }) {
  const blocker = cluster.status.conditions.find((condition) => condition.type === 'QuorumCriticalQueues' && condition.status === 'True');
  if (!blocker) return null;
  return <Callout tone="warn" title="Rolling restart blocked by queue quorum">
    <p>{blocker.message}</p>
    <Link to={`${base}/queue-recovery`}>Grow queue replicas safely</Link>
  </Callout>;
}
