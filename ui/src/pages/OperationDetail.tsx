// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { Link, useParams } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { api, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { Card, Callout, ErrorBlock, KV, Loading, PageHeader, Time, formatDuration } from '@/components/ui/Primitives';
import { elapsedSeconds } from '@/components/ui/presentation';
import { OpPhaseBadge } from './clusters/cluster-utils';
import { clusterPath } from './clusters/detail/ClusterDetail';

export function OperationDetailPage() {
  const { id = '' } = useParams();
  const auth = useAuth();
  const operation = useQuery({
    queryKey: [...keys.operations, 'detail', id],
    queryFn: () => unwrap(api.GET('/operations/{operation}', { params: { path: { operation: id } } })),
    enabled: Boolean(id) && auth.can('operations', 'read'),
    refetchInterval: (query) => query.state.data?.phase === 'Running' || query.state.data?.phase === 'Pending' ? 3000 : false,
  });
  const op = operation.data;
  return <div className="page">
    <PageHeader title="Operation details" crumbs={<Link to="/operations">Operations</Link>} description={id} />
    {!auth.can('operations', 'read') ? <Callout>You do not have permission to read operations.</Callout> : operation.isPending ? <Loading /> : operation.error ? <ErrorBlock error={operation.error} retry={() => operation.refetch()} /> : op ? <Card title={op.type}>
      <KV items={[
        ['ID', <span className="mono" style={{ overflowWrap: 'anywhere' }}>{op.id}</span>],
        ['Kind', op.type], ['Phase', <OpPhaseBadge phase={op.phase} />],
        ['Namespace', op.namespace],
        ['Cluster', <Link to={clusterPath(op.namespace, op.cluster)}>{op.cluster}</Link>],
        ['Target', 'Resource target is not provided by this API version.'],
        ['Actor', op.actor || 'Not recorded'],
        ['Message', op.message || (op.phase === 'Succeeded' ? 'Operation succeeded.' : 'No stage message recorded.')],
        ['Progress', op.progress === undefined ? 'Not reported' : `${Math.round(op.progress)}%`],
        ['Created (UTC)', <Time iso={op.createdAt} exact />],
        ['Started (UTC)', <Time iso={op.startedAt} exact />],
        ['Completed (UTC)', <Time iso={op.completedAt} exact />],
        [op.completedAt ? 'Duration' : 'Elapsed', formatDuration(elapsedSeconds(op.startedAt ?? op.createdAt, op.completedAt))],
      ]} />
      {op.error ? <ErrorBlock error={op.error} /> : null}
    </Card> : null}
  </div>;
}
