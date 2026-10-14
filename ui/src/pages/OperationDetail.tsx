// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { Link, useParams } from 'react-router-dom';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { Card, Callout, ErrorBlock, KV, Loading, PageHeader, Time, formatDuration } from '@/components/ui/Primitives';
import { elapsedSeconds } from '@/components/ui/presentation';
import { OpPhaseBadge } from './clusters/cluster-utils';
import { clusterPath } from './clusters/cluster-path';
import { DEFAULT_NAMESPACE, clusterObject } from '@/auth/permissions';
import { OperationTarget } from '@/components/OperationNotice';
import { isCancellable } from '@/components/operations';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';

export function OperationDetailPage() {
  const { id = '' } = useParams();
  const auth = useAuth();
  const queryClient = useQueryClient();
  const [confirmCancel, setConfirmCancel] = useState(false);
  const operation = useQuery({
    queryKey: [...keys.operations, 'detail', id],
    queryFn: () => unwrap(api.GET('/operations/{operation}', { params: { path: { operation: id } } })),
    enabled: Boolean(id) && auth.can('operations', 'read'),
    refetchInterval: (query) => query.state.data?.phase === 'Running' || query.state.data?.phase === 'Pending' ? 3000 : false,
  });
  const op = operation.data;
  const cancel = useMutation({
    mutationFn: () => unwrap(api.POST('/operations/{operation}/cancel', { params: { path: { operation: id } } })),
    onSuccess: (result) => {
      queryClient.setQueryData([...keys.operations, 'detail', id], result);
      void queryClient.invalidateQueries({ queryKey: keys.operations });
      void queryClient.invalidateQueries({ queryKey: keys.clusterSub(result.namespace || DEFAULT_NAMESPACE, result.cluster, 'operations') });
      setConfirmCancel(false);
    },
  });
  const canCancel = op && isCancellable(op, auth.can('clusters', 'update', clusterObject(op.namespace, op.cluster)));
  return <div className="page">
    <PageHeader title="Operation details" crumbs={<Link to="/operations">Operations</Link>} description={id} />
    {!auth.can('operations', 'read') ? <Callout>You do not have permission to read operations.</Callout> : operation.isPending ? <Loading /> : operation.error ? <ErrorBlock error={operation.error} retry={() => operation.refetch()} /> : op ? <Card title={op.type} actions={canCancel ? <Button onClick={() => setConfirmCancel(true)}>Cancel operation…</Button> : null}>
      <KV items={[
        ['ID', <span className="mono" style={{ overflowWrap: 'anywhere' }}>{op.id}</span>],
        ['Kind', op.type], ['Phase', <OpPhaseBadge phase={op.phase} />],
        ['Namespace', op.namespace],
        ['Cluster', <Link to={clusterPath(op.namespace, op.cluster)}>{op.cluster}</Link>],
        ['Target', <OperationTarget operation={op} />],
        ['Step', op.step || 'Not reported'],
        ['Cancellation requested', op.cancelRequested ? 'Yes — awaiting cancellation or cleanup.' : 'No'],
        ['Actor', op.actor || 'Not recorded'],
        ['Message', op.message || (op.phase === 'Succeeded' ? 'Operation succeeded.' : op.phase === 'Cancelled' ? 'Operation cancelled.' : 'No stage message recorded.')],
        ['Progress', op.progress === undefined ? 'Not reported' : `${Math.round(op.progress)}%`],
        ['Created (UTC)', <Time iso={op.createdAt} exact />],
        ['Started (UTC)', <Time iso={op.startedAt} exact />],
        ['Completed (UTC)', <Time iso={op.completedAt} exact />],
        [op.completedAt ? 'Duration' : 'Elapsed', formatDuration(elapsedSeconds(op.startedAt ?? op.createdAt, op.completedAt))],
      ]} />
      {op.error ? <ErrorBlock error={op.error} /> : null}
    </Card> : null}
    <ConfirmDialog open={confirmCancel} onClose={() => { setConfirmCancel(false); cancel.reset(); }} onConfirm={() => cancel.mutateAsync()} title="Cancel operation" confirmLabel="Request cancellation" busy={cancel.isPending} error={cancel.error}>
      <p className="small">Cancellation is cooperative and may require cleanup. Work already completed is not undone. The server refuses cancellation when the current step cannot be stopped safely.</p>
    </ConfirmDialog>
  </div>;
}
