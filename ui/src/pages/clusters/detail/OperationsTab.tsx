// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { Card, ErrorBlock, Loading, Segmented, Time } from '@/components/ui/Primitives';
import { OpPhaseBadge } from '../cluster-utils';
import { useClusterContext } from './ClusterDetail';

type Operation = Schemas['Operation'];
type PhaseFilter = 'all' | 'active' | 'Failed';

export function OperationsTable({ items, showCluster }: { items: Operation[]; showCluster?: boolean }) {
  return (
    <div className="table-wrap">
      <table className="table">
        <thead>
          <tr>
            <th>Operation</th>
            {showCluster ? <th>Cluster</th> : null}
            <th>Type</th>
            <th>Phase</th>
            <th>Actor</th>
            <th style={{ width: 200 }}>Progress</th>
            <th>Message</th>
            <th>Created</th>
            <th>Completed</th>
          </tr>
        </thead>
        <tbody>
          {items.map((op) => (
            <tr key={op.id}>
              <td className="mono xs">{op.id}</td>
              {showCluster ? <td className="mono small">{op.cluster}</td> : null}
              <td className="small">{op.type}</td>
              <td>
                <OpPhaseBadge phase={op.phase} />
              </td>
              <td className="small mono">{op.actor || '—'}</td>
              <td>
                {op.progress !== undefined ? (
                  <div className="row">
                    <div className="progress grow" role="progressbar" aria-valuenow={Math.round(op.progress)} aria-valuemin={0} aria-valuemax={100} aria-label={`${op.type} progress`}>
                      <span style={{ width: `${Math.min(100, Math.max(0, op.progress))}%` }} />
                    </div>
                    <span className="xs nowrap">{Math.round(op.progress)}%</span>
                  </div>
                ) : op.phase === 'Running' ? (
                  <span className="spinner" aria-label="Running" />
                ) : (
                  <span className="faint">—</span>
                )}
              </td>
              <td className="small">
                {op.message}
                {op.error ? (
                  <div className="xs" style={{ color: 'var(--danger)' }}>
                    {op.error.code}: {op.error.message}
                  </div>
                ) : null}
              </td>
              <td className="small">
                <Time iso={op.createdAt} />
              </td>
              <td className="small">
                <Time iso={op.completedAt} />
              </td>
            </tr>
          ))}
          {items.length === 0 ? (
            <tr>
              <td colSpan={showCluster ? 9 : 8} className="table-empty">
                No operations.
              </td>
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  );
}

export function OperationsTab() {
  const { name, can } = useClusterContext();
  const [filter, setFilter] = useState<PhaseFilter>('all');
  const ops = useQuery({
    queryKey: keys.clusterSub(name, 'operations'),
    queryFn: () => unwrap(api.GET('/clusters/{cluster}/operations', { params: { path: { cluster: name }, query: { limit: 200 } } })),
    refetchInterval: (q) => (q.state.data?.items.some((o) => o.phase === 'Running' || o.phase === 'Pending') ? 3000 : 15_000),
    enabled: can('read', 'operations'),
  });
  const items = (ops.data?.items ?? []).filter((o) => (filter === 'all' ? true : filter === 'active' ? o.phase === 'Running' || o.phase === 'Pending' : o.phase === 'Failed'));
  return (
    <Card
      title="Operations"
      flush
      actions={
        <Segmented<PhaseFilter>
          label="Filter"
          value={filter}
          onChange={setFilter}
          options={[
            { value: 'all', label: 'All' },
            { value: 'active', label: 'Active' },
            { value: 'Failed', label: 'Failed' },
          ]}
        />
      }
    >
      {ops.isPending ? <Loading /> : ops.error ? <div className="card-body"><ErrorBlock error={ops.error} retry={() => ops.refetch()} /></div> : <OperationsTable items={items} />}
    </Card>
  );
}
