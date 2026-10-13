// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { useClusters } from '@/api/hooks';
import { useAuth } from '@/auth/session';
import { DEFAULT_NAMESPACE } from '@/auth/permissions';
import { spansNamespaces, useNamespace } from '@/namespaces/useNamespace';
import { LinkButton } from '@/components/ui/Button';
import { Badge, Card, Empty, ErrorBlock, Loading, PageHeader, Time } from '@/components/ui/Primitives';
import { BackupSummary, HealthSummary, PhaseBadge, topologySummary } from './cluster-utils';
import { clusterPath } from './detail/ClusterDetail';

export function ClustersListPage() {
  const auth = useAuth();
  const { namespace } = useNamespace();
  const clusters = useClusters(namespace);
  const [filter, setFilter] = useState('');
  const allNamespaces = !namespace && spansNamespaces(clusters.data?.items ?? []);
  const newClusterTo = namespace ? `/clusters/new?namespace=${encodeURIComponent(namespace)}` : '/clusters/new';

  const items = useMemo(() => {
    const all = clusters.data?.items ?? [];
    const q = filter.trim().toLowerCase();
    return q ? all.filter((c) => c.name.includes(q) || (c.namespace || DEFAULT_NAMESPACE).includes(q) || c.spec.version.includes(q) || c.status.phase.toLowerCase().includes(q)) : all;
  }, [clusters.data, filter]);

  const counts = useMemo(() => {
    const all = clusters.data?.items ?? [];
    const ready = all.filter((c) => c.status.phase === 'Ready').length;
    const attention = all.filter((c) => c.status.phase === 'Degraded' || c.status.phase === 'Error').length;
    const placement = all.filter((c) => c.status.placement?.groups.some((g) => g.members.length > 1)).length;
    return { total: all.length, ready, attention, placement };
  }, [clusters.data]);

  return (
    <div className="page">
      <PageHeader
        title="Clusters"
        description={namespace ? `Database clusters managed in namespace ${namespace}.` : allNamespaces ? 'Database clusters managed across every namespace you can see.' : 'Database clusters managed in this Zerops project.'}
        actions={
          auth.can('clusters', 'create') ? (
            <LinkButton to={newClusterTo} variant="primary">
              + New cluster
            </LinkButton>
          ) : null
        }
      />

      {clusters.data && clusters.data.items.length > 0 ? (
        <div className="grid-4">
          <Card flush>
            <div className="stat">
              <span className="stat-label">Clusters</span>
              <span className="stat-value">{counts.total}</span>
            </div>
          </Card>
          <Card flush>
            <div className="stat">
              <span className="stat-label">Ready</span>
              <span className="stat-value" style={{ color: 'var(--ok)' }}>
                {counts.ready}
              </span>
            </div>
          </Card>
          <Card flush>
            <div className="stat">
              <span className="stat-label">Needs attention</span>
              <span className="stat-value" style={{ color: counts.attention ? 'var(--danger)' : undefined }}>
                {counts.attention}
              </span>
            </div>
          </Card>
          <Card flush>
            <div className="stat">
              <span className="stat-label">Co-located members</span>
              <span className="stat-value" style={{ color: counts.placement ? 'var(--warn)' : undefined }}>
                {counts.placement}
              </span>
              <span className="stat-sub">clusters with members detected on one host</span>
            </div>
          </Card>
        </div>
      ) : null}

      <Card
        flush
        title={`${items.length} cluster${items.length === 1 ? '' : 's'}`}
        actions={
          <input
            className="input"
            style={{ width: 240 }}
            type="search"
            placeholder={allNamespaces ? 'Filter by name, namespace, version, phase' : 'Filter by name, version, phase'}
            aria-label="Filter clusters"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
          />
        }
      >
        {clusters.isPending ? (
          <Loading />
        ) : clusters.error ? (
          <div className="card-body">
            <ErrorBlock error={clusters.error} retry={() => clusters.refetch()} />
          </div>
        ) : items.length === 0 ? (
          <Empty title={filter ? 'No clusters match' : 'No clusters yet'} action={!filter && auth.can('clusters', 'create') ? <LinkButton to={newClusterTo} variant="primary">Create your first cluster</LinkButton> : null}>
            {filter ? 'Try a different filter.' : 'Create a replica set or sharded cluster to get started.'}
          </Empty>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  {allNamespaces ? <th>Namespace</th> : null}
                  <th>Phase</th>
                  <th>Version</th>
                  <th>Topology</th>
                  <th>Members</th>
                  <th>Backups</th>
                  <th>Placement</th>
                  <th>Updated</th>
                </tr>
              </thead>
              <tbody>
                {items.map((c) => {
                  const colocated = c.status.placement?.groups.some((g) => g.members.length > 1);
                  const ns = c.namespace || DEFAULT_NAMESPACE;
                  return (
                    <tr key={`${ns}/${c.name}`}>
                      <td>
                        <Link to={clusterPath(ns, c.name)} className="strong mono">
                          {c.name}
                        </Link>
                      </td>
                      {allNamespaces ? <td className="mono small">{ns}</td> : null}
                      <td>
                        <PhaseBadge phase={c.status.phase} message={c.status.message} />
                      </td>
                      <td className="mono">
                        {c.status.version ?? c.spec.version}
                        {c.status.version && c.status.version !== c.spec.version ? <span className="faint"> → {c.spec.version}</span> : null}
                      </td>
                      <td className="small">{topologySummary(c)}</td>
                      <td>
                        <HealthSummary cluster={c} />
                      </td>
                      <td>
                        <BackupSummary cluster={c} />
                      </td>
                      <td>
                        {colocated ? (
                          <Badge tone="warn" title={c.status.placement?.warning}>
                            Co-located
                          </Badge>
                        ) : c.status.placement ? (
                          <Badge plain title={c.status.placement.warning}>
                            Unverified
                          </Badge>
                        ) : (
                          <span className="faint">—</span>
                        )}
                      </td>
                      <td className="small">
                        <Time iso={c.updatedAt} />
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </Card>
    </div>
  );
}
