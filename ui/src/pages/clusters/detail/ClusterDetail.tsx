// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { Link, Outlet, useNavigate, useOutletContext, useParams } from 'react-router-dom';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { useCluster, type Cluster } from '@/api/hooks';
import { useAuth } from '@/auth/session';
import type { Action, Resource } from '@/auth/permissions';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { Badge, Callout, ErrorBlock, Loading, Menu, MenuItem, PageHeader, Tabs } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { PhaseBadge, topologySummary } from '../cluster-utils';

export interface ClusterContext {
  cluster: Cluster;
  name: string;
  can: (action: Action, resource?: Resource) => boolean;
}

export function useClusterContext(): ClusterContext {
  return useOutletContext<ClusterContext>();
}

export function ClusterDetailLayout() {
  const { name = '' } = useParams();
  const auth = useAuth();
  const navigate = useNavigate();
  const toast = useToast();
  const queryClient = useQueryClient();
  const q = useCluster(name);
  const [confirm, setConfirm] = useState<null | 'restart' | 'rotate-tls' | 'resume'>(null);

  const action = useMutation({
    mutationFn: async (kind: 'restart' | 'rotate-tls' | 'resume') => {
      const path = { params: { path: { cluster: name } } };
      if (kind === 'restart') return unwrap(api.POST('/clusters/{cluster}/restart', path));
      if (kind === 'rotate-tls') return unwrap(api.POST('/clusters/{cluster}/rotate-tls', path));
      return unwrap(api.POST('/clusters/{cluster}/resume', path));
    },
    onSuccess: (_r, kind) => {
      void queryClient.invalidateQueries({ queryKey: keys.cluster(name) });
      void queryClient.invalidateQueries({ queryKey: keys.clusters });
      toast.success(kind === 'resume' ? 'Resuming cluster.' : kind === 'restart' ? 'Rolling restart started.' : 'TLS rotation started.');
      setConfirm(null);
      if (kind !== 'resume') navigate(`/clusters/${encodeURIComponent(name)}/operations`);
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  if (q.isPending) {
    return (
      <div className="page">
        <Loading label={`Loading ${name}…`} />
      </div>
    );
  }
  if (q.error || !q.data) {
    return (
      <div className="page">
        <PageHeader
          crumbs={
            <>
              <Link to="/clusters">Clusters</Link> <span>/</span> <span className="mono">{name}</span>
            </>
          }
          title={name}
        />
        <ErrorBlock error={q.error ?? new Error('Cluster not found')} retry={() => q.refetch()} />
      </div>
    );
  }
  const cluster = q.data;
  const can: ClusterContext['can'] = (act, resource = 'clusters') => auth.can(resource, act, name);
  const base = `/clusters/${encodeURIComponent(name)}`;
  const suspended = cluster.status.phase === 'Suspended';
  const colocated = cluster.status.placement?.groups.some((g) => g.members.length > 1);

  return (
    <div className="page">
      <PageHeader
        crumbs={
          <>
            <Link to="/clusters">Clusters</Link> <span>/</span> <span className="mono">{name}</span>
          </>
        }
        title={
          <span className="row" style={{ gap: 10 }}>
            <span className="mono">{name}</span>
            <PhaseBadge phase={cluster.status.phase} message={cluster.status.message} />
            {colocated ? (
              <Badge tone="warn" title={cluster.status.placement?.warning}>
                Co-located members
              </Badge>
            ) : null}
          </span>
        }
        description={
          <>
            {topologySummary(cluster)} · version {cluster.status.version ?? cluster.spec.version}
            {cluster.status.fcv ? ` · FCV ${cluster.status.fcv}` : ''}
            {cluster.status.message ? ` · ${cluster.status.message}` : ''}
          </>
        }
        actions={
          <>
            {suspended && can('update') ? (
              <Button variant="primary" onClick={() => setConfirm('resume')}>
                Resume cluster
              </Button>
            ) : null}
            {can('update') && !suspended ? (
              <Menu label="Actions" size="md">
                <MenuItem onClick={() => setConfirm('restart')}>Rolling restart…</MenuItem>
                <MenuItem onClick={() => setConfirm('rotate-tls')} disabled={cluster.spec.tls?.mode === 'disabled'} title={cluster.spec.tls?.mode === 'disabled' ? 'TLS is disabled' : undefined}>
                  Rotate TLS certificates…
                </MenuItem>
                <MenuItem onClick={() => navigate(`/clusters/new?from=${encodeURIComponent(name)}`)}>Clone into new cluster…</MenuItem>
              </Menu>
            ) : null}
          </>
        }
      />
      {cluster.status.phase === 'Error' || cluster.status.phase === 'Degraded' ? (
        <Callout tone={cluster.status.phase === 'Error' ? 'danger' : 'warn'} title={cluster.status.phase}>
          {cluster.status.message ?? cluster.status.conditions.find((c) => c.status !== 'True')?.message ?? 'See conditions on the overview tab.'}
        </Callout>
      ) : null}
      <Tabs
        items={[
          { to: base, label: 'Overview', end: true },
          { to: `${base}/topology`, label: 'Topology & scaling' },
          { to: `${base}/upgrades`, label: 'Upgrades' },
          { to: `${base}/backups`, label: 'Backups' },
          { to: `${base}/restores`, label: 'Restores' },
          { to: `${base}/users`, label: 'Users' },
          { to: `${base}/monitoring`, label: 'Monitoring' },
          { to: `${base}/events`, label: 'Events & logs' },
          { to: `${base}/operations`, label: 'Operations' },
          { to: `${base}/delete`, label: 'Delete' },
        ]}
      />
      <Outlet context={{ cluster, name, can } satisfies ClusterContext} />

      <ConfirmDialog
        open={confirm === 'restart'}
        onClose={() => setConfirm(null)}
        onConfirm={() => action.mutate('restart')}
        title="Rolling restart"
        confirmLabel="Restart members"
        busy={action.isPending}
      >
        <p className="small">Members restart one at a time, secondaries first and the primary last after a step-down. Writes pause briefly during the primary election.</p>
      </ConfirmDialog>
      <ConfirmDialog
        open={confirm === 'rotate-tls'}
        onClose={() => setConfirm(null)}
        onConfirm={() => action.mutate('rotate-tls')}
        title="Rotate TLS certificates"
        confirmLabel="Rotate"
        busy={action.isPending}
      >
        <p className="small">
          New member and server certificates are issued and rolled out with a rolling restart.
          {cluster.spec.tls?.ca === 'userProvided' ? ' For user-provided TLS, update the PEM inputs under Topology & scaling before rotating.' : ' Clients pinning the CA certificate keep working; the CA itself is retained.'}
        </p>
      </ConfirmDialog>
      <ConfirmDialog
        open={confirm === 'resume'}
        onClose={() => setConfirm(null)}
        onConfirm={() => action.mutate('resume')}
        title="Resume suspended cluster"
        confirmLabel="Resume"
        busy={action.isPending}
      >
        <p className="small">Runtime services are recreated on the retained volumes and the cluster is reconciled back to Ready.</p>
      </ConfirmDialog>
    </div>
  );
}
