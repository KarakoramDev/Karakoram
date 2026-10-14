// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useState } from 'react';
import { Link, Navigate, Outlet, useLocation, useNavigate, useOutletContext, useParams } from 'react-router-dom';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, ApiError, errorMessage, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { useCluster, useProviders, type Cluster, type Provider } from '@/api/hooks';
import { idempotencyHeaders, useIdempotencyKey } from '@/api/idempotency';
import { useAuth } from '@/auth/session';
import { DEFAULT_NAMESPACE, clusterObject, type Action, type Resource } from '@/auth/permissions';
import { rememberNamespace } from '@/namespaces/useNamespace';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { Badge, Callout, ErrorBlock, Loading, Menu, MenuItem, PageHeader, Tabs } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { PhaseBadge, topologySummary } from '../cluster-utils';
import { colocatedGroups, maintenanceCopy, observedVersion } from './detail-state';
import { clusterPath } from '../cluster-path';
import { OperationNotice } from '@/components/OperationNotice';
import { detailProvider, engines, hasCapability, supportsOperation, tabAvailable } from '@/engines';
import { detailTabState } from './detail-tab-state';

export interface ClusterContext {
  cluster: Cluster;
  provider: Provider | undefined;
  namespace: string;
  name: string;
  /** Namespace-aware route prefix of this cluster (`/clusters/<ns>/<name>`). */
  base: string;
  can: (action: Action, resource?: Resource) => boolean;
}

export function useClusterContext(): ClusterContext {
  return useOutletContext<ClusterContext>();
}

const TAB_SEGMENTS: Record<string, true> = { topology: true, upgrades: true, backups: true, restores: true, users: true, monitoring: true, events: true, operations: true, delete: true };

/** Pre-namespace `/clusters/<name>` links land on the default namespace. */
export function LegacyClusterRedirect() {
  const { name = '' } = useParams();
  return <Navigate to={clusterPath(DEFAULT_NAMESPACE, name)} replace />;
}

export function ClusterDetailLayout() {
  const { namespace = DEFAULT_NAMESPACE, name = '' } = useParams();
  const auth = useAuth();
  const navigate = useNavigate();
  const toast = useToast();
  const queryClient = useQueryClient();
  const q = useCluster(namespace, name);
  const providers = useProviders();
  const location = useLocation();
  const [confirm, setConfirm] = useState<null | 'restart' | 'rotate-tls' | 'resume'>(null);
  const base = clusterPath(namespace, name);
  const submission = useIdempotencyKey();

  const action = useMutation({
    mutationFn: async (kind: 'restart' | 'rotate-tls' | 'resume') => {
      const path = { headers: idempotencyHeaders(submission.key), params: { path: { cluster: name }, query: { namespace } } };
      if (kind === 'restart') return unwrap(api.POST('/clusters/{cluster}/restart', path));
      if (kind === 'rotate-tls') return unwrap(api.POST('/clusters/{cluster}/rotate-tls', path));
      return unwrap(api.POST('/clusters/{cluster}/resume', path));
    },
    onSuccess: (result, kind) => {
      submission.reset();
      void queryClient.invalidateQueries({ queryKey: keys.cluster(namespace, name) });
      void queryClient.invalidateQueries({ queryKey: keys.clusters });
      toast.success(<OperationNotice operationId={'operationId' in result ? result.operationId : 'id' in result ? result.id : undefined}>{kind === 'resume' ? 'Resuming cluster.' : kind === 'restart' ? 'Rolling restart started.' : 'TLS rotation started.'}</OperationNotice>);
      setConfirm(null);
      if (kind !== 'resume') navigate(`${base}/operations`);
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  // Collection views that follow from here keep showing this namespace.
  useEffect(() => {
    rememberNamespace(namespace);
  }, [namespace]);

  if (q.isPending) {
    return (
      <div className="page">
        <Loading label={`Loading ${name}…`} />
      </div>
    );
  }
  if (!q.data) {
    // A pre-namespace `/clusters/<name>/<tab>` link parses as namespace
    // `<name>` and cluster `<tab>`; when the server knows no such cluster,
    // resend it to the default namespace.
    if (q.error instanceof ApiError && q.error.status === 404 && TAB_SEGMENTS[name]) {
      return <Navigate to={`${clusterPath(DEFAULT_NAMESPACE, namespace)}/${name}`} replace />;
    }
    return (
      <div className="page">
        <PageHeader
          crumbs={
            <>
              <Link to="/clusters">Clusters</Link> <span>/</span> <span className="mono">{namespace}</span> <span>/</span> <span className="mono">{name}</span>
            </>
          }
          title={name}
        />
        <ErrorBlock error={q.error ?? new Error('Cluster not found')} retry={() => q.refetch()} />
      </div>
    );
  }
  const cluster = q.data;
  const provider = detailProvider(cluster.spec.provider, providers.error ? undefined : providers.data?.items.find((p) => p.name === cluster.spec.provider));
  const engine = engines[cluster.spec.provider];
  const tab = location.pathname.slice(base.length + 1).split('/')[0] ?? '';
  const tabState = detailTabState(provider, tab, providers.isPending);
  if (tabState === 'loading') {
    return (
      <div className="page">
        <Loading label="Loading provider metadata…" />
      </div>
    );
  }
  if (tabState === 'unsupported') return <Navigate to={base} replace />;
  const can: ClusterContext['can'] = (act, resource = 'clusters') => auth.can(resource, act, clusterObject(namespace, name));
  const suspended = cluster.status.phase === 'Suspended';
  const colocated = colocatedGroups(cluster.status.placement?.groups).length > 0;

  return (
    <div className="page">
      <PageHeader
        crumbs={
          <>
            <Link to="/clusters">Clusters</Link> <span>/</span>{' '}
            <Link to={`/clusters?namespace=${encodeURIComponent(namespace)}`} className="mono">
              {namespace}
            </Link>{' '}
            <span>/</span> <span className="mono">{name}</span>
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
            {topologySummary(cluster)} · running {observedVersion(cluster.status) || 'not yet observed'} · desired {cluster.spec.version}
            {hasCapability(provider, 'fcv') && cluster.status.fcv ? ` · FCV ${cluster.status.fcv}` : ''}
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
                {supportsOperation(provider, 'restart') ? <MenuItem onClick={() => setConfirm('restart')}>Rolling restart…</MenuItem> : null}
                {supportsOperation(provider, 'rotate-tls') ? <MenuItem onClick={() => setConfirm('rotate-tls')} disabled={cluster.spec.tls?.mode === 'disabled'} title={cluster.spec.tls?.mode === 'disabled' ? 'TLS is disabled' : undefined}>
                  Rotate TLS certificates…
                </MenuItem> : null}
                {hasCapability(provider, 'restores') ? <MenuItem onClick={() => navigate(`/clusters/new?namespace=${encodeURIComponent(namespace)}&from=${encodeURIComponent(name)}`)}>Clone into new cluster…</MenuItem> : null}
              </Menu>
            ) : null}
          </>
        }
      />
      {q.isError ? <Callout tone="warn" title="Cached status — latest refresh failed"><ErrorBlock error={q.error} retry={() => q.refetch()} /></Callout> : null}
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
          ...(can('delete') ? [{ to: `${base}/delete`, label: 'Delete' }] : []),
          ...(engine?.detailTabs ?? []).filter((t) => !t.capability || hasCapability(provider, t.capability)).map((t) => ({ to: `${base}/${t.id}`, label: t.label })),
        ].filter((t) => tabAvailable(provider, t.to.slice(base.length + 1)))}
      />
      <Outlet context={{ cluster, provider, namespace, name, base, can } satisfies ClusterContext} />

      <ConfirmDialog
        open={confirm === 'restart'}
        onClose={() => { setConfirm(null); action.reset(); submission.reset(); }}
        onConfirm={() => action.mutateAsync('restart')}
        title="Rolling restart"
        confirmLabel="Restart members"
        busy={action.isPending}
        error={action.error}
      >
        <p className="small">{maintenanceCopy(cluster.spec.provider, cluster.spec.components.engine.replicas)}</p>
      </ConfirmDialog>
      <ConfirmDialog
        open={confirm === 'rotate-tls'}
        onClose={() => { setConfirm(null); action.reset(); submission.reset(); }}
        onConfirm={() => action.mutateAsync('rotate-tls')}
        title="Rotate TLS certificates"
        confirmLabel="Rotate"
        busy={action.isPending}
        error={action.error}
      >
        <p className="small">
          New member and server certificates are issued and rolled out with a rolling restart.
          {cluster.spec.tls?.ca === 'userProvided' ? ' For user-provided TLS, update the PEM inputs under Topology & scaling before rotating.' : ' Clients pinning the CA certificate keep working; the CA itself is retained.'}
        </p>
      </ConfirmDialog>
      <ConfirmDialog
        open={confirm === 'resume'}
        onClose={() => { setConfirm(null); action.reset(); submission.reset(); }}
        onConfirm={() => action.mutateAsync('resume')}
        title="Resume suspended cluster"
        confirmLabel="Resume"
        busy={action.isPending}
        error={action.error}
      >
        <p className="small">Runtime services are recreated on the retained volumes and the cluster is reconciled back to Ready.</p>
      </ConfirmDialog>
    </div>
  );
}

export function EngineDetailTab() {
  const { cluster, base } = useClusterContext();
  const location = useLocation();
  const id = location.pathname.slice(base.length + 1);
  return engines[cluster.spec.provider]?.detailTabs?.find((tab) => tab.id === id)?.element ?? <Navigate to={base} replace />;
}
