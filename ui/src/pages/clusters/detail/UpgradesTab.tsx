// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { useProviders } from '@/api/hooks';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { Badge, Callout, Card, ErrorBlock, KV, Loading } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { useClusterContext } from './ClusterDetail';

export function UpgradesTab() {
  const { cluster, name, can } = useClusterContext();
  const toast = useToast();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const providers = useProviders();
  const [target, setTarget] = useState<string | null>(null);
  const [fcvTarget, setFcvTarget] = useState<string | null>(null);
  const [restartOpen, setRestartOpen] = useState(false);

  const provider = providers.data?.items.find((p) => p.name === cluster.spec.provider);
  const current = cluster.status.version ?? cluster.spec.version;
  const currentMajor = provider?.versions.find((v) => v.version === current)?.major ?? current.split('.').slice(0, 2).join('.');

  const paths = useMemo(() => {
    if (!provider) return [];
    return provider.upgradePaths
      .filter((p) => p.from === current || p.from === currentMajor || p.from === `${currentMajor}.x` || p.from === '*')
      .map((p) => {
        const versions = provider.versions.filter((v) => (v.version === p.to || v.major === p.to) && v.status !== 'unavailable' && v.version !== current);
        return { path: p, versions };
      })
      .filter((x) => x.versions.length > 0);
  }, [provider, current, currentMajor]);

  const editable = can('update') && (cluster.status.phase === 'Ready' || cluster.status.phase === 'Degraded');
  const upgrading = cluster.spec.version !== current;
  const fcvBehind = cluster.status.fcv && cluster.status.fcv !== currentMajor;

  const upgrade = useMutation({
    mutationFn: (version: string) => unwrap(api.PATCH('/clusters/{cluster}', { params: { path: { cluster: name } }, body: { spec: { version } } })),
    onSuccess: (c) => {
      queryClient.setQueryData(keys.cluster(name), c);
      void queryClient.invalidateQueries({ queryKey: keys.clusters });
      toast.success(`Upgrade to ${c.spec.version} requested.`);
      setTarget(null);
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  const raiseFcv = useMutation({
    mutationFn: (version: string) => unwrap(api.POST('/clusters/{cluster}/raise-fcv', { params: { path: { cluster: name } }, body: { version } })),
    onSuccess: (op) => {
      toast.success(`FCV raise started (operation ${op.id}).`);
      setFcvTarget(null);
      void queryClient.invalidateQueries({ queryKey: keys.cluster(name) });
      navigate(`/clusters/${encodeURIComponent(name)}/operations`);
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  const restart = useMutation({
    mutationFn: () => unwrap(api.POST('/clusters/{cluster}/restart', { params: { path: { cluster: name } } })),
    onSuccess: (op) => {
      toast.success(`Rolling restart started (operation ${op.id}).`);
      setRestartOpen(false);
      navigate(`/clusters/${encodeURIComponent(name)}/operations`);
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  if (providers.isPending) return <Loading label="Loading catalog…" />;
  if (providers.error) return <ErrorBlock error={providers.error} retry={() => providers.refetch()} />;

  const targetPath = paths.find((p) => p.versions.some((v) => v.version === target));

  return (
    <div className="stack" style={{ gap: 16 }}>
      <div className="grid-3">
        <Card flush>
          <div className="stat">
            <span className="stat-label">Running version</span>
            <span className="stat-value mono">{current}</span>
            <span className="stat-sub">{upgrading ? `upgrading to ${cluster.spec.version}` : 'matches desired version'}</span>
          </div>
        </Card>
        <Card flush>
          <div className="stat">
            <span className="stat-label">Feature compatibility (FCV)</span>
            <span className="stat-value mono">{cluster.status.fcv ?? '—'}</span>
            <span className="stat-sub">{fcvBehind ? `behind binary major ${currentMajor}` : 'aligned with binary version'}</span>
          </div>
        </Card>
        <Card flush>
          <div className="stat">
            <span className="stat-label">Per-member versions</span>
            <span className="stat-value" style={{ fontSize: 'var(--fs-md)' }}>
              {Array.from(new Set(cluster.status.members.map((m) => m.version ?? '?'))).join(', ') || '—'}
            </span>
            <span className="stat-sub">{cluster.status.members.length} members</span>
          </div>
        </Card>
      </div>

      {upgrading ? (
        <Callout tone="info" title="Upgrade in progress">
          Members are upgraded one by one: secondaries first, then the primary after a step-down. Watch per-member versions above and the Operations tab.
        </Callout>
      ) : null}

      <Card title="Available upgrades" flush>
        {paths.length === 0 ? (
          <div className="table-empty">No upgrade path from {current} in the catalog.</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Target</th>
                  <th>Type</th>
                  <th>Requirements</th>
                  <th>Status</th>
                  <th className="actions" />
                </tr>
              </thead>
              <tbody>
                {paths.flatMap(({ path, versions }) =>
                  versions.map((v) => (
                    <tr key={v.version}>
                      <td className="mono">
                        {v.version}
                        {v.default ? <Badge plain tone="accent">default</Badge> : null}
                      </td>
                      <td>{v.major === currentMajor ? 'Minor (rolling)' : 'Major (rolling, FCV stays until raised)'}</td>
                      <td className="small">
                        {[path.requiresBackup ? 'recent backup required' : null, path.requiresFCV ? `FCV ≥ ${path.requiresFCV}` : null].filter(Boolean).join(' · ') || '—'}
                      </td>
                      <td>
                        <Badge plain tone={v.status === 'supported' ? 'ok' : 'warn'}>
                          {v.status}
                        </Badge>
                      </td>
                      <td className="actions">
                        <Button size="sm" variant="primary" disabled={!editable || upgrading} onClick={() => setTarget(v.version)}>
                          Upgrade…
                        </Button>
                      </td>
                    </tr>
                  )),
                )}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      <div className="grid-2">
        <Card title="Feature compatibility version">
          <div className="stack">
            <p className="small muted">
              After a major upgrade the FCV stays at the previous major so you can roll back. Raising it enables new on-disk features and makes the upgrade permanent. Confirm backups are current first.
            </p>
            <KV items={[['Current FCV', cluster.status.fcv ?? '—'], ['Binary major', currentMajor]]} />
            <div>
              <Button disabled={!editable || !fcvBehind || upgrading} onClick={() => setFcvTarget(currentMajor)}>
                Raise FCV to {currentMajor}…
              </Button>
            </div>
          </div>
        </Card>
        <Card title="Rolling restart">
          <div className="stack">
            <p className="small muted">Restart every member in turn without losing quorum. Useful after configuration changes applied outside Karakoram or to recycle memory.</p>
            <div>
              <Button disabled={!editable} onClick={() => setRestartOpen(true)}>
                Rolling restart…
              </Button>
            </div>
          </div>
        </Card>
      </div>

      <ConfirmDialog open={target !== null} onClose={() => setTarget(null)} onConfirm={() => target && upgrade.mutate(target)} title={`Upgrade to ${target ?? ''}`} confirmLabel="Start upgrade" busy={upgrade.isPending}>
        <div className="stack">
          <p className="small">
            <span className="mono">{current}</span> → <span className="mono">{target}</span>. Members are upgraded one at a time with a short primary failover at the end.
          </p>
          {targetPath?.path.requiresBackup ? (
            <Callout tone="warn">This path requires a recent backup. Last backup: {cluster.status.backup?.lastBackup ?? 'none recorded'}.</Callout>
          ) : null}
          {targetPath?.path.requiresFCV && cluster.status.fcv && cluster.status.fcv < targetPath.path.requiresFCV ? (
            <Callout tone="danger">FCV {cluster.status.fcv} is below the required {targetPath.path.requiresFCV}. Raise FCV first.</Callout>
          ) : null}
        </div>
      </ConfirmDialog>

      <ConfirmDialog open={fcvTarget !== null} onClose={() => setFcvTarget(null)} onConfirm={() => fcvTarget && raiseFcv.mutate(fcvTarget)} title={`Raise FCV to ${fcvTarget ?? ''}`} confirmLabel="Raise FCV" danger typeToConfirm={name} busy={raiseFcv.isPending}>
        <Callout tone="warn" title="This cannot be undone">
          Downgrading the engine below {fcvTarget} becomes impossible once the FCV is raised. Make sure a backup from after the upgrade exists.
        </Callout>
      </ConfirmDialog>

      <ConfirmDialog open={restartOpen} onClose={() => setRestartOpen(false)} onConfirm={() => restart.mutate()} title="Rolling restart" confirmLabel="Restart" busy={restart.isPending}>
        <p className="small">Secondaries restart first; the primary steps down and restarts last. Expect a brief write pause during the election.</p>
      </ConfirmDialog>
    </div>
  );
}
