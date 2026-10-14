// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useMemo, useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { useProviders } from '@/api/hooks';
import { idempotencyHeaders, useIdempotencyKey } from '@/api/idempotency';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { Badge, Callout, Card, ErrorBlock, KV, Loading } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { useClusterContext } from './ClusterDetail';
import { FCV_GUIDANCE, fcvObservation, latestSuccessfulBackup, maintenanceCopy, observedVersion, requireUpgradeBackup } from './detail-state';
import { fetchBackupInventory } from './backup-inventory';

export function UpgradesTab() {
  const { cluster, namespace, name, base, can } = useClusterContext();
  const toast = useToast();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const providers = useProviders();
  const [target, setTarget] = useState<string | null>(null);
  const [fcvTarget, setFcvTarget] = useState<string | null>(null);
  const [restartOpen, setRestartOpen] = useState(false);
  const submission = useIdempotencyKey();
  const backups = useQuery({
    queryKey: keys.clusterSub(namespace, name, 'backups'),
    queryFn: () => fetchBackupInventory((cursor) => unwrap(api.GET('/clusters/{cluster}/backups', { params: { path: { cluster: name }, query: { namespace, limit: 200, cursor } } }))),
    enabled: can('read', 'backups'),
    refetchInterval: 15_000,
  });
  const lastSuccessful = latestSuccessfulBackup(backups.data?.items ?? []);

  const provider = providers.data?.items.find((p) => p.name === cluster.spec.provider);
  const running = observedVersion(cluster.status);
  const current = running && !running.includes(',') ? running : cluster.spec.version;
  const currentMajor = provider?.versions.find((v) => v.version === current)?.major ?? current.split('.').slice(0, 2).join('.');
  const maintenance = maintenanceCopy(cluster.spec.provider, cluster.spec.components.engine.replicas);

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
  const upgrading = cluster.status.phase === 'Upgrading' || (Boolean(running) && cluster.spec.version !== running);
  const fcvBehind = cluster.status.fcv && cluster.status.fcv !== currentMajor;

  const upgrade = useMutation({
    mutationFn: async (version: string) => {
      const path = paths.find((p) => p.versions.some((v) => v.version === version));
      requireUpgradeBackup(Boolean(path?.path.requiresBackup), Boolean(lastSuccessful));
      return unwrap(api.PATCH('/clusters/{cluster}', { headers: idempotencyHeaders(submission.key), params: { path: { cluster: name }, query: { namespace } }, body: { spec: { version } } }));
    },
    onSuccess: (c) => {
      submission.reset();
      queryClient.setQueryData(keys.cluster(namespace, name), c);
      void queryClient.invalidateQueries({ queryKey: keys.clusters });
      toast.success(`Upgrade to ${c.spec.version} requested.`);
      setTarget(null);
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  const raiseFcv = useMutation({
    mutationFn: (version: string) => unwrap(api.POST('/clusters/{cluster}/raise-fcv', { headers: idempotencyHeaders(submission.key), params: { path: { cluster: name }, query: { namespace } }, body: { version } })),
    onSuccess: (op) => {
      submission.reset();
      toast.success(`FCV raise started (operation ${op.id}).`);
      setFcvTarget(null);
      void queryClient.invalidateQueries({ queryKey: keys.cluster(namespace, name) });
      navigate(`${base}/operations`);
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  const restart = useMutation({
    mutationFn: () => unwrap(api.POST('/clusters/{cluster}/restart', { headers: idempotencyHeaders(submission.key), params: { path: { cluster: name }, query: { namespace } } })),
    onSuccess: (op) => {
      submission.reset();
      toast.success(`Rolling restart started (operation ${op.id}).`);
      setRestartOpen(false);
      navigate(`${base}/operations`);
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  if (providers.isPending) return <Loading label="Loading catalog…" />;
  if (providers.error) return <ErrorBlock error={providers.error} retry={() => providers.refetch()} />;

  const targetPath = paths.find((p) => p.versions.some((v) => v.version === target));

  return (
    <div className="stack" style={{ gap: 16 }}>
      {backups.error ? <ErrorBlock error={backups.error} retry={() => backups.refetch()} /> : null}
      <div className="grid-3">
        <Card flush>
          <div className="stat">
            <span className="stat-label">Running version</span>
            <span className="stat-value mono">{running || 'not yet observed'}</span>
            <span className="stat-sub">Desired version: <span className="mono">{cluster.spec.version}</span></span>
          </div>
        </Card>
        <Card flush>
          <div className="stat">
            <span className="stat-label">Feature compatibility (FCV)</span>
            <span className="stat-value mono">{cluster.status.fcv || '—'}</span>
            <span className="stat-sub">{fcvObservation(cluster.status.fcv, running, currentMajor)}</span>
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

      <Callout tone={cluster.spec.components.engine.replicas === 1 ? 'warn' : 'info'} title="Upgrade and restart availability">{maintenance}</Callout>
      {upgrading ? (
        <Callout tone="info" title="Upgrade in progress">
          {maintenance} Watch per-member versions above and the Operations tab.
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
                        {[path.requiresBackup ? lastSuccessful ? 'recent backup required' : can('read', 'backups') ? 'successful backup required — create one first' : 'backup verification required — ask an administrator' : null, path.requiresFCV ? `FCV ≥ ${path.requiresFCV}` : null].filter(Boolean).join(' · ') || '—'}
                      </td>
                      <td>
                        <Badge plain tone={v.status === 'supported' ? 'ok' : 'warn'}>
                          {v.status}
                        </Badge>
                      </td>
                      <td className="actions">
                        <Button size="sm" variant="primary" disabled={!editable || upgrading || (path.requiresBackup && !lastSuccessful)} onClick={() => setTarget(v.version)}>
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
              {FCV_GUIDANCE}
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
            <p className="small muted">{maintenance} Useful after configuration changes applied outside Karakoram or to recycle memory.</p>
            <div>
              <Button disabled={!editable} onClick={() => setRestartOpen(true)}>
                Rolling restart…
              </Button>
            </div>
          </div>
        </Card>
      </div>

      <ConfirmDialog open={target !== null} onClose={() => { setTarget(null); upgrade.reset(); submission.reset(); }} onConfirm={() => target && upgrade.mutateAsync(target)} title={`Upgrade to ${target ?? ''}`} confirmLabel="Start upgrade" busy={upgrade.isPending} error={upgrade.error}>
        <div className="stack">
          <p className="small">
            <span className="mono">{running || 'not yet observed'}</span> → <span className="mono">{target}</span>. {maintenance}
          </p>
          {targetPath?.path.requiresBackup ? (
            <Callout tone="warn">This path requires a recent backup. Last successful backup: {lastSuccessful?.name ?? 'none recorded'}. Confirm it is current enough for recovery before proceeding.</Callout>
          ) : null}
          {targetPath?.path.requiresFCV && cluster.status.fcv && cluster.status.fcv < targetPath.path.requiresFCV ? (
            <Callout tone="danger">FCV {cluster.status.fcv} is below the required {targetPath.path.requiresFCV}. Raise FCV first.</Callout>
          ) : null}
        </div>
      </ConfirmDialog>

      <ConfirmDialog open={fcvTarget !== null} onClose={() => { setFcvTarget(null); raiseFcv.reset(); submission.reset(); }} onConfirm={() => fcvTarget && raiseFcv.mutateAsync(fcvTarget)} title={`Raise FCV to ${fcvTarget ?? ''}`} confirmLabel="Raise FCV" danger typeToConfirm={name} busy={raiseFcv.isPending} error={raiseFcv.error}>
        <Callout tone="warn" title="This cannot be undone">
          Raising FCV enables new on-disk features and cannot be undone through Karakoram. Binary downgrades are not supported. Make sure a backup from after the upgrade exists.
        </Callout>
      </ConfirmDialog>

      <ConfirmDialog open={restartOpen} onClose={() => { setRestartOpen(false); restart.reset(); submission.reset(); }} onConfirm={() => restart.mutateAsync()} title="Rolling restart" confirmLabel="Restart" busy={restart.isPending} error={restart.error}>
        <p className="small">{maintenance}</p>
      </ConfirmDialog>
    </div>
  );
}
