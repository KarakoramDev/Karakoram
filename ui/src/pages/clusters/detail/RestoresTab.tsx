// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useState } from 'react';
import { useNavigate, useSearchParams } from 'react-router-dom';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { Button, LinkButton } from '@/components/ui/Button';
import { Dialog } from '@/components/ui/Dialog';
import { ChoiceGroup, TextField } from '@/components/ui/Field';
import { Callout, Card, ErrorBlock, Loading, Time, formatTime } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { BackupPicker, PitrPicker } from '../ClusterWizard';
import { OpPhaseBadge } from '../cluster-utils';
import { useClusterContext } from './ClusterDetail';

export function RestoresTab() {
  const { cluster, name, can } = useClusterContext();
  const [params, setParams] = useSearchParams();
  const restores = useQuery({
    queryKey: keys.clusterSub(name, 'restores'),
    queryFn: () => unwrap(api.GET('/clusters/{cluster}/restores', { params: { path: { cluster: name }, query: { limit: 200 } } })),
    refetchInterval: 15_000,
  });
  const [open, setOpen] = useState(params.has('backup') || params.has('pitr'));
  const canCreate = can('create', 'restores');
  const inFlight = restores.data?.items.some((r) => r.phase === 'Pending' || r.phase === 'Running');

  return (
    <div className="stack" style={{ gap: 16 }}>
      <div className="row-between wrap">
        <p className="small muted">
          In-place restores replace the data of <span className="mono">{name}</span>. To keep this cluster untouched, restore into a new cluster instead.
        </p>
        <div className="row">
          {can('create') ? (
            <LinkButton to={`/clusters/new?from=${encodeURIComponent(name)}`} size="md">
              Restore into new cluster
            </LinkButton>
          ) : null}
          {canCreate ? (
            <Button variant="primary" onClick={() => setOpen(true)} disabled={cluster.status.phase === 'Restoring' || inFlight}>
              Restore in place…
            </Button>
          ) : null}
        </div>
      </div>

      <Card title="Restore history" flush>
        {restores.isPending ? (
          <Loading />
        ) : restores.error ? (
          <div className="card-body">
            <ErrorBlock error={restores.error} retry={() => restores.refetch()} />
          </div>
        ) : restores.data.items.length === 0 ? (
          <div className="table-empty">No restores have been performed on this cluster.</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Phase</th>
                  <th>Source</th>
                  <th>Started</th>
                  <th>Completed</th>
                  <th>Error</th>
                </tr>
              </thead>
              <tbody>
                {restores.data.items.map((r) => (
                  <tr key={r.name}>
                    <td className="mono">{r.name}</td>
                    <td>
                      <OpPhaseBadge phase={r.phase} />
                    </td>
                    <td className="small">
                      {r.source.sourceCluster !== name ? <span className="mono">{r.source.sourceCluster} · </span> : null}
                      {r.source.backup ? <span className="mono">backup {r.source.backup}</span> : <span>point in time {formatTime(r.source.pitrTime)}</span>}
                    </td>
                    <td className="small">
                      <Time iso={r.startedAt} />
                    </td>
                    <td className="small">
                      <Time iso={r.completedAt} />
                    </td>
                    <td className="small" style={{ color: 'var(--danger)' }}>
                      {r.error ?? ''}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {open ? (
        <RestoreDialog
          cluster={name}
          initialBackup={params.get('backup') ?? undefined}
          onClose={() => {
            setOpen(false);
            if (params.has('backup') || params.has('pitr')) setParams({}, { replace: true });
          }}
        />
      ) : null}
    </div>
  );
}

function RestoreDialog({ cluster, initialBackup, onClose }: { cluster: string; initialBackup?: string; onClose: () => void }) {
  const toast = useToast();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const [mode, setMode] = useState<'backup' | 'pitr'>(initialBackup ? 'backup' : 'backup');
  const [backup, setBackup] = useState(initialBackup ?? '');
  const [pitrTime, setPitrTime] = useState('');
  const [name, setName] = useState(() => `restore-${new Date().toISOString().slice(0, 16).replace(/[-:T]/g, '')}`);
  useEffect(() => {
    if (initialBackup) setBackup(initialBackup);
  }, [initialBackup]);

  const create = useMutation({
    mutationFn: () =>
      unwrap(
        api.POST('/clusters/{cluster}/restores', {
          params: { path: { cluster } },
          body: { name, source: { sourceCluster: cluster, ...(mode === 'backup' ? { backup } : { pitrTime }) } },
        }),
      ),
    onSuccess: (r) => {
      toast.success(`Restore ${r.name} started.`);
      void queryClient.invalidateQueries({ queryKey: keys.cluster(cluster) });
      onClose();
      navigate(`/clusters/${encodeURIComponent(cluster)}/operations`);
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  const valid = /^[a-z0-9][a-z0-9-]{0,62}$/.test(name) && (mode === 'backup' ? backup !== '' : pitrTime !== '');

  return (
    <Dialog
      open
      onClose={onClose}
      title="Restore in place"
      wide
      onSubmit={() => valid && create.mutate()}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="danger" loading={create.isPending} disabled={!valid}>
            Restore and overwrite data
          </Button>
        </>
      }
    >
      <Callout tone="danger" title="Current data will be replaced">
        The cluster becomes unavailable while the restore runs. Members are stopped (physical) or data is dropped and reloaded (logical). Take a fresh backup first if you may need the current state.
      </Callout>
      <TextField label="Restore name" mono value={name} onChange={(e) => setName(e.target.value)} required />
      <ChoiceGroup<'backup' | 'pitr'>
        name="restore-mode"
        legend="Restore point"
        value={mode}
        onChange={setMode}
        options={[
          { value: 'backup', title: 'From backup', description: 'Exact state of a completed backup.' },
          { value: 'pitr', title: 'Point in time', description: 'Any instant within the PITR window.' },
        ]}
      />
      {mode === 'backup' ? <BackupPicker cluster={cluster} value={backup} onChange={setBackup} /> : <PitrPicker cluster={cluster} value={pitrTime} onChange={setPitrTime} />}
    </Dialog>
  );
}
