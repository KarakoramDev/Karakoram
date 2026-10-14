// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useMemo, useRef, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { useBackupStorages, useProviders } from '@/api/hooks';
import { idempotencyHeaders } from '@/api/idempotency';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog, Dialog } from '@/components/ui/Dialog';
import { ChoiceGroup, SelectField, Switch, TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, ErrorBlock, Loading, Menu, MenuItem, Time, formatBytes, formatTime } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { OperationNotice } from '@/components/OperationNotice';
import { SchedulesEditor } from '../editors/SchedulesEditor';
import { validateSchedules } from '../wizard/schedules';
import { BackupPhaseBadge } from '../cluster-utils';
import { useClusterContext } from './ClusterDetail';
import { latestSuccessfulBackup } from './detail-state';
import { fetchBackupInventory } from './backup-inventory';
import { useSubmissionKey } from './use-submission-key';

type BackupType = Schemas['Provider']['backupTypes'][number];

export function BackupsTab() {
  const { cluster, namespace, name, base, can } = useClusterContext();
  const toast = useToast();
  const navigate = useNavigate();
  const queryClient = useQueryClient();
  const storages = useBackupStorages(namespace);
  const providers = useProviders();
  const backups = useQuery({
    queryKey: keys.clusterSub(namespace, name, 'backups'),
    queryFn: () => fetchBackupInventory((cursor) => unwrap(api.GET('/clusters/{cluster}/backups', { params: { path: { cluster: name }, query: { namespace, limit: 200, cursor } } }))),
    refetchInterval: 15_000,
  });
  const pitr = useQuery({
    queryKey: keys.clusterSub(namespace, name, 'pitr'),
    queryFn: () => unwrap(api.GET('/clusters/{cluster}/pitr', { params: { path: { cluster: name }, query: { namespace } } })),
    refetchInterval: 60_000,
  });

  const [createOpen, setCreateOpen] = useState(false);
  const [deleteName, setDeleteName] = useState<string | null>(null);

  const provider = providers.data?.items.find((p) => p.name === cluster.spec.provider);
  const backupTypes: BackupType[] = provider?.backupTypes ?? ['logical', 'physical'];
  const storageNames = storages.data?.items.map((s) => s.name) ?? [];
  const canCreate = can('create', 'backups');
  const canDelete = can('delete', 'backups');
  const canRestore = can('create', 'restores');

  const del = useMutation({
    mutationFn: (backup: string) => unwrap(api.DELETE('/clusters/{cluster}/backups/{backup}', { params: { path: { cluster: name, backup }, query: { namespace } } })),
    onSuccess: () => {
      toast.success('Backup deletion queued.');
      setDeleteName(null);
      void queryClient.invalidateQueries({ queryKey: keys.clusterSub(namespace, name, 'backups') });
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  const win = pitr.data ?? cluster.status.backup?.pitrWindow;
  const lastSuccessful = latestSuccessfulBackup(backups.data?.items ?? []);

  return (
    <div className="stack" style={{ gap: 16 }}>
      <div className="grid-3">
        <Card flush>
          <div className="stat">
            <span className="stat-label">Primary storage</span>
            <span className="stat-value" style={{ fontSize: 'var(--fs-lg)' }}>
              {cluster.spec.backup?.storage ?? '—'}
            </span>
            <span className="stat-sub">{cluster.spec.backup?.storage ? '' : 'configure below to enable schedules and PITR'}</span>
          </div>
        </Card>
        <Card flush>
          <div className="stat">
            <span className="stat-label">Last successful backup</span>
            <span className="stat-value" style={{ fontSize: 'var(--fs-lg)' }}>
              {lastSuccessful?.name ?? '—'}
            </span>
            {lastSuccessful ? <span className="stat-sub"><Time iso={lastSuccessful.completedAt || lastSuccessful.startedAt} /></span> : null}
          </div>
        </Card>
        <Card flush>
          <div className="stat">
            <span className="stat-label">PITR window</span>
            <span className="stat-value" style={{ fontSize: 'var(--fs-lg)' }}>
              {win?.available && win.start && win.end ? `${formatTime(win.start)} → ${formatTime(win.end)}` : cluster.spec.backup?.pitr?.enabled ? 'not yet available' : 'PITR disabled'}
            </span>
            <span className="stat-sub">{win?.message ?? (win?.available ? 'continuous, PBM-verified' : '')}</span>
          </div>
        </Card>
      </div>

      <Card
        title="Backups"
        flush
        actions={
          canCreate ? (
            <Button variant="primary" size="sm" onClick={() => setCreateOpen(true)} disabled={storageNames.length === 0}>
              + Backup now
            </Button>
          ) : null
        }
      >
        {backups.isPending ? (
          <Loading />
        ) : backups.error ? (
          <div className="card-body">
            <ErrorBlock error={backups.error} retry={() => backups.refetch()} />
          </div>
        ) : backups.data.items.length === 0 ? (
          <div className="table-empty">No backups yet.{storageNames.length === 0 ? ' Add a backup storage in Settings first.' : ''}</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Phase</th>
                  <th>Type</th>
                  <th>Trigger</th>
                  <th>Storage</th>
                  <th>Started</th>
                  <th>Duration</th>
                  <th className="num">Size</th>
                  <th>Consistent to</th>
                  <th className="actions" />
                </tr>
              </thead>
              <tbody>
                {backups.data.items.map((b) => (
                  <tr key={b.name} id={`backup-${b.name}`}>
                    <td className="mono">
                      {b.name}
                      {b.error ? (
                        <div className="xs" style={{ color: 'var(--danger)' }}>
                          {b.error}
                        </div>
                      ) : null}
                    </td>
                    <td>
                      <BackupPhaseBadge phase={b.phase} />
                    </td>
                    <td>{b.type}</td>
                    <td className="small nowrap">
                      {b.trigger}
                      {b.schedule ? <span className="faint"> · {b.schedule}</span> : null}
                    </td>
                    <td className="small">
                      {b.storage}
                      <details>
                        <summary>Backup details</summary>
                        <div>Storage: <span className="mono">{b.storage}</span></div>
                        {b.pbmName ? <div>Native backup identifier: <span className="mono">{b.pbmName}</span></div> : null}
                        <div>Completed: <Time iso={b.completedAt} /></div>
                      </details>
                    </td>
                    <td className="small">
                      <Time iso={b.startedAt} />
                    </td>
                    <td className="small">{b.startedAt && b.completedAt ? `${Math.round((new Date(b.completedAt).getTime() - new Date(b.startedAt).getTime()) / 1000)}s` : '—'}</td>
                    <td className="num small nowrap">{formatBytes(b.size)}</td>
                    <td className="small nowrap">{formatTime(b.lastWriteTime)}</td>
                    <td className="actions">
                      {canRestore || canDelete || can('create') ? (
                        <Menu>
                          <MenuItem disabled={!canRestore || b.phase !== 'Succeeded'} onClick={() => navigate(`${base}/restores?backup=${encodeURIComponent(b.name)}`)}>
                            Restore in place…
                          </MenuItem>
                          <MenuItem disabled={!can('create') || b.phase !== 'Succeeded'} onClick={() => navigate(`/clusters/new?namespace=${encodeURIComponent(namespace)}&from=${encodeURIComponent(name)}&backup=${encodeURIComponent(b.name)}`)}>
                            Restore into new cluster…
                          </MenuItem>
                          <MenuItem danger disabled={!canDelete || b.phase === 'Deleting' || b.phase === 'Running'} onClick={() => setDeleteName(b.name)}>
                            Delete…
                          </MenuItem>
                        </Menu>
                      ) : null}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      <BackupPolicyCard storageNames={storageNames} backupTypes={backupTypes} />

      {createOpen ? <CreateBackupDialog namespace={namespace} cluster={name} storages={storageNames} defaultStorage={cluster.spec.backup?.storage} backupTypes={backupTypes} onClose={() => setCreateOpen(false)} /> : null}

      <ConfirmDialog open={deleteName !== null} onClose={() => { setDeleteName(null); del.reset(); }} onConfirm={() => deleteName && del.mutateAsync(deleteName)} title={`Delete backup ${deleteName ?? ''}`} confirmLabel="Delete" danger busy={del.isPending} error={del.error}>
        <p className="small">Backup data is removed from object storage. If PITR depends on this base backup, the restorable window shrinks accordingly.</p>
      </ConfirmDialog>
    </div>
  );
}

function CreateBackupDialog({ namespace, cluster, storages, defaultStorage, backupTypes, onClose }: { namespace: string; cluster: string; storages: string[]; defaultStorage?: string; backupTypes: BackupType[]; onClose: () => void }) {
  const toast = useToast();
  const queryClient = useQueryClient();
  const [name, setName] = useState(() => `manual-${new Date().toISOString().slice(0, 16).replace(/[-:T]/g, '')}`);
  const [storage, setStorage] = useState(defaultStorage ?? storages[0] ?? '');
  const [type, setType] = useState<BackupType>(backupTypes[0] ?? 'logical');
  const submission = useSubmissionKey(JSON.stringify({ name, storage, type }));
  const create = useMutation({
    mutationFn: () => unwrap(api.POST('/clusters/{cluster}/backups', { headers: idempotencyHeaders(submission.key), params: { path: { cluster }, query: { namespace } }, body: { name, storage, type } })),
    onSuccess: (b) => {
      submission.reset();
      toast.success(<OperationNotice operationId={b.operationId}>Backup {b.name} queued.</OperationNotice>);
      void queryClient.invalidateQueries({ queryKey: keys.clusterSub(namespace, cluster, 'backups') });
      onClose();
    },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const valid = /^[a-z0-9][a-z0-9-]{0,62}$/.test(name) && storage;
  return (
    <Dialog
      open
      onClose={onClose}
      title="Create backup"
      busy={create.isPending}
      error={create.error}
      onSubmit={() => valid && create.mutateAsync()}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={create.isPending} disabled={!valid}>
            Start backup
          </Button>
        </>
      }
    >
      <TextField label="Name" mono value={name} onChange={(e) => setName(e.target.value)} required autoFocus error={name && !/^[a-z0-9][a-z0-9-]{0,62}$/.test(name) ? 'Lowercase letters, digits and dashes.' : undefined} />
      <ChoiceGroup<BackupType>
        name="backup-type"
        legend="Type"
        value={type}
        onChange={setType}
        options={backupTypes.map((t) => ({
          value: t,
          title: t === 'logical' ? 'Logical' : 'Physical',
          description: t === 'logical' ? 'Dump via the wire protocol; portable, slower for large data.' : 'Copies data files; fast for large datasets, same engine major required on restore.',
        }))}
      />
      <SelectField label="Storage" value={storage} onChange={(e) => setStorage(e.target.value)} required>
        {storages.map((s) => (
          <option key={s} value={s}>
            {s}
          </option>
        ))}
      </SelectField>
    </Dialog>
  );
}

function BackupPolicyCard({ storageNames, backupTypes }: { storageNames: string[]; backupTypes: BackupType[] }) {
  const { cluster, namespace, name, can } = useClusterContext();
  const toast = useToast();
  const queryClient = useQueryClient();
  const persisted = useMemo(() => cluster.spec.backup ?? {}, [cluster.spec.backup]);
  const [storage, setStorage] = useState(persisted.storage ?? '');
  const [schedules, setSchedules] = useState(persisted.schedules ?? []);
  const [pitrEnabled, setPitrEnabled] = useState(persisted.pitr?.enabled ?? false);
  const [span, setSpan] = useState(persisted.pitr?.oplogSpanMinutes ?? 10);
  const persistedKey = JSON.stringify(persisted);
  const [seed, setSeed] = useState(persistedKey);
  const submission = useSubmissionKey(JSON.stringify({ storage, schedules, pitrEnabled, span }));
  const saving = useRef(false);
  useEffect(() => {
    if (persistedKey !== seed) {
      setStorage(persisted.storage ?? '');
      setSchedules(persisted.schedules ?? []);
      setPitrEnabled(persisted.pitr?.enabled ?? false);
      setSpan(persisted.pitr?.oplogSpanMinutes ?? 10);
      setSeed(persistedKey);
    }
  }, [persistedKey, seed, persisted]);

  const draft: Schemas['BackupSpec'] = { ...(storage ? { storage } : {}), schedules, pitr: { enabled: pitrEnabled, oplogSpanMinutes: span } };
  const dirty = storage !== (persisted.storage ?? '') || JSON.stringify(schedules) !== JSON.stringify(persisted.schedules ?? []) || pitrEnabled !== (persisted.pitr?.enabled ?? false) || span !== (persisted.pitr?.oplogSpanMinutes ?? 10);
  const errors = validateSchedules(schedules);
  if ((schedules.length || pitrEnabled) && !storage) errors.unshift('Choose a primary storage.');
  const editable = can('update');

  const save = useMutation({
    mutationFn: () =>
      unwrap(
        api.PATCH('/clusters/{cluster}', {
          headers: idempotencyHeaders(submission.key),
          params: { path: { cluster: name }, query: { namespace } },
          body: { spec: { backup: { storage: storage || null, schedules, pitr: { enabled: pitrEnabled, oplogSpanMinutes: span } } } },
        }),
      ),
    onSuccess: (c) => {
      submission.reset();
      queryClient.setQueryData(keys.cluster(namespace, name), c);
      toast.success(<OperationNotice operationId={c.operationId}>Backup policy updated.</OperationNotice>);
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  return (
    <Card
      title="Backup policy"
      actions={
        editable ? (
          <>
            {dirty ? (
              <Button
                size="sm"
                disabled={save.isPending}
                onClick={() => {
                  save.reset();
                  submission.reset();
                  setStorage(persisted.storage ?? '');
                  setSchedules(persisted.schedules ?? []);
                  setPitrEnabled(persisted.pitr?.enabled ?? false);
                  setSpan(persisted.pitr?.oplogSpanMinutes ?? 10);
                }}
              >
                Discard
              </Button>
            ) : null}
            <Button size="sm" variant="primary" disabled={!dirty || errors.length > 0} loading={save.isPending} onClick={() => {
              if (saving.current) return;
              saving.current = true;
              void save.mutateAsync().finally(() => { saving.current = false; }).catch(() => {});
            }}>
              Save policy
            </Button>
          </>
        ) : null
      }
    >
      {!editable ? <Callout tone="info">Backup policy is read-only. You need the cluster update permission to save changes.</Callout> : null}
      {save.error ? <ErrorBlock error={save.error} /> : null}
      <fieldset disabled={!editable || save.isPending} style={{ border: 0, padding: 0, margin: 0 }} className="stack">
        {storageNames.length === 0 ? (
          <Callout tone="warn">
            No backup storages exist in namespace <span className="mono">{namespace}</span>. <Link to={`/settings/backup-storages?namespace=${encodeURIComponent(namespace)}`}>Create one in Settings</Link>.
          </Callout>
        ) : null}
        <div className="grid-3" style={{ alignItems: 'end' }}>
          <SelectField label="Primary storage" value={storage} onChange={(e) => setStorage(e.target.value)}>
            <option value="">None</option>
            {storageNames.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </SelectField>
          <div className="field">
            <span className="field-label">Point-in-time recovery</span>
            <div style={{ height: 34, display: 'flex', alignItems: 'center' }}>
              <Switch label={pitrEnabled ? 'Enabled' : 'Disabled'} checked={pitrEnabled} disabled={!storage} onChange={(e) => setPitrEnabled(e.target.checked)} />
            </div>
          </div>
          <TextField label="Oplog slice (minutes)" type="number" min={1} value={span} disabled={!pitrEnabled} onChange={(e) => setSpan(Math.max(1, Number(e.target.value) || 10))} />
        </div>
        <div className="section-title">Schedules</div>
        <SchedulesEditor value={schedules} onChange={setSchedules} storages={storageNames} defaultStorage={storage || undefined} backupTypes={backupTypes} />
        {errors.length && dirty ? <span className="field-error">{errors[0]}</span> : null}
        {draft.pitr?.enabled && !persisted.pitr?.enabled ? <Badge plain tone="info">PITR starts after the next successful base backup.</Badge> : null}
      </fieldset>
    </Card>
  );
}
