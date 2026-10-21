// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, unwrap, type Schemas } from '@/api/client';
import { idempotencyHeaders, useIdempotencyKey } from '@/api/idempotency';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog, Dialog } from '@/components/ui/Dialog';
import { Switch, TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, Empty, ErrorBlock, formatBytes, KV, Loading, Time } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';

type Policy = Schemas['ControlPlaneBackupPolicy'];
type Backup = Schemas['ControlPlaneBackup'];

export function ControlPlaneBackupPage() {
  const auth = useAuth();
  const readable = auth.can('settings', 'read');
  const editable = auth.can('settings', 'update');
  const queryClient = useQueryClient();
  const toast = useToast();
  const attempt = useIdempotencyKey();
  const [editing, setEditing] = useState(false);
  const [running, setRunning] = useState(false);
  const [accepted, setAccepted] = useState<Backup | null>(null);
  const policy = useQuery({ queryKey: keys.controlPlaneBackupPolicy, queryFn: () => unwrap(api.GET('/control-plane/backup-policy')), enabled: readable });
  const history = useInfiniteQuery({
    queryKey: keys.controlPlaneBackups, enabled: readable,
    initialPageParam: undefined as string | undefined,
    queryFn: ({ pageParam }) => unwrap(api.GET('/control-plane/backups', { params: { query: { limit: 50, cursor: pageParam } } })),
    getNextPageParam: (page) => page.nextCursor || undefined,
    refetchInterval: 5_000,
  });
  const run = useMutation({
    mutationFn: () => unwrap(api.POST('/control-plane/backups', { headers: idempotencyHeaders(attempt.key) })),
    onSuccess: (backup) => {
      setAccepted(backup);
      setRunning(false);
      attempt.reset();
      void queryClient.invalidateQueries({ queryKey: keys.controlPlaneBackups });
      toast.success('Control-plane backup queued. Follow its status in backup history.');
    },
  });
  if (!readable) return <Callout>You do not have permission to view settings.</Callout>;
  const items = history.data?.pages.flatMap((page) => page.items) ?? [];
  const active = items.some((backup) => backup.phase === 'Pending' || backup.phase === 'Running');
  const acceptedReport = accepted ? items.find((backup) => backup.id === accepted.id) ?? accepted : null;
  return <>
    <Card title="Control-plane backup" actions={editable && policy.data ? <Button size="sm" onClick={() => setEditing(true)}>Edit policy</Button> : null}>
      <p className="small muted">Protect the installation state, including configuration, encrypted secrets and PKI. Database cluster backups are managed separately.</p>
      {policy.error ? <ErrorBlock error={policy.error} retry={() => policy.refetch()} /> : null}
      {policy.isPending ? <Loading /> : policy.data ? <KV items={[
        ['Scheduled backups', <Badge tone={policy.data.enabled ? 'ok' : 'neutral'}>{policy.data.enabled ? 'Enabled' : 'Disabled'}</Badge>],
        ['Storage', policy.data.storage.name ? `${policy.data.storage.namespace}/${policy.data.storage.name}` : 'Not configured'],
        ['Schedule (UTC)', <code>{policy.data.schedule}</code>],
        ['Retention', `${policy.data.retainCount} successful backups`],
      ]} /> : null}
      <Callout tone="warn" title="Keep the master key separately">Restoring this backup requires the same operator master key. Backend tokens and endpoint configuration supplied outside the database must also be restored separately.</Callout>
    </Card>
    <Card title="Backup history" actions={editable ? <Button size="sm" variant="primary" disabled={!policy.data?.storage.name || active || history.isPending || Boolean(history.error)} onClick={() => { run.reset(); attempt.reset(); setRunning(true); }}>Run backup</Button> : null}>
      <p className="small muted">Manual backups use the saved storage and retention policy, even when scheduling is disabled. Pending and running reports refresh every five seconds.</p>
      {acceptedReport ? <Callout tone={acceptedReport.phase === 'Failed' ? 'danger' : acceptedReport.phase === 'Succeeded' ? 'ok' : 'info'} title={`Backup ${acceptedReport.phase.toLowerCase()}`}><span className="mono">{acceptedReport.id}</span>{acceptedReport.error ? <p>{acceptedReport.error}</p> : null}</Callout> : null}
      {history.error ? <ErrorBlock error={history.error} retry={() => history.refetch()} /> : null}
      {history.isPending ? <Loading /> : !items.length && !history.error ? <Empty title="No control-plane backups">Save an S3 storage in the policy, then run a backup or enable its schedule.</Empty> : <div className="stack">
        {items.map((backup) => <details key={backup.id} className="card">
          <summary style={{ padding: 12, cursor: 'pointer' }}><span className="row wrap" style={{ display: 'inline-flex' }}><span className="mono small">{backup.id}</span><Badge tone={backup.phase === 'Succeeded' ? 'ok' : backup.phase === 'Failed' ? 'danger' : 'progress'}>{backup.phase}</Badge><span className="small muted">{backup.trigger} · <Time iso={backup.createdAt} /></span></span></summary>
          <div className="card-body"><BackupReport backup={backup} /></div>
        </details>)}
      </div>}
      {history.hasNextPage ? <Button loading={history.isFetchingNextPage} onClick={() => history.fetchNextPage()}>Load older backups</Button> : null}
    </Card>
    {editing && policy.data ? <BackupPolicyDialog policy={policy.data} onClose={() => setEditing(false)} /> : null}
    <ConfirmDialog open={running} onClose={() => setRunning(false)} title="Run control-plane backup" confirmLabel="Run backup" busy={run.isPending} error={run.error} onConfirm={() => run.mutateAsync()}>
      <p>Create a snapshot in <strong>{policy.data?.storage.namespace}/{policy.data?.storage.name}</strong> using the saved policy. Acceptance means queued, not completed.</p>
    </ConfirmDialog>
  </>;
}

export function BackupReport({ backup }: { backup: Backup }) {
  return <div className="stack">
    {backup.error ? <Callout tone="danger" title="Backup failed">{backup.error}</Callout> : null}
    <KV items={[
      ['Status', backup.phase], ['Trigger', backup.trigger], ['Storage', `${backup.storage.namespace}/${backup.storage.name}`],
      ['Created', <Time iso={backup.createdAt} />], ['Started', <Time iso={backup.startedAt} />], ['Completed', <Time iso={backup.completedAt} />],
      ['Object key', <span className="mono" style={{ overflowWrap: 'anywhere' }}>{backup.objectKey || '—'}</span>],
      ['Size', formatBytes(backup.sizeBytes)], ['SHA-256', <span className="mono" style={{ overflowWrap: 'anywhere' }}>{backup.sha256 || '—'}</span>],
      ['Schema version', backup.schemaVersion], ['Karakoram version', backup.karakoramVersion],
      ['Master-key fingerprint', <span className="mono" style={{ overflowWrap: 'anywhere' }}>{backup.keyFingerprint || '—'}</span>],
    ]} />
  </div>;
}

export function BackupPolicyDialog({ policy, onClose }: { policy: Policy; onClose: () => void }) {
  const queryClient = useQueryClient();
  const toast = useToast();
  const attempt = useIdempotencyKey();
  const [form, setForm] = useState({ enabled: policy.enabled, namespace: policy.storage.namespace || 'default', storage: policy.storage.name, schedule: policy.schedule, retainCount: String(policy.retainCount) });
  const save = useMutation({
    mutationFn: () => unwrap(api.PUT('/control-plane/backup-policy', { headers: idempotencyHeaders(attempt.key), body: {
      enabled: form.enabled, storage: { namespace: form.namespace.trim(), name: form.storage.trim() }, schedule: form.schedule.trim(), retainCount: Number(form.retainCount),
    } })),
    onSuccess: (saved) => {
      queryClient.setQueryData(keys.controlPlaneBackupPolicy, saved);
      void queryClient.invalidateQueries({ queryKey: keys.controlPlaneBackupPolicy });
      toast.success('Control-plane backup policy saved.');
      onClose();
    },
  });
  const set = (patch: Partial<typeof form>) => { attempt.reset(); save.reset(); setForm((current) => ({ ...current, ...patch })); };
  const valid = form.namespace.trim() && form.storage.trim() && form.schedule.trim() && Number.isInteger(Number(form.retainCount)) && Number(form.retainCount) >= 1 && Number(form.retainCount) <= 1000;
  return <Dialog open title="Edit control-plane backup policy" onClose={onClose} busy={save.isPending} error={save.error} onSubmit={() => valid ? save.mutateAsync() : false} footer={<><Button onClick={onClose}>Cancel</Button><Button variant="primary" type="submit" disabled={!valid} loading={save.isPending}>Save policy</Button></>}>
    <div className="stack">
      <Switch label="Enable scheduled backups" checked={form.enabled} onChange={(e) => set({ enabled: e.target.checked })} />
      <p className="small muted">Use an existing S3 backup storage. Its credentials remain in Backup storages and are never read back here.</p>
      <div className="grid-2"><TextField label="Storage namespace" required mono value={form.namespace} onChange={(e) => set({ namespace: e.target.value })} /><TextField label="Storage name" required mono value={form.storage} onChange={(e) => set({ storage: e.target.value })} /></div>
      <TextField label="Schedule (UTC)" required mono value={form.schedule} onChange={(e) => set({ schedule: e.target.value })} hint="Five-field cron expression; default 0 3 * * * runs daily at 03:00 UTC. Validated by the server." />
      <TextField label="Retain successful backups" required type="number" min={1} max={1000} step={1} value={form.retainCount} onChange={(e) => set({ retainCount: e.target.value })} hint="1–1000. Older successful objects from this installation are removed according to this policy." />
    </div>
  </Dialog>;
}
