// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { Link, useSearchParams } from 'react-router-dom';
import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { idempotencyHeaders } from '@/api/idempotency';
import { hasCapability, supportsOperation } from '@/engines';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog, Dialog } from '@/components/ui/Dialog';
import { SelectField, Switch, TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, ErrorBlock, KV, Loading, Time, formatDuration } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { clusterPath } from '../cluster-path';
import { useClusterContext } from './ClusterDetail';
import { useSubmissionKey } from './use-submission-key';
import { drillPolicyErrors, eligibleDrillBackups } from './ops-state';

type Drill = Schemas['BackupDrill'];

function DrillPhase({ phase }: { phase: Drill['phase'] }) {
  return <Badge tone={phase === 'Succeeded' ? 'ok' : phase === 'Failed' ? 'danger' : 'progress'}>{phase}</Badge>;
}

export function BackupDrills({ backups }: { backups: Schemas['Backup'][] }) {
  const { cluster, provider, namespace, name, can } = useClusterContext();
  const [params, setParams] = useSearchParams();
  const selected = params.get('drill');
  const [runOpen, setRunOpen] = useState(false);
  const [policyOpen, setPolicyOpen] = useState(false);
  const [backup, setBackup] = useState('');
  const toast = useToast();
  const queryClient = useQueryClient();
  const key = keys.clusterSub(namespace, name, 'backup-drills');
  const readable = can('read', 'restores');
  const runnable = can('create', 'restores') && hasCapability(provider, 'restores') && supportsOperation(provider, 'restore') && !cluster.managedBy && !['Suspended', 'Deleting'].includes(cluster.status.phase);
  const reports = useInfiniteQuery({
    queryKey: key,
    initialPageParam: undefined as string | undefined,
    queryFn: ({ pageParam }) => unwrap(api.GET('/clusters/{cluster}/backup-drills', { params: { path: { cluster: name }, query: { namespace, limit: 50, cursor: pageParam } } })),
    getNextPageParam: (page) => page.nextCursor || undefined,
    refetchInterval: 10_000,
    enabled: readable,
  });
  const submission = useSubmissionKey(JSON.stringify({ backup }));
  const run = useMutation({
    mutationFn: () => {
      if (backup && !eligibleDrillBackups(backups, cluster.spec.backup?.drill?.backupSchedule).some((b) => b.name === backup)) throw new Error('The selected backup no longer matches the drill policy. Choose an eligible backup or automatic selection.');
      return unwrap(api.POST('/clusters/{cluster}/backup-drills', { headers: idempotencyHeaders(submission.key), params: { path: { cluster: name }, query: { namespace } }, body: backup ? { backup } : {} }));
    },
    onSuccess: (drill) => { submission.reset(); setRunOpen(false); void queryClient.invalidateQueries({ queryKey: key }); toast.success('Backup drill queued.'); if (readable) { const next = new URLSearchParams(params); next.set('drill', drill.id); setParams(next); } },
  });
  const items = readable ? reports.data?.pages.flatMap((page) => page.items) ?? [] : [];
  const policy = cluster.spec.backup?.drill;
  const view = (id: string) => { const next = new URLSearchParams(params); next.set('drill', id); setParams(next); };
  const closeReport = () => { const next = new URLSearchParams(params); next.delete('drill'); setParams(next); };
  return <>
    <Card title="Drills" flush actions={<>{can('update') && !cluster.managedBy ? <Button size="sm" onClick={() => setPolicyOpen(true)}>Drill policy…</Button> : null}{runnable ? <Button size="sm" variant="primary" onClick={() => { setBackup(''); setRunOpen(true); }}>Run drill…</Button> : null}</>}>
      <div className="card-body stack">
        <p className="small muted">Prove recovery by restoring a successful backup into a temporary, same-namespace cluster. Reports remain after the target is cleaned up; the source cluster is not changed.</p>
        <p className="small">Scheduled drills: <strong>{policy?.enabled ? 'enabled' : 'disabled'}</strong>{policy?.enabled ? <> · <code>{policy.schedule}</code> UTC · timeout {policy.timeout}{policy.backupSchedule ? ` · backups from ${policy.backupSchedule}` : ' · all backup schedules'}</> : null}</p>
        {cluster.spec.provider === 'rabbitmq' ? <Callout tone="warn">RabbitMQ drills verify definitions only, not messages.</Callout> : null}
        {!readable ? <Callout tone="info">You need restore read permission to view drill reports.</Callout> : null}
        {readable && reports.error ? <ErrorBlock error={reports.error} retry={() => reports.refetch()} /> : null}
      </div>
      {readable && reports.isPending ? <Loading label="Loading drills…" /> : null}
      {readable && !reports.isPending && !reports.error && !items.length ? <div className="table-empty">No drill reports yet. Run a drill to test recovery.</div> : null}
      {items.length ? <div className="table-wrap"><table className="table"><thead><tr><th>Report</th><th>Phase</th><th>Backup</th><th>Trigger</th><th>Created</th><th>Restore time</th><th>Data age</th><th>Cleanup</th></tr></thead><tbody>{items.map((drill) => <tr key={drill.id}>
        <td><Button variant="ghost" size="sm" onClick={() => view(drill.id)}><span className="mono">{drill.id}</span></Button>{drill.error ? <div className="xs" style={{ color: 'var(--danger)' }}>{drill.error}</div> : null}</td><td><DrillPhase phase={drill.phase} /></td><td className="mono small">{drill.backup || 'selecting backup'}</td><td>{drill.trigger}</td><td className="small"><Time iso={drill.createdAt} /></td><td>{formatDuration(drill.restoreDurationSeconds)}</td><td>{formatDuration(drill.dataAgeSeconds)}</td><td><Badge tone={drill.cleanup?.phase === 'Failed' ? 'danger' : drill.cleanup?.phase === 'Done' ? 'ok' : 'neutral'}>{drill.cleanup?.phase || 'Pending'}</Badge></td>
      </tr>)}</tbody></table></div> : null}
      {readable && reports.hasNextPage ? <div className="card-body"><Button loading={reports.isFetchingNextPage} onClick={() => reports.fetchNextPage()}>Load more reports</Button></div> : null}
    </Card>
    <ConfirmDialog open={runOpen} onClose={() => { setRunOpen(false); run.reset(); submission.reset(); }} onConfirm={() => run.mutateAsync()} title="Run backup drill" confirmLabel="Run drill" busy={run.isPending} error={run.error}>
      <div className="stack"><Callout tone="info">This creates billable temporary resources using normal provisioning. The target is deleted after verification; failed cleanup and orphaned volumes are recorded in the report.</Callout><SelectField label="Backup" value={backup} onChange={(e) => setBackup(e.target.value)}><option value="">Latest successful eligible backup</option>{eligibleDrillBackups(backups, policy?.backupSchedule).map((b) => <option key={b.name} value={b.name}>{b.name} · {b.type}</option>)}</SelectField><p className="small muted">Timeout: {policy?.timeout || '6h'}. {policy?.backupSchedule ? `Both explicit and automatic selection are restricted to backups from schedule ${policy.backupSchedule}.` : 'Choose a successful backup, or automatically select the latest eligible successful backup.'}</p></div>
    </ConfirmDialog>
    {selected && readable ? <DrillReport id={selected} onClose={closeReport} /> : null}
    {policyOpen ? <DrillPolicyEditor onClose={() => setPolicyOpen(false)} /> : null}
  </>;
}

function DrillReport({ id, onClose }: { id: string; onClose: () => void }) {
  const { namespace, name } = useClusterContext();
  const report = useQuery({ queryKey: [...keys.clusterSub(namespace, name, 'backup-drill'), id], queryFn: () => unwrap(api.GET('/clusters/{cluster}/backup-drills/{drill}', { params: { path: { cluster: name, drill: id }, query: { namespace } } })), refetchInterval: 5_000 });
  const drill = report.data;
  return <Dialog open wide onClose={onClose} title={`Drill report ${id}`} footer={<Button onClick={onClose}>Close</Button>}>
    {report.isPending ? <Loading label="Loading drill report…" /> : null}
    {report.error ? <ErrorBlock error={report.error} retry={() => report.refetch()} /> : null}
    {drill ? <div className="stack">
      <div><DrillPhase phase={drill.phase} /></div>
      {drill.error ? <Callout tone="danger" title="Drill failed">{drill.error}</Callout> : null}
      {drill.limitations ? <Callout tone="warn" title="Verification limitations">{drill.limitations}</Callout> : null}
      <KV items={[
        ['Backup', <span className="mono">{drill.backup || 'not selected'}</span>], ['Backup type', drill.backupType || '—'], ['Trigger', drill.trigger],
        ['Temporary target', drill.targetCluster ? drill.cleanup?.phase === 'Done' ? <span className="mono">{drill.targetCluster} (removed)</span> : <Link to={clusterPath(namespace, drill.targetCluster)} onClick={onClose}>{drill.targetCluster}</Link> : 'not provisioned'],
        ['Created', <Time iso={drill.createdAt} />], ['Started', <Time iso={drill.startedAt} />], ['Ready', <Time iso={drill.readyAt} />], ['Completed', <Time iso={drill.completedAt} />],
        ['Restore duration', formatDuration(drill.restoreDurationSeconds)], ['Total duration', formatDuration(drill.totalDurationSeconds)], ['Backup data age at start', formatDuration(drill.dataAgeSeconds)], ['Cleanup', drill.cleanup?.phase || 'Pending'],
      ]} />
      <h4>Verification checks</h4>
      {drill.checks?.length ? <ul style={{ margin: 0, paddingLeft: 18 }}>{drill.checks.map((check, i) => <li key={`${check.name}-${i}`} className="small"><Badge tone={check.passed ? 'ok' : 'danger'}>{check.passed ? 'Passed' : 'Failed'}</Badge> {check.name}{check.message ? ` — ${check.message}` : ''}</li>)}</ul> : <p className="small muted">No checks recorded yet.</p>}
      {drill.cleanup?.orphanedVolumes.length ? <Callout tone="danger" title="Orphaned volumes require attention"><ul style={{ margin: 0, paddingLeft: 18 }}>{drill.cleanup.orphanedVolumes.map((volume) => <li className="mono small" key={volume}>{volume}</li>)}</ul></Callout> : null}
    </div> : null}
  </Dialog>;
}

function DrillPolicyEditor({ onClose }: { onClose: () => void }) {
  const { cluster, namespace, name } = useClusterContext();
  const [policy, setPolicy] = useState<Schemas['BackupDrillPolicy']>(() => cluster.spec.backup?.drill ?? { enabled: false, schedule: '0 4 * * 0', timeout: '6h' });
  const queryClient = useQueryClient();
  const toast = useToast();
  const submission = useSubmissionKey(JSON.stringify(policy));
  const errors = drillPolicyErrors(policy);
  const save = useMutation({
    mutationFn: () => unwrap(api.PATCH('/clusters/{cluster}', { headers: { ...idempotencyHeaders(submission.key), 'Content-Type': 'application/merge-patch+json' }, params: { path: { cluster: name }, query: { namespace } }, body: { spec: { backup: { drill: { ...policy, backupSchedule: policy.backupSchedule || null } } } } })),
    onSuccess: (c) => { queryClient.setQueryData(keys.cluster(namespace, name), c); void queryClient.invalidateQueries({ queryKey: keys.clusters }); submission.reset(); toast.success('Drill policy updated.'); onClose(); },
  });
  return <Dialog open onClose={onClose} title="Backup drill policy" busy={save.isPending} error={save.error} onSubmit={() => errors.length ? false : save.mutateAsync()} footer={<><Button onClick={onClose}>Cancel</Button><Button type="submit" variant="primary" loading={save.isPending} disabled={errors.length > 0}>Save policy</Button></>}>
    <div className="stack">
      <Switch label="Run scheduled recovery drills" checked={policy.enabled} onChange={(e) => setPolicy({ ...policy, enabled: e.target.checked })} />
      <TextField label="Schedule (UTC)" mono value={policy.schedule} placeholder="0 4 * * 0" hint="Five-field cron: minute, hour, day of month, month, day of week." onChange={(e) => setPolicy({ ...policy, schedule: e.target.value })} />
      <TextField label="Timeout" value={policy.timeout} hint="30m to 48h, including provisioning, restore and verification." onChange={(e) => setPolicy({ ...policy, timeout: e.target.value })} />
      <SelectField label="Backup schedule filter" value={policy.backupSchedule || ''} onChange={(e) => setPolicy({ ...policy, backupSchedule: e.target.value })}><option value="">All schedules and manual backups</option>{policy.backupSchedule && !cluster.spec.backup?.schedules?.some((s) => s.name === policy.backupSchedule) ? <option value={policy.backupSchedule}>{policy.backupSchedule} (not configured)</option> : null}{cluster.spec.backup?.schedules?.map((schedule) => <option key={schedule.name} value={schedule.name}>{schedule.name}</option>)}</SelectField>
      <Callout tone="info">Each scheduled drill restores the latest eligible successful backup into a temporary cluster in this namespace. It does not change the source cluster.</Callout>
      {errors.length ? <span className="field-error">{errors[0]}</span> : null}
    </div>
  </Dialog>;
}
