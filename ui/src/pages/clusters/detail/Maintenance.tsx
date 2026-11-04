// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { idempotencyHeaders } from '@/api/idempotency';
import { Button } from '@/components/ui/Button';
import { Dialog } from '@/components/ui/Dialog';
import { Switch, TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, ErrorBlock, Loading, Time } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { useClusterContext } from './ClusterDetail';
import { useSubmissionKey } from './use-submission-key';
import { maintenanceErrors, managedDrillTargetLocked } from './ops-state';

type Cluster = Schemas['Cluster'];

export function MaintenanceBadge({ cluster }: { cluster: Cluster }) {
  if (!cluster.spec.maintenance?.windows?.length) return <Badge plain>Maintenance: always open</Badge>;
  const status = cluster.status.maintenance;
  return <Badge tone={!status ? 'neutral' : status.open ? 'ok' : 'warn'} title={status?.open ? `Open until ${status.currentWindowEnd ?? 'not reported'}` : `Next start: ${status?.nextWindowStart ?? 'not reported'}`}>Maintenance: {!status ? 'not observed' : status.open ? 'open' : 'waiting'}</Badge>;
}

export function MaintenanceOverride({ cluster, checked, onChange }: { cluster: Cluster; checked: boolean; onChange: (checked: boolean) => void }) {
  return <div className="stack" style={{ marginTop: 14 }}>
    <p className="small muted">Without an override, new disruptive work waits for an open maintenance window. {cluster.status.maintenance?.nextWindowStart ? <>Next start: <Time iso={cluster.status.maintenance.nextWindowStart} />.</> : null}</p>
    <Switch label="Ignore maintenance window for this action" checked={checked} onChange={(e) => onChange(e.target.checked)} />
    {checked ? <Callout tone="warn">This action may start immediately, outside the configured maintenance windows.</Callout> : null}
  </div>;
}

export function MaintenanceCard() {
  const { cluster, namespace, can } = useClusterContext();
  const [open, setOpen] = useState(false);
  const owner = cluster.managedBy;
  const report = useQuery({
    queryKey: [...keys.clusterSub(namespace, owner?.cluster ?? '', 'backup-drill'), owner?.id ?? ''],
    queryFn: () => unwrap(api.GET('/clusters/{cluster}/backup-drills/{drill}', { params: { path: { cluster: owner?.cluster ?? '', drill: owner?.id ?? '' }, query: { namespace } } })),
    enabled: owner?.kind === 'backup-drill',
    refetchInterval: 10_000,
  });
  const locked = managedDrillTargetLocked(cluster, report.data);
  const editable = can('update') && !locked;
  return <Card title="Maintenance windows" actions={editable ? <Button size="sm" onClick={() => setOpen(true)}>Edit windows…</Button> : null}>
    <div className="stack">
      <div><MaintenanceBadge cluster={cluster} /></div>
      {locked ? <Callout tone="info" title="Controller-managed drill target">Maintenance policy is read-only until the owning drill has a terminal report. Active targets remain controller-owned; missing or inaccessible reports cannot release ownership.</Callout> : null}
      {owner && report.isPending ? <Loading label="Checking drill target ownership…" /> : null}
      {owner && report.error ? <ErrorBlock error={report.error} retry={() => report.refetch()} /> : null}
      <p className="small muted">Disruptive operations and desired-state changes start only during these windows. Healing, backups and restores are not delayed. An in-progress member disruption is never interrupted.</p>
      {cluster.spec.maintenance?.windows?.length ? <ul className="small" style={{ margin: 0, paddingLeft: 18 }}>{cluster.spec.maintenance.windows.map((w, i) => <li key={i}><code>{w.schedule}</code> · {w.duration} · {cluster.spec.maintenance?.timeZone}</li>)}</ul> : <p className="small">No windows configured; disruptive work may start at any time.</p>}
      {cluster.status.maintenance?.currentWindowEnd ? <p className="small">Current window ends <Time iso={cluster.status.maintenance.currentWindowEnd} /></p> : null}
      {cluster.status.maintenance?.nextWindowStart ? <p className="small">Next window starts <Time iso={cluster.status.maintenance.nextWindowStart} /></p> : null}
    </div>
    {open && editable ? <MaintenanceEditor onClose={() => setOpen(false)} /> : null}
  </Card>;
}

function MaintenanceEditor({ onClose }: { onClose: () => void }) {
  const { cluster, namespace, name } = useClusterContext();
  const queryClient = useQueryClient();
  const toast = useToast();
  const [policy, setPolicy] = useState<Schemas['MaintenancePolicy']>(() => cluster.spec.maintenance ?? { timeZone: 'UTC', windows: [] });
  const submission = useSubmissionKey(JSON.stringify(policy));
  const errors = maintenanceErrors(policy);
  const save = useMutation({
    mutationFn: () => unwrap(api.PATCH('/clusters/{cluster}', { headers: { ...idempotencyHeaders(submission.key), 'Content-Type': 'application/merge-patch+json' }, params: { path: { cluster: name }, query: { namespace } }, body: { spec: { maintenance: policy } } })),
    onSuccess: (c) => { queryClient.setQueryData(keys.cluster(namespace, name), c); void queryClient.invalidateQueries({ queryKey: keys.clusters }); toast.success('Maintenance policy updated.'); submission.reset(); onClose(); },
  });
  const windows = policy.windows ?? [];
  return <Dialog open onClose={onClose} title="Edit maintenance windows" busy={save.isPending} error={save.error} onSubmit={() => errors.length ? false : save.mutateAsync()} footer={<><Button onClick={onClose}>Cancel</Button><Button type="submit" variant="primary" disabled={errors.length > 0} loading={save.isPending}>Save windows</Button></>}>
    <div className="stack">
      <TextField label="Time zone" value={policy.timeZone} placeholder="UTC" hint="IANA name, for example Europe/Prague. Window start times use this zone, including daylight saving changes." onChange={(e) => setPolicy({ ...policy, timeZone: e.target.value })} />
      {windows.map((window, index) => <div className="stack" key={index} style={{ borderTop: '1px solid var(--border)', paddingTop: 12 }}>
        <div className="grid-2"><TextField label={`Window ${index + 1} start (cron)`} mono value={window.schedule} placeholder="0 2 * * 0" onChange={(e) => setPolicy({ ...policy, windows: windows.map((w, i) => i === index ? { ...w, schedule: e.target.value } : w) })} /><TextField label="Duration" value={window.duration} placeholder="2h" hint="15m to 168h" onChange={(e) => setPolicy({ ...policy, windows: windows.map((w, i) => i === index ? { ...w, duration: e.target.value } : w) })} /></div>
        <div><Button size="sm" onClick={() => setPolicy({ ...policy, windows: windows.filter((_, i) => i !== index) })}>Remove window {index + 1}</Button></div>
      </div>)}
      <div><Button disabled={windows.length >= 16} onClick={() => setPolicy({ ...policy, windows: [...windows, { schedule: '0 2 * * 0', duration: '2h' }] })}>+ Add window</Button></div>
      {!windows.length ? <Callout tone="info">No windows means always open.</Callout> : null}
      {errors.length ? <span className="field-error">{errors[0]}</span> : null}
    </div>
  </Dialog>;
}
