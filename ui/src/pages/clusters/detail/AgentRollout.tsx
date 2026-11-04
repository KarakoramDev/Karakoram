// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { idempotencyHeaders } from '@/api/idempotency';
import { supportsOperation } from '@/engines';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { Switch } from '@/components/ui/Field';
import { Callout } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { OperationNotice } from '@/components/OperationNotice';
import type { ClusterContext } from './ClusterDetail';
import { MaintenanceOverride } from './Maintenance';
import { useSubmissionKey } from './use-submission-key';

export function AgentRollout({ context }: { context: ClusterContext }) {
  const { cluster, provider, namespace, name, base, can } = context;
  const [open, setOpen] = useState(false);
  const [includeCurrent, setIncludeCurrent] = useState(false);
  const [ignoreMaintenanceWindow, setIgnore] = useState(false);
  const submission = useSubmissionKey(JSON.stringify({ includeCurrent, ignoreMaintenanceWindow }));
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const toast = useToast();
  const agents = cluster.status.agents;
  const outdated = agents?.outdated.length ?? 0;
  const unknown = agents?.unknown.length ?? cluster.status.members.filter((m) => !m.agent?.build).length;
  const upgrade = useMutation({
    mutationFn: () => unwrap(api.POST('/clusters/{cluster}/upgrade-agents', { headers: idempotencyHeaders(submission.key), params: { path: { cluster: name }, query: { namespace, ignoreMaintenanceWindow } }, body: { includeCurrent } })),
    onSuccess: (op) => { submission.reset(); setOpen(false); setIncludeCurrent(false); setIgnore(false); void queryClient.invalidateQueries({ queryKey: keys.cluster(namespace, name) }); toast.success(<OperationNotice operationId={op.id}>Agent upgrade queued.</OperationNotice>); navigate(`${base}/operations`); },
  });
  const close = () => { setOpen(false); setIncludeCurrent(false); setIgnore(false); submission.reset(); upgrade.reset(); };
  const allowed = can('update') && supportsOperation(provider, 'upgrade-agents') && !['Suspended', 'Deleting'].includes(cluster.status.phase);
  return <>
    <Callout tone={outdated || unknown ? 'warn' : 'info'} title={outdated ? 'Agent updates available' : unknown || !agents?.targetBuild ? 'Agent build identity unreported' : 'Agents are current'}>
      <div className="stack">
        <p className="small">{outdated} outdated · {unknown} unknown. Target version: <span className="mono">{agents?.targetVersion || 'not reported'}</span>. Agent upgrades retain the engine version and existing volumes.</p>
        {agents?.targetBuild ? <details className="small"><summary>Target build</summary><code style={{ overflowWrap: 'anywhere' }}>{agents.targetBuild}</code></details> : null}
        {allowed ? <div><Button size="sm" onClick={() => setOpen(true)}>Upgrade agents…</Button></div> : null}
      </div>
    </Callout>
    <ConfirmDialog open={open} onClose={close} onConfirm={() => outdated || unknown || includeCurrent ? upgrade.mutateAsync() : false} title="Upgrade cluster agents" confirmLabel="Upgrade agents" busy={upgrade.isPending} error={upgrade.error}>
      <p className="small">Redeploy the current agent executable one member at a time using the engine’s normal disruption and availability gates. Database versions and data volumes are unchanged. Unknown legacy agents are included.</p>
      <Switch label="Include members already on the current build" checked={includeCurrent} onChange={(e) => setIncludeCurrent(e.target.checked)} />
      {!outdated && !unknown && !includeCurrent ? <Callout tone="info">All reported agents are current. Select “Include members already on the current build” to redeploy them.</Callout> : null}
      <MaintenanceOverride cluster={cluster} checked={ignoreMaintenanceWindow} onChange={setIgnore} />
    </ConfirmDialog>
  </>;
}
