// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { idempotencyHeaders } from '@/api/idempotency';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { Callout, Card } from '@/components/ui/Primitives';
import { OperationNotice } from '@/components/OperationNotice';
import { useToast } from '@/components/ui/Toast';
import { supportsOperation } from '../index';
import { useClusterContext } from '@/pages/clusters/detail/ClusterDetail';
import { useSubmissionKey } from '@/pages/clusters/detail/use-submission-key';

export const QUORUM_CONFIRMATION = 'all-absent-members-fenced';
export function RecoveryTab() {
  const { cluster, provider, namespace, name, base, can } = useClusterContext();
  const [open, setOpen] = useState(false);
  const submission = useSubmissionKey(JSON.stringify({ namespace, name, generation: cluster.generation }));
  const client = useQueryClient();
  const navigate = useNavigate();
  const toast = useToast();
  const allowed = cluster.spec.provider === 'pxc' && supportsOperation(provider, 'recover-quorum');
  const editable = allowed && can('update') && ['Degraded', 'Error'].includes(cluster.status.phase);
  const recover = useMutation({
    mutationFn: () => unwrap(api.POST('/clusters/{cluster}/mysql/recover-quorum', {
      headers: idempotencyHeaders(submission.key),
      params: { path: { cluster: name }, query: { namespace } },
      body: { confirmation: QUORUM_CONFIRMATION },
    })),
    onSuccess: (result) => {
      submission.reset();
      setOpen(false);
      void client.invalidateQueries({ queryKey: keys.cluster(namespace, name) });
      toast.success(<OperationNotice operationId={result.id}>PXC quorum recovery requested.</OperationNotice>);
      navigate(`${base}/operations`);
    },
  });
  if (!allowed) return <Callout>Quorum recovery is available only for a PXC provider that advertises recover-quorum.</Callout>;
  return <div className="stack">
    <Callout tone="danger" title="Never bootstrap an unfenced partition">Forcing a Primary component can create split brain and conflicting committed histories. This is explicit minority recovery, not a restart, full-outage recovery or a way to bypass missing data evidence.</Callout>
    <Card title="Recover PXC quorum">
      <div className="stack">
        <p>Use only after confirming quorum loss and isolating application traffic. The server requires a common Galera state UUID, chooses the survivor with the highest committed position, and refuses recovery if a Primary component already exists.</p>
        <p>The server fences the exact missing owned members and waits for durable fencing completion before authorizing one survivor. Missing or conflicting evidence blocks recovery. Do not manually bootstrap another member while this operation is pending.</p>
        <p className="small muted">For a full-cluster outage, inspect Overview conditions and the automatic recovered-position comparison. Backup restore creates a new Galera history and belongs in Restores instead.</p>
        {!can('update') ? <Callout>Recovery requires cluster update permission.</Callout> : !editable ? <Callout>Explicit minority recovery can be requested only while the cluster is Degraded or Error. The backend independently validates quorum and fencing.</Callout> : null}
        <div><Button variant="danger" disabled={!editable || recover.isPending} onClick={() => { recover.reset(); setOpen(true); }}>Recover quorum…</Button></div>
      </div>
    </Card>
    <ConfirmDialog open={open} onClose={() => { setOpen(false); recover.reset(); }} title="Authorize fenced minority recovery" danger confirmLabel="Fence missing members and recover quorum" typeToConfirm={QUORUM_CONFIRMATION} busy={recover.isPending} error={recover.error} onConfirm={() => editable ? recover.mutateAsync() : false}>
      <Callout tone="danger">Confirm that all absent members must be fenced before bootstrap. You authorize the server to stop the missing owned members and recover one verified survivor. This cannot safely merge two divergent Primary histories and does not promise that an ambiguous client commit failed.</Callout>
      <p className="small">After acceptance, follow the existing operation, Galera eligibility, member count and application data. A timeout does not cancel recovery; do not submit a second bootstrap.</p>
    </ConfirmDialog>
  </div>;
}
