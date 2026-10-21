// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { Link } from 'react-router-dom';
import { api, errorMessage, unwrap } from '@/api/client';
import { idempotencyHeaders } from '@/api/idempotency';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { TextField } from '@/components/ui/Field';
import { Callout, Card } from '@/components/ui/Primitives';
import { useClusterContext } from '@/pages/clusters/detail/ClusterDetail';
import { useSubmissionKey } from '@/pages/clusters/detail/use-submission-key';

export function DistributedTopology() {
  const { cluster, namespace, name, can } = useClusterContext();
  const [confirm, setConfirm] = useState(false);
  const [strategy, setStrategy] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [operation, setOperation] = useState<string | null>(null);
  const submission = useSubmissionKey(strategy);
  if (cluster.spec.topology.type !== 'distributed') return null;
  const members = cluster.status.members.filter((m) => m.group !== undefined);
  const groups = [...new Set(members.map((m) => m.group!))].sort((a, b) => a - b);
  const topology = cluster.status.conditions.find((c) => c.type === 'CitusTopology');
  const rebalance = async () => {
    if (busy) return;
    setBusy(true); setError(null);
    try {
      const op = await unwrap(api.POST('/clusters/{cluster}/actions/rebalance', { params: { path: { cluster: name }, query: { namespace } }, headers: idempotencyHeaders(submission.key), body: strategy ? { strategy } : {} }));
      setOperation(op.id); setConfirm(false); submission.reset();
    } catch (e) { setError(errorMessage(e)); } finally { setBusy(false); }
  };
  return <Card title="Distributed groups">
    <div className="stack">
      <Callout tone={topology?.status === 'False' ? 'warn' : 'info'}>{topology?.status === 'False' ? 'Citus metadata drift detected. ' : ''}{topology?.message || topology?.reason || 'Waiting for Citus metadata observation.'}</Callout>
      <div className="table-wrap"><table className="table"><thead><tr><th>Group</th><th>Role</th><th>Leader</th><th>Members</th></tr></thead><tbody>{groups.map((group) => <tr key={group}><td>{group}</td><td>{group === 0 ? 'coordinator' : 'worker'}</td><td>{members.find((m) => m.group === group && m.role === 'primary')?.name ?? 'unknown'}</td><td>{members.filter((m) => m.group === group).map((m) => m.name).join(', ')}</td></tr>)}</tbody></table></div>
      <p className="small muted">Application traffic uses coordinator-only ProxySQL endpoints. Worker groups are not application endpoints.</p>
      {operation ? <p>Rebalance accepted: <Link to={`/operations/${operation}?namespace=${encodeURIComponent(namespace)}`}>{operation}</Link></p> : null}
      <Button disabled={busy || !can('update') || cluster.status.phase !== 'Ready'} onClick={() => { setError(null); setConfirm(true); }}>Rebalance</Button>
      <ConfirmDialog open={confirm} onClose={() => setConfirm(false)} title="Rebalance distributed shards" confirmLabel="Rebalance" busy={busy} error={error} onConfirm={rebalance}>
        <Callout tone="warn">Moves shard placements between worker groups and may increase database load. Track the returned operation; cancellation is cooperative.</Callout>
        <TextField label="Strategy (optional)" value={strategy} onChange={(e) => setStrategy(e.target.value)} hint="Leave empty to use the installed Citus default." />
      </ConfirmDialog>
    </div>
  </Card>;
}
