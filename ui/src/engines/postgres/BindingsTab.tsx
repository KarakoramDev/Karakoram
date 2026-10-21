// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { idempotencyHeaders } from '@/api/idempotency';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { Callout, Card, ErrorBlock, Loading } from '@/components/ui/Primitives';
import { useClusterContext } from '@/pages/clusters/detail/ClusterDetail';
import { useSubmissionKey } from '@/pages/clusters/detail/use-submission-key';

export function PostgresBindingsTab() {
  const { namespace, name, can } = useClusterContext();
  const client = useQueryClient();
  const [selected, setSelected] = useState<Schemas['PostgresBinding'] | null>(null);
  const submission = useSubmissionKey(selected?.id ?? '');
  const key = keys.clusterSub(namespace, name, 'postgres-bindings');
  const bindings = useQuery({ queryKey: key, queryFn: () => unwrap(api.GET('/clusters/{cluster}/postgres/bindings', { params: { path: { cluster: name }, query: { namespace } } })), enabled: can('read', 'database-users'), refetchInterval: 15_000 });
  const rotate = useMutation({
    mutationFn: (binding: string) => unwrap(api.POST('/clusters/{cluster}/postgres/bindings/{binding}/rotate', { headers: idempotencyHeaders(submission.key), params: { path: { cluster: name, binding }, query: { namespace } } })),
    onSuccess: () => { submission.reset(); setSelected(null); void client.invalidateQueries({ queryKey: key }); },
  });
  if (!can('read', 'database-users')) return <Callout>Reading bindings requires database-users read permission.</Callout>;
  return <div className="stack">
    <Callout>These managed consumers depend on this source. Delete consumers before deleting their source. Credentials are never displayed.</Callout>
    <Card title="Bindings" flush>
      {bindings.isPending ? <Loading /> : bindings.error ? <ErrorBlock error={bindings.error} retry={() => bindings.refetch()} /> : !bindings.data.items.length ? <div className="table-empty">No inbound bindings.</div> : <div className="table-wrap"><table className="table">
        <thead><tr><th>Consumer</th><th>Databases / extensions</th><th>Role</th><th>Supported majors</th><th>Generation / state</th><th>Actions</th></tr></thead>
        <tbody>{bindings.data.items.map((b) => <tr key={b.id}>
          <td>{b.consumer.kind}/{b.consumer.namespace}/{b.consumer.name}</td>
          <td>{b.databases.join(', ')}<br /><span className="muted">{b.extensions.join(', ') || 'No extensions'}</span></td>
          <td className="mono">{b.role}</td><td>{b.minMajor ?? 'any'}–{b.maxMajor ?? 'any'}</td><td>{b.credentialGeneration} / {b.state}</td>
          <td><Button size="sm" disabled={!can('update', 'database-users')} onClick={() => { rotate.reset(); setSelected(b); }}>Rotate…</Button></td>
        </tr>)}</tbody>
      </table></div>}
    </Card>
    <ConfirmDialog open={selected !== null} onClose={() => setSelected(null)} title="Rotate binding credentials" confirmLabel="Rotate credentials" busy={rotate.isPending} error={rotate.error} onConfirm={() => selected ? rotate.mutateAsync(selected.id).then(() => {}) : Promise.resolve()}>
      The old password will stop working. The managed consumer refreshes its configuration on reconciliation; existing connections may be interrupted.
    </ConfirmDialog>
  </div>;
}
