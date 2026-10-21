// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { Link } from 'react-router-dom';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { clusterPath } from '@/pages/clusters/cluster-path';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { Badge, Callout, Card, ErrorBlock, KV, Loading } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { useClusterContext } from '@/pages/clusters/detail/ClusterDetail';
import { bindingStatus, formatRetention, readOptions } from './spec';

type Namespace = Schemas['TemporalNamespace'];

/** Temporal's own namespace; never offered for deletion. */
const SYSTEM_NAMESPACE = 'temporal-system';

export function deletable(namespace: Namespace): boolean {
  return !namespace.managed && namespace.name !== 'default' && namespace.name !== SYSTEM_NAMESPACE;
}

function stateTone(state: string): 'ok' | 'warn' | 'danger' | 'neutral' {
  const lower = state.toLowerCase();
  if (lower === 'registered') return 'ok';
  if (lower === 'deleted') return 'danger';
  if (lower === 'deprecated' || lower === 'handover') return 'warn';
  return 'neutral';
}

export function NamespacesTab() {
  const { cluster, namespace, name, base, can } = useClusterContext();
  const toast = useToast();
  const queryClient = useQueryClient();
  const namespaces = useQuery({
    queryKey: keys.clusterSub(namespace, name, 'temporal-namespaces'),
    queryFn: () => unwrap(api.GET('/clusters/{cluster}/temporal/namespaces', { params: { path: { cluster: name }, query: { namespace } } })),
    refetchInterval: 30_000,
  });
  const [target, setTarget] = useState<Namespace | null>(null);
  const remove = useMutation({
    mutationFn: (n: Namespace) => unwrap(api.DELETE('/clusters/{cluster}/temporal/namespaces/{temporalNamespace}', { params: { path: { cluster: name, temporalNamespace: n.name }, query: { namespace } } })),
    onSuccess: (_, n) => {
      toast.success(`Deletion of namespace ${n.name} requested; Temporal removes its workflows asynchronously.`);
      setTarget(null);
      void queryClient.invalidateQueries({ queryKey: keys.clusterSub(namespace, name, 'temporal-namespaces') });
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  const binding = bindingStatus(cluster.status.conditions);
  const declared = readOptions(cluster.spec).namespaces;
  const items = namespaces.data?.items ?? [];

  return (
    <div className="stack" style={{ gap: 16 }}>
      <Card title="PostgreSQL persistence">
        {!binding ? (
          <p className="small muted" style={{ margin: 0 }}>The binding has not been reported yet.</p>
        ) : binding.waiting ? (
          <Callout tone="warn" title="Waiting for the bound PostgreSQL source">{binding.message ?? 'The source cluster is not ready; Temporal components start once the binding resolves.'}</Callout>
        ) : (
          <KV
            items={[
              ['Source cluster', binding.sourceCluster ? <Link className="mono" to={clusterPath(binding.sourceNamespace ?? namespace, binding.sourceCluster)}>{binding.sourceNamespace ?? namespace}/{binding.sourceCluster}</Link> : undefined],
              ['Sharing', binding.shared === undefined ? undefined : binding.shared ? <span><Badge tone="warn">Shared</Badge> <span className="small muted">{binding.consumerCount ?? 'several'} consumers use this source; restoring it rolls back every consumer.</span></span> : <Badge tone="ok">Dedicated</Badge>],
              ['Frontend', cluster.status.connection?.uri ? <span className="mono">{cluster.status.connection.uri}</span> : undefined],
            ]}
          />
        )}
      </Card>

      <Card title="Namespaces" flush actions={<Button size="sm" onClick={() => namespaces.refetch()} loading={namespaces.isFetching && !namespaces.isPending}>Refresh</Button>}>
        {namespaces.isPending ? (
          <Loading />
        ) : namespaces.error ? (
          <div className="card-body stack">
            <ErrorBlock error={namespaces.error} retry={() => namespaces.refetch()} />
            {declared.length ? <p className="small muted" style={{ margin: 0 }}>Declared in the spec: {declared.map((n) => `${n.name} (${n.retention})`).join(', ')}; plus default (72h).</p> : null}
          </div>
        ) : items.length === 0 ? (
          <div className="table-empty">No namespaces reported yet. The default namespace appears once a frontend is ready.</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Retention</th>
                  <th>State</th>
                  <th>Managed</th>
                  <th>Description</th>
                  <th className="actions">Action</th>
                </tr>
              </thead>
              <tbody>
                {items.map((n) => (
                  <tr key={n.name}>
                    <td className="mono">{n.name}</td>
                    <td className="small">{formatRetention(n.retention)}</td>
                    <td>
                      <Badge tone={stateTone(n.state)}>{n.state}</Badge>
                    </td>
                    <td>{n.managed ? <Badge tone="info" title="Declared in engineOptions.namespaces or the always-present default">Managed</Badge> : <Badge plain>Unmanaged</Badge>}</td>
                    <td className="small">{n.description || <span className="faint">—</span>}</td>
                    <td className="actions">
                      {can('update') && deletable(n) ? (
                        <Button size="sm" variant="danger" onClick={() => setTarget(n)}>
                          Delete…
                        </Button>
                      ) : null}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
      <p className="xs faint" style={{ margin: 0 }}>
        Managed namespaces are declared under Temporal options on the <Link to={`${base}/topology`}>Topology & scaling</Link> tab; remove the declaration there before deleting one. Namespaces created directly through the Temporal API are unmanaged.
      </p>

      <ConfirmDialog
        open={target !== null}
        onClose={() => { setTarget(null); remove.reset(); }}
        onConfirm={() => target && remove.mutateAsync(target)}
        title={`Delete namespace ${target?.name ?? ''}`}
        confirmLabel="Delete namespace"
        danger
        typeToConfirm={target?.name}
        busy={remove.isPending}
        error={remove.error}
      >
        <Callout tone="danger" title="Every workflow in this namespace is deleted">
          Temporal marks <span className="mono">{target?.name}</span> as deleted immediately and then removes all of its workflow executions, histories and visibility records asynchronously. Running workflows are terminated. This cannot be undone and is not covered by a Temporal backup taken afterwards.
        </Callout>
      </ConfirmDialog>
    </div>
  );
}
