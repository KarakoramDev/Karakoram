// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState, type ReactNode } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { Badge, Callout, Card, ErrorBlock, Loading } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { useClusterContext } from '@/pages/clusters/detail/ClusterDetail';
import { AccessEditor, type AccessEdit, type AccessKind, SYSTEM_USERS, SYSTEM_USER_EXPLANATION } from './AccessEditor';

export default function RabbitMQAccessTab() {
  const { namespace, name: cluster, can } = useClusterContext();
  const queryClient = useQueryClient();
  const toast = useToast();
  const [editor, setEditor] = useState<AccessEdit | null>(null);
  const [remove, setRemove] = useState<AccessEdit | null>(null);
  const allowed = can('read', 'database-users');
  const queryKey = keys.clusterSub(namespace, cluster, 'rabbitmq-access');
  const params = { path: { cluster }, query: { namespace } };
  const vhosts = useQuery({ queryKey: [...queryKey, 'vhosts'], queryFn: () => unwrap(api.GET('/clusters/{cluster}/rabbitmq/vhosts', { params })), enabled: allowed });
  const users = useQuery({ queryKey: [...queryKey, 'users'], queryFn: () => unwrap(api.GET('/clusters/{cluster}/rabbitmq/users', { params })), enabled: allowed });
  const permissions = useQuery({ queryKey: [...queryKey, 'permissions'], queryFn: () => unwrap(api.GET('/clusters/{cluster}/rabbitmq/permissions', { params })), enabled: allowed });
  const topics = useQuery({ queryKey: [...queryKey, 'topic-permissions'], queryFn: () => unwrap(api.GET('/clusters/{cluster}/rabbitmq/topic-permissions', { params })), enabled: allowed });
  const policies = useQuery({ queryKey: [...queryKey, 'policies'], queryFn: () => unwrap(api.GET('/clusters/{cluster}/rabbitmq/policies', { params })), enabled: allowed });
  const operatorPolicies = useQuery({ queryKey: [...queryKey, 'operator-policies'], queryFn: () => unwrap(api.GET('/clusters/{cluster}/rabbitmq/operator-policies', { params })), enabled: allowed });
  const invalidate = () => { void queryClient.invalidateQueries({ queryKey }); };

  const deletion = useMutation({
    mutationFn: async (edit: AccessEdit) => {
      const item = edit.existing;
      if (!item) throw new Error('Choose an existing RabbitMQ object.');
      switch (edit.kind) {
        case 'vhost':
          return unwrap(api.DELETE('/clusters/{cluster}/rabbitmq/vhosts/{vhost}', { params: { path: { cluster, vhost: edit.existing!.name }, query: { namespace } } }));
        case 'user':
          return unwrap(api.DELETE('/clusters/{cluster}/rabbitmq/users/{user}', { params: { path: { cluster, user: edit.existing!.name }, query: { namespace } } }));
        case 'permission':
          return unwrap(api.DELETE('/clusters/{cluster}/rabbitmq/permissions/{vhost}/{user}', { params: { path: { cluster, vhost: edit.existing!.vhost, user: edit.existing!.user }, query: { namespace } } }));
        case 'topic':
          return unwrap(api.DELETE('/clusters/{cluster}/rabbitmq/topic-permissions/{vhost}/{user}', { params: { path: { cluster, vhost: edit.existing!.vhost, user: edit.existing!.user }, query: { namespace } } }));
        case 'policy':
          return unwrap(api.DELETE('/clusters/{cluster}/rabbitmq/policies/{vhost}/{name}', { params: { path: { cluster, vhost: edit.existing!.vhost, name: edit.existing!.name }, query: { namespace } } }));
        case 'operatorPolicy':
          return unwrap(api.DELETE('/clusters/{cluster}/rabbitmq/operator-policies/{vhost}/{name}', { params: { path: { cluster, vhost: edit.existing!.vhost, name: edit.existing!.name }, query: { namespace } } }));
      }
    },
    onSuccess: () => { setRemove(null); invalidate(); toast.success('RabbitMQ object deleted.'); },
    onError: (error) => toast.error(errorMessage(error)),
  });

  if (!allowed) return <Callout>You do not have permission to read RabbitMQ access settings for this cluster.</Callout>;

  const canCreate = can('create', 'database-users');
  const canUpdate = can('update', 'database-users');
  const canDelete = can('delete', 'database-users');
  const actions = (edit: AccessEdit, protectedUser = false) => protectedUser ? <span className="stack"><Badge plain>System user</Badge><span className="small muted">{SYSTEM_USER_EXPLANATION} <a href="https://karakoram.dev/docs/rabbitmq/access-and-tls/" target="_blank" rel="noreferrer">RabbitMQ access guide ↗</a></span></span> : (
    <span className="row">
      {canUpdate ? <Button size="sm" onClick={() => setEditor(edit)}>Edit</Button> : null}
      {canDelete ? <Button size="sm" variant="ghost" onClick={() => setRemove(edit)}>Delete…</Button> : null}
    </span>
  );
  const add = (kind: AccessKind) => canCreate ? <Button size="sm" variant="primary" onClick={() => setEditor({ kind } as AccessEdit)}>+ Add</Button> : null;
  const policyColumns = ['Vhost', 'Name', 'Pattern', 'Apply to', 'Priority', 'Definition', ''];

  return (
    <div className="stack">
      <Callout>Access settings are applied directly to RabbitMQ. User tags control management access; vhost and topic permissions control application access. Policies and operator policies are evaluated by RabbitMQ.</Callout>
      <AccessTable title="Virtual hosts" columns={['Name', 'Default queue type', 'Description', 'Limits', '']} actions={add('vhost')} pending={vhosts.isPending} error={vhosts.error} retry={() => vhosts.refetch()} rows={vhosts.data?.items.map((item) => ({ id: item.name, cells: [<code>{item.name}</code>, item.defaultQueueType ?? 'quorum', item.description || '—', <span className="small">Connections: {item.limits?.maxConnections ?? 'unlimited'}<br />Queues: {item.limits?.maxQueues ?? 'unlimited'}</span>, actions({ kind: 'vhost', existing: item })] })) ?? []} />
      <AccessTable title="Users" columns={['Username', 'Tags', '']} actions={add('user')} pending={users.isPending} error={users.error} retry={() => users.refetch()} rows={users.data?.items.map((item) => ({ id: item.name, cells: [<code>{item.name}</code>, item.tags.join(', ') || 'No management access', actions({ kind: 'user', existing: item }, SYSTEM_USERS.includes(item.name))] })) ?? []} />
      <AccessTable title="Vhost permissions" columns={['Vhost', 'User', 'Configure regex', 'Write regex', 'Read regex', '']} actions={add('permission')} pending={permissions.isPending} error={permissions.error} retry={() => permissions.refetch()} rows={permissions.data?.items.map((item) => ({ id: JSON.stringify([item.vhost, item.user]), cells: [<code>{item.vhost}</code>, <code>{item.user}</code>, <code>{item.configure || '(none)'}</code>, <code>{item.write || '(none)'}</code>, <code>{item.read || '(none)'}</code>, actions({ kind: 'permission', existing: item }, SYSTEM_USERS.includes(item.user))] })) ?? []} />
      <AccessTable title="Topic permissions" columns={['Vhost', 'User', 'Exchange', 'Write regex', 'Read regex', '']} actions={add('topic')} pending={topics.isPending} error={topics.error} retry={() => topics.refetch()} rows={topics.data?.items.map((item) => ({ id: JSON.stringify([item.vhost, item.user, item.exchange]), cells: [<code>{item.vhost}</code>, <code>{item.user}</code>, <code>{item.exchange}</code>, <code>{item.write || '(none)'}</code>, <code>{item.read || '(none)'}</code>, actions({ kind: 'topic', existing: item }, SYSTEM_USERS.includes(item.user))] })) ?? []} />
      <AccessTable title="Policies" columns={policyColumns} actions={add('policy')} pending={policies.isPending} error={policies.error} retry={() => policies.refetch()} rows={policies.data?.items.map((item) => ({ id: JSON.stringify([item.vhost, item.name]), cells: [<code>{item.vhost}</code>, <code>{item.name}</code>, <code>{item.pattern}</code>, item.applyTo, item.priority, <code>{JSON.stringify(item.definition)}</code>, actions({ kind: 'policy', existing: item })] })) ?? []} />
      <AccessTable title="Operator policies" columns={policyColumns} actions={add('operatorPolicy')} pending={operatorPolicies.isPending} error={operatorPolicies.error} retry={() => operatorPolicies.refetch()} rows={operatorPolicies.data?.items.map((item) => ({ id: JSON.stringify([item.vhost, item.name]), cells: [<code>{item.vhost}</code>, <code>{item.name}</code>, <code>{item.pattern}</code>, item.applyTo, item.priority, <code>{JSON.stringify(item.definition)}</code>, actions({ kind: 'operatorPolicy', existing: item })] })) ?? []} />
      {editor ? <AccessEditor key={JSON.stringify(editor)} namespace={namespace} cluster={cluster} edit={editor} vhosts={vhosts.data?.items.map((item) => item.name) ?? []} users={users.data?.items.map((item) => item.name).filter((user) => !SYSTEM_USERS.includes(user)) ?? []} onClose={() => setEditor(null)} onSaved={invalidate} /> : null}
      <ConfirmDialog open={Boolean(remove)} onClose={() => { if (!deletion.isPending) setRemove(null); }} title="Delete RabbitMQ object?" confirmLabel="Delete" danger busy={deletion.isPending} onConfirm={() => { if (remove && canDelete) deletion.mutate(remove); }}>
        {remove?.kind === 'vhost' ? <Callout tone="danger">Deleting a virtual host also deletes all of its queues, exchanges, bindings and messages. This cannot be undone.</Callout> : remove?.kind === 'topic' ? <Callout tone="danger">This removes every topic-exchange permission for user <code>{remove.existing?.user}</code> in virtual host <code>{remove.existing?.vhost}</code>, not just the displayed exchange. RabbitMQ deletes topic permissions by user and virtual host.</Callout> : <p>Delete this {remove?.kind === 'operatorPolicy' ? 'operator policy' : remove?.kind}? This changes application access or broker behaviour immediately.</p>}
        {remove?.existing ? <pre className="mono small">{JSON.stringify(remove.existing, null, 2)}</pre> : null}
      </ConfirmDialog>
    </div>
  );
}

function AccessTable({ title, columns, rows, actions, pending, error, retry }: { title: string; columns: string[]; rows: { id: string; cells: ReactNode[] }[]; actions: ReactNode; pending: boolean; error: unknown; retry: () => unknown }) {
  return (
    <Card title={title} actions={actions} flush>
      {pending ? <Loading /> : error ? <div className="card-body"><ErrorBlock error={error} retry={retry} /></div> : rows.length === 0 ? <div className="table-empty">No {title.toLowerCase()}.</div> : (
        <div className="table-wrap"><table className="table">
          <thead><tr>{columns.map((column, index) => <th key={index} className={index === columns.length - 1 ? 'actions' : undefined}>{column}</th>)}</tr></thead>
          <tbody>{rows.map((row) => <tr key={row.id}>{row.cells.map((cell, index) => <td key={index} className={index === row.cells.length - 1 ? 'actions' : undefined}>{cell}</td>)}</tr>)}</tbody>
        </table></div>
      )}
    </Card>
  );
}
