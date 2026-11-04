// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap } from '@/api/client';
import { idempotencyHeaders } from '@/api/idempotency';
import { keys } from '@/api/keys';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { TextField } from '@/components/ui/Field';
import { Callout, Card, CodeLine, ErrorBlock, Loading } from '@/components/ui/Primitives';
import { useClusterContext } from '@/pages/clusters/detail/ClusterDetail';
import { useSubmissionKey } from '@/pages/clusters/detail/use-submission-key';

export function UsersTab() {
  const { cluster, namespace, name, can } = useClusterContext();
  const client = useQueryClient();
  const params = { path: { cluster: name }, query: { namespace } };
  const key = keys.clusterSub(namespace, name, 'neo4j-users');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [editing, setEditing] = useState<string | null>(null);
  const [generated, setGenerated] = useState<string | null>(null);
  const [deletion, setDeletion] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const submission = useSubmissionKey(JSON.stringify({ username, password, editing, deletion }));
  const headers = idempotencyHeaders(submission.key);
  const read = can('read', 'database-users');
  const writable = can('update') && !['Deleting', 'Suspended'].includes(cluster.status.phase);
  const users = useQuery({ queryKey: key, queryFn: () => unwrap(api.GET('/clusters/{cluster}/neo4j/users', { params })), enabled: read });
  const run = async (action: () => Promise<void>) => {
    if (busy) return;
    setBusy(true); setError(null); setGenerated(null);
    try { await action(); submission.reset(); void client.invalidateQueries({ queryKey: key }); }
    catch (e) { setError(errorMessage(e)); }
    finally { setBusy(false); }
  };
  return <div className="stack">
    <Callout tone="warn">Every Neo4j Community user has full administrative privileges, including full read and write access. Community does not support scoped roles. The managed <code>neo4j</code> administrator is rotated through System users, not here.</Callout>
    {error ? <Callout tone="danger">{error}</Callout> : null}
    {generated ? <Callout tone="warn" title="Save this password now"><p>This generated password is shown only once and cannot be retrieved from the user list.</p><CodeLine value={generated} /><Button onClick={() => setGenerated(null)}>Dismiss password</Button></Callout> : null}
    <Card title="Neo4j users">
      {!read ? <p>Reading users requires database-users read permission.</p> : users.isPending ? <Loading /> : users.error ? <ErrorBlock error={users.error} retry={() => users.refetch()} /> : <div className="table-wrap"><table className="table"><thead><tr><th>User</th><th>Password change required</th><th>Actions</th></tr></thead><tbody>
        {users.data?.items.map((user) => <tr key={user.name}><td className="mono">{user.name}</td><td>{user.passwordChangeRequired ? 'yes' : 'no'}</td><td><div className="row">
          <Button size="sm" disabled={user.name === 'neo4j' || !writable || busy || !can('update', 'database-users')} onClick={() => { setEditing(user.name); setPassword(''); }}>Set / generate password</Button>
          <Button size="sm" disabled={user.name === 'neo4j' || !writable || busy || !can('delete', 'database-users')} onClick={() => setDeletion(user.name)}>Delete</Button>
        </div></td></tr>)}
      </tbody></table>{users.data?.items.length === 0 ? <p className="small muted">No users found.</p> : null}</div>}
      <fieldset className="stack" style={{ border: 0, padding: 0 }} disabled={!writable || busy || !can(editing ? 'update' : 'create', 'database-users')}>
        <strong>{editing ? `Change password for ${editing}` : 'Create user'}</strong>
        {!editing ? <TextField label="User name" value={username} onChange={(e) => setUsername(e.target.value)} /> : null}
        <TextField label="Password (empty generates one)" type="password" autoComplete="new-password" value={password} onChange={(e) => setPassword(e.target.value)} />
        <div className="row"><Button variant="primary" disabled={!editing && (!username.trim() || username === 'neo4j')} onClick={() => void run(async () => {
          const body = password ? { password } : {};
          const result = editing
            ? await unwrap(api.POST('/clusters/{cluster}/neo4j/users/{user}/password', { headers, params: { ...params, path: { cluster: name, user: editing } }, body }))
            : await unwrap(api.POST('/clusters/{cluster}/neo4j/users', { headers, params, body: { name: username, ...body } }));
          setGenerated(result.generatedPassword ?? null); setPassword(''); setUsername(''); setEditing(null);
        })}>{editing ? 'Set / generate password' : 'Create user'}</Button>{editing ? <Button onClick={() => { setEditing(null); setPassword(''); }}>Cancel</Button> : null}</div>
      </fieldset>
    </Card>
    <ConfirmDialog open={deletion !== null} title={`Delete user ${deletion ?? ''}`} confirmLabel="Delete user" busy={busy} onClose={() => setDeletion(null)} onConfirm={() => void run(async () => {
      if (!deletion) return;
      await unwrap(api.DELETE('/clusters/{cluster}/neo4j/users/{user}', { headers, params: { ...params, path: { cluster: name, user: deletion } } }));
      setDeletion(null);
    })}><Callout tone="danger">This permanently removes the user's ability to authenticate. This action cannot be undone.</Callout></ConfirmDialog>
  </div>;
}
