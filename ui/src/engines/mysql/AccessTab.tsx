// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { idempotencyHeaders } from '@/api/idempotency';
import { keys } from '@/api/keys';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { Switch, TextField } from '@/components/ui/Field';
import { Callout, Card, CodeLine, ErrorBlock, Loading } from '@/components/ui/Primitives';
import { useClusterContext } from '@/pages/clusters/detail/ClusterDetail';
import { useSubmissionKey } from '@/pages/clusters/detail/use-submission-key';

export const PRIVILEGES: Schemas['MySQLGrantPrivileges'][] = ['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER', 'INDEX', 'REFERENCES', 'CREATE_VIEW', 'SHOW_VIEW', 'TRIGGER', 'EXECUTE', 'CREATE_ROUTINE', 'ALTER_ROUTINE', 'EVENT', 'LOCK_TABLES', 'CREATE_TEMPORARY_TABLES', 'ALL'];
const EMPTY_USER: Schemas['MySQLUserWrite'] = { name: '', locked: false, maxUserConnections: 0 };
export function MySQLAccessTab() {
  const { cluster, namespace, name, can } = useClusterContext();
  const client = useQueryClient(), params = { path: { cluster: name }, query: { namespace } }, key = keys.clusterSub(namespace, name, 'mysql-access');
  const read = can('read', 'database-users'), writable = !['Deleting', 'Suspended'].includes(cluster.status.phase);
  const [user, setUser] = useState<Schemas['MySQLUserWrite']>({ ...EMPTY_USER });
  const [editing, setEditing] = useState(false);
  const [database, setDatabase] = useState<Schemas['MySQLDatabaseWrite']>({ name: '', characterSet: 'utf8mb4', collation: 'utf8mb4_0900_ai_ci' });
  const [grant, setGrant] = useState<Schemas['MySQLGrant']>({ user: '', database: '', privileges: ['SELECT'] });
  const [filterUser, setFilterUser] = useState(''), [filterDatabase, setFilterDatabase] = useState('');
  const [password, setPassword] = useState<string | null>(null), [busy, setBusy] = useState(false), [error, setError] = useState<string | null>(null);
  const [retainCurrent, setRetainCurrent] = useState(false), [force, setForce] = useState(false);
  const [rotation, setRotation] = useState<string | null>(null);
  const [deletion, setDeletion] = useState<{ kind: 'user' | 'database'; name: string } | null>(null);
  const submission = useSubmissionKey(JSON.stringify({ user, database, grant, deletion, rotation, retainCurrent, force }));
  const users = useQuery({ queryKey: [...key, 'users'], queryFn: () => unwrap(api.GET('/clusters/{cluster}/mysql/users', { params })), enabled: read });
  const databases = useQuery({ queryKey: [...key, 'databases'], queryFn: () => unwrap(api.GET('/clusters/{cluster}/mysql/databases', { params })), enabled: read });
  const grants = useQuery({ queryKey: [...key, 'grants', filterUser, filterDatabase], queryFn: () => unwrap(api.GET('/clusters/{cluster}/mysql/grants', { params: { ...params, query: { namespace, user: filterUser || undefined, database: filterDatabase || undefined } } })), enabled: read && Boolean(filterUser || filterDatabase) });
  const headers = idempotencyHeaders(submission.key);
  const userParams = (value: string) => ({ ...params, path: { cluster: name, user: value } });
  const run = async (action: () => Promise<unknown>) => {
    if (busy) return;
    setBusy(true); setError(null);
    try { await action(); submission.reset(); void client.invalidateQueries({ queryKey: key }); } catch (e) { setError(errorMessage(e)); } finally { setBusy(false); }
  };
  const disabled = (action: 'create' | 'update' | 'delete') => !writable || busy || !can(action, 'database-users');
  return <div className="stack">
    <Callout>Manage application users here so ProxySQL receives authentication updates. Users created directly in SQL may not be routable through the proxy. Application users use caching_sha2_password, which requires TLS.</Callout>
    {error ? <Callout tone="danger">{error}</Callout> : null}
    {password ? <Callout tone="warn" title="Save this password now"><p>This password is displayed only once and is not available from the user list.</p><CodeLine value={password} /><Button onClick={() => setPassword(null)}>Dismiss password</Button></Callout> : null}
    <Card title="Users">
      {!read ? <p>Reading users requires database-users read permission.</p> : users.isPending ? <Loading /> : users.error ? <ErrorBlock error={users.error} retry={() => users.refetch()} /> : <div className="table-wrap"><table className="table"><thead><tr><th>User</th><th>Max connections</th><th>Locked</th><th>Proxy routable</th><th>Actions</th></tr></thead><tbody>{users.data?.items.map((u) => <tr key={u.name}><td>{u.name}@{u.host}</td><td>{u.maxUserConnections || 'unlimited'}</td><td>{u.locked ? 'yes' : 'no'}</td><td>{u.routableThroughProxy ? 'yes' : 'no'}</td><td><div className="row wrap"><Button disabled={disabled('update')} onClick={() => { setUser({ name: u.name, locked: u.locked, maxUserConnections: u.maxUserConnections }); setEditing(true); }}>Edit</Button><Button disabled={disabled('update')} onClick={() => { setRetainCurrent(false); setRotation(u.name); }}>Rotate password</Button><Button disabled={disabled('update') || !u.passwordRetained} onClick={() => void run(() => unwrap(api.POST('/clusters/{cluster}/mysql/users/{user}/password/discard-old', { headers, params: userParams(u.name) })))}>Discard old password</Button><Button disabled={disabled('delete')} onClick={() => setDeletion({ kind: 'user', name: u.name })}>Delete</Button></div></td></tr>)}</tbody></table></div>}
      <fieldset disabled={disabled(editing ? 'update' : 'create')} className="stack" style={{ border: 0, padding: 0 }}>
        <strong>{editing ? `Edit ${user.name}` : 'Create user'}</strong>
        <TextField label="User name" value={user.name} disabled={editing} onChange={(e) => setUser({ ...user, name: e.target.value })} />
        {!editing ? <TextField label="Password (empty generates one)" type="password" autoComplete="new-password" value={user.password ?? ''} onChange={(e) => setUser({ ...user, password: e.target.value || undefined })} /> : null}
        <TextField label="Max user connections (0 is unlimited)" type="number" min={0} step={1} value={user.maxUserConnections} onChange={(e) => setUser({ ...user, maxUserConnections: Number(e.target.value) })} />
        <Switch label="Locked" checked={user.locked} onChange={(e) => setUser({ ...user, locked: e.target.checked })} />
        <div className="row"><Button variant="primary" disabled={!user.name.trim() || !Number.isInteger(user.maxUserConnections) || user.maxUserConnections < 0} onClick={() => void run(async () => {
          if (editing) await unwrap(api.PATCH('/clusters/{cluster}/mysql/users/{user}', { headers, params: userParams(user.name), body: { locked: user.locked, maxUserConnections: user.maxUserConnections } }));
          else { const result = await unwrap(api.POST('/clusters/{cluster}/mysql/users', { headers, params, body: user })); setPassword(result.generatedPassword ?? user.password ?? null); }
          setUser({ ...EMPTY_USER }); setEditing(false);
        })}>{editing ? 'Save user' : 'Create user'}</Button>{editing ? <Button onClick={() => { setEditing(false); setUser({ ...EMPTY_USER }); }}>Cancel editing</Button> : null}</div>
      </fieldset>
    </Card>
    <Card title="Databases">
      {read && databases.isPending ? <Loading /> : null}{databases.error ? <ErrorBlock error={databases.error} retry={() => databases.refetch()} /> : null}
      {databases.data?.items.map((d) => <div className="row-between" key={d.name}><code>{d.name} · {d.characterSet} / {d.collation}</code><Button disabled={disabled('delete')} onClick={() => { setForce(false); setDeletion({ kind: 'database', name: d.name }); }}>Delete</Button></div>)}
      <fieldset disabled={disabled('create')} className="stack" style={{ border: 0, padding: 0 }}>{(['name', 'characterSet', 'collation'] as const).map((field) => <TextField key={field} label={`Database ${field}`} value={database[field]} onChange={(e) => setDatabase({ ...database, [field]: e.target.value })} />)}<Button variant="primary" disabled={!database.name.trim() || !database.characterSet.trim() || !database.collation.trim()} onClick={() => void run(async () => { await unwrap(api.POST('/clusters/{cluster}/mysql/databases', { headers, params, body: database })); setDatabase({ ...database, name: '' }); })}>Create database</Button></fieldset>
    </Card>
    <Card title="Grants">
      <TextField label="Filter by user" value={filterUser} onChange={(e) => setFilterUser(e.target.value)} /><TextField label="Filter by database" value={filterDatabase} onChange={(e) => setFilterDatabase(e.target.value)} />
      {!filterUser && !filterDatabase ? <p>Choose a user or database filter to inspect grants.</p> : read && grants.isPending ? <Loading /> : null}
      {grants.error ? <ErrorBlock error={grants.error} retry={() => grants.refetch()} /> : null}
      {grants.data?.items.map((g, i) => <div className="row-between" key={i}><code>{g.user} → {g.database}: {g.privileges.join(', ')}</code><Button disabled={disabled('update')} onClick={() => void run(() => unwrap(api.DELETE('/clusters/{cluster}/mysql/grants', { headers, params, body: g })))}>Revoke</Button></div>)}
      <fieldset disabled={disabled('update')} className="stack" style={{ border: 0, padding: 0 }}><TextField label="Grantee user" value={grant.user} onChange={(e) => setGrant({ ...grant, user: e.target.value })} /><TextField label="Grant database" value={grant.database} onChange={(e) => setGrant({ ...grant, database: e.target.value })} />{PRIVILEGES.map((p) => <Switch key={p} label={p} checked={grant.privileges.includes(p)} onChange={(e) => setGrant({ ...grant, privileges: e.target.checked ? p === 'ALL' ? ['ALL'] : [...grant.privileges.filter((v) => v !== 'ALL'), p] : grant.privileges.filter((v) => v !== p) })} />)}<Button variant="primary" disabled={!grant.user.trim() || !grant.database.trim() || !grant.privileges.length} onClick={() => void run(async () => { await unwrap(api.POST('/clusters/{cluster}/mysql/grants', { headers, params, body: grant })); setFilterUser(grant.user); setFilterDatabase(grant.database); })}>Grant privileges</Button></fieldset>
    </Card>
    <ConfirmDialog open={Boolean(rotation)} title={`Rotate password for ${rotation}`} confirmLabel="Rotate password" onClose={() => setRotation(null)} busy={busy} onConfirm={() => void run(async () => { if (!rotation || disabled('update')) return; const result = await unwrap(api.POST('/clusters/{cluster}/mysql/users/{user}/password', { headers, params: userParams(rotation), body: { retainCurrent } })); setPassword(result.generatedPassword); setRotation(null); })}><Switch label="Keep current password until discarded" checked={retainCurrent} onChange={(e) => setRetainCurrent(e.target.checked)} /><p>The new password is shown once. Discard the old password after all clients have migrated.</p></ConfirmDialog>
    <ConfirmDialog open={Boolean(deletion)} title={`Delete ${deletion?.kind} ${deletion?.name}`} confirmLabel="Delete" onClose={() => setDeletion(null)} busy={busy} onConfirm={() => void run(async () => { if (!deletion || disabled('delete')) return; if (deletion.kind === 'user') await unwrap(api.DELETE('/clusters/{cluster}/mysql/users/{user}', { headers, params: userParams(deletion.name) })); else await unwrap(api.DELETE('/clusters/{cluster}/mysql/databases/{database}', { headers, params: { path: { cluster: name, database: deletion.name }, query: { namespace, force } } })); setDeletion(null); })}><Callout tone="danger">This cannot be undone. Database deletion permanently removes its data.</Callout>{deletion?.kind === 'database' ? <Switch label="Force database deletion" checked={force} onChange={(e) => setForce(e.target.checked)} /> : null}</ConfirmDialog>
  </div>;
}
