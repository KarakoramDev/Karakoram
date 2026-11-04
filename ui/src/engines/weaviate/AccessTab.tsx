// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
import { useReducer, useRef, useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { api, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { Button } from '@/components/ui/Button';
import { Dialog, ConfirmDialog } from '@/components/ui/Dialog';
import { Checkbox, TextField, TextAreaField as TextareaField } from '@/components/ui/Field';
import { Callout, Card, ErrorBlock, Loading } from '@/components/ui/Primitives';
import { useClusterContext } from '@/pages/clusters/detail/ClusterDetail';
import { BUILTIN_ROLES, parsePermissions, privilegedRoles, protectedUser, secretState } from './access-state';

type Edit = { kind: 'user'; existing?: Schemas['WeaviateUser'] } | { kind: 'role'; existing?: Schemas['WeaviateRole'] };
type Removal = { kind: 'user' | 'role'; name: string };
export default function AccessTab() {
  const { namespace, name: cluster, can } = useClusterContext();
  const queryClient = useQueryClient();
  const queryKey = keys.clusterSub(namespace, cluster, 'weaviate-access');
  const params = { path: { cluster }, query: { namespace } };
  const read = can('read', 'database-users');
  const users = useQuery({ queryKey: [...queryKey, 'users'], queryFn: () => unwrap(api.GET('/clusters/{cluster}/weaviate/users', { params })), enabled: read });
  const roles = useQuery({ queryKey: [...queryKey, 'roles'], queryFn: () => unwrap(api.GET('/clusters/{cluster}/weaviate/roles', { params })), enabled: read });
  const [edit, setEdit] = useState<Edit | null>(null);
  const [remove, setRemove] = useState<Removal | null>(null);
  const [name, setName] = useState('');
  const [roleNames, setRoleNames] = useState<string[]>([]);
  const [active, setActive] = useState(true);
  const [permissions, setPermissions] = useState('[]');
  const [secret, reveal] = useReducer(secretState, null);
  const [busy, setBusy] = useState(false);
  const lock = useRef(false);
  const [error, setError] = useState<unknown>(null);
  const roleWrite = can('update', 'database-users') && can('update', 'clusters');
  const clearSecret = () => reveal({ type: 'clear' });
  const open = (value: Edit) => {
    clearSecret(); setError(null); setEdit(value); setName(value.existing?.name ?? '');
    setRoleNames(value.kind === 'user' ? value.existing?.roles ?? [] : []);
    setActive(value.kind === 'user' ? value.existing?.active ?? true : true);
    setPermissions(value.kind === 'role' ? JSON.stringify(value.existing?.permissions ?? [], null, 2) : '[]');
  };
  // Secret-bearing calls deliberately bypass React Query's mutation cache.
  const run = async (action: () => Promise<void>) => {
    if (lock.current) return;
    lock.current = true; setBusy(true); setError(null); clearSecret();
    try { await action(); void queryClient.invalidateQueries({ queryKey }); }
    catch (e) { setError(e); }
    finally { lock.current = false; setBusy(false); }
  };
  const saveAllowed = edit?.kind === 'role' ? roleWrite : can(edit?.existing ? 'update' : 'create', 'database-users') && (!privilegedRoles(roleNames) || can('update', 'clusters'));
  const save = () => run(async () => {
    if (!edit || !saveAllowed || !name.trim()) return;
    if (edit.kind === 'user') {
      if (edit.existing) await unwrap(api.PUT('/clusters/{cluster}/weaviate/users/{user}', { params: { path: { cluster, user: name }, query: { namespace } }, body: { roles: roleNames, active } }));
      else {
        const result = await unwrap(api.POST('/clusters/{cluster}/weaviate/users', { params, body: { name: name.trim(), roles: roleNames } }));
        reveal({ type: 'reveal', key: result.apiKey });
      }
    } else {
      const body = { name: name.trim(), permissions: parsePermissions(permissions) };
      if (edit.existing) await unwrap(api.PUT('/clusters/{cluster}/weaviate/roles/{role}', { params: { path: { cluster, role: name }, query: { namespace } }, body }));
      else await unwrap(api.POST('/clusters/{cluster}/weaviate/roles', { params, body }));
    }
    setEdit(null);
  });
  const deleteAllowed = remove?.kind === 'role' ? roleWrite : can('delete', 'database-users');
  if (!read) return <Callout>You do not have permission to read Weaviate access settings for this cluster.</Callout>;
  return <div className="stack">
    <Callout>Weaviate API keys and roles control database access, separately from Karakoram management permissions. Protected system identities and built-in roles are read-only. <a href="https://karakoram.dev/docs/weaviate/access-and-tls/" target="_blank" rel="noreferrer">Weaviate access guide ↗</a></Callout>
    {error && !edit && !remove ? <ErrorBlock error={error} /> : null}
    <Card title="Users" actions={<Button disabled={!can('create', 'database-users') || busy} onClick={() => open({ kind: 'user' })}>Create user</Button>}>
      {users.isPending ? <Loading /> : users.error ? <ErrorBlock error={users.error} retry={() => users.refetch()} /> : <div className="table-wrap"><table className="table"><thead><tr>{['Name', 'Type', 'Active', 'Roles', 'Last used', ''].map((v) => <th key={v}>{v}</th>)}</tr></thead><tbody>{users.data?.items.map((user) => <tr key={user.name}>
        <td><code>{user.name}</code></td><td>{user.userType}</td><td>{user.active ? 'Yes' : 'No'}</td><td>{user.roles.join(', ') || '—'}</td><td>{user.lastUsedAt ?? 'Never'}</td><td>{protectedUser(user) ? 'Protected identity · read-only' : <span className="row">
          <Button size="sm" disabled={!can('update', 'database-users') || busy} onClick={() => open({ kind: 'user', existing: user })}>Edit</Button>
          <Button size="sm" disabled={!can('update', 'database-users') || (privilegedRoles(user.roles) && !can('update', 'clusters')) || busy} onClick={() => { void run(async () => {
            if (!can('update', 'database-users') || (privilegedRoles(user.roles) && !can('update', 'clusters'))) return;
            const result = await unwrap(api.POST('/clusters/{cluster}/weaviate/users/{user}/rotate-key', { params: { path: { cluster, user: user.name }, query: { namespace } } }));
            reveal({ type: 'reveal', key: result.apiKey });
          }); }}>Rotate key</Button>
          <Button size="sm" disabled={!can('delete', 'database-users') || busy} onClick={() => { clearSecret(); setError(null); setRemove({ kind: 'user', name: user.name }); }}>Delete…</Button>
        </span>}</td>
      </tr>)}</tbody></table>{users.data?.items.length === 0 ? <p className="table-empty">No application users.</p> : null}</div>}
    </Card>
    <Card title="Roles" actions={<Button disabled={!roleWrite || busy} onClick={() => open({ kind: 'role' })}>Create role</Button>}>
      {roles.isPending ? <Loading /> : roles.error ? <ErrorBlock error={roles.error} retry={() => roles.refetch()} /> : <div className="stack">{roles.data?.items.map((role) => <section key={role.name} className="stack"><div className="row"><strong>{role.name}</strong>{BUILTIN_ROLES.includes(role.name) ? <span className="small muted">Built-in · read-only</span> : <><Button size="sm" disabled={!roleWrite || busy} onClick={() => open({ kind: 'role', existing: role })}>Replace</Button><Button size="sm" disabled={!roleWrite || busy} onClick={() => { clearSecret(); setError(null); setRemove({ kind: 'role', name: role.name }); }}>Delete…</Button></>}</div><details><summary>Permissions ({role.permissions.length})</summary><pre>{JSON.stringify(role.permissions, null, 2)}</pre></details></section>)}{roles.data?.items.length === 0 ? <p>No roles.</p> : null}</div>}
    </Card>
    <Dialog open={Boolean(edit)} title={`${edit?.existing ? 'Update' : 'Create'} Weaviate ${edit?.kind ?? 'user'}`} onClose={() => { setEdit(null); setError(null); clearSecret(); }} busy={busy} error={error} onSubmit={save} footer={<Button type="submit" variant="primary" disabled={!saveAllowed || !name.trim() || busy}>Save</Button>}>
      <div className="stack"><TextField label="Name" value={name} required disabled={Boolean(edit?.existing)} onChange={(e) => setName(e.target.value)} />
        {edit?.kind === 'user' ? <><fieldset className="stack"><legend>Roles (replaces the complete list)</legend>{roles.data?.items.map((role) => <Checkbox key={role.name} label={role.name} disabled={!roleNames.includes(role.name) && privilegedRoles([role.name]) && !can('update', 'clusters')} checked={roleNames.includes(role.name)} onChange={(e) => setRoleNames(e.target.checked ? [...roleNames, role.name] : roleNames.filter((v) => v !== role.name))} />)}</fieldset>{privilegedRoles(roleNames) && !can('update', 'clusters') ? <Callout tone="warn">Assigning roles other than viewer or read-only also requires clusters/update, including existing assignments.</Callout> : null}{edit.existing ? <Checkbox label="Active" checked={active} onChange={(e) => setActive(e.target.checked)} /> : null}</> : <TextareaField label="Permissions JSON array" rows={14} value={permissions} onChange={(e) => setPermissions(e.target.value)} />}
      </div>
    </Dialog>
    <ConfirmDialog open={Boolean(remove)} title={`Delete Weaviate ${remove?.kind}?`} onClose={() => { setRemove(null); setError(null); }} busy={busy} error={error} danger confirmLabel="Delete" onConfirm={() => run(async () => {
      if (!remove || !deleteAllowed) return;
      if (remove.kind === 'user') await unwrap(api.DELETE('/clusters/{cluster}/weaviate/users/{user}', { params: { path: { cluster, user: remove.name }, query: { namespace } } }));
      else await unwrap(api.DELETE('/clusters/{cluster}/weaviate/roles/{role}', { params: { path: { cluster, role: remove.name }, query: { namespace } } }));
      setRemove(null);
    })}><p>Delete <code>{remove?.name}</code>? This changes database access immediately and cannot be undone.</p></ConfirmDialog>
    <Dialog open={secret !== null} title="Store this API key securely" onClose={clearSecret} footer={<Button onClick={clearSecret}>I have saved it — close</Button>}>
      <Callout tone="warn">This key is shown once. Closing this dialog or leaving the tab clears it. GET cannot retrieve it; rotation immediately replaces the old key. Do not put keys in URLs or logs.</Callout>
      <pre className="mono">{secret}</pre><Button onClick={() => { if (secret) void navigator.clipboard.writeText(secret).catch(() => setError(new Error('Clipboard unavailable. Copy the displayed key manually.'))); }}>Copy API key</Button>
    </Dialog>
  </div>;
}
