// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { idempotencyHeaders } from '@/api/idempotency';
import { keys } from '@/api/keys';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { SelectField, Switch, TextField } from '@/components/ui/Field';
import { Callout, Card, CodeLine, ErrorBlock, Loading } from '@/components/ui/Primitives';
import { useClusterContext } from '@/pages/clusters/detail/ClusterDetail';
import { useSubmissionKey } from '@/pages/clusters/detail/use-submission-key';

const EMPTY_ROLE: Schemas['PostgresRoleWrite'] = { name: '', login: true, superuser: false, createdb: false, createrole: false, replication: false, bypassrls: false, connectionLimit: -1, memberOf: [] };

export function PostgresAccessTab() {
  const { cluster, namespace, name, can } = useClusterContext();
  const client = useQueryClient();
  const params = { path: { cluster: name }, query: { namespace } };
  const key = keys.clusterSub(namespace, name, 'postgres-access');
  const read = can('read', 'database-users');
  const writable = cluster.status.phase !== 'Deleting' && cluster.status.phase !== 'Suspended';
  const [role, setRole] = useState<Schemas['PostgresRoleWrite']>({ ...EMPTY_ROLE });
  const [editing, setEditing] = useState(false);
  const [database, setDatabase] = useState<Schemas['PostgresDatabase']>({ name: '', owner: '', encoding: 'UTF8', locale: 'C.UTF-8', allowConnections: true });
  const [extensionsText, setExtensionsText] = useState('');
  const [grant, setGrant] = useState<Schemas['PostgresGrant']>({ database: '', role: '', privileges: ['CONNECT'] });
  const [selectedDatabase, setSelectedDatabase] = useState('');
  const [password, setPassword] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [force, setForce] = useState(false);
  const [deletion, setDeletion] = useState<{ kind: 'role' | 'database'; name: string } | null>(null);
  const submission = useSubmissionKey(JSON.stringify({ role, database, extensionsText, grant, deletion, force }));
  const roles = useQuery({ queryKey: [...key, 'roles'], queryFn: () => unwrap(api.GET('/clusters/{cluster}/postgres/roles', { params })), enabled: read });
  const databases = useQuery({ queryKey: [...key, 'databases'], queryFn: () => unwrap(api.GET('/clusters/{cluster}/postgres/databases', { params })), enabled: read });
  const grants = useQuery({ queryKey: [...key, 'grants', selectedDatabase], queryFn: () => unwrap(api.GET('/clusters/{cluster}/postgres/grants', { params: { ...params, query: { namespace, database: selectedDatabase } } })), enabled: read && Boolean(selectedDatabase) });
  const headers = idempotencyHeaders(submission.key);
  const run = async (action: () => Promise<unknown>) => {
    if (busy) return;
    setBusy(true); setError(null);
    try { await action(); submission.reset(); void client.invalidateQueries({ queryKey: key }); } catch (e) { setError(errorMessage(e)); } finally { setBusy(false); }
  };
  return <div className="stack">
    <Callout>Manage application roles through this API so ProxySQL receives login passwords. Roles created directly in SQL are not synchronized to the proxy. System identities are managed separately.</Callout>
    {error ? <Callout tone="danger">{error}</Callout> : null}
    {password ? <Callout tone="warn" title="Save this password now"><p>This password is displayed only once. It is not available from the role list.</p><CodeLine value={password} /><Button onClick={() => setPassword(null)}>Dismiss password</Button></Callout> : null}
    <Card title="Roles">
      {!read ? <p>Reading roles requires database-users read permission.</p> : roles.isPending ? <Loading /> : roles.error ? <ErrorBlock error={roles.error} retry={() => roles.refetch()} /> : <div className="table-wrap"><table className="table"><thead><tr><th>Role</th><th>Login</th><th>Membership</th><th>Actions</th></tr></thead><tbody>{roles.data?.items.map((r) => <tr key={r.name}><td className="mono">{r.name}</td><td>{r.login ? 'yes' : 'no'}</td><td>{r.memberOf.join(', ') || 'none'}</td><td><div className="row"><Button size="sm" disabled={!writable || busy || !can('update', 'database-users')} onClick={() => { setRole({ ...r }); setEditing(true); }}>Edit</Button><Button size="sm" disabled={!writable || busy || !can('update', 'database-users')} onClick={() => void run(async () => { const result = await unwrap(api.POST('/clusters/{cluster}/postgres/roles/{role}/password', { headers, params: { ...params, path: { cluster: name, role: r.name } } })); setPassword(result.generatedPassword); })}>Rotate password</Button><Button size="sm" disabled={!writable || busy || !can('delete', 'database-users')} onClick={() => setDeletion({ kind: 'role', name: r.name })}>Delete</Button></div></td></tr>)}{!roles.data?.items.length ? <tr><td colSpan={4}>No application roles.</td></tr> : null}</tbody></table></div>}
      <fieldset disabled={!writable || busy || !can(editing ? 'update' : 'create', 'database-users')} className="stack" style={{ border: 0, padding: 0 }}>
        <strong>{editing ? `Edit ${role.name}` : 'Create role'}</strong>
        <TextField label="Role name" value={role.name} disabled={editing} onChange={(e) => setRole({ ...role, name: e.target.value })} />
        <TextField label={editing ? 'New password (empty keeps existing)' : 'Password (empty generates one)'} type="password" autoComplete="new-password" value={role.password ?? ''} onChange={(e) => setRole({ ...role, password: e.target.value || undefined })} />
        {(['login', 'createdb', 'createrole', 'superuser', 'replication', 'bypassrls'] as const).map((field) => <Switch key={field} label={field} checked={role[field]} disabled={['superuser', 'replication', 'bypassrls'].includes(field) && !can('update')} onChange={(e) => setRole({ ...role, [field]: e.target.checked })} />)}
        <TextField label="Connection limit (-1 is unlimited)" type="number" min={-1} value={role.connectionLimit} onChange={(e) => setRole({ ...role, connectionLimit: Number(e.target.value) })} />
        <TextField label="Member of (comma separated; e.g. pg_read_all_data, pg_monitor)" value={role.memberOf?.join(', ') ?? ''} onChange={(e) => setRole({ ...role, memberOf: e.target.value.split(',').map((v) => v.trim()).filter(Boolean) })} />
        <TextField label="Valid until (RFC 3339 timestamp, optional)" value={role.validUntil ?? ''} onChange={(e) => setRole({ ...role, validUntil: e.target.value || undefined })} />
        <div className="row"><Button variant="primary" disabled={!role.name.trim() || !Number.isInteger(role.connectionLimit) || role.connectionLimit < -1 || Boolean(role.validUntil && !Number.isFinite(Date.parse(role.validUntil)))} onClick={() => void run(async () => {
          if (editing) await unwrap(api.PUT('/clusters/{cluster}/postgres/roles/{role}', { headers, params: { ...params, path: { cluster: name, role: role.name } }, body: role }));
          else { const result = await unwrap(api.POST('/clusters/{cluster}/postgres/roles', { headers, params, body: role })); if (result.generatedPassword) setPassword(result.generatedPassword); }
          setRole({ ...EMPTY_ROLE }); setEditing(false);
        })}>{editing ? 'Save role' : 'Create role'}</Button>{editing ? <Button onClick={() => { setEditing(false); setRole({ ...EMPTY_ROLE }); }}>Cancel editing</Button> : null}</div>
      </fieldset>
    </Card>
    <Card title="Databases">
      {databases.error ? <ErrorBlock error={databases.error} retry={() => databases.refetch()} /> : null}
      <div className="table-wrap"><table className="table"><thead><tr><th>Database</th><th>Owner</th><th>Encoding / locale</th><th>Connections</th><th /></tr></thead><tbody>{databases.data?.items.map((d) => <tr key={d.name}><td className="mono">{d.name}</td><td>{d.owner}</td><td>{d.encoding} / {d.locale}</td><td>{d.allowConnections ? 'allowed' : 'disabled'}</td><td><Button size="sm" disabled={!writable || busy || !can('delete', 'database-users')} onClick={() => { setForce(false); setDeletion({ kind: 'database', name: d.name }); }}>Delete</Button></td></tr>)}</tbody></table></div>
      <fieldset disabled={!writable || busy || !can('create', 'database-users')} className="stack" style={{ border: 0, padding: 0 }}>
        {(['name', 'owner', 'encoding', 'locale'] as const).map((field) => <TextField key={field} label={`Database ${field}`} value={database[field]} onChange={(e) => setDatabase({ ...database, [field]: e.target.value })} />)}
        <TextField label="Extensions (optional, comma separated)" value={extensionsText} onChange={(e) => setExtensionsText(e.target.value)} />
        <Switch label="Allow connections" checked={database.allowConnections} onChange={(e) => setDatabase({ ...database, allowConnections: e.target.checked })} />
        <Button variant="primary" disabled={!database.name.trim() || !database.owner.trim()} onClick={() => void run(async () => { const extensions = extensionsText.split(',').map((v) => v.trim()).filter(Boolean); await unwrap(api.POST('/clusters/{cluster}/postgres/databases', { headers, params, body: { ...database, extensions } })); setDatabase({ ...database, name: '' }); })}>Create database</Button>
      </fieldset>
    </Card>
    <Card title="Grants">
      <SelectField label="Inspect database" value={selectedDatabase} onChange={(e) => { setSelectedDatabase(e.target.value); setGrant({ ...grant, database: e.target.value }); }}><option value="">Choose database</option>{databases.data?.items.map((d) => <option key={d.name} value={d.name}>{d.name}</option>)}</SelectField>
      {grants.error ? <ErrorBlock error={grants.error} retry={() => grants.refetch()} /> : null}
      {grants.data?.items.map((g, i) => <div className="row-between" key={i}><code>{g.role} → {g.database}{g.schema ? `.${g.schema}` : ''}: {g.privileges.join(', ')}</code><Button size="sm" disabled={!writable || busy || !can('update', 'database-users')} onClick={() => void run(async () => { await unwrap(api.DELETE('/clusters/{cluster}/postgres/grants', { headers, params, body: g })); })}>Revoke</Button></div>)}
      <fieldset disabled={!writable || busy || !can('update', 'database-users')} className="stack" style={{ border: 0, padding: 0 }}>
        <TextField label="Grantee role" value={grant.role} onChange={(e) => setGrant({ ...grant, role: e.target.value })} />
        <TextField label="Database" value={grant.database} onChange={(e) => setGrant({ ...grant, database: e.target.value })} />
        <TextField label="Schema (optional)" value={grant.schema ?? ''} onChange={(e) => setGrant({ ...grant, schema: e.target.value || undefined, privileges: e.target.value ? ['USAGE'] : ['CONNECT'] })} />
        {(grant.schema ? ['USAGE', 'CREATE'] as const : ['CONNECT', 'CREATE', 'TEMP'] as const).map((p) => <Switch key={p} label={p} checked={grant.privileges.includes(p)} onChange={(e) => setGrant({ ...grant, privileges: e.target.checked ? [...grant.privileges, p] : grant.privileges.filter((v) => v !== p) })} />)}
        <Button variant="primary" disabled={!grant.role.trim() || !grant.database.trim() || !grant.privileges.length} onClick={() => void run(async () => { await unwrap(api.POST('/clusters/{cluster}/postgres/grants', { headers, params, body: grant })); setSelectedDatabase(grant.database); })}>Grant privileges</Button>
      </fieldset>
    </Card>
    <ConfirmDialog open={Boolean(deletion)} title={`Delete ${deletion?.kind} ${deletion?.name}`} confirmLabel="Delete" onClose={() => setDeletion(null)} busy={busy} onConfirm={() => void run(async () => {
      if (!deletion) return;
      if (deletion.kind === 'role') await unwrap(api.DELETE('/clusters/{cluster}/postgres/roles/{role}', { headers, params: { ...params, path: { cluster: name, role: deletion.name } } }));
      else await unwrap(api.DELETE('/clusters/{cluster}/postgres/databases/{database}', { headers, params: { path: { cluster: name, database: deletion.name }, query: { namespace, force } } }));
      setDeletion(null);
    })}><Callout tone="danger">This operation cannot be undone. Database deletion permanently removes its data.</Callout>{deletion?.kind === 'database' ? <Switch label="Force deletion (terminate existing connections)" checked={force} onChange={(e) => setForce(e.target.checked)} /> : null}</ConfirmDialog>
  </div>;
}
