// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { idempotencyHeaders, useIdempotencyKey } from '@/api/idempotency';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog, Dialog } from '@/components/ui/Dialog';
import { TextAreaField, TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, CodeLine, ErrorBlock, Loading, Menu, MenuItem, Time } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { OperationNotice } from '@/components/OperationNotice';
import { useClusterContext } from './ClusterDetail';
import { assignedRoles, assignedUsers, passwordValid } from './detail-state';
import { useSubmissionKey } from './use-submission-key';

type DBUser = Schemas['DatabaseUser'];
type DBRole = Schemas['DatabaseRole'];
type RoleRef = Schemas['RoleReference'];

const BUILTIN_ROLES = ['read', 'readWrite', 'dbAdmin', 'dbOwner', 'userAdmin', 'readAnyDatabase', 'readWriteAnyDatabase', 'clusterMonitor', 'backup', 'restore'];

function parseRoles(text: string): RoleRef[] {
  return text
    .split(/[\n,]+/)
    .map((s) => s.trim())
    .filter(Boolean)
    .map((s) => {
      const at = s.lastIndexOf('@');
      return at > 0 ? { role: s.slice(0, at), db: s.slice(at + 1) } : { role: s, db: '' };
    });
}

function rolesText(roles: RoleRef[]): string {
  return roles.map((r) => `${r.role}@${r.db}`).join('\n');
}

export function UsersTab() {
  const { namespace, name, can } = useClusterContext();
  const toast = useToast();
  const queryClient = useQueryClient();
  const users = useQuery({ queryKey: keys.clusterSub(namespace, name, 'database-users'), queryFn: () => unwrap(api.GET('/clusters/{cluster}/database-users', { params: { path: { cluster: name }, query: { namespace, limit: 500 } } })) });
  const roles = useQuery({ queryKey: keys.clusterSub(namespace, name, 'database-roles'), queryFn: () => unwrap(api.GET('/clusters/{cluster}/database-roles', { params: { path: { cluster: name }, query: { namespace, limit: 500 } } })) });
  const system = useQuery({ queryKey: keys.clusterSub(namespace, name, 'system-users'), queryFn: () => unwrap(api.GET('/clusters/{cluster}/system-users', { params: { path: { cluster: name }, query: { namespace } } })) });

  const [userDialog, setUserDialog] = useState<null | { mode: 'create' } | { mode: 'edit'; user: DBUser }>(null);
  const [roleDialog, setRoleDialog] = useState<null | { mode: 'create' } | { mode: 'edit'; role: DBRole }>(null);
  const [deleteUser, setDeleteUser] = useState<DBUser | null>(null);
  const [deleteRole, setDeleteRole] = useState<DBRole | null>(null);
  const [rotate, setRotate] = useState<Schemas['SystemUser'] | null>(null);
  const submission = useIdempotencyKey();

  const canWrite = can('update', 'database-users') || can('create', 'database-users');
  const canDelete = can('delete', 'database-users');

  const invalidate = () => {
    void queryClient.invalidateQueries({ queryKey: keys.clusterSub(namespace, name, 'database-users') });
    void queryClient.invalidateQueries({ queryKey: keys.clusterSub(namespace, name, 'database-roles') });
    void queryClient.invalidateQueries({ queryKey: keys.clusterSub(namespace, name, 'system-users') });
  };

  const delUser = useMutation({
    mutationFn: (u: DBUser) => unwrap(api.DELETE('/clusters/{cluster}/database-users/{database}/{username}', { params: { path: { cluster: name, database: u.database, username: u.username }, query: { namespace } } })),
    onSuccess: () => {
      toast.success('User deleted.');
      setDeleteUser(null);
      invalidate();
    },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const delRole = useMutation({
    mutationFn: (r: DBRole) => unwrap(api.DELETE('/clusters/{cluster}/database-roles/{database}/{role}', { params: { path: { cluster: name, database: r.database, role: r.name }, query: { namespace } } })),
    onSuccess: () => {
      toast.success('Role deleted.');
      setDeleteRole(null);
      invalidate();
    },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const rotateUser = useMutation({
    mutationFn: (u: Schemas['SystemUser']) => unwrap(api.POST('/clusters/{cluster}/system-users/{user}/rotate', { headers: idempotencyHeaders(submission.key), params: { path: { cluster: name, user: u.name }, query: { namespace } } })),
    onSuccess: (op) => {
      submission.reset();
      toast.success(<OperationNotice operationId={op.id}>Rotation started.</OperationNotice>);
      setRotate(null);
      invalidate();
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  return (
    <div className="stack" style={{ gap: 16 }}>
      <Card
        title="Application users"
        flush
        actions={
          can('create', 'database-users') ? (
            <Button size="sm" variant="primary" onClick={() => setUserDialog({ mode: 'create' })}>
              + Add user
            </Button>
          ) : null
        }
      >
        {users.isPending ? (
          <Loading />
        ) : users.error ? (
          <div className="card-body">
            <ErrorBlock error={users.error} retry={() => users.refetch()} />
          </div>
        ) : users.data.items.length === 0 ? (
          <div className="table-empty">No application users. System users are managed separately below.</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>User</th>
                  <th>Auth database</th>
                  <th>Roles</th>
                  <th className="actions" />
                </tr>
              </thead>
              <tbody>
                {users.data.items.map((u) => (
                  <tr key={`${u.database}/${u.username}`}>
                    <td className="mono">{u.username}</td>
                    <td className="mono">{u.database}</td>
                    <td>
                      <span className="row wrap" style={{ gap: 4 }}>
                        {u.roles.map((r) => (
                          <Badge key={`${r.role}@${r.db}`} plain>
                            {r.role}@{r.db}
                          </Badge>
                        ))}
                      </span>
                    </td>
                    <td className="actions">
                      {canWrite || canDelete ? (
                        <Menu>
                          <MenuItem disabled={!can('update', 'database-users')} onClick={() => setUserDialog({ mode: 'edit', user: u })}>
                            Edit roles / password…
                          </MenuItem>
                          <MenuItem danger disabled={!canDelete} onClick={() => setDeleteUser(u)}>
                            Delete…
                          </MenuItem>
                        </Menu>
                      ) : null}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      <Card
        title="Custom roles"
        flush
        actions={
          can('create', 'database-users') ? (
            <Button size="sm" onClick={() => setRoleDialog({ mode: 'create' })}>
              + Add role
            </Button>
          ) : null
        }
      >
        {roles.isPending ? (
          <Loading />
        ) : roles.error ? (
          <div className="card-body">
            <ErrorBlock error={roles.error} retry={() => roles.refetch()} />
          </div>
        ) : roles.data.items.length === 0 ? (
          <div className="table-empty">No custom roles. Built-in roles ({BUILTIN_ROLES.slice(0, 4).join(', ')}, …) are always available.</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Role</th>
                  <th>Database</th>
                  <th>Privileges</th>
                  <th>Inherits</th>
                  <th className="actions" />
                </tr>
              </thead>
              <tbody>
                {roles.data.items.map((r) => (
                  <tr key={`${r.database}/${r.name}`}>
                    <td className="mono">{r.name}</td>
                    <td className="mono">{r.database}</td>
                    <td className="small">
                      {r.privileges.map((p, i) => (
                        <div key={i} className="mono xs">
                          {p.resource.anyResource ? 'anyResource' : p.resource.cluster ? 'cluster' : `${p.resource.db ?? ''}.${p.resource.collection ?? '*'}`}: {p.actions.join(', ')}
                        </div>
                      ))}
                    </td>
                    <td className="small">{r.roles.map((x) => `${x.role}@${x.db}`).join(', ') || '—'}</td>
                    <td className="actions">
                      {canWrite || canDelete ? (
                        <Menu>
                          <MenuItem disabled={!can('update', 'database-users')} onClick={() => setRoleDialog({ mode: 'edit', role: r })}>
                            Edit…
                          </MenuItem>
                          <MenuItem danger disabled={!canDelete} onClick={() => setDeleteRole(r)}>
                            Delete…
                          </MenuItem>
                        </Menu>
                      ) : null}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      <Card title="System users" flush>
        {system.isPending ? (
          <Loading />
        ) : system.error ? (
          <div className="card-body">
            <ErrorBlock error={system.error} retry={() => system.refetch()} />
          </div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Purpose</th>
                  <th>Username</th>
                  <th>Roles</th>
                  <th>Last rotated</th>
                  <th className="actions" />
                </tr>
              </thead>
              <tbody>
                {system.data.items.map((u) => (
                  <tr key={u.name}>
                    <td className="mono">{u.name}</td>
                    <td className="mono">{u.username}</td>
                    <td className="small">{u.roles.map((r) => `${r.role}@${r.db}`).join(', ')}</td>
                    <td className="small">
                      <Time iso={u.lastRotatedAt} />
                    </td>
                    <td className="actions">
                      {can('update') ? (
                        <Button size="sm" onClick={() => setRotate(u)}>
                          Rotate password…
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

      {userDialog ? <UserDialog namespace={namespace} cluster={name} existing={userDialog.mode === 'edit' ? userDialog.user : undefined} onClose={() => setUserDialog(null)} onSaved={invalidate} /> : null}
      {roleDialog ? <RoleDialog namespace={namespace} cluster={name} existing={roleDialog.mode === 'edit' ? roleDialog.role : undefined} onClose={() => setRoleDialog(null)} onSaved={invalidate} /> : null}

      <ConfirmDialog open={deleteUser !== null} onClose={() => { setDeleteUser(null); delUser.reset(); }} onConfirm={() => deleteUser && delUser.mutateAsync(deleteUser)} title={`Delete user ${deleteUser?.username ?? ''}`} confirmLabel="Delete" danger busy={delUser.isPending} error={delUser.error}>
        <p className="small">Open connections authenticated as this user are closed.</p>
      </ConfirmDialog>
      <ConfirmDialog open={deleteRole !== null} onClose={() => { setDeleteRole(null); delRole.reset(); }} onConfirm={() => deleteRole && delRole.mutateAsync(deleteRole)} title={`Delete role ${deleteRole?.name ?? ''}`} confirmLabel="Delete" danger busy={delRole.isPending} error={delRole.error}>
        <p className="small">Remove this role's assignments from all users and roles before deleting it. Deletion is refused while any assignments remain; privileges are never removed automatically.</p>
        {deleteRole && users.data ? <p className="small">Assigned users: {assignedUsers(users.data.items, deleteRole).join(', ') || 'none'}</p> : <p className="small">User assignments could not be loaded; review them before deleting.</p>}
        {deleteRole && roles.data ? <p className="small">Dependent roles: {assignedRoles(roles.data.items, deleteRole).join(', ') || 'none'}</p> : <p className="small">Role assignments could not be loaded; review them before deleting.</p>}
      </ConfirmDialog>
      <ConfirmDialog open={rotate !== null} onClose={() => { setRotate(null); rotateUser.reset(); submission.reset(); }} onConfirm={() => rotate && rotateUser.mutateAsync(rotate)} title={`Rotate ${rotate?.name ?? ''} password`} confirmLabel="Rotate" busy={rotateUser.isPending} error={rotateUser.error}>
        <p className="small">
          A new random password is generated and propagated to every component that uses it (agents, backup agent, monitoring). The new value is visible through Reveal credentials on the overview.
        </p>
      </ConfirmDialog>
    </div>
  );
}

function UserDialog({ namespace, cluster, existing, onClose, onSaved }: { namespace: string; cluster: string; existing?: DBUser; onClose: () => void; onSaved: () => void }) {
  const toast = useToast();
  const [username, setUsername] = useState(existing?.username ?? '');
  const [database, setDatabase] = useState(existing?.database ?? 'admin');
  const [password, setPassword] = useState('');
  const [generatedPassword, setGeneratedPassword] = useState<string | null>(null);
  const [roles, setRoles] = useState(existing ? rolesText(existing.roles) : 'readWrite@');
  const submission = useSubmissionKey(JSON.stringify({ username, database, password, roles }));
  const parsed = parseRoles(roles);
  const rolesOk = parsed.length > 0 && parsed.every((r) => r.role && r.db);
  const valid = username.trim() && database.trim() && rolesOk && passwordValid(password);

  const save = useMutation({
    mutationFn: () =>
      existing
        ? unwrap(
            api.PUT('/clusters/{cluster}/database-users/{database}/{username}', {
              headers: idempotencyHeaders(submission.key),
              params: { path: { cluster, database: existing.database, username: existing.username }, query: { namespace } },
              body: { roles: parsed, ...(password ? { password } : {}) },
            }),
          )
        : unwrap(api.POST('/clusters/{cluster}/database-users', { headers: idempotencyHeaders(submission.key), params: { path: { cluster }, query: { namespace } }, body: { username: username.trim(), database: database.trim(), ...(password ? { password } : {}), roles: parsed } })),
    onSuccess: (user) => {
      submission.reset();
      toast.success(existing ? 'User updated.' : 'User created.');
      onSaved();
      if (!existing && 'generatedPassword' in user && typeof user.generatedPassword === 'string' && user.generatedPassword) {
        setGeneratedPassword(user.generatedPassword);
      } else {
        onClose();
      }
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  if (generatedPassword) {
    return (
      <Dialog open onClose={onClose} title="Database user created — copy password now" footer={<Button onClick={onClose}>Done</Button>}>
        <div className="stack">
          <Callout tone="warn">This generated password is shown only once. Store it in your secret manager before closing this dialog.</Callout>
          <p className="small">User <span className="mono">{username.trim()}</span> authenticates against <span className="mono">{database.trim()}</span>.</p>
          <CodeLine value={generatedPassword} />
        </div>
      </Dialog>
    );
  }

  return (
    <Dialog
      open
      onClose={onClose}
      title={existing ? `Edit ${existing.username}` : 'Add database user'}
      busy={save.isPending}
      error={save.error}
      onSubmit={() => valid && save.mutateAsync()}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={save.isPending} disabled={!valid}>
            {existing ? 'Save' : 'Create user'}
          </Button>
        </>
      }
    >
      <div className="grid-2">
        <TextField label="Username" mono value={username} onChange={(e) => setUsername(e.target.value)} required disabled={Boolean(existing)} autoFocus={!existing} />
        <TextField label="Authentication database" mono value={database} onChange={(e) => setDatabase(e.target.value)} required disabled={Boolean(existing)} hint="Usually admin or the application database." />
      </div>
      <TextField
        label={existing ? 'New password (leave empty to keep)' : 'Password (leave empty to generate)'}
        type="password"
        autoComplete="new-password"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
        hint="At least 8 characters when supplied; MongoDB SASLprep supports Unicode 3.2 characters, not emoji or control characters. Leave empty to generate on creation or keep the current password when editing."
        error={!passwordValid(password) ? 'Use at least 8 characters.' : undefined}
      />
      <TextAreaField
        label="Roles (role@database, one per line)"
        mono
        rows={4}
        value={roles}
        onChange={(e) => setRoles(e.target.value)}
        hint={`Built-in: ${BUILTIN_ROLES.join(', ')}. Custom roles by name.`}
        error={roles.trim() && !rolesOk ? 'Each entry needs the form role@database.' : undefined}
      />
    </Dialog>
  );
}

function RoleDialog({ namespace, cluster, existing, onClose, onSaved }: { namespace: string; cluster: string; existing?: DBRole; onClose: () => void; onSaved: () => void }) {
  const toast = useToast();
  const [name, setName] = useState(existing?.name ?? '');
  const [database, setDatabase] = useState(existing?.database ?? 'admin');
  const [privText, setPrivText] = useState(existing ? existing.privileges.map((p) => `${p.resource.anyResource ? 'anyResource' : p.resource.cluster ? 'cluster' : `${p.resource.db ?? ''}.${p.resource.collection ?? ''}`} ${p.actions.join(',')}`).join('\n') : '');
  const [inherits, setInherits] = useState(existing ? rolesText(existing.roles) : '');
  const submission = useSubmissionKey(JSON.stringify({ name, database, privText, inherits }));

  const parsePrivileges = (): Schemas['Privilege'][] | null => {
    const out: Schemas['Privilege'][] = [];
    for (const raw of privText.split('\n')) {
      const line = raw.trim();
      if (!line) continue;
      const [res, acts] = line.split(/\s+/, 2);
      if (!res || !acts) return null;
      const actions = acts
        .split(',')
        .map((a) => a.trim())
        .filter(Boolean);
      if (!actions.length) return null;
      let resource: Schemas['PrivilegeResource'];
      if (res === 'cluster') resource = { cluster: true };
      else if (res === 'anyResource') resource = { anyResource: true };
      else {
        const dot = res.indexOf('.');
        resource = dot < 0 ? { db: res, collection: '' } : { db: res.slice(0, dot), collection: res.slice(dot + 1) };
      }
      out.push({ resource, actions });
    }
    return out;
  };
  const privileges = parsePrivileges();
  const inherited = parseRoles(inherits);
  const valid = /^[A-Za-z0-9_-]+$/.test(name) && database.trim() && privileges !== null && (privileges.length > 0 || inherited.length > 0) && inherited.every((r) => r.role && r.db);

  const save = useMutation({
    mutationFn: () => {
      const body = { privileges: privileges ?? [], roles: inherited };
      return existing
        ? unwrap(api.PUT('/clusters/{cluster}/database-roles/{database}/{role}', { headers: idempotencyHeaders(submission.key), params: { path: { cluster, database: existing.database, role: existing.name }, query: { namespace } }, body }))
        : unwrap(api.POST('/clusters/{cluster}/database-roles', { headers: idempotencyHeaders(submission.key), params: { path: { cluster }, query: { namespace } }, body: { name, database: database.trim(), ...body } }));
    },
    onSuccess: () => {
      submission.reset();
      toast.success(existing ? 'Role updated.' : 'Role created.');
      onSaved();
      onClose();
    },
  });

  return (
    <Dialog
      open
      onClose={onClose}
      title={existing ? `Edit role ${existing.name}` : 'Add custom role'}
      busy={save.isPending}
      error={save.error}
      wide
      onSubmit={() => valid && save.mutateAsync()}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={save.isPending} disabled={!valid}>
            {existing ? 'Save' : 'Create role'}
          </Button>
        </>
      }
    >
      <div className="grid-2">
        <TextField label="Role name" mono value={name} onChange={(e) => setName(e.target.value)} required disabled={Boolean(existing)} autoFocus={!existing} />
        <TextField label="Database" mono value={database} onChange={(e) => setDatabase(e.target.value)} required disabled={Boolean(existing)} />
      </div>
      <TextAreaField
        label="Privileges (resource actions, one per line)"
        mono
        rows={5}
        value={privText}
        onChange={(e) => setPrivText(e.target.value)}
        hint="Resource: db.collection (empty collection = whole db), cluster, or anyResource. Actions comma-separated, e.g. `shop.orders find,insert,update`."
        error={privText.trim() && privileges === null ? 'Each line: <resource> <action,action>' : undefined}
      />
      <TextAreaField label="Inherited roles (role@database, one per line)" mono rows={3} value={inherits} onChange={(e) => setInherits(e.target.value)} />
    </Dialog>
  );
}
