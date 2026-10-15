// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useState, type ReactNode } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog, Dialog } from '@/components/ui/Dialog';
import { Switch, TextAreaField, TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, CodeLine, CopyButton, ErrorBlock, KV, Loading, Menu, MenuItem, Time, type Tone } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { useClusterContext } from '@/pages/clusters/detail/ClusterDetail';
import { hasCapability } from '../index';
import { COMPONENT_LABEL, EXPIRATION_RE, ROLE_NAME_RE, USERNAME_RE, formatIndexPrivileges, memberKind, parseIndexPrivileges, parseRoleDescriptors, validUserPassword, visibleElasticRoles, type MemberKind } from './spec';

import { applicationDrafts, applicationGrants, forgetInvalidation, invalidationState, INVALIDATION_WAIT_MS, pendingInvalidations, rememberInvalidation, type ApplicationDraft, type PendingInvalidation } from './access-state';
type ElasticUser = Schemas['ElasticUser'];
type ElasticRole = Schemas['ElasticRole'];
type ElasticAPIKey = Schemas['ElasticAPIKey'];

const ENDPOINT_LABEL: Record<string, string> = { elasticsearch: 'Elasticsearch HTTPS', kibana: 'Kibana', apm: 'APM Server', 'logstash-beats': 'Logstash Beats input', 'logstash-http': 'Logstash HTTP input' };
const KIND_LABEL: Record<MemberKind, string> = { master: 'Dedicated masters', data: 'Data nodes', 'master,data': 'Nodes (master + data)', kibana: COMPONENT_LABEL.kibana, logstash: COMPONENT_LABEL.logstash, apm: COMPONENT_LABEL.apm };
const KIND_ORDER: MemberKind[] = ['master,data', 'master', 'data', 'kibana', 'logstash', 'apm'];

function list(text: string): string[] {
  return text
    .split(/[\n,]+/)
    .map((s) => s.trim())
    .filter(Boolean);
}

export function ElasticAccessTab() {
  const { cluster, provider, namespace, name, can } = useClusterContext();
  const toast = useToast();
  const queryClient = useQueryClient();
  const [pendingKeys, setPendingKeys] = useState<PendingInvalidation[]>(() => pendingInvalidations(sessionStorage, namespace, name));
  const [now, setNow] = useState(Date.now);
  const checkingKeys = pendingKeys.some((marker) => now - marker.startedAt < INVALIDATION_WAIT_MS);
  const path = { params: { path: { cluster: name }, query: { namespace, limit: 500 } } };
  const users = useQuery({ queryKey: keys.clusterSub(namespace, name, 'elastic-users'), queryFn: () => unwrap(api.GET('/clusters/{cluster}/elasticsearch/users', path)) });
  const roles = useQuery({ queryKey: keys.clusterSub(namespace, name, 'elastic-roles'), queryFn: () => unwrap(api.GET('/clusters/{cluster}/elasticsearch/roles', path)) });
  const apiKeys = useQuery({ queryKey: keys.clusterSub(namespace, name, 'elastic-api-keys'), queryFn: () => unwrap(api.GET('/clusters/{cluster}/elasticsearch/api-keys', path)), refetchInterval: checkingKeys ? 2000 : false });

  const [userDialog, setUserDialog] = useState<null | { user?: ElasticUser }>(null);
  const [roleDialog, setRoleDialog] = useState<null | { role?: ElasticRole }>(null);
  const [showBuiltInRoles, setShowBuiltInRoles] = useState(false);
  const [keyDialog, setKeyDialog] = useState(false);
  const [deleteUser, setDeleteUser] = useState<ElasticUser | null>(null);
  const [deleteRole, setDeleteRole] = useState<ElasticRole | null>(null);
  const [invalidateKey, setInvalidateKey] = useState<ElasticAPIKey | null>(null);

  const canCreate = can('create', 'database-users');
  const canUpdate = can('update', 'database-users');
  const canDelete = can('delete', 'database-users');
  const invalidate = (sub: 'elastic-users' | 'elastic-roles' | 'elastic-api-keys') => void queryClient.invalidateQueries({ queryKey: keys.clusterSub(namespace, name, sub) });

  const delUser = useMutation({
    mutationFn: (u: ElasticUser) => unwrap(api.DELETE('/clusters/{cluster}/elasticsearch/users/{username}', { params: { path: { cluster: name, username: u.username }, query: { namespace } } })),
    onSuccess: () => {
      toast.success('User deleted.');
      setDeleteUser(null);
      invalidate('elastic-users');
    },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const delRole = useMutation({
    mutationFn: (r: ElasticRole) => unwrap(api.DELETE('/clusters/{cluster}/elasticsearch/roles/{role}', { params: { path: { cluster: name, role: r.name }, query: { namespace } } })),
    onSuccess: () => {
      toast.success('Role deleted.');
      setDeleteRole(null);
      invalidate('elastic-roles');
    },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const delKey = useMutation({
    mutationFn: (k: ElasticAPIKey) => {
      const marker = { id: k.id, startedAt: Date.now() };
      rememberInvalidation(sessionStorage, namespace, name, marker);
      setPendingKeys((items) => [...items.filter((item) => item.id !== k.id), marker]);
      return unwrap(api.DELETE('/clusters/{cluster}/elasticsearch/api-keys/{id}', { params: { path: { cluster: name, id: k.id }, query: { namespace } } }));
    },
    onSuccess: () => {
      setInvalidateKey(null);
      invalidate('elastic-api-keys');
    },
    onError: (e, key) => {
      const marker = { id: key.id, startedAt: 0 };
      rememberInvalidation(sessionStorage, namespace, name, marker);
      setPendingKeys((items) => items.map((item) => item.id === key.id ? marker : item));
      toast.error(errorMessage(e));
      invalidate('elastic-api-keys');
    },
  });

  useEffect(() => {
    if (!checkingKeys) return;
    const timer = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(timer);
  }, [checkingKeys]);

  useEffect(() => {
    const items = apiKeys.data?.items;
    if (!items || apiKeys.isFetching) return;
    const completed = pendingKeys.filter((marker) => invalidationState(marker, items.find((key) => key.id === marker.id), Date.now()) === 'invalidated');
    if (!completed.length) return;
    for (const marker of completed) {
      forgetInvalidation(sessionStorage, namespace, name, marker.id);
      toast.success(`API key ${marker.id} invalidated.`);
    }
    setPendingKeys((items) => items.filter((item) => !completed.some((marker) => marker.id === item.id)));
  }, [apiKeys.data, apiKeys.isFetching, pendingKeys, namespace, name, toast]);

  const roleItems = roles.data?.items ?? [];
  const visibleRoles = visibleElasticRoles(roleItems, showBuiltInRoles);
  const builtInRoleCount = roleItems.reduce((count, role) => count + (role.reserved ? 1 : 0), 0);
  const roleNames = roleItems.map((r) => r.name);
  const accessible = hasCapability(provider, 'elasticAccess');

  return (
    <div className="stack" style={{ gap: 16 }}>
      <div className="grid-2">
        <StackTopology members={cluster.status.members} />
        <Card title="Endpoints">
          <ElasticEndpoints connection={cluster.status.connection} members={cluster.status.members} phase={cluster.status.phase} />
          <CredentialsReveal namespace={namespace} name={name} allowed={can('read', 'cluster-credentials')} />
        </Card>
      </div>

      {!accessible ? <Callout tone="info">Native users, roles and API keys are managed through the Elasticsearch security API once the provider reports the elasticAccess capability.</Callout> : null}

      <Card
        title="Users"
        flush
        actions={
          canCreate ? (
            <Button size="sm" variant="primary" onClick={() => setUserDialog({})}>
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
          <div className="table-empty">No native users. The built-in elastic superuser is available under Reveal credentials.</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>User</th>
                  <th>Full name</th>
                  <th>Roles</th>
                  <th>Status</th>
                  <th className="actions" />
                </tr>
              </thead>
              <tbody>
                {users.data.items.map((u) => (
                  <tr key={u.username}>
                    <td className="mono">{u.username}</td>
                    <td className="small">
                      {u.fullName || '—'}
                      {u.email ? <span className="faint xs"> · {u.email}</span> : null}
                    </td>
                    <td>
                      <span className="row wrap" style={{ gap: 4 }}>
                        {u.roles.map((r) => (
                          <Badge key={r} plain>
                            {r}
                          </Badge>
                        ))}
                      </span>
                    </td>
                    <td>
                      <Badge tone={u.enabled ? 'ok' : 'warn'} plain>
                        {u.enabled ? 'enabled' : 'disabled'}
                      </Badge>
                    </td>
                    <td className="actions">
                      {canUpdate || canDelete ? (
                        <Menu>
                          <MenuItem disabled={!canUpdate} onClick={() => setUserDialog({ user: u })}>
                            Edit…
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
        title="Roles"
        flush
        actions={
          <div className="row wrap" style={{ gap: 12 }}>
            {builtInRoleCount > 0 ? <Switch label={`Show built-in roles (${builtInRoleCount})`} checked={showBuiltInRoles} onChange={(e) => setShowBuiltInRoles(e.target.checked)} /> : null}
            {canCreate ? (
              <Button size="sm" onClick={() => setRoleDialog({})}>
                + Add role
              </Button>
            ) : null}
          </div>
        }
      >
        {roles.isPending ? (
          <Loading />
        ) : roles.error ? (
          <div className="card-body">
            <ErrorBlock error={roles.error} retry={() => roles.refetch()} />
          </div>
        ) : visibleRoles.length === 0 ? (
          <div className="table-empty">{builtInRoleCount > 0 && !showBuiltInRoles ? 'No custom roles. Show built-in roles to view the reserved roles.' : 'No roles reported.'}</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Role</th>
                  <th>Cluster privileges</th>
                  <th>Index privileges</th>
                  <th className="actions" />
                </tr>
              </thead>
              <tbody>
                {visibleRoles.map((r) => (
                  <tr key={r.name}>
                    <td>
                      <span className="row" style={{ gap: 6 }}>
                        <span className="mono">{r.name}</span>
                        {r.reserved ? (
                          <Badge plain tone="info">
                            built-in
                          </Badge>
                        ) : null}
                      </span>
                    </td>
                    <td className="small mono">{r.cluster.join(', ') || '—'}</td>
                    <td className="small">
                      {r.indices.map((p, i) => (
                        <div key={i} className="mono xs">
                          {p.names.join(', ')}: {p.privileges.join(', ')}
                          {p.allowRestrictedIndices ? ' (restricted)' : ''}
                          {p.query !== undefined ? <div>Document query: {p.query}</div> : null}
                          {p.fieldSecurity !== undefined ? <div>Field security: {JSON.stringify(p.fieldSecurity)}</div> : null}
                        </div>
                      ))}
                      {r.indices.length === 0 ? '—' : null}
                      {r.applications.length ? <div className="xs faint">+ {r.applications.length} application privilege{r.applications.length > 1 ? 's' : ''}</div> : null}
                    </td>
                    <td className="actions">
                      {!r.reserved && (canUpdate || canDelete) ? (
                        <Menu>
                          <MenuItem disabled={!canUpdate} onClick={() => setRoleDialog({ role: r })}>
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

      <Card
        title="API keys"
        flush
        actions={
          canCreate ? (
            <Button size="sm" onClick={() => setKeyDialog(true)}>
              + Create API key
            </Button>
          ) : null
        }
      >
        {pendingKeys.length ? <Callout tone="info">{checkingKeys ? 'Invalidating… Checking API-key status; this continues after a reload.' : 'Invalidation is not confirmed. If the key is still active, retry invalidation below.'}</Callout> : null}
        {apiKeys.isPending ? (
          <Loading />
        ) : apiKeys.error ? (
          <div className="card-body">
            <ErrorBlock error={apiKeys.error} retry={() => apiKeys.refetch()} />
          </div>
        ) : apiKeys.data.items.length === 0 ? (
          <div className="table-empty">No API keys created through Karakoram. Keys are shown once at creation and can be invalidated here.</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>ID</th>
                  <th>Owner</th>
                  <th>Created</th>
                  <th>Expires</th>
                  <th>Status</th>
                  <th className="actions" />
                </tr>
              </thead>
              <tbody>
                {apiKeys.data.items.map((k) => {
                  const expired = k.expiration !== undefined && k.expiration < Date.now();
                  const revocation = invalidationState(pendingKeys.find((marker) => marker.id === k.id), k, now);
                  const tone: Tone = k.invalidated ? 'neutral' : expired ? 'warn' : 'ok';
                  return (
                    <tr key={k.id}>
                      <td className="mono">{k.name}</td>
                      <td className="mono xs">{k.id}</td>
                      <td className="mono small">{k.username}</td>
                      <td className="small">
                        <Time iso={new Date(k.creation).toISOString()} />
                      </td>
                      <td className="small">{k.expiration === undefined ? 'never' : <Time iso={new Date(k.expiration).toISOString()} />}</td>
                      <td>
                        <Badge tone={tone} plain>
                          {revocation === 'checking' ? 'Invalidating…' : revocation === 'retry' ? 'still active — retry invalidation' : k.invalidated ? 'invalidated' : expired ? 'expired' : 'active'}
                        </Badge>
                      </td>
                      <td className="actions">
                        {canDelete && !k.invalidated ? (
                          <Button size="sm" disabled={revocation === 'checking'} onClick={() => setInvalidateKey(k)}>
                            {revocation === 'retry' ? 'Retry invalidation…' : 'Invalidate…'}
                          </Button>
                        ) : null}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {userDialog ? <UserDialog namespace={namespace} cluster={name} existing={userDialog.user} roleNames={roleNames} onClose={() => setUserDialog(null)} onSaved={() => invalidate('elastic-users')} /> : null}
      {roleDialog ? <RoleDialog namespace={namespace} cluster={name} existing={roleDialog.role} onClose={() => setRoleDialog(null)} onSaved={() => invalidate('elastic-roles')} /> : null}
      {keyDialog ? <APIKeyDialog namespace={namespace} cluster={name} onClose={() => setKeyDialog(false)} onSaved={() => invalidate('elastic-api-keys')} /> : null}

      <ConfirmDialog open={deleteUser !== null} onClose={() => setDeleteUser(null)} onConfirm={() => deleteUser && delUser.mutate(deleteUser)} title={`Delete user ${deleteUser?.username ?? ''}`} confirmLabel="Delete" danger busy={delUser.isPending}>
        <p className="small">Sessions and API keys owned by this user stop working immediately.</p>
      </ConfirmDialog>
      <ConfirmDialog open={deleteRole !== null} onClose={() => setDeleteRole(null)} onConfirm={() => deleteRole && delRole.mutate(deleteRole)} title={`Delete role ${deleteRole?.name ?? ''}`} confirmLabel="Delete" danger busy={delRole.isPending}>
        <p className="small">Users mapped to this role lose its privileges immediately.</p>
      </ConfirmDialog>
      <ConfirmDialog open={invalidateKey !== null} onClose={() => setInvalidateKey(null)} onConfirm={() => invalidateKey && delKey.mutate(invalidateKey)} title={`Invalidate API key ${invalidateKey?.name ?? ''}`} confirmLabel="Invalidate" danger busy={delKey.isPending}>
        <p className="small">Requests authenticated with this key are rejected from now on. Invalidation cannot be undone.</p>
      </ConfirmDialog>
    </div>
  );
}

function StackTopology({ members }: { members: Schemas['Member'][] }) {
  const groups = new Map<MemberKind, Schemas['Member'][]>();
  for (const m of members) {
    const kind = memberKind(m);
    if (!kind) continue;
    groups.set(kind, [...(groups.get(kind) ?? []), m]);
  }
  const elected = members.find((m) => m.state === 'elected-master');
  return (
    <Card title="Stack topology">
      {groups.size === 0 ? (
        <p className="small muted">Members appear once the first node is deployed.</p>
      ) : (
        <KV
          items={[
            ...KIND_ORDER.filter((kind) => groups.has(kind)).map((kind): [string, ReactNode] => {
              const list = groups.get(kind)!;
              const healthy = list.filter((m) => m.health).length;
              return [
                KIND_LABEL[kind],
                <span className="row wrap" style={{ gap: 6 }}>
                  <Badge tone={healthy === list.length ? 'ok' : healthy === 0 ? 'danger' : 'warn'}>
                    {healthy}/{list.length} healthy
                  </Badge>
                  <span className="mono xs faint">{list.map((m) => m.name).join(', ')}</span>
                </span>,
              ];
            }),
            ['Elected master', elected ? <span className="mono">{elected.name}</span> : <span className="faint">not yet elected</span>],
          ]}
        />
      )}
    </Card>
  );
}

export function ElasticEndpoints({ connection, members, phase }: { connection: Schemas['ConnectionSummary'] | undefined; members: Pick<Schemas['Member'], 'role' | 'state' | 'health'>[]; phase: Schemas['Cluster']['status']['phase'] }) {
  const kibana = members.filter((member) => member.role === 'kibana');
  const kibanaReady = kibana.length > 0 && kibana.every((member) => member.health && member.state === 'joined');
  const endpoints = connection?.endpoints ?? [];
  if (!endpoints.length && !connection?.hosts?.length) return <p className="small muted">Endpoints appear once the cluster is initialized.</p>;
  return (
    <div className="stack" style={{ gap: 12, marginBottom: 12 }}>
      {endpoints.map((endpoint) => (
        <div key={`${endpoint.name}:${endpoint.url}`} className="stack" style={{ gap: 4 }}>
          <strong className="small">
            {ENDPOINT_LABEL[endpoint.name] ?? endpoint.name} <Badge plain tone={endpoint.public ? 'info' : 'neutral'}>{endpoint.public ? 'Public' : 'Private'}</Badge>
          </strong>
          {endpoint.name.toLowerCase() === 'kibana' && phase === 'Suspended' ? (
            <Callout tone="info">Kibana is suspended. Resume the cluster to make it available.</Callout>
          ) : endpoint.name.toLowerCase() === 'kibana' && !kibanaReady ? (
            <Callout tone="info">Kibana is starting—retry shortly. Its link will be available once Kibana reports ready.</Callout>
          ) : endpoint.public && endpoint.url.startsWith('https://') ? (
            <span className="row" style={{ gap: 6 }}>
              <a className="mono small" href={endpoint.url} target="_blank" rel="noreferrer">
                {endpoint.url}
              </a>
              <CopyButton value={endpoint.url} />
            </span>
          ) : (
            <CodeLine value={endpoint.url} />
          )}
        </div>
      ))}
      {connection?.hosts?.length ? <KV items={[['Node hosts', <span className="mono small">{connection.hosts.join(', ')}</span>], connection.publicHosts?.length ? ['Public hosts', <span className="mono small">{connection.publicHosts.join(', ')}</span>] : null]} /> : null}
    </div>
  );
}

/** Same on-demand reveal as the overview: fetched on click, never cached, hidden again on request. */
function CredentialsReveal({ namespace, name, allowed }: { namespace: string; name: string; allowed: boolean }) {
  const [data, setData] = useState<Schemas['Connection'] | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  if (!allowed) return <p className="xs faint">Revealing the elastic password, APM secret token and CA requires the cluster-credentials read permission.</p>;
  if (!data) {
    return (
      <div className="stack" style={{ gap: 6 }}>
        <div>
          <Button
            size="sm"
            loading={busy}
            onClick={async () => {
              setBusy(true);
              setErr(null);
              try {
                setData(await unwrap(api.GET('/clusters/{cluster}/connection', { params: { path: { cluster: name }, query: { namespace } } })));
              } catch (e) {
                setErr(errorMessage(e));
              } finally {
                setBusy(false);
              }
            }}
          >
            Reveal credentials
          </Button>
        </div>
        {err ? <span className="field-error">{err}</span> : null}
        <p className="xs faint">The elastic superuser password, the APM secret token and the cluster CA are fetched on demand, never cached, and the reveal is recorded in the event log.</p>
      </div>
    );
  }
  return (
    <div className="stack">
      <div className="row-between">
        <strong className="small">Credentials</strong>
        <Button size="sm" variant="ghost" onClick={() => setData(null)}>
          Hide
        </Button>
      </div>
      <div className="table-wrap">
        <table className="table">
          <thead>
            <tr>
              <th>Credential</th>
              <th>Secret</th>
              <th className="actions">URI</th>
            </tr>
          </thead>
          <tbody>
            {data.credentials.map((c) => (
              <tr key={c.username}>
                <td className="mono">{c.username}</td>
                <td>
                  <span className="row">
                    <code className="small">{c.password}</code>
                    <CopyButton value={c.password} label="Copy" />
                  </span>
                </td>
                <td className="actions">{c.uri ? <CopyButton value={c.uri} label="Copy URI" /> : null}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {data.caCertificate ? (
        <details>
          <summary className="small">CA certificate</summary>
          <CodeLine value={data.caCertificate} />
        </details>
      ) : null}
    </div>
  );
}

function UserDialog({ namespace, cluster, existing, roleNames, onClose, onSaved }: { namespace: string; cluster: string; existing?: ElasticUser; roleNames: string[]; onClose: () => void; onSaved: () => void }) {
  const toast = useToast();
  const [username, setUsername] = useState(existing?.username ?? '');
  const [password, setPassword] = useState('');
  const [fullName, setFullName] = useState(existing?.fullName ?? '');
  const [email, setEmail] = useState(existing?.email ?? '');
  const [roles, setRoles] = useState(existing?.roles.join(', ') ?? '');
  const [enabled, setEnabled] = useState(existing?.enabled ?? true);
  const parsedRoles = list(roles);
  const passwordOk = validUserPassword(password, Boolean(existing));
  const valid = USERNAME_RE.test(username) && parsedRoles.length > 0 && passwordOk;

  const save = useMutation({
    mutationFn: () => {
      const body = { username: existing?.username ?? username, enabled, roles: parsedRoles, ...(fullName.trim() ? { fullName: fullName.trim() } : {}), ...(email.trim() ? { email: email.trim() } : {}) };
      return existing
        ? unwrap(api.PUT('/clusters/{cluster}/elasticsearch/users/{username}', { params: { path: { cluster, username: existing.username }, query: { namespace } }, body: { ...body, ...(password ? { password } : {}) } }))
        : unwrap(api.POST('/clusters/{cluster}/elasticsearch/users', { params: { path: { cluster }, query: { namespace } }, body: { ...body, password } }));
    },
    onSuccess: () => {
      toast.success(existing ? 'User updated.' : 'User created.');
      onSaved();
      onClose();
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  return (
    <Dialog
      open
      onClose={onClose}
      title={existing ? `Edit ${existing.username}` : 'Add Elasticsearch user'}
      onSubmit={() => valid && save.mutate()}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={save.isPending} disabled={!valid}>
            {existing ? 'Save' : 'Create user'}
          </Button>
        </>
      }
    >
      {save.error ? <Callout tone="danger">{errorMessage(save.error)}</Callout> : null}
      <div className="grid-2">
        <TextField label="Username" mono value={username} onChange={(e) => setUsername(e.target.value)} required disabled={Boolean(existing)} autoFocus={!existing} error={username && !USERNAME_RE.test(username) ? 'Printable ASCII without leading or trailing spaces.' : undefined} />
        <TextField
          label={existing ? 'New password (leave empty to keep)' : 'Password'}
          type="password"
          autoComplete="new-password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required={!existing}
          minLength={8}
          hint="At least 8 characters (Elasticsearch minimum)."
        />
      </div>
      <div className="grid-2">
        <TextField label="Full name (optional)" value={fullName} onChange={(e) => setFullName(e.target.value)} />
        <TextField label="Email (optional)" type="email" value={email} onChange={(e) => setEmail(e.target.value)} />
      </div>
      <TextAreaField label="Roles (comma or newline separated)" mono rows={3} value={roles} onChange={(e) => setRoles(e.target.value)} hint={roleNames.length ? `Available: ${roleNames.slice(0, 12).join(', ')}${roleNames.length > 12 ? ', …' : ''}` : 'Built-in roles such as viewer, editor, kibana_admin or custom roles created below.'} error={roles.trim() && !parsedRoles.length ? 'At least one role is required.' : undefined} />
      <Switch label="Enabled" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />
    </Dialog>
  );
}

function RoleDialog({ namespace, cluster, existing, onClose, onSaved }: { namespace: string; cluster: string; existing?: ElasticRole; onClose: () => void; onSaved: () => void }) {
  const toast = useToast();
  const [name, setName] = useState(existing?.name ?? '');
  const [clusterPrivileges, setClusterPrivileges] = useState(existing?.cluster.join(', ') ?? '');
  const [indicesText, setIndicesText] = useState(existing ? formatIndexPrivileges(existing.indices) : '');
  const [applications, setApplications] = useState<ApplicationDraft[]>(() => applicationDrafts(existing?.applications ?? []));
  const grants = applicationGrants(applications);
  const [error, setError] = useState<string | null>(null);
  const indices = parseIndexPrivileges(indicesText);
  const clusterList = list(clusterPrivileges);
  const valid = ROLE_NAME_RE.test(name) && indices !== null && grants !== null;

  const save = useMutation({
    mutationFn: () => {
      const body: ElasticRole = { name: existing?.name ?? name, cluster: clusterList, indices: indices ?? [], applications: grants ?? [] };
      return existing
        ? unwrap(api.PUT('/clusters/{cluster}/elasticsearch/roles/{role}', { params: { path: { cluster, role: existing.name }, query: { namespace } }, body }))
        : unwrap(api.POST('/clusters/{cluster}/elasticsearch/roles', { params: { path: { cluster }, query: { namespace } }, body }));
    },
    onSuccess: () => {
      toast.success(existing ? 'Role updated.' : 'Role created.');
      onSaved();
      onClose();
    },
    onError: (e) => setError(errorMessage(e)),
  });

  return (
    <Dialog
      open
      onClose={onClose}
      title={existing ? `Edit role ${existing.name}` : 'Add role'}
      wide
      onSubmit={() => valid && save.mutate()}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={save.isPending} disabled={!valid}>
            {existing ? 'Save' : 'Create role'}
          </Button>
        </>
      }
    >
      {error ? <Callout tone="danger">{error}</Callout> : null}
      <TextField label="Role name" mono value={name} onChange={(e) => setName(e.target.value)} required disabled={Boolean(existing)} autoFocus={!existing} error={name && !ROLE_NAME_RE.test(name) ? 'Letters, digits, _ . and - only.' : undefined} />
      <TextField label="Cluster privileges (comma-separated)" mono value={clusterPrivileges} onChange={(e) => setClusterPrivileges(e.target.value)} hint="For example monitor, manage_index_templates, manage_ilm." />
      <TextAreaField
        label="Index privileges (one entry per line or JSON array)"
        mono
        rows={5}
        value={indicesText}
        onChange={(e) => setIndicesText(e.target.value)}
        hint="Each line: <index patterns> <privileges> [restricted], lists comma-separated. Use a JSON array with query and fieldSecurity for document- or field-level restrictions. DLS/FLS requires an Elasticsearch licence tier that includes those features (not Basic)."
        error={indicesText.trim() && indices === null ? 'Use <patterns> <privileges> [restricted] per line or a JSON array of index privileges with valid security restrictions.' : undefined}
      />
      <div className="stack" style={{ gap: 12 }}>
        <strong className="small">Application privileges (Kibana)</strong>
        {applications.map((grant, index) => (
          <div className="stack" key={index} style={{ gap: 8 }}>
            <TextField label="Application" mono value={grant.application} required onChange={(event) => setApplications((items) => items.map((item, i) => i === index ? { ...item, application: event.target.value } : item))} hint="For example kibana-.kibana" />
            <div className="grid-2">
              <TextField label="Application privileges (comma-separated)" mono required value={grant.privileges} onChange={(event) => setApplications((items) => items.map((item, i) => i === index ? { ...item, privileges: event.target.value } : item))} hint="For example feature_discover.read" />
              <TextField label="Resources (comma-separated)" mono required value={grant.resources} onChange={(event) => setApplications((items) => items.map((item, i) => i === index ? { ...item, resources: event.target.value } : item))} hint="For example space:default or *" />
            </div>
            <Button size="sm" onClick={() => setApplications((items) => items.filter((_, i) => i !== index))}>Remove application grant</Button>
          </div>
        ))}
        <div><Button size="sm" onClick={() => setApplications((items) => [...items, { application: '', privileges: '', resources: '' }])}>+ Add application grant</Button></div>
        {grants === null ? <p className="field-error">Each application grant needs an application, at least one privilege and at least one resource.</p> : null}
      </div>
    </Dialog>
  );
}

function APIKeyDialog({ namespace, cluster, onClose, onSaved }: { namespace: string; cluster: string; onClose: () => void; onSaved: () => void }) {
  const toast = useToast();
  const [name, setName] = useState('');
  const [expiration, setExpiration] = useState('');
  const [descriptors, setDescriptors] = useState('');
  const [created, setCreated] = useState<Schemas['ElasticAPIKeyCreated'] | null>(null);
  const parsed = parseRoleDescriptors(descriptors);
  const expirationOk = !expiration.trim() || EXPIRATION_RE.test(expiration.trim());
  const valid = name.trim().length > 0 && !parsed.error && expirationOk;

  const save = useMutation({
    mutationFn: () => unwrap(api.POST('/clusters/{cluster}/elasticsearch/api-keys', { params: { path: { cluster }, query: { namespace } }, body: { name: name.trim(), ...(expiration.trim() ? { expiration: expiration.trim() } : {}), roleDescriptors: parsed.value! } })),
    onSuccess: (key) => {
      toast.success('API key created.');
      onSaved();
      setCreated(key);
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  if (created) {
    return (
      <Dialog open onClose={onClose} title="API key created — copy it now" footer={<Button onClick={onClose}>Done</Button>}>
        <div className="stack">
          <Callout tone="warn">The key is shown only once. Store it in your secret manager before closing this dialog.</Callout>
          <KV
            items={[
              ['Name', <span className="mono">{created.name}</span>],
              ['ID', <span className="mono">{created.id}</span>],
              ['Expires', created.expiration === undefined ? 'never' : <Time iso={new Date(created.expiration).toISOString()} />],
            ]}
          />
          <strong className="small">Authorization header value (base64 id:key)</strong>
          <CodeLine value={`ApiKey ${created.encoded}`} />
          <details>
            <summary className="small">Raw key</summary>
            <CodeLine value={created.apiKey} />
          </details>
        </div>
      </Dialog>
    );
  }

  return (
    <Dialog
      open
      onClose={onClose}
      title="Create API key"
      onSubmit={() => valid && save.mutate()}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={save.isPending} disabled={!valid}>
            Create key
          </Button>
        </>
      }
    >
      <div className="grid-2">
        <TextField label="Name" mono value={name} onChange={(e) => setName(e.target.value)} required autoFocus />
        <TextField label="Expiration (optional)" mono value={expiration} onChange={(e) => setExpiration(e.target.value)} placeholder="30d" hint="Elasticsearch time value such as 30d or 12h; empty means never." error={!expirationOk ? 'Use a number followed by d, h, m or s.' : undefined} />
      </div>
      <TextAreaField
        label="Role descriptors (required JSON)"
        mono
        rows={6}
        value={descriptors}
        onChange={(e) => setDescriptors(e.target.value)}
        placeholder={'{\n  "logs-reader": { "cluster": ["monitor"], "indices": [{ "names": ["logs-*"], "privileges": ["read"] }] }\n}'}
        hint="Provide at least one descriptor with narrowly scoped permissions. Keys cannot inherit the control-plane identity's privileges."
        error={parsed.error}
      />
    </Dialog>
  );
}
