// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState, type ReactNode } from 'react';
import { Link } from 'react-router-dom';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, ApiError, errorMessage, unwrap } from '@/api/client';
import { useNamespaceDetail, useNamespaces, type Namespace } from '@/api/hooks';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { isNamespaceName } from '@/namespaces/useNamespace';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog, Dialog } from '@/components/ui/Dialog';
import { TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, ErrorBlock, KV, Loading, Menu, MenuItem, Time, formatRelative, type Tone } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { namespaceHealth } from '@/layout/NamespaceSwitcher';

const PHASE_TONE: Record<Namespace['phase'], Tone> = { Ready: 'ok', Pending: 'progress', Error: 'danger', Deleting: 'warn' };
const RELAY_TONE: Record<Namespace['relay']['phase'], Tone> = { NotRequired: 'neutral', Pending: 'progress', Deploying: 'progress', Ready: 'ok', Error: 'danger' };

/** 422 responses carry field-level details; surface them next to the message. */
function validationDetails(err: unknown): string[] {
  if (!(err instanceof ApiError) || !err.details || typeof err.details !== 'object') return [];
  return Object.entries(err.details as Record<string, unknown>).map(([k, v]) => `${k}: ${typeof v === 'string' ? v : JSON.stringify(v)}`);
}

function TokenBadge({ token }: { token: Namespace['token'] }) {
  return (
    <span className="row wrap" style={{ gap: 4 }}>
      <Badge tone={token.valid ? 'ok' : 'danger'} title={token.lastError}>
        {token.valid ? 'valid' : 'invalid'}
      </Badge>
      <Badge plain>{token.source === 'env' ? 'from environment' : 'stored'}</Badge>
    </span>
  );
}

function RelayCell({ ns }: { ns: Namespace }) {
  const r = ns.relay;
  if (ns.default || r.phase === 'NotRequired') return <span className="small faint">direct networking</span>;
  const outdated = r.version && r.desiredVersion && r.version !== r.desiredVersion;
  return (
    <div className="stack" style={{ gap: 3 }}>
      <span className="row wrap" style={{ gap: 4 }}>
        <Badge tone={RELAY_TONE[r.phase]} title={r.lastError}>
          {r.phase}
        </Badge>
        <Badge plain tone={r.connected ? 'ok' : 'warn'}>
          {r.connected ? 'connected' : 'disconnected'}
        </Badge>
        {outdated ? (
          <Badge plain tone="warn" title={`running ${r.version}, control plane ${r.desiredVersion}`}>
            update pending
          </Badge>
        ) : null}
      </span>
      <span className="xs faint">
        {r.lastSeenAt ? `seen ${formatRelative(r.lastSeenAt)}` : 'never seen'}
        {r.version ? ` · v${r.version}` : ''}
      </span>
    </div>
  );
}

export function NamespacesPage() {
  const auth = useAuth();
  const toast = useToast();
  const queryClient = useQueryClient();
  const namespaces = useNamespaces(auth.can('namespaces', 'read'));
  const [dialog, setDialog] = useState<null | { mode: 'create' } | { mode: 'detail' | 'rotate' | 'describe' | 'remove'; namespace: Namespace }>(null);
  const invalidate = () => queryClient.invalidateQueries({ queryKey: keys.namespaces });

  const validate = useMutation({
    mutationFn: (name: string) => unwrap(api.POST('/namespaces/{namespace}/validate', { params: { path: { namespace: name } } })),
    onSuccess: (ns) => {
      queryClient.setQueryData(keys.namespace(ns.name), ns);
      void invalidate();
      toast[ns.token.valid ? 'success' : 'error'](ns.token.valid ? `${ns.name}: token and project verified.` : `${ns.name}: ${ns.token.lastError ?? 'token validation failed.'}`);
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  if (!auth.can('namespaces', 'read')) return <Callout tone="info">You do not have permission to view namespaces.</Callout>;

  const canUpdate = (ns: Namespace) => auth.can('namespaces', 'update', ns.name);
  const canDelete = (ns: Namespace) => auth.can('namespaces', 'delete', ns.name);

  return (
    <>
      <Card
        title="Namespaces"
        flush
        actions={
          auth.can('namespaces', 'create') ? (
            <Button size="sm" variant="primary" onClick={() => setDialog({ mode: 'create' })}>
              + Add namespace
            </Button>
          ) : null
        }
      >
        {namespaces.isPending ? (
          <Loading />
        ) : namespaces.error ? (
          <div className="card-body">
            <ErrorBlock error={namespaces.error} retry={() => namespaces.refetch()} />
          </div>
        ) : namespaces.data.items.length === 0 ? (
          <div className="table-empty">No namespaces visible to you.</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Project</th>
                  <th>Phase</th>
                  <th>Token</th>
                  <th>Relay</th>
                  <th className="num">Clusters</th>
                  <th className="actions" />
                </tr>
              </thead>
              <tbody>
                {namespaces.data.items.map((ns) => {
                  const health = namespaceHealth(ns);
                  return (
                    <tr key={ns.name}>
                      <td>
                        <span className="row" style={{ gap: 8 }}>
                          <button type="button" className="link-button mono strong" onClick={() => setDialog({ mode: 'detail', namespace: ns })}>
                            {ns.name}
                          </button>
                          {ns.default ? (
                            <Badge plain tone="accent">
                              default
                            </Badge>
                          ) : null}
                        </span>
                        {ns.description ? <div className="xs faint">{ns.description}</div> : null}
                      </td>
                      <td className="small">
                        {ns.projectName || <span className="faint">—</span>}
                        <div className="xs mono faint">{ns.projectId}</div>
                      </td>
                      <td>
                        <Badge tone={PHASE_TONE[ns.phase]} title={health.label}>
                          {ns.phase}
                        </Badge>
                      </td>
                      <td>
                        <TokenBadge token={ns.token} />
                      </td>
                      <td>
                        <RelayCell ns={ns} />
                      </td>
                      <td className="num">
                        <Link to={`/clusters?namespace=${encodeURIComponent(ns.name)}`}>{ns.clusterCount}</Link>
                      </td>
                      <td className="actions">
                        <Menu>
                          <MenuItem onClick={() => setDialog({ mode: 'detail', namespace: ns })}>Details…</MenuItem>
                          <MenuItem onClick={() => validate.mutate(ns.name)} disabled={!canUpdate(ns) || validate.isPending}>
                            Re-validate token
                          </MenuItem>
                          <MenuItem onClick={() => setDialog({ mode: 'rotate', namespace: ns })} disabled={!canUpdate(ns) || ns.default} title={ns.default ? 'The default token is managed by the control-plane environment' : undefined}>
                            Rotate token…
                          </MenuItem>
                          <MenuItem onClick={() => setDialog({ mode: 'describe', namespace: ns })} disabled={!canUpdate(ns)}>
                            Edit description…
                          </MenuItem>
                          <MenuItem danger onClick={() => setDialog({ mode: 'remove', namespace: ns })} disabled={!canDelete(ns) || ns.default} title={ns.default ? 'The default namespace cannot be removed' : undefined}>
                            Remove…
                          </MenuItem>
                        </Menu>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </Card>
      <p className="xs faint">
        Each namespace is one Zerops project with its own project-scoped token. The default namespace is the control plane&apos;s own project and is configured through the service environment; other namespaces reach their clusters through a relay service that
        Karakoram deploys into the project.
      </p>

      {dialog?.mode === 'create' ? <CreateNamespaceDialog onClose={() => setDialog(null)} /> : null}
      {dialog?.mode === 'detail' ? (
        <NamespaceDetailDialog
          name={dialog.namespace.name}
          onClose={() => setDialog(null)}
          onValidate={canUpdate(dialog.namespace) ? () => validate.mutate(dialog.namespace.name) : undefined}
          validating={validate.isPending}
          onRotate={canUpdate(dialog.namespace) && !dialog.namespace.default ? () => setDialog({ mode: 'rotate', namespace: dialog.namespace }) : undefined}
          onDescribe={canUpdate(dialog.namespace) ? () => setDialog({ mode: 'describe', namespace: dialog.namespace }) : undefined}
          onRemove={canDelete(dialog.namespace) && !dialog.namespace.default ? () => setDialog({ mode: 'remove', namespace: dialog.namespace }) : undefined}
        />
      ) : null}
      {dialog?.mode === 'rotate' ? <RotateTokenDialog namespace={dialog.namespace} onClose={() => setDialog(null)} /> : null}
      {dialog?.mode === 'describe' ? <DescriptionDialog namespace={dialog.namespace} onClose={() => setDialog(null)} /> : null}
      {dialog?.mode === 'remove' ? <RemoveNamespaceDialog namespace={dialog.namespace} onClose={() => setDialog(null)} /> : null}
    </>
  );
}

function CreateNamespaceDialog({ onClose }: { onClose: () => void }) {
  const toast = useToast();
  const queryClient = useQueryClient();
  const [form, setForm] = useState({ name: '', projectId: '', token: '', description: '' });
  const [error, setError] = useState<{ message: string; details: string[] } | null>(null);
  const set = (patch: Partial<typeof form>) => setForm((f) => ({ ...f, ...patch }));
  const valid = isNamespaceName(form.name) && form.name !== 'default' && form.projectId.trim() !== '' && form.token !== '';

  const create = useMutation({
    mutationFn: () => unwrap(api.POST('/namespaces', { body: { name: form.name, projectId: form.projectId.trim(), token: form.token, ...(form.description.trim() ? { description: form.description.trim() } : {}) } })),
    onSuccess: (ns) => {
      toast.success(`Namespace ${ns.name} registered; relay deployment started.`);
      void queryClient.invalidateQueries({ queryKey: keys.namespaces });
      onClose();
    },
    onError: (e) => setError({ message: errorMessage(e), details: validationDetails(e) }),
    onSettled: () => set({ token: '' }),
  });

  return (
    <Dialog
      open
      onClose={onClose}
      title="Add namespace"
      busy={create.isPending}
      onSubmit={() => valid ? create.mutateAsync() : false}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={create.isPending} disabled={!valid}>
            Validate and add
          </Button>
        </>
      }
    >
      {error ? (
        <Callout tone="danger" title={error.details.length ? 'The namespace could not be registered' : undefined}>
          {error.message}
          {error.details.length ? (
            <ul style={{ margin: '6px 0 0', paddingLeft: 18 }}>
              {error.details.map((d) => (
                <li key={d}>{d}</li>
              ))}
            </ul>
          ) : null}
        </Callout>
      ) : null}
      <TextField
        label="Name"
        mono
        required
        value={form.name}
        onChange={(e) => set({ name: e.target.value })}
        hint="2–32 lowercase letters, digits or hyphens; start with a letter and end with a letter or digit."
        error={form.name && (!isNamespaceName(form.name) || form.name === 'default') ? (form.name === 'default' ? 'default is reserved for the control-plane project.' : 'Use 2–32 lowercase letters, digits or hyphens, starting with a letter and ending with a letter or digit.') : undefined}
        autoFocus
      />
      <TextField label="Zerops project ID" mono required value={form.projectId} onChange={(e) => set({ projectId: e.target.value })} hint="Each project can belong to only one namespace." autoComplete="off" />
      <TextField label="Project token" type="password" required value={form.token} onChange={(e) => set({ token: e.target.value })} hint="A project-scoped Zerops access token. Validated against the project before anything is saved; sealed at rest and never returned." autoComplete="new-password" />
      <TextField label="Description (optional)" value={form.description} onChange={(e) => set({ description: e.target.value })} />
      <p className="xs faint">After validation, Karakoram deploys a relay service into the project; clusters can be created once the relay reports Ready.</p>
    </Dialog>
  );
}

function NamespaceDetailDialog({
  name,
  onClose,
  onValidate,
  validating,
  onRotate,
  onDescribe,
  onRemove,
}: {
  name: string;
  onClose: () => void;
  onValidate?: () => void;
  validating: boolean;
  onRotate?: () => void;
  onDescribe?: () => void;
  onRemove?: () => void;
}) {
  const q = useNamespaceDetail(name);
  const ns = q.data;
  const r = ns?.relay;
  const direct = ns?.default || r?.phase === 'NotRequired';
  const certExpiry = r?.certNotAfter ? new Date(r.certNotAfter).getTime() - Date.now() : null;
  const certSoon = certExpiry !== null && certExpiry < 30 * 24 * 3600 * 1000;

  return (
    <Dialog
      open
      onClose={onClose}
      wide
      title={
        <span className="row" style={{ gap: 10 }}>
          <span className="mono">{name}</span>
          {ns ? (
            <Badge tone={PHASE_TONE[ns.phase]}>
              {ns.phase}
            </Badge>
          ) : null}
          {ns?.default ? (
            <Badge plain tone="accent">
              default
            </Badge>
          ) : null}
        </span>
      }
      footer={
        <>
          {onRemove ? (
            <Button variant="danger" onClick={onRemove}>
              Remove…
            </Button>
          ) : null}
          <span className="grow" />
          {onDescribe ? <Button onClick={onDescribe}>Edit description…</Button> : null}
          {onValidate ? (
            <Button onClick={onValidate} loading={validating}>
              Re-validate
            </Button>
          ) : null}
          {onRotate ? (
            <Button variant="primary" onClick={onRotate}>
              Rotate token…
            </Button>
          ) : null}
          <Button onClick={onClose}>Close</Button>
        </>
      }
    >
      {q.isPending ? (
        <Loading />
      ) : q.error || !ns ? (
        <ErrorBlock error={q.error ?? new Error('Namespace not found')} retry={() => q.refetch()} />
      ) : (
        <div className="stack" style={{ gap: 18 }}>
          {ns.description ? <p className="small muted">{ns.description}</p> : null}
          <Section title="Project">
            <KV
              items={[
                ['Project', ns.projectName || undefined],
                ['Project ID', <span className="mono">{ns.projectId}</span>],
                ['Clusters', <Link to={`/clusters?namespace=${encodeURIComponent(ns.name)}`}>{ns.clusterCount}</Link>],
                ['Created', <Time iso={ns.createdAt} />],
                ['Updated', <Time iso={ns.updatedAt} />],
              ]}
            />
          </Section>
          <Section title="Token">
            {ns.token.source === 'env' ? <Callout tone="info">Managed by the control-plane environment. Rotate it by updating the service secret and restarting the control plane.</Callout> : null}
            <KV
              items={[
                ['Status', <TokenBadge token={ns.token} />],
                ['Source', ns.token.source === 'env' ? 'control-plane environment' : 'stored (sealed at rest)'],
                ['Last validated', <Time iso={ns.token.lastValidatedAt} />],
                ['Rotated', <Time iso={ns.token.rotatedAt} />],
                ns.token.lastError ? ['Last error', <span style={{ color: 'var(--danger)' }}>{ns.token.lastError}</span>] : null,
              ]}
            />
          </Section>
          <Section title="Relay">
            {direct ? (
              <p className="small muted">This namespace uses direct private networking; no relay is deployed.</p>
            ) : (
              <KV
                items={[
                  ['Hostname', r?.hostname ? <span className="mono">{r.hostname}</span> : undefined],
                  [
                    'Phase',
                    <span className="row wrap" style={{ gap: 4 }}>
                      <Badge tone={RELAY_TONE[r!.phase]}>{r!.phase}</Badge>
                      <Badge plain tone={r!.connected ? 'ok' : 'warn'}>
                        {r!.connected ? 'connected' : 'disconnected'}
                      </Badge>
                    </span>,
                  ],
                  ['Connected since', <Time iso={r?.connectedSince} />],
                  ['Last seen', <Time iso={r?.lastSeenAt} />],
                  [
                    'Version',
                    r?.version ? (
                      <>
                        <span className="mono">{r.version}</span>
                        {r.desiredVersion && r.desiredVersion !== r.version ? (
                          <span className="faint">
                            {' '}
                            → <span className="mono">{r.desiredVersion}</span> pending
                          </span>
                        ) : null}
                      </>
                    ) : r?.desiredVersion ? (
                      <span className="faint">
                        not reported · desired <span className="mono">{r.desiredVersion}</span>
                      </span>
                    ) : undefined,
                  ],
                  [
                    'Certificate expires',
                    r?.certNotAfter ? (
                      <span style={certSoon ? { color: 'var(--warn)' } : undefined}>
                        <Time iso={r.certNotAfter} />
                        {certSoon ? ' (renewal due)' : ''}
                      </span>
                    ) : undefined,
                  ],
                  ['Reconnects', String(r?.reconnects ?? 0)],
                  r?.lastError ? ['Last error', <span style={{ color: 'var(--danger)' }}>{r.lastError}</span>] : null,
                ]}
              />
            )}
          </Section>
        </div>
      )}
    </Dialog>
  );
}

function Section({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="stack" style={{ gap: 8 }}>
      <div className="section-title">{title}</div>
      {children}
    </div>
  );
}

function RotateTokenDialog({ namespace, onClose }: { namespace: Namespace; onClose: () => void }) {
  const toast = useToast();
  const queryClient = useQueryClient();
  const [token, setToken] = useState('');
  const [error, setError] = useState<{ message: string; details: string[] } | null>(null);

  const rotate = useMutation({
    mutationFn: (value: string) => unwrap(api.PUT('/namespaces/{namespace}/token', { params: { path: { namespace: namespace.name } }, body: { token: value } })),
    onSuccess: (ns) => {
      queryClient.setQueryData(keys.namespace(ns.name), ns);
      void queryClient.invalidateQueries({ queryKey: keys.namespaces });
      toast.success(`Token for ${ns.name} rotated.`);
      onClose();
    },
    onError: (e) => setError({ message: errorMessage(e), details: validationDetails(e) }),
  });

  return (
    <Dialog
      open
      onClose={onClose}
      title={`Rotate token for ${namespace.name}`}
      busy={rotate.isPending}
      onSubmit={() => {
        if (!token) return false;
        const value = token;
        // The secret leaves component state before the request is in flight.
        setToken('');
        return rotate.mutateAsync(value);
      }}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={rotate.isPending} disabled={!token}>
            Validate and rotate
          </Button>
        </>
      }
    >
      {error ? (
        <Callout tone="danger">
          {error.message}
          {error.details.length ? (
            <ul style={{ margin: '6px 0 0', paddingLeft: 18 }}>
              {error.details.map((d) => (
                <li key={d}>{d}</li>
              ))}
            </ul>
          ) : null}
        </Callout>
      ) : null}
      <p className="small muted">
        The new token must belong to project <span className="mono">{namespace.projectId}</span>. It is validated first and then swapped atomically; the relay keeps running.
      </p>
      <TextField label="New project token" type="password" required value={token} onChange={(e) => setToken(e.target.value)} autoComplete="new-password" autoFocus />
      <p className="xs faint">Tokens are sealed at rest and never returned by the API or kept in the browser.</p>
    </Dialog>
  );
}

function DescriptionDialog({ namespace, onClose }: { namespace: Namespace; onClose: () => void }) {
  const toast = useToast();
  const queryClient = useQueryClient();
  const [description, setDescription] = useState(namespace.description ?? '');
  const [error, setError] = useState<string | null>(null);
  const save = useMutation({
    mutationFn: () => unwrap(api.PATCH('/namespaces/{namespace}', { params: { path: { namespace: namespace.name } }, body: { description: description.trim() } })),
    onSuccess: (ns) => {
      queryClient.setQueryData(keys.namespace(ns.name), ns);
      void queryClient.invalidateQueries({ queryKey: keys.namespaces });
      toast.success('Description updated.');
      onClose();
    },
    onError: (e) => setError(errorMessage(e)),
  });
  return (
    <Dialog
      open
      onClose={onClose}
      title={`Describe ${namespace.name}`}
      busy={save.isPending}
      onSubmit={() => save.mutateAsync()}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={save.isPending}>
            Save
          </Button>
        </>
      }
    >
      {error ? <Callout tone="danger">{error}</Callout> : null}
      <TextField label="Description" value={description} onChange={(e) => setDescription(e.target.value)} hint="Leave empty to clear." autoFocus />
    </Dialog>
  );
}

function RemoveNamespaceDialog({ namespace, onClose }: { namespace: Namespace; onClose: () => void }) {
  const toast = useToast();
  const queryClient = useQueryClient();
  const [error, setError] = useState<string | null>(null);
  const remove = useMutation({
    mutationFn: () => unwrap(api.DELETE('/namespaces/{namespace}', { params: { path: { namespace: namespace.name } } })),
    onSuccess: () => {
      toast.success(`Removing namespace ${namespace.name}; its relay service is being deleted.`);
      void queryClient.invalidateQueries({ queryKey: keys.namespaces });
      onClose();
    },
    onError: (e) => setError(e instanceof ApiError && e.status === 409 ? `Refused: ${e.message}` : errorMessage(e)),
  });
  return (
    <ConfirmDialog open onClose={onClose} onConfirm={() => remove.mutateAsync()} title={`Remove namespace ${namespace.name}`} confirmLabel="Remove" danger typeToConfirm={namespace.name} busy={remove.isPending}>
      {error ? <Callout tone="danger">{error}</Callout> : null}
      <p className="small">
        The relay service is deleted from project <span className="mono">{namespace.projectId}</span>, then the namespace and its sealed token are removed. The project itself is not touched.
      </p>
      <p className="small">
        Removal is refused while any cluster (including ones still deleting), backup storage, retained backup record or pending orphaned-volume record exists in the namespace
        {namespace.clusterCount > 0 ? ` — it currently has ${namespace.clusterCount} cluster${namespace.clusterCount === 1 ? '' : 's'}` : ''}.
      </p>
    </ConfirmDialog>
  );
}
