// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog, Dialog } from '@/components/ui/Dialog';
import { TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, CodeLine, ErrorBlock, Loading, Menu, MenuItem, Time } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';

type Token = Schemas['APIToken'];

export function ApiTokensPage() {
  const auth = useAuth();
  const toast = useToast();
  const queryClient = useQueryClient();
  const tokens = useQuery({ queryKey: keys.apiTokens, queryFn: () => unwrap(api.GET('/api-tokens', { params: { query: { limit: 500 } } })) });
  const [create, setCreate] = useState(false);
  const [edit, setEdit] = useState<Token | null>(null);
  const [del, setDel] = useState<Token | null>(null);
  const [created, setCreated] = useState<Schemas['CreatedAPIToken'] | null>(null);
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: keys.apiTokens });

  const remove = useMutation({
    mutationFn: (id: string) => unwrap(api.DELETE('/api-tokens/{token}', { params: { path: { token: id } } })),
    onSuccess: () => {
      toast.success('Token revoked.');
      setDel(null);
      invalidate();
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  const expired = (t: Token) => t.expiresAt !== undefined && new Date(t.expiresAt) < new Date();

  return (
    <>
      {created ? (
        <Card title="Token created — copy it now">
          <div className="stack">
            <Callout tone="warn">This is the only time the secret is shown. Store it in your CLI config or secret manager.</Callout>
            <CodeLine value={created.secret} />
            <p className="small muted">
              Use as <code>Authorization: Bearer {created.secret.slice(0, 6)}…</code> or <code>karactl auth login --token</code>. Token <span className="mono">{created.token.name}</span> acts with your permissions.
            </p>
            <div>
              <Button onClick={() => setCreated(null)}>Done</Button>
            </div>
          </div>
        </Card>
      ) : null}
      <Card
        title="API tokens"
        flush
        actions={
          <Button size="sm" variant="primary" onClick={() => setCreate(true)}>
            + Create token
          </Button>
        }
      >
        {tokens.isPending ? (
          <Loading />
        ) : tokens.error ? (
          <div className="card-body">
            <ErrorBlock error={tokens.error} retry={() => tokens.refetch()} />
          </div>
        ) : tokens.data.items.length === 0 ? (
          <div className="table-empty">No API tokens. Tokens authenticate the CLI and automation with your permissions.</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Prefix</th>
                  <th>Owner</th>
                  <th>Created</th>
                  <th>Last used</th>
                  <th>Expires</th>
                  <th className="actions" />
                </tr>
              </thead>
              <tbody>
                {tokens.data.items.map((t) => (
                  <tr key={t.id}>
                    <td>{t.name}</td>
                    <td className="mono small">{t.prefix}…</td>
                    <td className="mono small">{t.owner}</td>
                    <td className="small">
                      <Time iso={t.createdAt} />
                    </td>
                    <td className="small">
                      <Time iso={t.lastUsedAt} />
                    </td>
                    <td className="small">
                      {t.expiresAt ? (
                        <span className="row">
                          <Time iso={t.expiresAt} />
                          {expired(t) ? <Badge plain tone="danger">expired</Badge> : null}
                        </span>
                      ) : (
                        <Badge plain>never</Badge>
                      )}
                    </td>
                    <td className="actions">
                      <Menu>
                        <MenuItem onClick={() => setEdit(t)} disabled={t.owner !== auth.user?.name && !auth.can('api-tokens', 'update')}>
                          Rename / expiry…
                        </MenuItem>
                        <MenuItem danger onClick={() => setDel(t)} disabled={t.owner !== auth.user?.name && !auth.can('api-tokens', 'delete')}>
                          Revoke…
                        </MenuItem>
                      </Menu>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </Card>
      {create ? (
        <TokenDialog
          onClose={() => setCreate(false)}
          onCreated={(c) => {
            setCreated(c);
            invalidate();
          }}
        />
      ) : null}
      {edit ? <TokenDialog existing={edit} onClose={() => setEdit(null)} onSaved={invalidate} /> : null}
      <ConfirmDialog open={del !== null} onClose={() => setDel(null)} onConfirm={() => del && remove.mutate(del.id)} title={`Revoke token ${del?.name ?? ''}`} confirmLabel="Revoke" danger busy={remove.isPending}>
        <p className="small">Clients using this token stop working immediately.</p>
      </ConfirmDialog>
    </>
  );
}

function toLocalInput(iso: string | undefined): string {
  if (!iso) return '';
  const d = new Date(iso);
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function TokenDialog({ existing, onClose, onCreated, onSaved }: { existing?: Token; onClose: () => void; onCreated?: (c: Schemas['CreatedAPIToken']) => void; onSaved?: () => void }) {
  const toast = useToast();
  const [name, setName] = useState(existing?.name ?? '');
  const [expires, setExpires] = useState(toLocalInput(existing?.expiresAt));
  const [error, setError] = useState<string | null>(null);
  const expiresAt = expires ? new Date(expires).toISOString() : undefined;
  const valid = name.trim().length > 0 && (!expiresAt || new Date(expiresAt) > new Date());

  const save = useMutation({
    mutationFn: async () => {
      if (existing) {
        await unwrap(api.PUT('/api-tokens/{token}', { params: { path: { token: existing.id } }, body: { name: name.trim(), expiresAt: expiresAt ?? null } }));
        return null;
      }
      return unwrap(api.POST('/api-tokens', { body: { name: name.trim(), ...(expiresAt ? { expiresAt } : {}) } }));
    },
    onSuccess: (c) => {
      if (c) onCreated?.(c);
      else {
        toast.success('Token updated.');
        onSaved?.();
      }
      onClose();
    },
    onError: (e) => setError(errorMessage(e)),
  });

  return (
    <Dialog
      open
      onClose={onClose}
      title={existing ? `Edit token ${existing.name}` : 'Create API token'}
      onSubmit={() => valid && save.mutate()}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={save.isPending} disabled={!valid}>
            {existing ? 'Save' : 'Create token'}
          </Button>
        </>
      }
    >
      {error ? <Callout tone="danger">{error}</Callout> : null}
      <TextField label="Name" required autoFocus value={name} onChange={(e) => setName(e.target.value)} placeholder="ci-deploy" />
      <TextField label="Expires (optional)" type="datetime-local" value={expires} onChange={(e) => setExpires(e.target.value)} hint="Leave empty for a non-expiring token." error={expiresAt && new Date(expiresAt) <= new Date() ? 'Expiry must be in the future.' : undefined} />
      {existing ? null : <p className="xs faint">The token inherits your permissions and is shown exactly once after creation.</p>}
    </Dialog>
  );
}
