// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useRef, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog, Dialog } from '@/components/ui/Dialog';
import { Checkbox, TextAreaField, TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, CodeLine, ErrorBlock, Loading, Menu, MenuItem, Time } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { buildTokenRestrictions, tokenNameError, tokenExpiry, tokenRestrictionSummary, tokenSubmission, type TokenRestrictionInput, type TokenSubmission } from './token-form';

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
              Use as <code>Authorization: Bearer {created.secret.slice(0, 6)}…</code>. Token <span className="mono">{created.token.name}</span> acts within your permissions and its restrictions.
            </p>
            <p className="small muted">For the CLI, supply the secret through your secure runtime environment:</p>
            <CodeLine value={`export KARAKORAM_SERVER=${window.location.origin}\nexport KARAKORAM_TOKEN='${created.secret}'\nkaractl clusters list`} />
            <a className="small" href="/api/v1/openapi.yaml" target="_blank" rel="noreferrer">API reference (OpenAPI)</a>
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
                  <th>ID</th>
                  <th>Owner</th>
                  <th>Restrictions</th>
                  <th>Created</th>
                  <th>Last used</th>
                  <th>Expires</th>
                  <th className="actions" />
                </tr>
              </thead>
              <tbody>
                {tokens.data.items.map((t) => (
                  <tr key={t.id}>
                    <td title={t.name} style={{ maxWidth: 240, whiteSpace: 'normal', overflowWrap: 'anywhere' }}>{t.name}</td>
                    <td className="mono small" title={t.id} style={{ maxWidth: 160, whiteSpace: 'normal', overflowWrap: 'anywhere' }}>{t.id}</td>
                    <td className="mono small">{t.owner}</td>
                    <td className="small" style={{ maxWidth: 260, whiteSpace: 'normal' }}><Badge plain>{tokenRestrictionSummary(t.restrictions)}</Badge></td>
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
      <ConfirmDialog open={del !== null} onClose={() => setDel(null)} onConfirm={() => del ? remove.mutateAsync(del.id).catch(() => undefined) : false} title={`Revoke token ${del?.name ?? ''}`} confirmLabel="Revoke" danger busy={remove.isPending}>
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
  const [restrictionInput, setRestrictionInput] = useState<TokenRestrictionInput>({
    readOnly: false, namespaces: '', clusters: '', permissions: '', allowCredentialReveal: false,
  });
  const restrictionForm = buildTokenRestrictions(restrictionInput);
  const [error, setError] = useState<string | null>(null);
  const expiry = tokenExpiry(expires);
  const expiresAt = expiry.expiresAt;
  const nameProblem = tokenNameError(name);
  const valid = !nameProblem && !expiry.error && (!!existing || Object.keys(restrictionForm.errors).length === 0);
  const submission = useRef(tokenSubmission());

  const save = useMutation({
    mutationFn: async (submitted: TokenSubmission) => {
      if (existing) {
        await unwrap(api.PUT('/api-tokens/{token}', { params: { path: { token: existing.id } }, body: { name: submitted.name, expiresAt: submitted.expiresAt ?? null } }));
        return null;
      }
      return unwrap(api.POST('/api-tokens', { headers: { 'Idempotency-Key': submitted.key }, body: { name: submitted.name, ...(submitted.expiresAt ? { expiresAt: submitted.expiresAt } : {}), ...(submitted.restrictions ? { restrictions: submitted.restrictions } : {}) } }));
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
    onSettled: () => submission.current.release(),
  });

  return (
    <Dialog
      open
      onClose={onClose}
      title={existing ? `Edit token ${existing.name}` : 'Create API token'}
      onSubmit={() => {
        if (!valid) return false;
        const submitted = submission.current.begin(name, expiresAt, existing ? undefined : restrictionForm.restrictions);
        if (!submitted) return false;
        setError(null);
        return save.mutateAsync(submitted).catch(() => undefined);
      }}
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
      <TextField label="Name" required autoFocus value={name} onChange={(e) => setName(e.target.value)} placeholder="ci-deploy" hint="1–128 characters after trimming; duplicate names are allowed." error={nameProblem} />
      <TextField label="Expires (optional)" type="datetime-local" value={expires} onChange={(e) => setExpires(e.target.value)} hint="Leave empty for a non-expiring token." error={expiry.error} />
      {existing ? (
        <div className="stack">
          <p className="small muted">Restrictions are fixed at creation and cannot be changed. Create a new token to use different restrictions.</p>
          <dl className="kv">
            <dt>Restrictions</dt><dd>{tokenRestrictionSummary(existing.restrictions)}</dd>
            {existing.restrictions ? <>
              <dt>Read-only</dt><dd>{existing.restrictions.readOnly ? 'Yes' : 'No'}</dd>
              <dt>Namespaces</dt><dd>{existing.restrictions.namespaces?.join(', ') || 'All permitted by your RBAC'}</dd>
              <dt>Clusters</dt><dd>{existing.restrictions.clusters?.join(', ') || 'All permitted by your RBAC'}</dd>
              <dt>Permissions</dt><dd>{existing.restrictions.permissions?.join(', ') || 'All permitted by your RBAC'}</dd>
              <dt>Credential reveal</dt><dd>{existing.restrictions.allowCredentialReveal ? 'Allowed within your RBAC' : 'Denied'}</dd>
            </> : null}
          </dl>
        </div>
      ) : (
        <>
          <details>
            <summary className="small">Restrictions</summary>
            <div className="stack" style={{ marginTop: 12 }}>
              <p className="small muted">Optional limits only remove permissions; they never grant access. Leave everything empty and unchecked for an unrestricted token. Restrictions cannot be changed later.</p>
              <Checkbox label="Read-only" checked={restrictionInput.readOnly} onChange={(e) => setRestrictionInput({ ...restrictionInput, readOnly: e.target.checked })} />
              <TextAreaField label="Namespaces" rows={2} mono value={restrictionInput.namespaces} onChange={(e) => setRestrictionInput({ ...restrictionInput, namespaces: e.target.value })} placeholder="production, staging" hint="Comma- or newline-separated namespace names. Empty allows all namespaces within your other limits." error={restrictionForm.errors.namespaces} />
              <TextAreaField label="Clusters" rows={2} mono value={restrictionInput.clusters} onChange={(e) => setRestrictionInput({ ...restrictionInput, clusters: e.target.value })} placeholder="production/database" hint="Comma- or newline-separated namespace/name pairs. Empty allows all clusters within your other limits." error={restrictionForm.errors.clusters} />
              <TextAreaField label="Permissions" rows={2} mono value={restrictionInput.permissions} onChange={(e) => setRestrictionInput({ ...restrictionInput, permissions: e.target.value })} placeholder="clusters:read, backups:*" hint="Comma- or newline-separated resource:action pairs; action may be *. Empty keeps your permissions within the other limits." error={restrictionForm.errors.permissions} />
              <Checkbox label="Allow credential reveal" description="Warning: allows revealing credentials and other secret responses within your permissions." checked={restrictionInput.allowCredentialReveal} onChange={(e) => setRestrictionInput({ ...restrictionInput, allowCredentialReveal: e.target.checked })} />
              <p className="xs faint">Restricted tokens deny credential reveal unless explicitly allowed above.</p>
            </div>
          </details>
          <p className="xs faint">The token inherits your permissions subject to these restrictions and is shown exactly once after creation.</p>
        </>
      )}
    </Dialog>
  );
}
