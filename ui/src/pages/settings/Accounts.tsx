// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog, Dialog } from '@/components/ui/Dialog';
import { Checkbox, TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, ErrorBlock, Loading, Menu, MenuItem, Time } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';

type Account = Schemas['Account'];

export function AccountsPage() {
  const auth = useAuth();
  const toast = useToast();
  const queryClient = useQueryClient();
  const accounts = useQuery({ queryKey: keys.accounts, queryFn: () => unwrap(api.GET('/accounts', { params: { query: { limit: 500 } } })), enabled: auth.can('accounts', 'read') });
  const [create, setCreate] = useState(false);
  const [reset, setReset] = useState<Account | null>(null);
  const [del, setDel] = useState<Account | null>(null);
  const invalidate = () => void queryClient.invalidateQueries({ queryKey: keys.accounts });

  const update = useMutation({
    mutationFn: ({ name, body }: { name: string; body: Schemas['UpdateAccountRequest'] }) => unwrap(api.PUT('/accounts/{account}', { params: { path: { account: name } }, body })),
    onSuccess: () => {
      toast.success('Account updated.');
      invalidate();
    },
    onError: (e) => toast.error(errorMessage(e)),
  });
  const remove = useMutation({
    mutationFn: (name: string) => unwrap(api.DELETE('/accounts/{account}', { params: { path: { account: name } } })),
    onSuccess: () => {
      toast.success('Account deleted.');
      setDel(null);
      invalidate();
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  if (!auth.can('accounts', 'read')) return <Callout tone="info">You do not have permission to manage accounts.</Callout>;

  return (
    <>
      <Card
        title="Local accounts"
        flush
        actions={
          auth.can('accounts', 'create') ? (
            <Button size="sm" variant="primary" onClick={() => setCreate(true)}>
              + Add account
            </Button>
          ) : null
        }
      >
        {accounts.isPending ? (
          <Loading />
        ) : accounts.error ? (
          <div className="card-body">
            <ErrorBlock error={accounts.error} retry={() => accounts.refetch()} />
          </div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Status</th>
                  <th>Created</th>
                  <th>Updated</th>
                  <th className="actions" />
                </tr>
              </thead>
              <tbody>
                {accounts.data.items.map((a) => {
                  const self = a.name === auth.user?.name;
                  return (
                    <tr key={a.name}>
                      <td className="mono">
                        {a.name}
                        {self ? <span className="faint"> (you)</span> : null}
                      </td>
                      <td>
                        <span className="row wrap" style={{ gap: 4 }}>
                          <Badge tone={a.enabled ? 'ok' : 'danger'}>{a.enabled ? 'enabled' : 'disabled'}</Badge>
                          {a.mustChangePassword ? <Badge plain tone="warn">must change password</Badge> : null}
                        </span>
                      </td>
                      <td className="small">
                        <Time iso={a.createdAt} />
                      </td>
                      <td className="small">
                        <Time iso={a.updatedAt} />
                      </td>
                      <td className="actions">
                        <Menu>
                          <MenuItem disabled={!auth.can('accounts', 'update')} onClick={() => setReset(a)}>
                            Reset password…
                          </MenuItem>
                          <MenuItem disabled={!auth.can('accounts', 'update') || self} onClick={() => update.mutate({ name: a.name, body: { enabled: !a.enabled, mustChangePassword: a.mustChangePassword } })}>
                            {a.enabled ? 'Disable' : 'Enable'}
                          </MenuItem>
                          <MenuItem disabled={!auth.can('accounts', 'update')} onClick={() => update.mutate({ name: a.name, body: { enabled: a.enabled, mustChangePassword: !a.mustChangePassword } })}>
                            {a.mustChangePassword ? 'Clear password-change flag' : 'Require password change'}
                          </MenuItem>
                          <MenuItem danger disabled={!auth.can('accounts', 'delete') || self} onClick={() => setDel(a)}>
                            Delete…
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
      <p className="xs faint">Permissions are assigned under Roles &amp; permissions by mapping <code>user:&lt;name&gt;</code> to roles. SSO users appear as <code>group:&lt;group&gt;</code> subjects, not as local accounts.</p>

      {create ? <CreateAccountDialog onClose={() => setCreate(false)} onSaved={invalidate} /> : null}
      {reset ? <ResetPasswordDialog account={reset} onClose={() => setReset(null)} onSaved={invalidate} /> : null}
      <ConfirmDialog open={del !== null} onClose={() => setDel(null)} onConfirm={() => del && remove.mutate(del.name)} title={`Delete account ${del?.name ?? ''}`} confirmLabel="Delete" danger typeToConfirm={del?.name} busy={remove.isPending}>
        <p className="small">All sessions and API tokens owned by this account are revoked immediately.</p>
      </ConfirmDialog>
    </>
  );
}

function CreateAccountDialog({ onClose, onSaved }: { onClose: () => void; onSaved: () => void }) {
  const toast = useToast();
  const [name, setName] = useState('');
  const [password, setPassword] = useState('');
  const [enabled, setEnabled] = useState(true);
  const [mustChange, setMustChange] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const valid = /^[a-z][a-z0-9._-]{1,62}$/.test(name) && password.length >= 12;
  const create = useMutation({
    mutationFn: () => unwrap(api.POST('/accounts', { body: { name, password, enabled, mustChangePassword: mustChange } })),
    onSuccess: () => {
      toast.success(`Account ${name} created.`);
      onSaved();
      onClose();
    },
    onError: (e) => setError(errorMessage(e)),
  });
  return (
    <Dialog
      open
      onClose={onClose}
      title="Add local account"
      onSubmit={() => valid && create.mutate()}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={create.isPending} disabled={!valid}>
            Create account
          </Button>
        </>
      }
    >
      {error ? <Callout tone="danger">{error}</Callout> : null}
      <TextField label="Username" mono required autoFocus value={name} onChange={(e) => setName(e.target.value)} hint="Lowercase letters, digits, dots, dashes." />
      <TextField label="Initial password" type="password" autoComplete="new-password" required minLength={12} value={password} onChange={(e) => setPassword(e.target.value)} hint="At least 12 characters; share out of band." />
      <Checkbox label="Enabled" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />
      <Checkbox label="Require password change at first sign-in" checked={mustChange} onChange={(e) => setMustChange(e.target.checked)} />
    </Dialog>
  );
}

function ResetPasswordDialog({ account, onClose, onSaved }: { account: Account; onClose: () => void; onSaved: () => void }) {
  const toast = useToast();
  const [password, setPassword] = useState('');
  const [mustChange, setMustChange] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const reset = useMutation({
    mutationFn: () => unwrap(api.POST('/accounts/{account}/reset-password', { params: { path: { account: account.name } }, body: { password, mustChangePassword: mustChange } })),
    onSuccess: () => {
      toast.success('Password reset.');
      onSaved();
      onClose();
    },
    onError: (e) => setError(errorMessage(e)),
  });
  return (
    <Dialog
      open
      onClose={onClose}
      title={`Reset password for ${account.name}`}
      onSubmit={() => password.length >= 12 && reset.mutate()}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={reset.isPending} disabled={password.length < 12}>
            Reset password
          </Button>
        </>
      }
    >
      {error ? <Callout tone="danger">{error}</Callout> : null}
      <TextField label="New password" type="password" autoComplete="new-password" required minLength={12} autoFocus value={password} onChange={(e) => setPassword(e.target.value)} />
      <Checkbox label="Require the user to change it at next sign-in" checked={mustChange} onChange={(e) => setMustChange(e.target.checked)} />
      <p className="xs faint">Existing sessions and tokens of this account may be invalidated by server policy.</p>
    </Dialog>
  );
}
