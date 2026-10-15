// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, ApiError, errorMessage, unwrap, type Schemas } from '@/api/client';
import { useBackupStorages, useNamespaceOptions } from '@/api/hooks';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { DEFAULT_NAMESPACE, clusterObject } from '@/auth/permissions';
import { spansNamespaces, useNamespace } from '@/namespaces/useNamespace';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog, Dialog } from '@/components/ui/Dialog';
import { Checkbox, SelectField, TextField } from '@/components/ui/Field';
import { Badge, Card, ErrorBlock, Loading, Menu, MenuItem, Time } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';

import { storageTransport } from '@/components/ui/presentation';
import { deleteWithReconciliation } from './storage-outcome';
import { idempotencyHeaders, useIdempotencyKey } from '@/api/idempotency';
import { replaceStorageInList, storageFailureLabel } from './storage-verification';
type Storage = Schemas['BackupStorage'];

export function BackupStoragesPage() {
  const auth = useAuth();
  const toast = useToast();
  const queryClient = useQueryClient();
  const { namespace } = useNamespace();
  const storages = useBackupStorages(namespace);
  const namespaceOptions = useNamespaceOptions();
  const showNamespace = !namespace && spansNamespaces(storages.data?.items ?? []);
  const [dialog, setDialog] = useState<null | { mode: 'create' } | { mode: 'edit'; storage: Storage }>(null);
  const [del, setDel] = useState<Storage | null>(null);
  const [checking, setChecking] = useState<Record<string, boolean>>({});

  const storageKey = (s: Storage) => clusterObject(s.namespace, s.name);
  const canOn = (s: Storage, action: 'update' | 'delete') => auth.can('backup-storages', action, storageKey(s));

  const verify = useMutation({
    mutationFn: (s: Storage) => unwrap(api.POST('/backup-storages/{storage}/verify', { params: { path: { storage: s.name }, query: { namespace: s.namespace || DEFAULT_NAMESPACE } } })),
    onMutate: (s) => setChecking((v) => ({ ...v, [storageKey(s)]: true })),
    onSuccess: () => queryClient.invalidateQueries({ queryKey: keys.backupStorages }),
    onError: (e) => toast.error(errorMessage(e)),
    onSettled: (_res, _error, s) => setChecking((v) => ({ ...v, [storageKey(s)]: false })),
  });
  const remove = useMutation({
    mutationFn: (s: Storage) => {
      const params = { path: { storage: s.name }, query: { namespace: s.namespace || DEFAULT_NAMESPACE } };
      return deleteWithReconciliation(() => unwrap(api.DELETE('/backup-storages/{storage}', { params })), () => unwrap(api.GET('/backup-storages/{storage}', { params })), (error) => error instanceof ApiError && error.status === 404);
    },
    onSuccess: () => {
      toast.success('Storage deleted.');
      setDel(null);
      void queryClient.invalidateQueries({ queryKey: keys.backupStorages });
    },
    onError: (e) => toast.error(errorMessage(e)),
  });

  return (
    <>
      <Card
        title={namespace ? `Backup storages in ${namespace}` : 'Backup storages'}
        flush
        actions={
          auth.can('backup-storages', 'create') ? (
            <Button size="sm" variant="primary" onClick={() => setDialog({ mode: 'create' })}>
              + Add storage
            </Button>
          ) : null
        }
      >
        {storages.isPending ? (
          <Loading />
        ) : storages.error ? (
          <div className="card-body">
            <ErrorBlock error={storages.error} retry={() => storages.refetch()} />
          </div>
        ) : storages.data.items.length === 0 ? (
          <div className="table-empty">No S3 storages yet. Zerops Object Storage or any S3-compatible endpoint works.</div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Name</th>
                  {showNamespace ? <th>Namespace</th> : null}
                  <th>Endpoint</th>
                  <th>Bucket / prefix</th>
                  <th>Region</th>
                  <th>Options</th>
                  <th>Verification</th>
                  <th>Updated</th>
                  <th className="actions" />
                </tr>
              </thead>
              <tbody>
                {storages.data.items.map((s) => {
                  const v = s.verification;
                  const pending = checking[storageKey(s)];
                  return (
                    <tr key={storageKey(s)}>
                      <td className="mono" title={s.name}>
                        {s.name}
                        {s.description ? <div className="xs faint">{s.description}</div> : null}
                      </td>
                      {showNamespace ? <td className="mono small">{s.namespace || DEFAULT_NAMESPACE}</td> : null}
                      <td className="mono small" title={s.endpointURL}>{s.endpointURL}</td>
                      <td className="mono small">
                        {s.bucket}
                        {s.prefix ? `/${s.prefix}` : ''}
                      </td>
                      <td className="small">{s.region}</td>
                      <td className="small">
                        <span className="row wrap" style={{ gap: 4 }}>
                          {s.forcePathStyle ? <Badge plain>path-style</Badge> : null}
                          <Badge plain tone={s.endpointURL.startsWith('https://') && s.verifyTLS ? 'neutral' : 'warn'}>{storageTransport(s.endpointURL, s.verifyTLS ?? true)}</Badge>
                          {s.credentialsConfigured ? null : <Badge plain tone="danger">no credentials</Badge>}
                        </span>
                      </td>
                      <td className="small">
                        {pending ? (
                          <span className="row">
                            <span className="spinner" /> checking…
                          </span>
                        ) : v ? (
                          <Badge tone={v.verified ? 'ok' : 'danger'} title={v.message}>
                            {v.verified ? 'Last check passed' : 'Last check failed'}
                          </Badge>
                        ) : (
                          <Button size="sm" onClick={() => verify.mutate(s)} disabled={!canOn(s, 'update')}>
                            Verify
                          </Button>
                        )}
                        {v && !pending ? <div className="xs faint">{v.failureReason ? <strong>{storageFailureLabel(v.failureReason)}: </strong> : null}{v.message}<div>Checked <Time iso={v.checkedAt} /></div></div> : !pending ? <div className="xs faint">Not checked.</div> : null}
                      </td>
                      <td className="small">
                        <Time iso={s.updatedAt} />
                      </td>
                      <td className="actions">
                        <Menu>
                          <MenuItem onClick={() => verify.mutate(s)} disabled={!canOn(s, 'update') || pending}>
                            Verify access
                          </MenuItem>
                          <MenuItem onClick={() => setDialog({ mode: 'edit', storage: s })} disabled={!canOn(s, 'update')}>
                            Edit…
                          </MenuItem>
                          <MenuItem danger onClick={() => setDel(s)} disabled={!canOn(s, 'delete')}>
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
      {dialog ? <StorageDialog existing={dialog.mode === 'edit' ? dialog.storage : undefined} namespaces={namespaceOptions} defaultNamespace={namespace} onClose={() => setDialog(null)} /> : null}
      <ConfirmDialog open={del !== null} onClose={() => setDel(null)} onConfirm={() => del ? remove.mutateAsync(del) : false} title={`Delete storage ${del?.name ?? ''}`} confirmLabel="Delete" danger busy={remove.isPending} error={remove.error}>
        <p className="small">Only unused storages can be deleted; the server refuses while clusters, backups or restores reference it. Data in the bucket is not touched.</p>
      </ConfirmDialog>
    </>
  );
}

function StorageDialog({ existing, namespaces, defaultNamespace, onClose }: { existing?: Storage; namespaces: string[]; defaultNamespace: string | undefined; onClose: () => void }) {
  const toast = useToast();
  const queryClient = useQueryClient();
  const attempt = useIdempotencyKey();
  const [form, setForm] = useState({
    namespace: existing?.namespace || defaultNamespace || (namespaces.length === 1 ? namespaces[0]! : ''),
    name: existing?.name ?? '',
    description: existing?.description ?? '',
    endpointURL: existing?.endpointURL ?? '',
    region: existing?.region ?? 'us-east-1',
    bucket: existing?.bucket ?? '',
    prefix: existing?.prefix ?? '',
    forcePathStyle: existing?.forcePathStyle ?? true,
    verifyTLS: existing?.verifyTLS ?? true,
    accessKeyId: '',
    secretAccessKey: '',
  });
  const [error, setError] = useState<unknown>(null);
  const set = (patch: Partial<typeof form>) => {
    if (error) { attempt.reset(); setError(null); }
    setForm((f) => ({ ...f, ...patch }));
  };
  const credsGiven = form.accessKeyId !== '' || form.secretAccessKey !== '';
  const valid = form.namespace && (existing || /^[a-z][a-z0-9-]{1,62}$/.test(form.name)) && /^https?:\/\//.test(form.endpointURL) && form.bucket && form.region && (existing ? !credsGiven || (form.accessKeyId && form.secretAccessKey) : form.accessKeyId && form.secretAccessKey);

  const save = useMutation({
    mutationFn: () => {
      const common = {
        type: 's3' as const,
        endpointURL: form.endpointURL.trim(),
        region: form.region.trim(),
        bucket: form.bucket.trim(),
        prefix: form.prefix.trim() || undefined,
        description: form.description.trim() || undefined,
        forcePathStyle: form.forcePathStyle,
        verifyTLS: form.verifyTLS,
      };
      const credentials = { accessKeyId: form.accessKeyId, secretAccessKey: form.secretAccessKey };
      const namespace = form.namespace || DEFAULT_NAMESPACE;
      return existing
        ? unwrap(api.PUT('/backup-storages/{storage}', { headers: idempotencyHeaders(attempt.key), params: { path: { storage: existing.name }, query: { namespace } }, body: { ...common, ...(credsGiven ? { credentials } : {}) } }))
        : unwrap(api.POST('/backup-storages', { headers: idempotencyHeaders(attempt.key), params: { query: { namespace } }, body: { ...common, name: form.name, credentials } }));
    },
    onSuccess: (saved) => {
      queryClient.setQueriesData<Schemas['BackupStorageList']>({ queryKey: keys.backupStorages }, (current) => replaceStorageInList(current, saved));
      attempt.reset();
      toast.success(existing ? 'Storage updated.' : 'Storage created. Run Verify to check access.');
      void queryClient.invalidateQueries({ queryKey: keys.backupStorages });
      onClose();
    },
    onError: (e) => setError(e),
  });

  return (
    <Dialog
      open
      onClose={onClose}
      title={existing ? `Edit ${existing.name}` : 'Add backup storage'}
      wide
      busy={save.isPending}
      error={error}
      onSubmit={() => valid ? save.mutateAsync() : false}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={save.isPending} disabled={!valid}>
            {existing ? 'Save' : 'Create'}
          </Button>
        </>
      }
    >
      {existing || namespaces.length === 1 ? null : (
        <SelectField label="Namespace" required value={form.namespace} onChange={(e) => set({ namespace: e.target.value })} hint="Storages belong to one namespace; only clusters in that namespace can use them.">
          <option value="">Choose…</option>
          {namespaces.map((n) => (
            <option key={n} value={n}>
              {n}
            </option>
          ))}
        </SelectField>
      )}
      <div className="grid-2">
        <TextField label="Name" mono required value={form.name} onChange={(e) => set({ name: e.target.value })} disabled={Boolean(existing)} hint={`2–63 lowercase letters, digits or hyphens, starting with a letter. A trailing hyphen is allowed; unique within namespace ${form.namespace || '…'}.`} error={!existing && form.name && !/^[a-z][a-z0-9-]{1,62}$/.test(form.name) ? 'Use 2–63 lowercase letters, digits or hyphens, starting with a letter.' : undefined} autoFocus={!existing} />
        <TextField label="Description" value={form.description} onChange={(e) => set({ description: e.target.value })} />
      </div>
      <TextField label="Endpoint URL" mono required value={form.endpointURL} onChange={(e) => set({ endpointURL: e.target.value })} placeholder="https://s3.example.com" hint="S3-compatible endpoint. Zerops Object Storage: https://storage-<region>.zerops.io" />
      <div className="grid-3">
        <TextField label="Bucket" mono required value={form.bucket} onChange={(e) => set({ bucket: e.target.value })} />
        <TextField label="Prefix (optional)" mono value={form.prefix} onChange={(e) => set({ prefix: e.target.value })} placeholder="karakoram/" />
        <TextField label="Region" mono required value={form.region} onChange={(e) => set({ region: e.target.value })} />
      </div>
      <div className="grid-2">
        <TextField label={existing ? 'Access key ID (leave empty to keep)' : 'Access key ID'} mono autoComplete="off" value={form.accessKeyId} onChange={(e) => set({ accessKeyId: e.target.value })} required={!existing} />
        <TextField label={existing ? 'Secret access key (leave empty to keep)' : 'Secret access key'} type="password" autoComplete="new-password" value={form.secretAccessKey} onChange={(e) => set({ secretAccessKey: e.target.value })} required={!existing} />
      </div>
      <div className="row wrap" style={{ gap: 20 }}>
        <Checkbox label="Force path-style addressing" description="Required by most non-AWS endpoints, including Zerops Object Storage." checked={form.forcePathStyle} onChange={(e) => set({ forcePathStyle: e.target.checked })} />
        <Checkbox label="Verify TLS certificate" description="Disable only for self-signed test endpoints." checked={form.verifyTLS} onChange={(e) => set({ verifyTLS: e.target.checked })} />
      </div>
      <p className="xs faint">Credentials are encrypted at rest and never returned by the API.</p>
    </Dialog>
  );
}
