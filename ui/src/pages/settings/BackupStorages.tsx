// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useMutation, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { useBackupStorages } from '@/api/hooks';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog, Dialog } from '@/components/ui/Dialog';
import { Checkbox, TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, ErrorBlock, Loading, Menu, MenuItem, Time } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';

type Storage = Schemas['BackupStorage'];

export function BackupStoragesPage() {
  const auth = useAuth();
  const toast = useToast();
  const queryClient = useQueryClient();
  const storages = useBackupStorages();
  const [dialog, setDialog] = useState<null | { mode: 'create' } | { mode: 'edit'; storage: Storage }>(null);
  const [del, setDel] = useState<Storage | null>(null);
  const [verifications, setVerifications] = useState<Record<string, Schemas['StorageVerification'] | 'pending'>>({});

  const verify = useMutation({
    mutationFn: (name: string) => unwrap(api.POST('/backup-storages/{storage}/verify', { params: { path: { storage: name } } })),
    onMutate: (name) => setVerifications((v) => ({ ...v, [name]: 'pending' })),
    onSuccess: (res, name) => setVerifications((v) => ({ ...v, [name]: res })),
    onError: (e, name) => setVerifications((v) => ({ ...v, [name]: { verified: false, message: errorMessage(e), checkedAt: new Date().toISOString() } })),
  });
  const remove = useMutation({
    mutationFn: (name: string) => unwrap(api.DELETE('/backup-storages/{storage}', { params: { path: { storage: name } } })),
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
        title="Backup storages"
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
                  const v = verifications[s.name];
                  return (
                    <tr key={s.name}>
                      <td className="mono">
                        {s.name}
                        {s.description ? <div className="xs faint">{s.description}</div> : null}
                      </td>
                      <td className="mono small">{s.endpointURL}</td>
                      <td className="mono small">
                        {s.bucket}
                        {s.prefix ? `/${s.prefix}` : ''}
                      </td>
                      <td className="small">{s.region}</td>
                      <td className="small">
                        <span className="row wrap" style={{ gap: 4 }}>
                          {s.forcePathStyle ? <Badge plain>path-style</Badge> : null}
                          {s.verifyTLS ? <Badge plain tone="ok">TLS verified</Badge> : <Badge plain tone="warn">TLS unverified</Badge>}
                          {s.credentialsConfigured ? null : <Badge plain tone="danger">no credentials</Badge>}
                        </span>
                      </td>
                      <td className="small">
                        {v === 'pending' ? (
                          <span className="row">
                            <span className="spinner" /> checking…
                          </span>
                        ) : v ? (
                          <Badge tone={v.verified ? 'ok' : 'danger'} title={v.message}>
                            {v.verified ? 'OK' : 'Failed'}
                          </Badge>
                        ) : (
                          <Button size="sm" onClick={() => verify.mutate(s.name)} disabled={!auth.can('backup-storages', 'update')}>
                            Verify
                          </Button>
                        )}
                        {v && v !== 'pending' ? <div className="xs faint">{v.message}</div> : null}
                      </td>
                      <td className="small">
                        <Time iso={s.updatedAt} />
                      </td>
                      <td className="actions">
                        <Menu>
                          <MenuItem onClick={() => verify.mutate(s.name)} disabled={!auth.can('backup-storages', 'update')}>
                            Verify access
                          </MenuItem>
                          <MenuItem onClick={() => setDialog({ mode: 'edit', storage: s })} disabled={!auth.can('backup-storages', 'update')}>
                            Edit…
                          </MenuItem>
                          <MenuItem danger onClick={() => setDel(s)} disabled={!auth.can('backup-storages', 'delete')}>
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
      {dialog ? <StorageDialog existing={dialog.mode === 'edit' ? dialog.storage : undefined} onClose={() => setDialog(null)} /> : null}
      <ConfirmDialog open={del !== null} onClose={() => setDel(null)} onConfirm={() => del && remove.mutate(del.name)} title={`Delete storage ${del?.name ?? ''}`} confirmLabel="Delete" danger busy={remove.isPending}>
        <p className="small">Only unused storages can be deleted; the server refuses while clusters, backups or restores reference it. Data in the bucket is not touched.</p>
      </ConfirmDialog>
    </>
  );
}

function StorageDialog({ existing, onClose }: { existing?: Storage; onClose: () => void }) {
  const toast = useToast();
  const queryClient = useQueryClient();
  const [form, setForm] = useState({
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
  const [error, setError] = useState<string | null>(null);
  const set = (patch: Partial<typeof form>) => setForm((f) => ({ ...f, ...patch }));
  const credsGiven = form.accessKeyId !== '' || form.secretAccessKey !== '';
  const valid = /^[a-z][a-z0-9-]{1,62}$/.test(form.name) && /^https?:\/\//.test(form.endpointURL) && form.bucket && form.region && (existing ? !credsGiven || (form.accessKeyId && form.secretAccessKey) : form.accessKeyId && form.secretAccessKey);

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
      return existing
        ? unwrap(api.PUT('/backup-storages/{storage}', { params: { path: { storage: existing.name } }, body: { ...common, ...(credsGiven ? { credentials } : {}) } }))
        : unwrap(api.POST('/backup-storages', { body: { ...common, name: form.name, credentials } }));
    },
    onSuccess: () => {
      toast.success(existing ? 'Storage updated.' : 'Storage created. Run Verify to check access.');
      void queryClient.invalidateQueries({ queryKey: keys.backupStorages });
      onClose();
    },
    onError: (e) => setError(errorMessage(e)),
  });

  return (
    <Dialog
      open
      onClose={onClose}
      title={existing ? `Edit ${existing.name}` : 'Add backup storage'}
      wide
      onSubmit={() => valid && save.mutate()}
      footer={
        <>
          <Button onClick={onClose}>Cancel</Button>
          <Button type="submit" variant="primary" loading={save.isPending} disabled={!valid}>
            {existing ? 'Save' : 'Create'}
          </Button>
        </>
      }
    >
      {error ? <Callout tone="danger">{error}</Callout> : null}
      <div className="grid-2">
        <TextField label="Name" mono required value={form.name} onChange={(e) => set({ name: e.target.value.toLowerCase() })} disabled={Boolean(existing)} hint="Lowercase, referenced by clusters and schedules." autoFocus={!existing} />
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
