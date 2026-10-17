// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { idempotencyHeaders } from '@/api/idempotency';
import { keys } from '@/api/keys';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog } from '@/components/ui/Dialog';
import { TextAreaField, TextField } from '@/components/ui/Field';
import { Callout, Card, ErrorBlock, Loading } from '@/components/ui/Primitives';
import { useClusterContext } from '@/pages/clusters/detail/ClusterDetail';
import { useSubmissionKey } from '@/pages/clusters/detail/use-submission-key';
import { validateFunction } from './function-validation';

export function FunctionsTab() {
  const { cluster, namespace, name, can } = useClusterContext();
  const client = useQueryClient();
  const params = { path: { cluster: name }, query: { namespace } };
  const key = keys.clusterSub(namespace, name, 'supabase-functions');
  const functions = useQuery({ queryKey: key, queryFn: () => unwrap(api.GET('/clusters/{cluster}/supabase/functions', { params })), enabled: can('read') });
  const [functionName, setFunctionName] = useState('');
  const [files, setFiles] = useState<Schemas['SupabaseFunction']>({ 'index.ts': '' });
  const [smtpPassword, setSMTPPassword] = useState('');
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [deletion, setDeletion] = useState<string | null>(null);
  const submission = useSubmissionKey(JSON.stringify({ functionName, files, smtpPassword, deletion }));
  const editable = can('update') && !['Deleting', 'Suspended'].includes(cluster.status.phase);
  const run = async (action: () => Promise<void>) => {
    if (busy) return;
    setBusy(true); setError(null); setMessage(null);
    try { await action(); submission.reset(); } catch (e) { setError(errorMessage(e)); } finally { setBusy(false); }
  };
  const errors = validateFunction(functionName, files);
  return <div className="stack">
    {error ? <Callout tone="danger">{error}</Callout> : null}{message ? <Callout>{message}</Callout> : null}
    <Card title="Edge functions">
      {functions.isPending ? <Loading /> : functions.error ? <ErrorBlock error={functions.error} retry={() => functions.refetch()} /> : functions.data?.items.length ? functions.data.items.map((fn) => <div className="row-between" key={fn}><code>{fn}</code><div className="row"><Button disabled={busy} onClick={() => void run(async () => { const source = await unwrap(api.GET('/clusters/{cluster}/supabase/functions/{name}', { params: { ...params, path: { cluster: name, name: fn } } })); setFunctionName(fn); setFiles(source); })}>Edit source</Button><Button disabled={!editable || busy} onClick={() => setDeletion(fn)}>Delete</Button></div></div>) : <p className="small muted">No deployed functions.</p>}
      <fieldset disabled={!editable || busy} className="stack" style={{ border: 0, padding: 0 }}>
        <TextField label="Function name" value={functionName} onChange={(e) => setFunctionName(e.target.value)} />
        {Object.entries(files).map(([path, source], index) => <div className="stack" key={index}><TextField label="Relative source path" value={path} onChange={(e) => setFiles(Object.fromEntries(Object.entries(files).map(([p, s]) => [p === path ? e.target.value : p, s])))} /><TextAreaField label={`${path || 'File'} source`} mono rows={12} value={source} onChange={(e) => setFiles({ ...files, [path]: e.target.value })} /><Button onClick={() => setFiles(Object.fromEntries(Object.entries(files).filter(([p]) => p !== path)))}>Remove file</Button></div>)}
        <div className="row"><Button disabled={'' in files} onClick={() => setFiles({ ...files, '': '' })}>Add source file</Button><Button variant="primary" disabled={errors.length > 0} onClick={() => void run(async () => {
          await unwrap(api.PUT('/clusters/{cluster}/supabase/functions/{name}', { headers: idempotencyHeaders(submission.key), params: { ...params, path: { cluster: name, name: functionName } }, body: files }));
          void client.invalidateQueries({ queryKey: key }); setMessage('Function deployment submitted; enabled functions members receive the new source during reconciliation.');
        })}>Deploy function</Button></div>
        {errors.length ? <span className="field-error">{errors[0]}</span> : null}
      </fieldset>
      {!cluster.spec.components.extra?.functions?.replicas ? <Callout tone="warn">Enable the functions component in Components to serve deployed function sources.</Callout> : null}
    </Card>
    <Card title="SMTP password">
      <p className="small muted">Write-only secret. The current password cannot be read. Configure the remaining SMTP settings in Components.</p>
      <TextField label="New SMTP password" type="password" autoComplete="new-password" value={smtpPassword} disabled={!editable || busy} onChange={(e) => setSMTPPassword(e.target.value)} />
      <Button disabled={!editable || busy || !smtpPassword} variant="primary" onClick={() => void run(async () => { await unwrap(api.PUT('/clusters/{cluster}/supabase/smtp-password', { headers: idempotencyHeaders(submission.key), params, body: { password: smtpPassword } })); setSMTPPassword(''); setMessage('SMTP password updated.'); })}>Set SMTP password</Button>
    </Card>
    <ConfirmDialog open={Boolean(deletion)} title={`Delete function ${deletion}`} confirmLabel="Delete" busy={busy} onClose={() => setDeletion(null)} onConfirm={() => void run(async () => { if (!deletion) return; await unwrap(api.DELETE('/clusters/{cluster}/supabase/functions/{name}', { headers: idempotencyHeaders(submission.key), params: { ...params, path: { cluster: name, name: deletion } } })); if (functionName === deletion) { setFunctionName(''); setFiles({ 'index.ts': '' }); } setDeletion(null); void client.invalidateQueries({ queryKey: key }); })}><Callout tone="danger">The function source will be removed from the cluster and its running function services.</Callout></ConfirmDialog>
  </div>;
}
