// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useInfiniteQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { idempotencyHeaders, useIdempotencyKey } from '@/api/idempotency';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog, Dialog } from '@/components/ui/Dialog';
import { Checkbox, SelectField, Switch, TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, Empty, ErrorBlock, KV, Loading, Time } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { channelBody, channelForm, channelFormError, destinationChanged, notificationCategories, updateChannelForm, type Channel, type ChannelForm } from './notification-form';

export function NotificationsPage() {
  const auth = useAuth();
  const editable = auth.can('settings', 'update');
  const queryClient = useQueryClient();
  const toast = useToast();
  const [editor, setEditor] = useState<{ channel?: Channel } | null>(null);
  const [action, setAction] = useState<{ kind: 'test' | 'delete'; channel: Channel } | null>(null);
  const [result, setResult] = useState<{ name: string; result: Schemas['NotificationTestResult'] } | null>(null);
  const attempt = useIdempotencyKey();
  const channels = useInfiniteQuery({
    queryKey: keys.notificationChannels, enabled: auth.can('settings', 'read'),
    initialPageParam: undefined as string | undefined,
    queryFn: ({ pageParam }) => unwrap(api.GET('/notification-channels', { params: { query: { limit: 100, cursor: pageParam } } })),
    getNextPageParam: (page) => page.nextCursor || undefined,
    refetchInterval: 15_000,
  });
  const mutate = useMutation({
    mutationFn: async () => {
      if (!action) throw new Error('Choose a channel.');
      const request = { headers: idempotencyHeaders(attempt.key), params: { path: { channel: action.channel.name } } };
      if (action.kind === 'delete') { await unwrap(api.DELETE('/notification-channels/{channel}', request)); return null; }
      return unwrap(api.POST('/notification-channels/{channel}/test', request));
    },
    onSuccess: (testResult) => {
      if (action && testResult) setResult({ name: action.channel.name, result: testResult });
      else toast.success('Notification channel deleted.');
      setAction(null);
      attempt.reset();
      void queryClient.invalidateQueries({ queryKey: keys.notificationChannels });
    },
    onError: (error) => toast.error(errorMessage(error)),
  });
  if (!auth.can('settings', 'read')) return <Callout>You do not have permission to view settings.</Callout>;
  const items = channels.data?.pages.flatMap((page) => page.items) ?? [];
  const openAction = (kind: 'test' | 'delete', channel: Channel) => { mutate.reset(); attempt.reset(); setAction({ kind, channel }); };
  return <>
    <Card title="Notifications" actions={editable ? <Button size="sm" variant="primary" onClick={() => setEditor({})}>Add channel</Button> : null}>
      <p className="small muted">Deliver operational events to webhooks, Slack or email. Channels apply to this installation, independently of the namespace selected in the sidebar.</p>
      {result ? <Callout tone={result.result.delivered ? 'ok' : 'danger'} title={`Test ${result.result.delivered ? 'delivered' : 'failed'}: ${result.name}`}>
        {result.result.delivered ? 'The saved destination accepted the test notification.' : result.result.error || 'The destination did not accept the test notification.'}
        <Button size="sm" variant="ghost" onClick={() => setResult(null)}>Dismiss</Button>
      </Callout> : null}
      {channels.error ? <ErrorBlock error={channels.error} retry={() => channels.refetch()} /> : null}
      {channels.isPending ? <Loading /> : !items.length && !channels.error ? <Empty title="No notification channels">Add a destination to receive operational alerts.</Empty> : <div className="stack">
        {items.map((channel) => <Card key={channel.name} title={<span className="row wrap"><span className="mono">{channel.name}</span><Badge>{channel.type}</Badge><Badge tone={channel.enabled ? 'ok' : 'neutral'}>{channel.enabled ? 'Enabled' : 'Disabled'}</Badge></span>} actions={editable ? <>
          <Button size="sm" onClick={() => setEditor({ channel })}>Edit</Button>
          <Button size="sm" onClick={() => openAction('test', channel)}>Test</Button>
          <Button size="sm" variant="danger" onClick={() => openAction('delete', channel)}>Delete</Button>
        </> : null}>
          <KV items={[
            ['Destination', channel.type === 'webhook' ? channel.webhook?.host || 'Webhook host not reported' : channel.type === 'slack' ? 'Slack incoming webhook (write-only)' : `${channel.email?.host}:${channel.email?.port} · ${channel.email?.security}`],
            ['Categories', channel.categories?.length ? channel.categories.join(', ') : 'All categories'],
            ['Namespaces', channel.namespaces?.length ? channel.namespaces.join(', ') : 'All namespaces + installation-wide events'],
            ['Last delivery', <Time iso={channel.status?.lastDeliveryAt} />],
            ['Delivery queue', `${channel.status?.pending ?? 0} pending · ${channel.status?.failed ?? 0} failed`],
          ]} />
          {channel.type === 'webhook' ? <p className="xs muted">URL {channel.webhook?.urlConfigured ? 'configured' : 'not configured'} · Signing secret {channel.webhook?.signingSecretConfigured ? 'configured' : 'not configured'}</p> : channel.type === 'email' ? <p className="xs muted">Password {channel.email?.passwordConfigured ? 'configured' : 'not configured'} · To: {channel.email?.to.join(', ')}</p> : <p className="xs muted">Webhook URL {channel.slack?.webhookUrlConfigured ? 'configured' : 'not configured'}</p>}
          {channel.status?.lastError ? <Callout tone="warn" title="Last delivery error">{channel.status.lastError}<div className="xs"><Time iso={channel.status.lastErrorAt} /></div></Callout> : null}
        </Card>)}
      </div>}
      {channels.hasNextPage ? <Button loading={channels.isFetchingNextPage} onClick={() => channels.fetchNextPage()}>Load more channels</Button> : null}
    </Card>
    {editor ? <ChannelDialog existing={editor.channel} onClose={() => setEditor(null)} /> : null}
    <ConfirmDialog open={action !== null} onClose={() => setAction(null)} title={`${action?.kind === 'delete' ? 'Delete' : 'Test'} channel ${action?.channel.name ?? ''}`} confirmLabel={action?.kind === 'delete' ? 'Delete channel' : 'Send test'} danger={action?.kind === 'delete'} busy={mutate.isPending} error={mutate.error} onConfirm={() => mutate.mutateAsync()}>
      <p>{action?.kind === 'delete' ? 'This removes the channel and its saved secrets. It will no longer receive notifications.' : 'Send a real notification to the saved destination. This tests delivery, not category or namespace matching.'}</p>
    </ConfirmDialog>
  </>;
}

export function ChannelDialog({ existing, onClose }: { existing?: Channel; onClose: () => void }) {
  const queryClient = useQueryClient();
  const toast = useToast();
  const attempt = useIdempotencyKey();
  const [form, setForm] = useState(() => channelForm(existing));
  const save = useMutation({
    mutationFn: () => {
      const body = channelBody(form, existing);
      return existing ? unwrap(api.PUT('/notification-channels/{channel}', { headers: idempotencyHeaders(attempt.key), params: { path: { channel: existing.name } }, body }))
        : unwrap(api.POST('/notification-channels', { headers: idempotencyHeaders(attempt.key), body }));
    },
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: keys.notificationChannels });
      toast.success(existing ? 'Notification channel updated.' : 'Notification channel created.');
      onClose();
    },
  });
  const set = (patch: Partial<ChannelForm>) => { attempt.reset(); save.reset(); setForm((current) => updateChannelForm(current, patch, existing)); };
  const invalid = channelFormError(form, existing);
  const changed = destinationChanged(form, existing);
  return <Dialog open wide title={existing ? `Edit channel ${existing.name}` : 'Add notification channel'} onClose={onClose} busy={save.isPending} error={save.error} onSubmit={() => invalid ? false : save.mutateAsync()} footer={<><Button onClick={onClose}>Cancel</Button><Button type="submit" variant="primary" disabled={Boolean(invalid)} loading={save.isPending}>{existing ? 'Save channel' : 'Create channel'}</Button></>}>
    <div className="stack">
      <Callout>Secrets are write-only. Empty secret fields keep the saved values only for an unchanged destination. Changing the destination clears entered credentials; re-enter them for the new destination.</Callout>
      <div className="grid-2">
        <TextField label="Channel name" required mono value={form.name} readOnly={Boolean(existing)} onChange={(e) => set({ name: e.target.value })} hint="1–40 lowercase letters, digits or hyphens; start with a letter." />
        <SelectField label="Channel type" value={form.type} onChange={(e) => set({ type: e.target.value as Channel['type'] })}><option value="webhook">Webhook</option><option value="slack">Slack</option><option value="email">Email</option></SelectField>
      </div>
      <Switch label="Enable channel" checked={form.enabled} onChange={(e) => set({ enabled: e.target.checked })} />
      {form.type === 'webhook' ? <>
        <TextField label={existing?.type === 'webhook' && existing.webhook?.urlConfigured ? 'Webhook URL (empty keeps saved)' : 'Webhook URL'} type="password" autoComplete="new-password" value={form.url} onChange={(e) => set({ url: e.target.value })} hint={existing?.webhook?.host ? `Saved host: ${existing.webhook.host}. The full URL is never returned.` : 'HTTPS required unless the operator explicitly allowlists the host.'} />
        <TextField label="Signing secret" type="password" autoComplete="new-password" value={form.signingSecret} onChange={(e) => set({ signingSecret: e.target.value })} hint={existing?.type === 'webhook' && existing.webhook?.signingSecretConfigured ? changed ? 'Destination changed: re-enter the signing secret.' : 'Configured. Leave empty to keep.' : 'Optional HMAC-SHA256 signing secret.'} />
      </> : form.type === 'slack' ? <TextField label="Slack incoming webhook URL" type="password" autoComplete="new-password" value={form.slackURL} onChange={(e) => set({ slackURL: e.target.value })} hint={existing?.type === 'slack' && existing.slack?.webhookUrlConfigured ? 'Configured. Leave empty to keep.' : 'Write-only; never shown again.'} /> : <div className="grid-2">
        <TextField label="SMTP host" required value={form.host} onChange={(e) => set({ host: e.target.value })} />
        <TextField label="SMTP port" type="number" min={1} max={65535} required value={form.port} onChange={(e) => set({ port: e.target.value })} />
        <SelectField label="Transport security" value={form.security} onChange={(e) => set({ security: e.target.value as ChannelForm['security'] })}><option value="starttls">STARTTLS (required)</option><option value="tls">Implicit TLS</option></SelectField>
        <TextField label="SMTP username" value={form.username} onChange={(e) => set({ username: e.target.value })} />
        <TextField label="SMTP password" type="password" autoComplete="new-password" value={form.password} onChange={(e) => set({ password: e.target.value })} hint={existing?.type === 'email' && existing.email?.passwordConfigured ? changed ? 'Destination changed: enter a new password.' : 'Configured. Leave empty to keep.' : 'Required when a username is set. Write-only.'} />
        <TextField label="Sender" required value={form.from} onChange={(e) => set({ from: e.target.value })} />
        <TextField label="Recipients" required value={form.to} onChange={(e) => set({ to: e.target.value })} hint="1–20 addresses, separated by commas." />
      </div>}
      <fieldset style={{ margin: 0, padding: 12, border: '1px solid var(--border)' }}><legend>Event categories</legend>
        <Checkbox label="All categories" checked={form.allCategories} onChange={(e) => set({ allCategories: e.target.checked })} />
        {!form.allCategories ? <div className="grid-2">{notificationCategories.map((category) => <Checkbox key={category} label={category} checked={form.categories.includes(category)} onChange={(e) => set({ categories: e.target.checked ? [...form.categories, category] : form.categories.filter((value) => value !== category) })} />)}</div> : null}
      </fieldset>
      <TextField label="Namespaces" value={form.namespaces} onChange={(e) => set({ namespaces: e.target.value })} hint="Comma-separated namespace names. Empty means all namespaces, including installation-wide events. A specific list excludes installation-wide events." />
      {invalid ? <p className="small muted" role="status">{invalid}</p> : null}
    </div>
  </Dialog>;
}
