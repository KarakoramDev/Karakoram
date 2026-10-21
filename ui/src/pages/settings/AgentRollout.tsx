// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, unwrap, type Schemas } from '@/api/client';
import { idempotencyHeaders, useIdempotencyKey } from '@/api/idempotency';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { Button } from '@/components/ui/Button';
import { Dialog } from '@/components/ui/Dialog';
import { Switch, TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, ErrorBlock, KV, Loading } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';

type Rollout = Schemas['AgentRolloutSettings'];

export function AgentRolloutPage() {
  const auth = useAuth();
  const [editing, setEditing] = useState(false);
  const settings = useQuery({ queryKey: keys.agentRollout, queryFn: () => unwrap(api.GET('/settings/agent-rollout')), enabled: auth.can('settings', 'read'), refetchInterval: 60_000 });
  if (!auth.can('settings', 'read')) return <Callout>You do not have permission to view settings.</Callout>;
  return <>
    <Card title="Agent rollout" actions={auth.can('settings', 'update') && settings.data ? <Button size="sm" onClick={() => setEditing(true)}>Edit rollout settings</Button> : null}>
      <p className="small muted">Keep member agents aligned with the control-plane executable without changing database engine versions.</p>
      {settings.error ? <ErrorBlock error={settings.error} retry={() => settings.refetch()} /> : null}
      {settings.isPending ? <Loading /> : settings.data ? <KV items={[
        ['Automatic rollout', <Badge tone={settings.data.automatic ? 'ok' : 'neutral'}>{settings.data.automatic ? 'Enabled' : 'Disabled'}</Badge>],
        ['Concurrent clusters', settings.data.maxConcurrentClusters],
      ]} /> : null}
      <Callout>Rollouts proceed one member at a time through each engine’s safety checks and respect maintenance windows. Failed rollouts for a build are not automatically retried; rerun them from the cluster after resolving the cause.</Callout>
    </Card>
    <Card title="Current control-plane build">
      <p className="small muted">This is the agent build targeted by new rollouts. Older agents that do not report a build appear as unknown on the cluster.</p>
      {settings.isPending ? <Loading /> : settings.data?.currentBuild ? <KV items={[
        ['Version', settings.data.currentBuild.version || 'Unknown'],
        ['Commit', <span className="mono" style={{ overflowWrap: 'anywhere' }}>{settings.data.currentBuild.commit || 'Unknown'}</span>],
        ['Build identity', <span className="mono" style={{ overflowWrap: 'anywhere' }}>{settings.data.currentBuild.build || 'Unknown'}</span>],
      ]} /> : <p className="muted">Current build identity is not reported.</p>}
    </Card>
    {editing && settings.data ? <AgentRolloutDialog settings={settings.data} onClose={() => setEditing(false)} /> : null}
  </>;
}

export function AgentRolloutDialog({ settings, onClose }: { settings: Rollout; onClose: () => void }) {
  const queryClient = useQueryClient();
  const toast = useToast();
  const attempt = useIdempotencyKey();
  const [automatic, setAutomatic] = useState(settings.automatic);
  const [concurrency, setConcurrency] = useState(String(settings.maxConcurrentClusters));
  const save = useMutation({
    mutationFn: () => unwrap(api.PUT('/settings/agent-rollout', { headers: idempotencyHeaders(attempt.key), body: { automatic, maxConcurrentClusters: Number(concurrency) } })),
    onSuccess: (saved) => {
      queryClient.setQueryData(keys.agentRollout, saved);
      void queryClient.invalidateQueries({ queryKey: keys.agentRollout });
      toast.success('Agent rollout settings saved.');
      onClose();
    },
  });
  const valid = Number.isInteger(Number(concurrency)) && Number(concurrency) >= 1 && Number(concurrency) <= 5;
  return <Dialog open title="Edit agent rollout settings" onClose={onClose} busy={save.isPending} error={save.error} onSubmit={() => valid ? save.mutateAsync() : false} footer={<><Button onClick={onClose}>Cancel</Button><Button type="submit" variant="primary" disabled={!valid} loading={save.isPending}>Save rollout settings</Button></>}>
    <div className="stack">
      <Switch label="Automatically roll out new agent builds" checked={automatic} onChange={(e) => { attempt.reset(); save.reset(); setAutomatic(e.target.checked); }} />
      <TextField label="Maximum concurrent clusters" required type="number" min={1} max={5} step={1} value={concurrency} onChange={(e) => { attempt.reset(); save.reset(); setConcurrency(e.target.value); }} hint="1–5 clusters installation-wide. Members within each cluster roll one at a time." />
      <Callout tone="warn">Enabling automatic rollout allows disruptive member restarts after a control-plane upgrade. Maintenance windows and engine-native safety gates still apply.</Callout>
    </div>
  </Dialog>;
}
