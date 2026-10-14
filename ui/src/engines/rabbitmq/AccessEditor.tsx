// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useMutation } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { Button } from '@/components/ui/Button';
import { Dialog } from '@/components/ui/Dialog';
import { SelectField, TextAreaField, TextField } from '@/components/ui/Field';
import { Callout, CodeLine } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { parseDefinition, parseOptionalLimit, parsePriority, parseUserTags } from './helpers';

export const SYSTEM_USERS: readonly string[] = ['karakoram', 'admin', 'monitor', 'guest'];
type AccessItems = {
  vhost: Schemas['RabbitMQVhost'];
  user: Schemas['RabbitMQUser'];
  permission: Schemas['RabbitMQPermission'];
  topic: Schemas['RabbitMQTopicPermission'];
  policy: Schemas['RabbitMQPolicy'];
  operatorPolicy: Schemas['RabbitMQPolicy'];
};
export type AccessKind = keyof AccessItems;
export type AccessEdit = { [K in AccessKind]: { kind: K; existing?: AccessItems[K] } }[AccessKind];
const LABELS: Record<AccessKind, string> = { vhost: 'virtual host', user: 'user', permission: 'vhost permission', topic: 'topic permission', policy: 'policy', operatorPolicy: 'operator policy' };

export function AccessEditor({ namespace, cluster, edit, vhosts, users, onClose, onSaved }: { namespace: string; cluster: string; edit: AccessEdit; vhosts: string[]; users: string[]; onClose: () => void; onSaved: () => void }) {
  const existing = edit.existing;
  const isPolicy = edit.kind === 'policy' || edit.kind === 'operatorPolicy';
  const isPermission = edit.kind === 'permission' || edit.kind === 'topic';
  const toast = useToast();
  const [name, setName] = useState(existing && 'name' in existing ? existing.name : '');
  const [vhost, setVhost] = useState(existing && 'vhost' in existing ? existing.vhost : vhosts.includes('/') ? '/' : vhosts[0] ?? '');
  const [user, setUser] = useState(existing && 'user' in existing ? existing.user : users[0] ?? '');
  const [tags, setTags] = useState(existing && 'tags' in existing ? existing.tags.join(', ') : '');
  const [password, setPassword] = useState('');
  const [generatedPassword, setGeneratedPassword] = useState<string | null>(null);
  const [queueType, setQueueType] = useState<Schemas['RabbitMQVhost']['defaultQueueType']>(edit.kind === 'vhost' ? edit.existing?.defaultQueueType ?? 'quorum' : 'quorum');
  const [description, setDescription] = useState(edit.kind === 'vhost' ? edit.existing?.description ?? '' : '');
  const [maxConnections, setMaxConnections] = useState(edit.kind === 'vhost' ? String(edit.existing?.limits?.maxConnections ?? '') : '');
  const [maxQueues, setMaxQueues] = useState(edit.kind === 'vhost' ? String(edit.existing?.limits?.maxQueues ?? '') : '');
  const [configure, setConfigure] = useState(edit.kind === 'permission' ? edit.existing?.configure ?? '.*' : '.*');
  const [write, setWrite] = useState(existing && 'write' in existing ? existing.write : '.*');
  const [read, setRead] = useState(existing && 'read' in existing ? existing.read : '.*');
  const [exchange, setExchange] = useState(edit.kind === 'topic' ? edit.existing?.exchange ?? '' : '');
  const [pattern, setPattern] = useState(isPolicy ? edit.existing?.pattern ?? '.*' : '.*');
  const [applyTo, setApplyTo] = useState<Schemas['RabbitMQPolicy']['applyTo']>(isPolicy ? edit.existing?.applyTo ?? 'all' : 'all');
  const [priority, setPriority] = useState(isPolicy ? String(edit.existing?.priority ?? 0) : '0');
  const [definition, setDefinition] = useState(isPolicy ? JSON.stringify(edit.existing?.definition ?? {}, null, 2) : '{}');
  const [validationError, setValidationError] = useState<string | null>(null);

  const save = useMutation({
    mutationFn: async () => {
      const params = { path: { cluster }, query: { namespace } };
      const parsedTags = tags.split(/[\n,]+/).map((tag) => tag.trim()).filter(Boolean);
      switch (edit.kind) {
        case 'vhost': {
          if (!name) throw new Error('A virtual host name is required.');
          const body = { name, defaultQueueType: queueType, description, tags: parsedTags, limits: { maxConnections: parseOptionalLimit(maxConnections, 'Connection limit') ?? -1, maxQueues: parseOptionalLimit(maxQueues, 'Queue limit') ?? -1 } };
          if (edit.existing) await unwrap(api.PUT('/clusters/{cluster}/rabbitmq/vhosts/{vhost}', { params: { path: { cluster, vhost: edit.existing.name }, query: { namespace } }, body }));
          else await unwrap(api.POST('/clusters/{cluster}/rabbitmq/vhosts', { params, body }));
          break;
        }
        case 'user': {
          if (!name.trim()) throw new Error('A username is required.');
          if (SYSTEM_USERS.includes(name)) throw new Error('System users are managed separately.');
          const body = { name, tags: parseUserTags(tags), ...(password ? { password } : {}) };
          if (edit.existing) await unwrap(api.PUT('/clusters/{cluster}/rabbitmq/users/{user}', { params: { path: { cluster, user: edit.existing.name }, query: { namespace } }, body }));
          else {
            const created = await unwrap(api.POST('/clusters/{cluster}/rabbitmq/users', { params, body }));
            return created.generatedPassword;
          }
          break;
        }
        case 'permission': {
          if (!vhost || !user || SYSTEM_USERS.includes(user)) throw new Error('Choose a virtual host and application user.');
          const body = { vhost, user, configure, write, read };
          if (edit.existing) await unwrap(api.PUT('/clusters/{cluster}/rabbitmq/permissions/{vhost}/{user}', { params: { path: { cluster, vhost: edit.existing.vhost, user: edit.existing.user }, query: { namespace } }, body }));
          else await unwrap(api.POST('/clusters/{cluster}/rabbitmq/permissions', { params, body }));
          break;
        }
        case 'topic': {
          if (!vhost || !user || !exchange.trim() || SYSTEM_USERS.includes(user)) throw new Error('Choose a virtual host, application user and topic exchange.');
          const body = { vhost, user, exchange, write, read };
          if (edit.existing) await unwrap(api.PUT('/clusters/{cluster}/rabbitmq/topic-permissions/{vhost}/{user}', { params: { path: { cluster, vhost: edit.existing.vhost, user: edit.existing.user }, query: { namespace } }, body }));
          else await unwrap(api.POST('/clusters/{cluster}/rabbitmq/topic-permissions', { params, body }));
          break;
        }
        case 'policy':
        case 'operatorPolicy': {
          if (!vhost || !name.trim() || !pattern) throw new Error('A virtual host, name and pattern are required.');
          const body = { vhost, name, pattern, applyTo, priority: parsePriority(priority), definition: parseDefinition(definition) };
          if (edit.kind === 'policy') {
            if (edit.existing) await unwrap(api.PUT('/clusters/{cluster}/rabbitmq/policies/{vhost}/{name}', { params: { path: { cluster, vhost: edit.existing.vhost, name: edit.existing.name }, query: { namespace } }, body }));
            else await unwrap(api.POST('/clusters/{cluster}/rabbitmq/policies', { params, body }));
          } else {
            if (edit.existing) await unwrap(api.PUT('/clusters/{cluster}/rabbitmq/operator-policies/{vhost}/{name}', { params: { path: { cluster, vhost: edit.existing.vhost, name: edit.existing.name }, query: { namespace } }, body }));
            else await unwrap(api.POST('/clusters/{cluster}/rabbitmq/operator-policies', { params, body }));
          }
          break;
        }
      }
      return undefined;
    },
    onSuccess: (generated) => {
      toast.success(`${LABELS[edit.kind][0]!.toUpperCase()}${LABELS[edit.kind].slice(1)} ${existing ? 'updated' : 'created'}.`);
      onSaved();
      setPassword('');
      if (generated) setGeneratedPassword(generated);
      else onClose();
    },
    onError: (error) => setValidationError(errorMessage(error)),
  });
  const valid = edit.kind === 'vhost' || edit.kind === 'user' ? Boolean(name.trim()) : Boolean(vhost && (isPolicy ? name.trim() && pattern : user && (edit.kind !== 'topic' || exchange.trim())));
  const close = () => { if (!save.isPending) onClose(); };

  if (generatedPassword) return (
    <Dialog open onClose={onClose} title="User created — copy password now" footer={<Button onClick={onClose}>Done</Button>}>
      <div className="stack">
        <Callout tone="warn">This generated password is shown only once. Store it in your secret manager before closing this dialog.</Callout>
        <p>User <code>{name}</code> has been created. Set its vhost permissions before connecting an application.</p>
        <CodeLine value={generatedPassword} />
      </div>
    </Dialog>
  );

  return (
    <Dialog open wide={isPolicy || isPermission} onClose={close} title={`${existing ? 'Edit' : 'Add'} ${LABELS[edit.kind]}`} onSubmit={() => { if (valid && !save.isPending) { setValidationError(null); save.mutate(); } }} footer={<><Button onClick={close} disabled={save.isPending}>Cancel</Button><Button type="submit" variant="primary" loading={save.isPending} disabled={!valid}>{existing ? 'Save' : 'Create'}</Button></>}>
      <div className="stack">
        {validationError ? <Callout tone="danger">{validationError}</Callout> : null}
        {edit.kind === 'vhost' || edit.kind === 'user' || isPolicy ? <TextField label={edit.kind === 'user' ? 'Username' : 'Name'} value={name} onChange={(event) => setName(event.target.value)} required mono disabled={Boolean(existing)} autoFocus={!existing} /> : null}
        {isPolicy || isPermission ? <SelectField label="Virtual host" value={vhost} onChange={(event) => setVhost(event.target.value)} required disabled={Boolean(existing)}><option value="">Choose a virtual host…</option>{[...new Set([...vhosts, ...(vhost ? [vhost] : [])])].map((value) => <option key={value} value={value}>{value}</option>)}</SelectField> : null}
        {isPermission ? <>
          <SelectField label="Application user" value={user} onChange={(event) => setUser(event.target.value)} required disabled={Boolean(existing)}><option value="">Choose a user…</option>{[...new Set([...users, ...(user ? [user] : [])])].map((value) => <option key={value} value={value}>{value}</option>)}</SelectField>
          <p className="small muted">Patterns use RabbitMQ (Erlang) regular expressions. An empty pattern denies access; <code>.*</code> matches every resource. Topic permissions match routing keys.</p>
          {edit.kind === 'topic' ? <TextField label="Topic exchange" value={exchange} onChange={(event) => setExchange(event.target.value)} required mono disabled={Boolean(existing)} /> : <TextField label="Configure regex" value={configure} onChange={(event) => setConfigure(event.target.value)} mono />}
          <div className="grid-2"><TextField label="Write regex" value={write} onChange={(event) => setWrite(event.target.value)} mono /><TextField label="Read regex" value={read} onChange={(event) => setRead(event.target.value)} mono /></div>
        </> : null}
        {edit.kind === 'vhost' ? <>
          <SelectField label="Default queue type" value={queueType} onChange={(event) => setQueueType(event.target.value as Schemas['RabbitMQVhost']['defaultQueueType'])}><option value="quorum">Quorum</option><option value="classic">Classic</option><option value="stream">Stream</option></SelectField>
          <TextAreaField label="Description" value={description} onChange={(event) => setDescription(event.target.value)} rows={3} />
          <TextField label="Tags (comma-separated)" value={tags} onChange={(event) => setTags(event.target.value)} />
          <div className="grid-2"><TextField label="Maximum connections" type="number" min={-1} step={1} value={maxConnections} onChange={(event) => setMaxConnections(event.target.value)} hint="Empty or -1: unlimited; 0 prevents connections." /><TextField label="Maximum queues" type="number" min={-1} step={1} value={maxQueues} onChange={(event) => setMaxQueues(event.target.value)} hint="Empty or -1: unlimited; 0 prevents queue creation." /></div>
          <p className="small muted">The default queue type applies to newly declared queues. It does not convert existing queues.</p>
        </> : null}
        {edit.kind === 'user' ? <>
          <TextField label="Tags (comma-separated)" value={tags} onChange={(event) => setTags(event.target.value)} hint="administrator, monitoring, management, policymaker; leave empty for application-only access." />
          <TextField label={existing ? 'New password (leave empty to keep)' : 'Password (leave empty to generate)'} type="password" autoComplete="new-password" value={password} onChange={(event) => setPassword(event.target.value)} />
          <Callout>User tags grant management UI privileges, not permission to use a virtual host. Add vhost permissions separately.</Callout>
        </> : null}
        {isPolicy ? <>
          <TextField label="Pattern regex" value={pattern} onChange={(event) => setPattern(event.target.value)} required mono />
          <div className="grid-2"><SelectField label="Apply to" value={applyTo} onChange={(event) => setApplyTo(event.target.value as Schemas['RabbitMQPolicy']['applyTo'])}>{['all', 'queues', 'exchanges', 'classic_queues', 'quorum_queues', 'streams'].map((value) => <option key={value} value={value}>{value}</option>)}</SelectField><TextField label="Priority" type="number" value={priority} onChange={(event) => setPriority(event.target.value)} min={0} step={1} required /></div>
          <TextAreaField label="Definition JSON" value={definition} onChange={(event) => setDefinition(event.target.value)} required mono rows={8} hint={'JSON object, for example {"message-ttl": 60000}. Supported keys depend on the policy and queue type.'} />
          {edit.kind === 'operatorPolicy' ? <Callout>Operator policies enforce resource limits independently of application policies. RabbitMQ combines applicable definitions and uses the more restrictive numeric limit.</Callout> : null}
        </> : null}
      </div>
    </Dialog>
  );
}
