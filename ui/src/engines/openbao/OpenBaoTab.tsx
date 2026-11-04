// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useEffect, useReducer, useRef, useState } from 'react';
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { useClusters } from '@/api/hooks';
import { idempotencyHeaders } from '@/api/idempotency';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { clusterObject, type Action, type Resource } from '@/auth/permissions';
import { Button } from '@/components/ui/Button';
import { ConfirmDialog, Dialog } from '@/components/ui/Dialog';
import { SelectField, TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, CodeLine, CopyButton, ErrorBlock, KV, Loading, Time, type Tone } from '@/components/ui/Primitives';
import { useToast } from '@/components/ui/Toast';
import { useClusterContext } from '@/pages/clusters/detail/ClusterDetail';
import { useSubmissionKey } from '@/pages/clusters/detail/use-submission-key';
import { INITIAL_REVEAL, revealReducer, revealUnsealKeys, shareLabel, unsealKeysExport } from './reveal';
import { ACCESS_LEVELS, CONDITION_GUIDANCE, DEFAULT_MAX_TTL_SECONDS, DEFAULT_TTL_SECONDS, MEMBER_STATE_LABEL, TARGET_PROVIDER_LABEL, VOTER_LABEL, buildConnectionWrite, connectionTargets, databaseScope, emptyRole, formatTtl, readOptions, sealLabel, sealOverview, storageLabel, type MemberSealState, type RoleDraft } from './spec';

type Can = (action: Action, resource?: Resource) => boolean;

export default function OpenBaoTab() {
  const { cluster, namespace, name, can } = useClusterContext();
  const auth = useAuth();
  const targetCan: TargetCan = (action, target) => auth.can('database-users', action, clusterObject(namespace, target));
  return (
    <div className="stack" style={{ gap: 20 }}>
      <SealOverview cluster={cluster} />
      <Card title="Unseal keys and root token">
        <UnsealKeysPanel namespace={namespace} name={name} allowed={can('reveal', 'openbao-unseal-keys')} />
      </Card>
      <DatabaseConnections namespace={namespace} name={name} can={can} targetCan={targetCan} />
    </div>
  );
}

const STATE_TONE: Record<MemberSealState, Tone> = { active: 'ok', standby: 'ok', sealed: 'warn', uninitialized: 'neutral', pending: 'progress', leaving: 'warn', unknown: 'danger' };

export function SealOverview({ cluster }: { cluster: Schemas['Cluster'] }) {
  const options = readOptions(cluster.spec);
  const overview = sealOverview(cluster.status);
  const suspended = cluster.status.phase === 'Suspended';
  const notices = (['initializationLost', 'quorumBlocked', 'sealed'] as const).filter((key) => overview.conditions[key]);
  return (
    <Card title="Seal and initialization">
      <div className="stack">
        {notices.map((key) => <Callout key={key} tone={CONDITION_GUIDANCE[key].tone} title={CONDITION_GUIDANCE[key].title}>
          <p>{overview.conditions[key]!.message}</p>
          <p className="small">{CONDITION_GUIDANCE[key].guidance}</p>
        </Callout>)}
        {!notices.length && !suspended && overview.uninitialized.length === cluster.status.members.length && cluster.status.members.length > 0 ? <Callout tone="info" title="Initializing">The first member is being initialized; the other members join once it is unsealed and active.</Callout> : null}
        <KV items={[
          ['Seal', sealLabel(options.seal.type)],
          [options.seal.type === 'shamir' ? 'Unseal shares' : 'Recovery shares', `${options.unseal.threshold} of ${options.unseal.shares} required`],
          ['Storage', storageLabel(options)],
          ['Active member', overview.active ?? (suspended ? 'Runtime suspended' : 'None (no unsealed active node)')],
          ['Log level', options.logLevel],
        ]} />
        {cluster.status.members.length ? <div className="table-wrap"><table className="table">
          <thead><tr><th>Member</th><th>Seal state</th><th>Raft</th><th>Version</th></tr></thead>
          <tbody>{overview.members.map((member) => <tr key={member.name}>
            <td><code>{member.name}</code></td>
            <td><Badge tone={suspended ? 'neutral' : STATE_TONE[member.state]}>{MEMBER_STATE_LABEL[member.state]}</Badge></td>
            <td>{member.voter ? <Badge tone={suspended ? 'neutral' : member.voter.status === 'True' ? 'ok' : member.voter.status === 'False' ? 'warn' : 'neutral'} title={member.voter.message}>{VOTER_LABEL[member.voter.reason] ?? member.voter.reason}</Badge> : <span className="faint">—</span>}</td>
            <td>{member.version ?? <span className="faint">—</span>}</td>
          </tr>)}</tbody>
        </table></div> : <p className="small muted">Members appear once the cluster is deployed.</p>}
        <p className="xs faint">Sealed members are re-unsealed by the control plane without operator action. The API listens on port 8200 over TLS; the Raft/forwarding listener on 8201 is cluster-internal.</p>
      </div>
    </Card>
  );
}

/** Reveal is one audited POST per confirmation; the result lives only in this panel's state. */
export function UnsealKeysPanel({ namespace, name, allowed }: { namespace: string; name: string; allowed: boolean }) {
  if (!allowed) return <p className="xs faint">Revealing unseal keys requires the <code>openbao-unseal-keys</code> reveal permission, which only administrators hold by default. Every reveal is recorded in the audit log.</p>;
  return <IdentityUnsealKeysPanel key={JSON.stringify([namespace, name])} namespace={namespace} name={name} />;
}

function IdentityUnsealKeysPanel({ namespace, name }: { namespace: string; name: string }) {
  const [state, dispatch] = useReducer(revealReducer, INITIAL_REVEAL);
  const [confirming, setConfirming] = useState(false);
  const request = useRef<AbortController | null>(null);
  useEffect(() => () => { request.current?.abort(); }, []);

  if (state.keys) {
    const keys = state.keys;
    return (
      <div className="stack">
        <div className="row-between">
          <strong className="small">{keys.recoveryShares ? 'Recovery shares' : 'Unseal shares'} · {keys.threshold} of {keys.shares.length} required · seal {keys.sealType}</strong>
          <span className="row"><CopyButton value={unsealKeysExport(name, keys)} label="Copy all" /><Button size="sm" variant="ghost" onClick={() => dispatch({ type: 'hide' })}>Hide</Button></span>
        </div>
        <Callout tone="danger" title="Store these offline now">
          Anyone holding {keys.threshold} shares can unseal this cluster or generate a new root token; the root token has unrestricted access. Put the shares and the token in an offline password manager or sealed envelopes, split between people where possible. They are shown only while this panel is open and are not kept by the browser. Karakoram holds its own copy to unseal members; if the control plane's master key is ever lost, these are the only way to recover the cluster.
        </Callout>
        {keys.shares.map((share, index) => <div key={index}><strong className="small">{shareLabel(keys, index)}</strong><CodeLine value={share} /></div>)}
        <div><strong className="small">Initial root token</strong><CodeLine value={keys.initialRootToken} /></div>
        <p className="xs faint">Initialized <Time iso={keys.initializedAt} exact />. Karakoram manages the cluster with its own periodic token; the root token is for emergency recovery and should be revoked after creating your own admin policies.</p>
      </div>
    );
  }

  return (
    <div className="stack" style={{ gap: 6 }}>
      <p className="small">The shares and the initial root token were produced once, when the first member was initialized, and are held encrypted by the control plane. Reveal them to store an offline copy; the response is fetched on demand, never cached, and the reveal is audited.</p>
      <div><Button size="sm" loading={state.busy} onClick={() => setConfirming(true)}>Reveal unseal keys…</Button></div>
      {state.error ? <span className="field-error">{state.error}</span> : null}
      <ConfirmDialog
        open={confirming}
        onClose={() => { if (!state.busy) setConfirming(false); }}
        title="Reveal unseal keys and root token"
        confirmLabel="Reveal"
        danger
        typeToConfirm={name}
        busy={state.busy}
        onConfirm={async () => {
          request.current?.abort();
          const controller = new AbortController();
          request.current = controller;
          dispatch({ type: 'request' });
          try {
            const keys = await unwrap(revealUnsealKeys(api, namespace, name, name, controller.signal));
            if (controller.signal.aborted) return;
            dispatch({ type: 'resolved', keys });
            setConfirming(false);
          } catch (error) {
            if (controller.signal.aborted) return;
            dispatch({ type: 'failed', error: errorMessage(error) });
            setConfirming(false);
          }
        }}
      >
        <Callout tone="warn">Revealing exposes the material that can unseal the cluster and the root token with unrestricted access. Make sure nobody can see your screen and have your offline storage ready. The reveal is recorded in the audit log with your identity.</Callout>
      </ConfirmDialog>
    </div>
  );
}

const PHASE_TONE: Record<Schemas['OpenBaoDatabaseConnection']['phase'], Tone> = { pending: 'progress', ready: 'ok', deleting: 'warn', failed: 'danger' };

/** database-users permission on a *target* cluster of this namespace (create for new connections, delete for removal). */
type TargetCan = (action: 'create' | 'delete', target: string) => boolean;

export function DatabaseConnections({ namespace, name, can, targetCan }: { namespace: string; name: string; can: Can; targetCan: TargetCan }) {
  const queryClient = useQueryClient();
  const toast = useToast();
  const [creating, setCreating] = useState(false);
  const [remove, setRemove] = useState<Schemas['OpenBaoDatabaseConnection'] | null>(null);
  const allowed = can('read');
  const queryKey = keys.clusterSub(namespace, name, 'openbao-connections');
  const params = { path: { cluster: name }, query: { namespace } };
  const connections = useQuery({ queryKey, queryFn: () => unwrap(api.GET('/clusters/{cluster}/openbao/database-connections', { params })), enabled: allowed, refetchInterval: (query) => query.state.data?.items.some((item) => item.phase === 'pending' || item.phase === 'deleting') ? 5_000 : false });
  const clusters = useClusters(namespace);
  const targets = connectionTargets(clusters.data?.items ?? [], namespace).filter((c) => c.name !== name && targetCan('create', c.name));
  const canCreate = can('update');
  const canDelete = (connection: Schemas['OpenBaoDatabaseConnection']) => can('update') && targetCan('delete', connection.targetCluster);
  const deletion = useMutation({
    mutationFn: (connection: string) => unwrap(api.DELETE('/clusters/{cluster}/openbao/database-connections/{connection}', { params: { path: { cluster: name, connection }, query: { namespace } } })),
    onSuccess: () => { setRemove(null); void queryClient.invalidateQueries({ queryKey }); toast.success('Database connection removal started.'); },
    onError: (error) => toast.error(errorMessage(error)),
  });

  const items = connections.data?.items ?? [];
  return (
    <Card title="Database connections" actions={canCreate ? <Button size="sm" variant="primary" onClick={() => setCreating(true)}>+ Add connection</Button> : null} flush>
      {!allowed ? <div className="card-body"><Callout>You do not have permission to read this cluster's database connections.</Callout></div>
        : connections.isPending ? <Loading />
        : connections.error ? <div className="card-body"><ErrorBlock error={connections.error} retry={() => connections.refetch()} /></div>
        : items.length === 0 ? <div className="table-empty">No database connections. A connection lets OpenBao issue short-lived credentials for a PostgreSQL, MongoDB replica-set or RabbitMQ cluster in this namespace.</div>
        : <div className="table-wrap"><table className="table">
          <thead><tr><th>Name</th><th>Target</th><th>Mount</th><th>Phase</th><th>Roles</th><th className="actions"></th></tr></thead>
          <tbody>{items.map((item) => <tr key={item.name}>
            <td><code>{item.name}</code></td>
            <td><code>{item.targetCluster}</code><br /><span className="small muted">{TARGET_PROVIDER_LABEL[item.targetProvider] ?? item.targetProvider}</span></td>
            <td><code>{item.mountPath}</code></td>
            <td><Badge tone={PHASE_TONE[item.phase]} title={item.message}>{item.phase}</Badge>{item.message ? <div className="xs muted">{item.message}</div> : null}</td>
            <td><div className="stack" style={{ gap: 4 }}>{item.roles.map((role) => <div key={role.name} className="small">
              <strong>{role.name}</strong> · {role.access}{role.database ? <> · <code>{role.database}</code></> : null} · TTL {formatTtl(role.defaultTtlSeconds, DEFAULT_TTL_SECONDS)}, max {formatTtl(role.maxTtlSeconds, DEFAULT_MAX_TTL_SECONDS)}
              <div><code>{role.credentialsPath}</code></div>
            </div>)}</div></td>
            <td className="actions">{canDelete(item) && item.phase !== 'deleting' ? <Button size="sm" variant="ghost" onClick={() => setRemove(item)}>Delete…</Button> : null}</td>
          </tr>)}</tbody>
        </table></div>}
      <div className="card-body"><p className="xs faint">Applications read credentials from the listed path with their own OpenBao token, for example <code>bao read database/creds/&lt;connection&gt;-&lt;role&gt;</code>. Creating a connection requires <code>database-users:create</code> on the target cluster; Karakoram provisions a dedicated administrative identity there.</p></div>
      {creating ? <ConnectionDialog namespace={namespace} cluster={name} targets={targets} loading={clusters.isPending} onClose={() => setCreating(false)} onSaved={() => { void queryClient.invalidateQueries({ queryKey }); }} /> : null}
      <ConfirmDialog open={Boolean(remove)} onClose={() => { if (!deletion.isPending) setRemove(null); }} title="Delete database connection?" confirmLabel="Delete" danger busy={deletion.isPending} typeToConfirm={remove?.name} onConfirm={() => { if (remove) deletion.mutate(remove.name); }}>
        <Callout tone="danger">Deleting <code>{remove?.name}</code> revokes every credential OpenBao issued through it, unmounts <code>{remove?.mountPath}</code> and removes the administrative identity Karakoram created on <code>{remove?.targetCluster}</code>. Applications still using issued credentials lose access immediately.</Callout>
      </ConfirmDialog>
    </Card>
  );
}

type Target = Pick<Schemas['Cluster'], 'name' | 'spec'>;

export function ConnectionDialog({ namespace, cluster, targets, loading, onClose, onSaved }: { namespace: string; cluster: string; targets: Target[]; loading: boolean; onClose: () => void; onSaved: () => void }) {
  const toast = useToast();
  const [name, setName] = useState('');
  const [target, setTarget] = useState('');
  const [roles, setRoles] = useState<RoleDraft[]>([emptyRole()]);
  const [problems, setProblems] = useState<string[]>([]);
  const targetProvider = targets.find((c) => c.name === target)?.spec.provider;
  const scope = targetProvider ? databaseScope(targetProvider) : undefined;
  const submission = useSubmissionKey(JSON.stringify({ namespace, cluster, name, target, roles }));
  const save = useMutation({
    mutationFn: (body: Schemas['OpenBaoDatabaseConnectionWrite']) => unwrap(api.POST('/clusters/{cluster}/openbao/database-connections', { headers: idempotencyHeaders(submission.key), params: { path: { cluster }, query: { namespace } }, body })),
    onSuccess: (created) => { submission.reset(); toast.success(`Database connection ${created.name} is being configured.`); onSaved(); onClose(); },
    onError: (error) => setProblems([errorMessage(error)]),
  });
  const setRole = (index: number, patch: Partial<RoleDraft>) => setRoles((current) => current.map((role, i) => i === index ? { ...role, ...patch } : role));
  const close = () => { if (!save.isPending) onClose(); };

  return (
    <Dialog open wide onClose={close} title="Add database connection" onSubmit={() => {
      if (save.isPending) return;
      const { body, errors } = buildConnectionWrite(name, target, targetProvider, roles);
      setProblems(errors);
      if (!errors.length) save.mutate(body);
    }} footer={<><Button onClick={close} disabled={save.isPending}>Cancel</Button><Button type="submit" variant="primary" loading={save.isPending}>Create</Button></>}>
      <div className="stack">
        {problems.length ? <Callout tone="danger"><ul style={{ margin: 0, paddingLeft: 18 }}>{problems.map((problem) => <li key={problem}>{problem}</li>)}</ul></Callout> : null}
        <div className="grid-2">
          <TextField label="Connection name" value={name} onChange={(event) => setName(event.target.value)} required mono autoFocus hint="Lowercase letters, digits and dashes; becomes part of the credentials path." />
          <SelectField label="Target cluster" value={target} onChange={(event) => setTarget(event.target.value)} required hint={loading ? 'Loading clusters…' : targets.length ? 'PostgreSQL, MongoDB replica sets and RabbitMQ in this namespace where you may create database users.' : 'No eligible cluster in this namespace, or you lack database-users:create on them.'}>
            <option value="">Choose a cluster…</option>
            {targets.map((c) => <option key={c.name} value={c.name}>{c.name} — {TARGET_PROVIDER_LABEL[c.spec.provider] ?? c.spec.provider}</option>)}
          </SelectField>
        </div>
        <Callout>Karakoram creates a dedicated administrative identity on the target through its own access API and configures the OpenBao plugin with it; the identity's secret is stored encrypted and never shown. Members must reach the target's private endpoints.</Callout>
        <fieldset className="stack" style={{ border: 0, margin: 0, padding: 0 }}>
          <legend className="field-label">Roles</legend>
          {roles.map((role, index) => <div key={index} className="stack" style={{ gap: 6 }}>
            <div className="grid-2">
              <TextField label={`Role ${index + 1} name`} value={role.name} onChange={(event) => setRole(index, { name: event.target.value })} required mono />
              <SelectField label="Access" value={role.access} onChange={(event) => setRole(index, { access: event.target.value as RoleDraft['access'] })}>{ACCESS_LEVELS.map((level) => <option key={level} value={level}>{level === 'readonly' ? 'Read only' : 'Read and write'}</option>)}</SelectField>
            </div>
            {scope ? <TextField label={scope.label} value={role.database} onChange={(event) => setRole(index, { database: event.target.value })} mono hint={scope.hint} /> : null}
            <div className="grid-2">
              <TextField label="Default TTL (seconds)" type="number" min={1} step={1} value={role.defaultTtl} onChange={(event) => setRole(index, { defaultTtl: event.target.value })} placeholder={String(DEFAULT_TTL_SECONDS)} hint="Lifetime of issued credentials unless the client asks for less." />
              <TextField label="Maximum TTL (seconds)" type="number" min={1} step={1} value={role.maxTtl} onChange={(event) => setRole(index, { maxTtl: event.target.value })} placeholder={String(DEFAULT_MAX_TTL_SECONDS)} hint="Upper bound for renewals." />
            </div>
            {roles.length > 1 ? <div><Button size="sm" variant="ghost" onClick={() => setRoles((current) => current.filter((_, i) => i !== index))}>Remove role {index + 1}</Button></div> : null}
          </div>)}
          <div><Button size="sm" onClick={() => setRoles((current) => [...current, emptyRole()])}>+ Add role</Button></div>
        </fieldset>
        <p className="small muted">Each role issues credentials at <code>&lt;mount&gt;/creds/&lt;connection&gt;-&lt;role&gt;</code>. Read-only roles can read every database{targetProvider === 'postgres' ? ' (pg_read_all_data)' : ''}; read-write roles can also modify data. Neither grants administrative rights.</p>
      </div>
    </Dialog>
  );
}
