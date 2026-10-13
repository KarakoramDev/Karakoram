// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { Button } from '@/components/ui/Button';
import { Badge, Callout, Card, CodeLine, CopyButton, ErrorBlock, KV, Loading, Meter, Time, formatBytes, formatDuration, type Tone } from '@/components/ui/Primitives';
import { useClusterContext } from './ClusterDetail';
import { resourcesSummary } from '../cluster-utils';

type Member = Schemas['Member'];

const ROLE_TONE: Record<Member['role'], Tone> = {
  primary: 'accent',
  secondary: 'neutral',
  arbiter: 'neutral',
  'config-primary': 'accent',
  'config-secondary': 'neutral',
  router: 'info',
  unknown: 'warn',
};

export function OverviewTab() {
  const { cluster, namespace, name, can } = useClusterContext();
  const members = useQuery({
    queryKey: keys.clusterSub(namespace, name, 'members'),
    queryFn: () => unwrap(api.GET('/clusters/{cluster}/members', { params: { path: { cluster: name }, query: { namespace } } })),
    refetchInterval: 10_000,
  });
  const placement = useQuery({
    queryKey: keys.clusterSub(namespace, name, 'placement'),
    queryFn: () => unwrap(api.GET('/clusters/{cluster}/placement', { params: { path: { cluster: name }, query: { namespace } } })),
    refetchInterval: 60_000,
  });
  const balancer = useQuery({
    queryKey: keys.clusterSub(namespace, name, 'balancer'),
    queryFn: () => unwrap(api.GET('/clusters/{cluster}/balancer', { params: { path: { cluster: name }, query: { namespace } } })),
    enabled: cluster.spec.topology.type === 'sharded',
    refetchInterval: 30_000,
  });

  const list = members.data?.items ?? cluster.status.members;
  const spec = cluster.spec;
  const conn = cluster.status.connection;
  const pl = placement.data ?? cluster.status.placement;
  const colocated = pl?.groups.filter((g) => g.members.length > 1) ?? [];

  return (
    <div className="stack" style={{ gap: 20 }}>
      <div className="grid-4">
        <Card flush>
          <div className="stat">
            <span className="stat-label">Members healthy</span>
            <span className="stat-value">
              {list.filter((m) => m.health).length}
              <span className="faint" style={{ fontSize: 'var(--fs-md)' }}>
                {' '}
                / {list.length}
              </span>
            </span>
          </div>
        </Card>
        <Card flush>
          <div className="stat">
            <span className="stat-label">Max replication lag</span>
            <span className="stat-value">{formatDuration(Math.max(0, ...list.map((m) => m.lagSeconds ?? 0)))}</span>
          </div>
        </Card>
        <Card flush>
          <div className="stat">
            <span className="stat-label">Last backup</span>
            <span className="stat-value" style={{ fontSize: 'var(--fs-lg)' }}>
              {cluster.status.backup?.lastBackup ?? '—'}
            </span>
            <span className="stat-sub">{cluster.status.backup?.pitrWindow?.available ? `PITR until ${new Date(cluster.status.backup.pitrWindow.end ?? '').toLocaleTimeString()}` : 'PITR window unavailable'}</span>
          </div>
        </Card>
        <Card flush>
          <div className="stat">
            <span className="stat-label">Spec generation</span>
            <span className="stat-value">
              {cluster.status.observedGeneration}
              <span className="faint" style={{ fontSize: 'var(--fs-md)' }}>
                {' '}
                / {cluster.generation}
              </span>
            </span>
            <span className="stat-sub">{cluster.status.observedGeneration < cluster.generation ? 'reconciling changes' : 'in sync'}</span>
          </div>
        </Card>
      </div>

      <Callout tone={colocated.length ? 'warn' : 'info'} title={colocated.length ? 'Members share a physical host' : 'Placement is best effort'}>
        {pl?.warning ?? 'Zerops does not guarantee that members are placed on separate physical hosts; Karakoram cannot promise tolerance to a host failure.'}
        {colocated.length ? (
          <ul style={{ margin: '6px 0 0', paddingLeft: 18 }}>
            {colocated.map((g) => (
              <li key={g.name} className="mono xs">
                {g.name}: {g.members.join(', ')}
              </li>
            ))}
          </ul>
        ) : null}
      </Callout>

      <Card title="Members" flush actions={members.isFetching ? <span className="spinner" aria-label="Refreshing" /> : null}>
        {members.error && !list.length ? (
          <div className="card-body">
            <ErrorBlock error={members.error} retry={() => members.refetch()} />
          </div>
        ) : (
          <div className="table-wrap">
            <table className="table">
              <thead>
                <tr>
                  <th>Member</th>
                  <th>Set</th>
                  <th>Role</th>
                  <th>Health</th>
                  <th className="num">Lag</th>
                  <th>Version</th>
                  <th>CPU</th>
                  <th>RAM</th>
                  <th>Disk</th>
                  <th>Uptime</th>
                  <th>Agent</th>
                  <th>Host group</th>
                </tr>
              </thead>
              <tbody>
                {list.map((m) => (
                  <tr key={m.name}>
                    <td>
                      <span className="mono">{m.name}</span>
                      <details className="small">
                        <summary>Platform resources</summary>
                        <div>Allocated: {m.resources ? `${m.resources.cpuCores} cores · ${m.resources.memoryGB} GiB RAM · ${m.resources.diskGB} GiB root disk` : 'unknown'}</div>
                        <div>Autoscaling bounds: {m.autoscaling ? resourcesSummary(m.autoscaling) : 'unknown'}</div>
                        {m.autoscaling?.startCpu !== undefined ? <div>Start CPU: {m.autoscaling.startCpu} cores</div> : null}
                        <div className="faint">Platform bounds are distinct from current allocation. Root disk is not the database volume.</div>
                      </details>
                    </td>
                    <td className="mono">{m.set}</td>
                    <td>
                      <span className="row" style={{ gap: 6 }}>
                        <Badge tone={ROLE_TONE[m.role]} plain>
                          {m.role}
                        </Badge>
                        {m.state && m.state.toLowerCase() !== m.role.replace('config-', '') ? <span className="xs faint">{m.state}</span> : null}
                      </span>
                    </td>
                    <td>
                      <Badge tone={m.health ? 'ok' : 'danger'}>{m.health ? 'healthy' : 'unhealthy'}</Badge>
                    </td>
                    <td className="num small">{m.lagSeconds === undefined ? '—' : formatDuration(m.lagSeconds)}</td>
                    <td className="mono small">{m.version ?? '—'}</td>
                    <td>{m.cpuUsage === undefined ? '—' : <Meter value={m.cpuUsage} max={100} unit="%" compact />}</td>
                    <td className="small nowrap">{formatBytes(m.ramUsageBytes)}</td>
                    <td>{m.diskUsageBytes === undefined ? '—' : <Meter value={m.diskUsageBytes / 2 ** 30} max={m.diskTotalBytes ? m.diskTotalBytes / 2 ** 30 : undefined} unit="GiB" />}</td>
                    <td className="small">{formatDuration(m.uptime)}</td>
                    <td>
                      {m.agent ? (
                        <Badge tone={m.agent.connected ? 'ok' : 'warn'} plain title={m.agent.lastSeenAt ? `last seen ${m.agent.lastSeenAt}` : undefined}>
                          {m.agent.connected ? 'connected' : 'offline'}
                          {m.agent.version ? ` ${m.agent.version}` : ''}
                        </Badge>
                      ) : (
                        '—'
                      )}
                    </td>
                    <td className="mono xs">{m.placementGroup ?? '—'}</td>
                  </tr>
                ))}
                {list.length === 0 ? (
                  <tr>
                    <td colSpan={12} className="table-empty">
                      {members.isPending ? <Loading /> : 'No members reported yet.'}
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      <div className="grid-2">
        <Card title="Connection">
          {conn?.hosts?.length ? (
            <div className="stack">
              <KV
                items={[
                  ['Replica set', conn.replicaSet ? <span className="mono">{conn.replicaSet}</span> : null],
                  ['Private hosts', <span className="mono small">{conn.hosts.join(', ')}</span>],
                  conn.publicHosts?.length ? ['Public hosts', <span className="mono small">{conn.publicHosts.join(', ')}</span>] : null,
                  ['TLS', `${spec.tls?.mode ?? 'requireTLS'}${spec.tls?.ca === 'userProvided' ? ' (user CA)' : ''}`],
                ]}
              />
              <CodeLine value={conn.uri} />
              <CredentialsReveal namespace={namespace} name={name} allowed={can('read', 'cluster-credentials')} />
            </div>
          ) : (
            <p className="small muted">Connection details appear once the cluster is initialized.</p>
          )}
        </Card>
        <Card title="Specification">
          <KV
            items={[
              ['Engine', `${spec.provider} ${spec.version}`],
              ['Members', resourcesSummary(spec.components.engine.resources)],
              spec.components.configServer && ['Config servers', `${spec.components.configServer.replicas} × ${resourcesSummary(spec.components.configServer.resources)}`],
              spec.components.router && ['Routers', `${spec.components.router.replicas} × ${resourcesSummary(spec.components.router.resources, true)}`],
              ['Backup storage', spec.backup?.storage ?? 'none'],
              ['PITR', spec.backup?.pitr?.enabled ? `on, ${spec.backup.pitr.oplogSpanMinutes} min slices` : 'off'],
              ['Exposure', spec.exposure?.public ? `public ${spec.exposure.ipType} · allowlist ${(spec.exposure.allowlist ?? []).join(', ') || 'none'}` : 'private'],
              ['Monitoring', spec.monitoring?.enabled === false ? 'disabled' : 'enabled'],
              balancer.data && ['Balancer', <span>{balancer.data.enabled ? 'enabled' : 'disabled'}{balancer.data.running ? ' · running' : ''}{balancer.data.mode ? ` · ${balancer.data.mode}` : ''}</span>],
              ['Created', <Time iso={cluster.createdAt} />],
            ]}
          />
        </Card>
      </div>

      <Card title="Conditions" flush>
        <div className="table-wrap">
          <table className="table">
            <thead>
              <tr>
                <th>Type</th>
                <th>Status</th>
                <th>Reason</th>
                <th>Message</th>
                <th>Since</th>
              </tr>
            </thead>
            <tbody>
              {cluster.status.conditions.map((c) => (
                <tr key={c.type}>
                  <td className="mono small">{c.type}</td>
                  <td>
                    <Badge tone={c.status === 'True' ? 'ok' : c.status === 'False' ? 'warn' : 'neutral'} plain>
                      {c.status}
                    </Badge>
                  </td>
                  <td className="small">{c.reason}</td>
                  <td className="small">{c.message}</td>
                  <td className="small">
                    <Time iso={c.lastTransitionTime} />
                  </td>
                </tr>
              ))}
              {cluster.status.conditions.length === 0 ? (
                <tr>
                  <td colSpan={5} className="table-empty">
                    No conditions reported.
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
      </Card>
    </div>
  );
}

function CredentialsReveal({ namespace, name, allowed }: { namespace: string; name: string; allowed: boolean }) {
  const [data, setData] = useState<Schemas['Connection'] | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  if (!allowed) {
    return <p className="xs faint">Revealing credentials requires the cluster-credentials read permission.</p>;
  }
  if (!data) {
    return (
      <div className="stack" style={{ gap: 6 }}>
        <div>
          <Button
            size="sm"
            loading={busy}
            onClick={async () => {
              setBusy(true);
              setErr(null);
              try {
                setData(await unwrap(api.GET('/clusters/{cluster}/connection', { params: { path: { cluster: name }, query: { namespace } } })));
              } catch (e) {
                setErr(errorMessage(e));
              } finally {
                setBusy(false);
              }
            }}
          >
            Reveal credentials
          </Button>
        </div>
        {err ? <span className="field-error">{err}</span> : null}
        <p className="xs faint">Passwords are fetched on demand, never cached, and the reveal is recorded in the event log.</p>
      </div>
    );
  }
  return (
    <div className="stack">
      <div className="row-between">
        <strong className="small">Credentials</strong>
        <Button size="sm" variant="ghost" onClick={() => setData(null)}>
          Hide
        </Button>
      </div>
      <div className="table-wrap">
        <table className="table">
          <thead>
            <tr>
              <th>User</th>
              <th>Auth DB</th>
              <th>Password</th>
              <th className="actions">URI</th>
            </tr>
          </thead>
          <tbody>
            {data.credentials.map((c) => (
              <tr key={`${c.database}/${c.username}`}>
                <td className="mono">{c.username}</td>
                <td className="mono">{c.database}</td>
                <td>
                  <span className="row">
                    <code className="small">{c.password}</code>
                    <CopyButton value={c.password} label="Copy" />
                  </span>
                </td>
                <td className="actions">
                  <CopyButton value={c.uri} label="Copy URI" />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {data.caCertificate ? (
        <details>
          <summary className="small">CA certificate</summary>
          <CodeLine value={data.caCertificate} />
        </details>
      ) : null}
    </div>
  );
}
