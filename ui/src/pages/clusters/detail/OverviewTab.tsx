// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { Link } from 'react-router-dom';
import { useQuery } from '@tanstack/react-query';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { keys } from '@/api/keys';
import { Button } from '@/components/ui/Button';
import { Badge, Callout, Card, CodeLine, CopyButton, ErrorBlock, KV, Loading, Meter, Time, formatBytes, formatDuration } from '@/components/ui/Primitives';
import { useClusterContext } from './ClusterDetail';
import { MemberHealthBadge, MemberRole, resourcesSummary, useMemberRoles } from '../cluster-utils';
import { memberHealthCounts } from '../member-status';
import { certificateDownload, colocatedGroups, credentialDatabaseLabel, elasticHealthNotices, elasticMemberSummary, generationState, knownHost, psmdbConnect } from './detail-state';
import { hasCapability, hasReplicaSetDetails } from '@/engines';
import { QuorumCallout } from '@/engines/rabbitmq/QuorumCallout';
import { kibanaReady } from '@/engines/elasticsearch/spec';
import { ConnectionHelp } from '@/engines/elasticsearch/ConnectionHelp';
import { elasticResourceRows } from '@/engines/elasticsearch/resource-summary';

export function OverviewTab() {
  const { cluster, provider, namespace, name, base, can } = useClusterContext();
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
    enabled: hasCapability(provider, 'balancer') && cluster.spec.topology.type === 'sharded',
    refetchInterval: 30_000,
  });

  const list = members.data?.items ?? cluster.status.members;
  const roles = useMemberRoles(list, base);
  const health = memberHealthCounts(list);
  const spec = cluster.spec;
  const replicaSetDetails = hasReplicaSetDetails(spec.provider);
  const conn = cluster.status.connection;
  const pl = placement.data ?? cluster.status.placement;
  const colocated = colocatedGroups(pl?.groups);
  const placementUnknown = !pl?.groups.length || pl.groups.some((g) => !knownHost(g.name));
  const suspended = cluster.status.phase === 'Suspended';
  const elasticMembers = spec.provider === 'elasticsearch' ? elasticMemberSummary(spec.components, list) : undefined;
  const healthNotices = spec.provider === 'elasticsearch' && !suspended ? elasticHealthNotices(cluster.status.conditions) : [];

  return (
    <div className="stack" style={{ gap: 20 }}>
      {suspended ? <Callout tone="warn" title="Runtime suspended">Endpoints are unavailable. Member health, agent observations and platform allocations below are historical, not live runtime status.</Callout> : null}
      {spec.provider === 'rabbitmq' ? <QuorumCallout cluster={cluster} base={base} /> : null}
      {healthNotices.map((notice) => <Callout key={notice.title} tone="warn" title={notice.title}>
        {notice.message} See the <a href="https://github.com/KarakoramDev/Karakoram/blob/main/docs/user-guide/elasticsearch.md#scale-upgrade-restart-and-replace" target="_blank" rel="noreferrer">Elasticsearch scaling guide</a>.
      </Callout>)}
      <div className={replicaSetDetails ? 'grid-4' : 'grid-3'}>
        <Card flush>
          <div className="stat">
            <span className="stat-label">{suspended ? 'Historical members (runtime suspended)' : elasticMembers ? 'Elasticsearch nodes healthy' : 'Members healthy'}</span>
            <span className="stat-value">
              {suspended ? '—' : elasticMembers ? elasticMembers.healthy : health.healthy}
              <span className="faint" style={{ fontSize: 'var(--fs-md)' }}>
                {' '}
                / {suspended ? list.length : elasticMembers ? elasticMembers.observed : health.total}
              </span>
            </span>
            {elasticMembers ? <span className="stat-sub">{elasticMembers.detail}</span> : null}
            {!suspended && health.leaving > 0 ? <span className="stat-sub"><Badge tone="progress">{health.leaving} Leaving</Badge></span> : null}
          </div>
        </Card>
        {replicaSetDetails ? (
          <Card flush>
            <div className="stat">
              <span className="stat-label">Max replication lag</span>
              <span className="stat-value">{formatDuration(Math.max(0, ...list.map((m) => m.lagSeconds ?? 0)))}</span>
            </div>
          </Card>
        ) : null}
        <Card flush>
          <div className="stat">
            <span className="stat-label">Last backup</span>
            <span className="stat-value" style={{ fontSize: 'var(--fs-lg)' }}>
              {cluster.status.backup?.lastBackup ?? '—'}
            </span>
            {hasCapability(provider, 'pitr') ? <span className="stat-sub">{cluster.status.backup?.pitrWindow?.available ? `PITR until ${new Date(cluster.status.backup.pitrWindow.end ?? '').toLocaleTimeString()}` : 'PITR window unavailable'}</span> : null}
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
            <span className="stat-sub">{generationState(cluster.status.phase, cluster.status.observedGeneration, cluster.generation)}</span>
          </div>
        </Card>
      </div>

      <Callout tone={colocated.length ? 'warn' : 'info'} title={colocated.length ? 'Members share a physical host' : placementUnknown ? 'Placement unknown until members report' : 'Placement is best effort'}>
        {placementUnknown ? 'Host information has not been reported for every member. Co-location cannot be determined yet. Zerops does not guarantee physical-host anti-affinity.' : pl?.warning || 'Zerops does not guarantee that members are placed on separate physical hosts; Karakoram cannot promise tolerance to a host failure.'}
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
                  {replicaSetDetails ? <th>Set</th> : null}
                  <th>Role</th>
                  <th>Health</th>
                  {replicaSetDetails ? <th className="num">Lag</th> : null}
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
                    {replicaSetDetails ? <td className="mono">{m.set}</td> : null}
                    <td>
                      <span className="row" style={{ gap: 6 }}>
                        <MemberRole member={m} roles={roles} />
                        {m.state && m.state !== 'LEAVING' && m.state.toLowerCase() !== m.role.replace('config-', '') ? <span className="xs faint">{m.state}</span> : null}
                      </span>
                    </td>
                    <td>
                      <MemberHealthBadge member={m} suspended={suspended} />
                    </td>
                    {replicaSetDetails ? <td className="num small">{m.lagSeconds === undefined ? '—' : formatDuration(m.lagSeconds)}</td> : null}
                    <td className="mono small">{m.version ?? '—'}</td>
                    <td>{m.cpuUsage === undefined ? '—' : <Meter value={m.cpuUsage} max={100} unit="%" compact />}</td>
                    <td className="small nowrap">{formatBytes(m.ramUsageBytes)}</td>
                    <td>{m.diskUsageBytes === undefined ? '—' : <Meter value={m.diskUsageBytes / 2 ** 30} max={m.diskTotalBytes ? m.diskTotalBytes / 2 ** 30 : undefined} unit="GiB" />}</td>
                    <td className="small">{formatDuration(m.uptime)}</td>
                    <td>
                      {m.agent ? (
                        <Badge tone={suspended ? 'neutral' : m.agent.connected ? 'ok' : 'warn'} plain title={m.agent.lastSeenAt ? `last seen ${m.agent.lastSeenAt}` : undefined}>
                          {suspended ? 'historical observation' : m.agent.connected ? 'connected' : 'offline'}
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
                    <td colSpan={replicaSetDetails ? 12 : 10} className="table-empty">
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
          {conn?.endpoints?.map((endpoint) => <div key={`${endpoint.name}:${endpoint.url}`} className="stack" style={{ marginBottom: 12 }}>
            <strong className="small">{endpoint.name} <Badge plain>{endpoint.public ? 'Public' : 'Private'}</Badge></strong>
            {spec.provider === 'elasticsearch' && endpoint.name === 'kibana' && (suspended || !kibanaReady(list)) ? <span className="small muted">{suspended ? 'Kibana is suspended. Resume the cluster to make it available.' : 'Kibana is starting — retry shortly.'}</span> : <CodeLine value={endpoint.url} />}
          </div>)}
          {conn?.hosts?.length ? (
            <div className="stack">
              <KV
                items={[
                  replicaSetDetails ? ['Replica set', conn.replicaSet ? <span className="mono">{conn.replicaSet}</span> : null] : null,
                  ['Private hosts', <span className="mono small">{conn.hosts.join(', ')}</span>],
                  conn.publicHosts?.length ? ['Public hosts', <span className="mono small">{conn.publicHosts.join(', ')}</span>] : null,
                  ['TLS', `${spec.tls?.mode ?? 'requireTLS'}${spec.tls?.ca === 'userProvided' ? ' (user CA)' : ''}`],
                ]}
              />
              <CodeLine value={conn.uri} />
              {spec.provider === 'psmdb' && conn.replicaSet && Boolean(conn.publicHosts?.length) ? <Callout tone="warn">Public seeds advertise private member hostnames. External drivers must resolve and reach the discovered members. <code>directConnection=true</code> is for diagnostics only, not external HA/failover. See the <a href="https://github.com/KarakoramDev/Karakoram/blob/main/docs/user-guide/users-tls-access.md" target="_blank" rel="noreferrer">TLS and access guide</a>.</Callout> : null}
              <CredentialsReveal namespace={namespace} name={name} base={base} provider={spec.provider} allowed={can('read', 'cluster-credentials')} />
            </div>
          ) : (
            conn?.endpoints?.length ? null : <p className="small muted">Connection details appear once the cluster is initialized.</p>
          )}
          {spec.provider === 'elasticsearch' ? <ConnectionHelp connection={conn} /> : null}
        </Card>
        <Card title="Specification">
          <KV
            items={[
              ['Engine', `${spec.provider} ${spec.version}`],
              ...(spec.provider === 'elasticsearch' ? elasticResourceRows(spec) : [['Members', resourcesSummary(spec.components.engine.resources)] as [string, string]]),
              spec.components.configServer && ['Config servers', `${spec.components.configServer.replicas} × ${resourcesSummary(spec.components.configServer.resources)}`],
              spec.components.router && ['Routers', `${spec.components.router.replicas} × ${resourcesSummary(spec.components.router.resources, true)}`],
              ['Backup storage', spec.backup?.storage ?? 'none'],
              hasCapability(provider, 'pitr') && ['PITR', spec.backup?.pitr?.enabled ? `on, ${spec.backup.pitr.oplogSpanMinutes} min slices` : 'off'],
              ['Exposure', spec.exposure?.public ? `public ${spec.exposure.ipType} · allowlist ${(spec.exposure.allowlist ?? []).join(', ') || 'none'}` : 'private'],
              ['Monitoring', spec.monitoring?.enabled === false ? 'disabled' : 'enabled'],
              hasCapability(provider, 'balancer') && balancer.data && ['Balancer', <span>{balancer.data.enabled ? 'enabled' : 'disabled'}{balancer.data.running ? ' · running' : ''}{balancer.data.mode ? ` · ${balancer.data.mode}` : ''}</span>],
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
                  <td className="small">{c.reason || '—'}</td>
                  <td className="small">{c.message || '—'}</td>
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

function CredentialsReveal({ namespace, name, base, provider, allowed }: { namespace: string; name: string; base: string; provider: string; allowed: boolean }) {
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
  const databaseLabel = credentialDatabaseLabel(provider, data.credentials);
  const example = provider === 'psmdb' && data.credentials[0]?.uri ? psmdbConnect(data.credentials[0].uri) : null;
  const caDownload = data.caCertificate ? certificateDownload(data.caCertificate) : null;
  return (
    <div className="stack">
      <div className="row-between">
        <strong className="small">Credentials</strong>
        <Button size="sm" variant="ghost" onClick={() => setData(null)}>
          Hide
        </Button>
      </div>
      <Callout tone="warn">These are administrative accounts with broad privileges, not normal application identities. <Link to={`${base}/${provider === 'elasticsearch' ? 'elastic' : 'users'}`}>Create a scoped application user</Link> and use its own credentials in your application.</Callout>
      <div className="table-wrap">
        <table className="table">
          <thead>
            <tr>
              <th>User</th>
              {databaseLabel ? <th>{databaseLabel}</th> : null}
              <th>Password</th>
              <th>Endpoint / URI</th>
            </tr>
          </thead>
          <tbody>
            {data.credentials.map((c, index) => (
              <tr key={`${c.database}/${c.username}/${index}`}>
                <td className="mono">{c.username}</td>
                {databaseLabel ? <td className="mono">{c.database}</td> : null}
                <td>
                  <span className="row">
                    <code className="small">{c.password}</code>
                    <CopyButton value={c.password} label="Copy" />
                  </span>
                </td>
                <td>
                  <code className="small" style={{ overflowWrap: 'anywhere' }}>{c.uri}</code>
                  <CopyButton value={c.uri} label="Copy URI" />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {data.caCertificate ? (
        <div className="stack" style={{ gap: 8 }}>
          <strong className="small">CA certificate</strong>
          <textarea className="input mono" readOnly aria-label="CA certificate PEM" rows={8} value={data.caCertificate} onFocus={(e) => e.currentTarget.select()} />
          <div className="row">
            <CopyButton value={data.caCertificate} label="Copy CA" />
            {caDownload ? <a download={caDownload.filename} href={caDownload.href}>Download ca.pem</a> : null}
          </div>
        </div>
      ) : null}
      {example ? <details>
        <summary className="small">How to connect (MongoDB)</summary>
        <p className="small">Download <code>ca.pem</code> above. Run from a host inside the project network for private endpoints. Replace these administrative credentials with a scoped application user's username, password and authentication database; URI-encode credentials.</p>
        <strong className="small">mongosh</strong>
        <CodeLine value={example.shell} />
        <strong className="small">Driver URI</strong>
        <CodeLine value={example.driver} />
        <p className="xs faint">Keep TLS certificate verification enabled. The CA file path is relative to the client process.</p>
      </details> : null}
    </div>
  );
}
