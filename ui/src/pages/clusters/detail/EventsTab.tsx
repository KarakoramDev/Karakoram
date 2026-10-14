// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useCallback, useEffect, useRef, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, API_BASE, unwrap, type Schemas } from '@/api/client';
import type { operations } from '@/api/schema';
import { subscribeSSE } from '@/api/events';
import { keys } from '@/api/keys';
import { DEFAULT_NAMESPACE } from '@/auth/permissions';
import { namespaceQuery } from '@/namespaces/useNamespace';
import { Button } from '@/components/ui/Button';
import { Checkbox, SelectField, TextField } from '@/components/ui/Field';
import { Badge, Card, ErrorBlock, Loading, formatTime } from '@/components/ui/Primitives';
import { useClusterContext } from './ClusterDetail';

type KEvent = Schemas['Event'];
type Process = NonNullable<NonNullable<operations['getMemberLogs']['parameters']['query']>['process']>;

export function EventTable({ items, showCluster, showNamespace, showResource }: { items: KEvent[]; showCluster?: boolean; showNamespace?: boolean; showResource?: boolean }) {
  const extra = (showNamespace ? 1 : 0) + (showCluster ? 1 : 0) + (showResource ? 2 : 0);
  return (
    <div className="table-wrap">
      <table className="table">
        <thead>
          <tr>
            <th>Time</th>
            {showNamespace ? <th>Namespace</th> : null}
            {showCluster ? <th>Cluster</th> : null}
            {showResource ? <><th>Resource</th><th>Target</th></> : null}
            <th>Type</th>
            <th>Message</th>
            <th>Actor</th>
            <th>Operation</th>
          </tr>
        </thead>
        <tbody>
          {items.map((e) => (
            <tr key={e.id}>
              <td className="small nowrap">{formatTime(e.createdAt)}</td>
              {showNamespace ? <td className="mono small">{e.namespace || (e.cluster ? DEFAULT_NAMESPACE : '—')}</td> : null}
              {showCluster ? <td className="mono small">{e.cluster || '—'}</td> : null}
              {showResource ? <><td className="mono small">{e.resource || '—'}</td><td className="mono small">{e.target || '—'}</td></> : null}
              <td>
                <Badge plain tone={e.type.includes('fail') || e.type.includes('error') ? 'danger' : e.type.includes('warn') ? 'warn' : 'neutral'}>
                  {e.type}
                </Badge>
              </td>
              <td className="small">
                {e.message}
                {e.reason ? <span className="faint"> ({e.reason})</span> : null}
              </td>
              <td className="small mono">{e.actor || '—'}</td>
              <td className="small mono">{e.operationId ?? ''}</td>
            </tr>
          ))}
          {items.length === 0 ? (
            <tr>
              <td colSpan={5 + extra} className="table-empty">
                No events.
              </td>
            </tr>
          ) : null}
        </tbody>
      </table>
    </div>
  );
}

/** Merges the durable event list with live frames using the same exact filters. */
export function useLiveEvents(scope: { namespace?: string; cluster?: string; actor?: string; resource?: string }, enabled: boolean) {
  const { namespace, cluster, actor, resource } = scope;
  const query = { ...(cluster ? { cluster } : {}), ...namespaceQuery(namespace), ...(actor ? { actor } : {}), ...(resource ? { resource } : {}) };
  const list = useQuery({
    queryKey: [...(cluster ? keys.clusterSub(namespace ?? DEFAULT_NAMESPACE, cluster, 'events') : keys.eventList(namespace)), actor ?? '', resource ?? ''],
    queryFn: () => unwrap(api.GET('/events', { params: { query: { ...query, limit: 200, order: 'desc' } } })),
    enabled,
  });
  const [live, setLive] = useState<KEvent[]>([]);
  const [connected, setConnected] = useState(false);
  useEffect(() => {
    if (!enabled) return;
    setLive([]);
    const query = new URLSearchParams(namespaceQuery(namespace));
    if (cluster) query.set('cluster', cluster);
    if (actor) query.set('actor', actor);
    if (resource) query.set('resource', resource);
    const url = `${API_BASE}/events/stream${query.size ? `?${query}` : ''}`;
    return subscribeSSE(
      url,
      (f) => {
        try {
          const ev = JSON.parse(f.data) as KEvent;
          if ((!namespace || ev.namespace === namespace || (!ev.namespace && ev.cluster && namespace === DEFAULT_NAMESPACE)) && (!cluster || ev.cluster === cluster) && (!actor || ev.actor === actor) && (!resource || ev.resource === resource)) {
            setLive((xs) => (xs.some((x) => x.id === ev.id) ? xs : [ev, ...xs].slice(0, 500)));
          }
        } catch {
          /* ignore malformed frame */
        }
      },
      setConnected,
    );
  }, [namespace, cluster, enabled, actor, resource]);
  const ids = new Set(live.map((e) => e.id));
  const merged = [...live, ...(list.data?.items ?? []).filter((e) => !ids.has(e.id))]
    .filter((e) => (!namespace || e.namespace === namespace || (!e.namespace && e.cluster && namespace === DEFAULT_NAMESPACE)) && (!cluster || e.cluster === cluster) && (!actor || e.actor === actor) && (!resource || e.resource === resource))
    .sort((a, b) => b.id - a.id);
  return { list, items: merged, connected };
}

export function EventsTab() {
  const { cluster, namespace, name, can } = useClusterContext();
  const events = useLiveEvents({ namespace, cluster: name }, can('read', 'events') || can('read', 'audit'));
  const members = cluster.status.members;
  const [member, setMember] = useState(members[0]?.name ?? '');
  const [process, setProcess] = useState<Process>('mongod');
  const [lines, setLines] = useState(200);
  const [follow, setFollow] = useState(false);
  const [filter, setFilter] = useState('');
  const logRef = useRef<HTMLPreElement>(null);

  const selectedMember = members.find((m) => m.name === member);
  const defaultProcess: Process = selectedMember?.role === 'router' ? 'mongos' : 'mongod';
  useEffect(() => {
    setProcess(defaultProcess);
  }, [defaultProcess]);

  const logs = useQuery({
    queryKey: [...keys.clusterSub(namespace, name, 'logs'), member, process, lines],
    queryFn: () => unwrap(api.GET('/clusters/{cluster}/members/{member}/logs', { params: { path: { cluster: name, member }, query: { namespace, process, lines } } })),
    enabled: Boolean(member) && can('read'),
    refetchInterval: follow ? 5000 : false,
  });
  const scrollToEnd = useCallback(() => {
    const el = logRef.current;
    if (el && follow) el.scrollTop = el.scrollHeight;
  }, [follow]);
  useEffect(scrollToEnd, [logs.data, scrollToEnd]);

  const visible = (logs.data?.lines ?? []).filter((l) => !filter || l.message.toLowerCase().includes(filter.toLowerCase()));

  return (
    <div className="stack" style={{ gap: 16 }}>
      <Card
        title="Events"
        flush
        actions={
          <Badge tone={events.connected ? 'ok' : 'warn'} plain>
            {events.connected ? 'live' : 'polling'}
          </Badge>
        }
      >
        {events.list.isPending ? <Loading /> : events.list.error ? <div className="card-body"><ErrorBlock error={events.list.error} retry={() => events.list.refetch()} /></div> : <EventTable items={events.items} />}
      </Card>

      <Card
        title="Member logs"
        actions={
          <Button size="sm" onClick={() => logs.refetch()} loading={logs.isFetching}>
            Refresh
          </Button>
        }
      >
        <div className="stack">
          <div className="row wrap" style={{ alignItems: 'flex-end', gap: 12 }}>
            <SelectField label="Member" value={member} onChange={(e) => setMember(e.target.value)} className="grow" style={{ minWidth: 160 }}>
              {members.map((m) => (
                <option key={m.name} value={m.name}>
                  {m.name} ({m.role})
                </option>
              ))}
            </SelectField>
            <SelectField label="Process" value={process} onChange={(e) => setProcess(e.target.value as Process)}>
              <option value="mongod">mongod</option>
              <option value="mongos">mongos</option>
              <option value="pbm-agent">pbm-agent</option>
              <option value="agent">karakoram agent</option>
            </SelectField>
            <SelectField label="Lines" value={lines} onChange={(e) => setLines(Number(e.target.value))}>
              {[100, 200, 500, 1000, 5000].map((n) => (
                <option key={n} value={n}>
                  {n}
                </option>
              ))}
            </SelectField>
            <TextField label="Filter" value={filter} onChange={(e) => setFilter(e.target.value)} placeholder="text match" className="grow" />
            <div style={{ height: 34, display: 'flex', alignItems: 'center' }}>
              <Checkbox label="Follow" checked={follow} onChange={(e) => setFollow(e.target.checked)} />
            </div>
          </div>
          {members.length === 0 ? (
            <p className="small muted">No members to read logs from yet.</p>
          ) : logs.error ? (
            <ErrorBlock error={logs.error} retry={() => logs.refetch()} />
          ) : (
            <pre className="log-view" ref={logRef} aria-live={follow ? 'polite' : undefined}>
              {logs.isPending ? 'Loading…' : visible.length === 0 ? 'No log lines.' : visible.map((l) => `${l.timestamp} ${l.message}`).join('\n')}
            </pre>
          )}
        </div>
      </Card>
    </div>
  );
}
