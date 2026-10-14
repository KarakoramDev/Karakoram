// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { namespaceQuery, spansNamespaces, useNamespace } from '@/namespaces/useNamespace';
import { Badge, Callout, Card, ErrorBlock, Loading, PageHeader, Segmented } from '@/components/ui/Primitives';
import { EventTable, useLiveEvents } from './clusters/detail/EventsTab';
import { OperationsTable } from './clusters/detail/OperationsTab';
import { EventFilters, useEventFilters } from '@/components/EventFilters';

export function EventsPage() {
  const auth = useAuth();
  const { namespace } = useNamespace();
  const canAudit = auth.can('audit', 'read');
  const canEvents = auth.can('events', 'read') || canAudit;
  const filters = useEventFilters();
  const events = useLiveEvents({ ...filters, namespace }, canEvents);
  return (
    <div className="page">
      <PageHeader title="Events" description={namespace ? `Durable activity and authorized audit log of namespace ${namespace}, updated live.` : canAudit ? 'Durable cluster and authorized audit activity across namespaces, updated live.' : 'Durable activity log across every cluster you can see, updated live.'} />
      <Card title="Event filters">
        <EventFilters audit={canAudit} cluster />
        <p className="small muted">Exact, case-sensitive matches combined with the namespace selection. Filters are saved in the URL. Only activity authorized for your account is included; an empty result is not proof that no activity occurred.</p>
      </Card>
      <Card
        flush
        title="Recent events"
        actions={
          <Badge tone={events.connected && !events.stale ? 'ok' : 'warn'} plain>
            {events.stale ? 'connection lost — data may be stale' : events.connected ? 'live' : 'reconnecting — data may be stale'}
          </Badge>
        }
      >
        {!canEvents ? <div className="card-body"><Callout tone="info">You do not have permission to view events.</Callout></div> : events.list.isPending ? <Loading /> : events.list.error ? <div className="card-body"><ErrorBlock error={events.list.error} retry={() => events.list.refetch()} /></div> : <EventTable items={events.items} showCluster showNamespace={!namespace && spansNamespaces(events.items)} showResource={canAudit} />}
      </Card>
    </div>
  );
}

export function OperationsPage() {
  const auth = useAuth();
  const { namespace } = useNamespace();
  const [filter, setFilter] = useState<'all' | 'active' | 'Failed'>('all');
  const ops = useQuery({
    queryKey: keys.operationList(namespace),
    queryFn: () => unwrap(api.GET('/operations', { params: { query: { limit: 200, ...namespaceQuery(namespace) } } })),
    refetchInterval: (q) => (q.state.data?.items.some((o) => o.phase === 'Running' || o.phase === 'Pending') ? 3000 : 15_000),
    enabled: auth.can('operations', 'read'),
  });
  const items = (ops.data?.items ?? []).filter((o) => (filter === 'all' ? true : filter === 'active' ? o.phase === 'Running' || o.phase === 'Pending' : o.phase === 'Failed'));
  return (
    <div className="page">
      <PageHeader title="Operations" description={`Track cluster changes, backups, restores and maintenance${namespace ? ` in namespace ${namespace}` : ''}. Open an operation for its target, progress and cancellation status.`} />
      <Card
        flush
        title="Operations"
        actions={
          <Segmented
            label="Filter"
            value={filter}
            onChange={setFilter}
            options={[
              { value: 'all', label: 'All' },
              { value: 'active', label: 'Active' },
              { value: 'Failed', label: 'Failed' },
            ]}
          />
        }
      >
        {ops.isPending ? <Loading /> : ops.error ? <div className="card-body"><ErrorBlock error={ops.error} retry={() => ops.refetch()} /></div> : <OperationsTable items={items} showCluster showNamespace={!namespace && spansNamespaces(items)} />}
      </Card>
    </div>
  );
}

export function NotFoundPage() {
  return (
    <div className="page">
      <PageHeader title="Page not found" description="The address does not match any screen." />
    </div>
  );
}
