// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { Badge, Card, ErrorBlock, Loading, PageHeader, Segmented } from '@/components/ui/Primitives';
import { EventTable, useLiveEvents } from './clusters/detail/EventsTab';
import { OperationsTable } from './clusters/detail/OperationsTab';

export function EventsPage() {
  const auth = useAuth();
  const events = useLiveEvents(undefined, auth.can('events', 'read'));
  return (
    <div className="page">
      <PageHeader title="Events" description="Durable activity log across every cluster you can see, updated live." />
      <Card
        flush
        title="Recent events"
        actions={
          <Badge tone={events.connected ? 'ok' : 'warn'} plain>
            {events.connected ? 'live' : 'polling'}
          </Badge>
        }
      >
        {events.list.isPending ? <Loading /> : events.list.error ? <div className="card-body"><ErrorBlock error={events.list.error} retry={() => events.list.refetch()} /></div> : <EventTable items={events.items} showCluster />}
      </Card>
    </div>
  );
}

export function OperationsPage() {
  const auth = useAuth();
  const [filter, setFilter] = useState<'all' | 'active' | 'Failed'>('all');
  const ops = useQuery({
    queryKey: keys.operations,
    queryFn: () => unwrap(api.GET('/operations', { params: { query: { limit: 200 } } })),
    refetchInterval: (q) => (q.state.data?.items.some((o) => o.phase === 'Running' || o.phase === 'Pending') ? 3000 : 15_000),
    enabled: auth.can('operations', 'read'),
  });
  const items = (ops.data?.items ?? []).filter((o) => (filter === 'all' ? true : filter === 'active' ? o.phase === 'Running' || o.phase === 'Pending' : o.phase === 'Failed'));
  return (
    <div className="page">
      <PageHeader title="Operations" description="Durable, resumable operations: backups, restores, restarts, rotations." />
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
        {ops.isPending ? <Loading /> : ops.error ? <div className="card-body"><ErrorBlock error={ops.error} retry={() => ops.refetch()} /></div> : <OperationsTable items={items} showCluster />}
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
