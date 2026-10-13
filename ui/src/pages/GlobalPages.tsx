// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { api, unwrap } from '@/api/client';
import { keys } from '@/api/keys';
import { useAuth } from '@/auth/session';
import { namespaceQuery, spansNamespaces, useNamespace } from '@/namespaces/useNamespace';
import { Button } from '@/components/ui/Button';
import { TextField } from '@/components/ui/Field';
import { Badge, Callout, Card, ErrorBlock, Loading, PageHeader, Segmented } from '@/components/ui/Primitives';
import { EventTable, useLiveEvents } from './clusters/detail/EventsTab';
import { OperationsTable } from './clusters/detail/OperationsTab';

export function EventsPage() {
  const auth = useAuth();
  const { namespace } = useNamespace();
  const canAudit = auth.can('audit', 'read');
  const canEvents = auth.can('events', 'read') || canAudit;
  const [actor, setActor] = useState('');
  const [resource, setResource] = useState('');
  const [filters, setFilters] = useState<{ actor?: string; resource?: string }>({});
  const events = useLiveEvents({ namespace, ...(canAudit ? filters : {}) }, canEvents);
  return (
    <div className="page">
      <PageHeader title="Events" description={namespace ? `Durable activity and authorized audit log of namespace ${namespace}, updated live.` : canAudit ? 'Durable cluster and authorized audit activity across namespaces, updated live.' : 'Durable activity log across every cluster you can see, updated live.'} />
      {canAudit ? (
        <Card title="Audit filters">
          <form className="row wrap" style={{ alignItems: 'flex-end', gap: 12 }} onSubmit={(e) => {
            e.preventDefault();
            setFilters({ actor: actor || undefined, resource: resource || undefined });
          }}>
            <TextField label="Actor" value={actor} onChange={(e) => setActor(e.target.value)} placeholder="Exact actor identity" className="grow" />
            <TextField label="Resource" value={resource} onChange={(e) => setResource(e.target.value)} placeholder="e.g. accounts, clusters, namespaces" className="grow" />
            <Button type="submit">Apply filters</Button>
          </form>
          <p className="small muted">Exact, case-sensitive matches combined with the namespace selection; blank fields include all values. Audit records require audit:read for their object. Cluster permissions still apply.</p>
        </Card>
      ) : null}
      <Card
        flush
        title="Recent events"
        actions={
          <Badge tone={events.connected ? 'ok' : 'warn'} plain>
            {events.connected ? 'live' : 'polling'}
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
      <PageHeader title="Operations" description={`Durable, resumable operations: backups, restores, restarts, rotations${namespace ? ` in namespace ${namespace}` : ''}.`} />
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
