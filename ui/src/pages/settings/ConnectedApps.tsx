// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';
import { api, unwrap, errorMessage } from '@/api/client';
import { Button } from '@/components/ui/Button';
import { Callout } from '@/components/ui/Primitives';

export function useOAuthGrants() {
  return useQuery({ queryKey: ['oauth-grants'], queryFn: () => unwrap(api.GET('/oauth-grants')) });
}
export function ConnectedAppsPage() {
  const query = useOAuthGrants();
  const cache = useQueryClient();
  const revoke = useMutation({ mutationFn: (id: string) => unwrap(api.DELETE('/oauth-grants/{grant}', { params: { path: { grant: id } } })), onSuccess: () => cache.invalidateQueries({ queryKey: ['oauth-grants'] }) });
  if (query.isPending) return <p>Loading connected apps…</p>;
  if (query.error) return <Callout tone="danger">{errorMessage(query.error)}</Callout>;
  if (!query.data?.enabled) return <p>OAuth is disabled on this installation.</p>;
  return <section className="stack"><h2>Connected apps</h2><p>Apps only have the scopes and namespace limits you approved, intersected with your current permissions. Revoking disconnects all tokens for that grant.</p>
    {revoke.error ? <Callout tone="danger">{errorMessage(revoke.error)}</Callout> : null}
    {!query.data.items.length ? <p>No connected apps.</p> : query.data.items.map((grant) => <article className="stack" key={grant.id}>
      <h3>{grant.clientName}</h3><p>{grant.clientHost} · {grant.owner}</p><p>{grant.clientId}</p>
      <p>Scopes: {grant.scopes.join(', ')}</p><p>Namespaces: {grant.namespaces.join(', ') || 'All'}</p>
      <p>Created: {new Date(grant.createdAt).toLocaleString()} · Last used: {grant.lastUsedAt ? new Date(grant.lastUsedAt).toLocaleString() : 'Never'}</p>
      <Button disabled={revoke.isPending} onClick={() => { if (window.confirm(`Disconnect ${grant.clientName}?`)) revoke.mutate(grant.id); }}>Revoke</Button>
    </article>)}
  </section>;
}
