// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { useState } from 'react';
import { api, errorMessage, unwrap, type Schemas } from '@/api/client';
import { Button } from '@/components/ui/Button';
import { Callout, CodeLine } from '@/components/ui/Primitives';

export function SupabaseCredentialsReveal({ namespace, name, allowed }: { namespace: string; name: string; allowed: boolean }) {
  const [credentials, setCredentials] = useState<Schemas['SupabaseCredentials'] | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  if (!allowed) return <p className="xs faint">Revealing Supabase keys requires cluster-credentials read permission.</p>;
  return <div className="stack">
    {credentials ? <>
      <div className="row-between"><strong>Supabase credentials</strong><Button size="sm" onClick={() => setCredentials(null)}>Hide keys</Button></div>
      <Callout tone="warn">Service-role and secret keys bypass row-level security. Never include them in browser code. The anon and publishable keys are intended for clients protected by RLS.</Callout>
      {Object.entries(credentials).map(([key, value]) => <div key={key}><strong className="small">{key}</strong>{(key === 'apiUrl' || key === 'studioUrl') && /^https?:\/\//.test(value) ? <p><a href={value} target="_blank" rel="noreferrer">{value}</a></p> : <CodeLine value={value} />}</div>)}
    </> : <Button size="sm" loading={busy} onClick={async () => { setBusy(true); setError(null); try { setCredentials(await unwrap(api.GET('/clusters/{cluster}/supabase/credentials', { params: { path: { cluster: name }, query: { namespace } } }))); } catch (e) { setError(errorMessage(e)); } finally { setBusy(false); } }}>Reveal Supabase keys</Button>}
    {error ? <span className="field-error">{error}</span> : null}
  </div>;
}
