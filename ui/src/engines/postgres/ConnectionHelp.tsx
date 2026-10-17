// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { CodeLine } from '@/components/ui/Primitives';

export function ConnectionHelp({ connection, tlsMode }: { connection: Schemas['ConnectionSummary'] | undefined; tlsMode: string | undefined }) {
  const host = connection?.hosts?.[0];
  const sslmode = tlsMode === 'disabled' ? 'disable' : 'verify-full';
  const uri = `postgresql://USER:PASSWORD@${host || 'PROXY_HOST:5432'}/DATABASE?sslmode=${sslmode}${sslmode === 'verify-full' ? '&sslrootcert=ca.pem' : ''}`;
  return <details><summary className="small">How to connect (psql / libpq)</summary>
    <p className="small">Use the ProxySQL endpoint for application connections; single-member clusters without a proxy connect directly. Use a scoped login role from Postgres access. URI-encode usernames, passwords and database names.</p>
    <CodeLine value={uri} /><CodeLine value={`psql '${uri}'`} />
    <p className="small">{tlsMode === 'disabled' ? 'TLS is disabled for client traffic.' : 'Reveal database credentials to download ca.pem. sslmode=verify-full verifies the CA and endpoint hostname; preferTLS also allows unencrypted clients, while requireTLS rejects them.'} Private endpoints require network access to the cluster. Public hosts, when exposed, appear separately above.</p>
  </details>;
}
