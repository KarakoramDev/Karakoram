// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { CodeLine } from '@/components/ui/Primitives';
export function ConnectionHelp({ connection }: { connection: Schemas['ConnectionSummary'] | undefined }) {
  const endpoint = connection?.hosts?.[0] || 'PROXY_HOST:3306';
  const host = endpoint.replace(/:\d+$/, '').replace(/^\[|\]$/g, '');
  const port = endpoint.match(/:(\d+)$/)?.[1] ?? '3306';
  return <details><summary className="small">How to connect (mysql / driver URI)</summary>
    <p className="small">Use the ProxySQL application endpoint, or connect directly for single-member clusters without a proxy. Create a scoped user in MySQL access. URI-encode credentials and database names. IPv6 hosts in URIs require brackets.</p>
    <CodeLine value={`mysql --host='${host}' --port=${port} --user=USER --password --database=DATABASE --ssl-mode=VERIFY_CA --ssl-ca=ca.pem`} />
    <CodeLine value={`mysql://USER:PASSWORD@${endpoint}/DATABASE?ssl-mode=VERIFY_CA&ssl-ca=ca.pem`} />
    <p className="small">Download ca.pem from database credentials. caching_sha2_password requires TLS. VERIFY_CA verifies the certificate authority; use VERIFY_IDENTITY where your driver and endpoint certificates support hostname verification. URI TLS option names vary by driver: configure CA verification explicitly. Private endpoints require network access to the cluster.</p>
  </details>;
}
