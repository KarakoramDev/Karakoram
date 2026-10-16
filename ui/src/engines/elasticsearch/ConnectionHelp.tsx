// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { CodeLine } from '@/components/ui/Primitives';

export function ConnectionHelp({ connection }: { connection: Schemas['ConnectionSummary'] | undefined }) {
  const endpoints = connection?.endpoints?.filter((endpoint) => endpoint.name === 'elasticsearch' && endpoint.url.startsWith('https://')) ?? [];
  return <details>
    <summary className="small">How to connect (Elasticsearch)</summary>
    <p className="small">Use an Elasticsearch HTTPS endpoint below. Reveal credentials to obtain the <code>elastic</code> user's password and download <code>ca.pem</code>. Keep certificate verification enabled; curl prompts for the password without putting it in your command history.</p>
    <p className="small">Private endpoints only resolve and are reachable from workloads in the same Zerops project. Run private-endpoint commands there, not on your workstation or the management relay.</p>
    {endpoints.map((endpoint) => <div key={endpoint.url}>
      <strong className="small">{endpoint.public ? 'Public HTTPS' : 'Private HTTPS'}</strong>
      <CodeLine value={`curl --cacert ca.pem -u elastic "${endpoint.url.replace(/\/$/, '')}/_cluster/health"`} />
    </div>)}
    {!endpoints.length ? <p className="small muted">The HTTPS endpoint appears after Elasticsearch initializes.</p> : null}
    <p className="xs faint">Use a scoped native user for applications. <a href="https://karakoram.dev/docs/elasticsearch/access-and-security/#connect-and-reveal-credentials" target="_blank" rel="noreferrer">Elasticsearch connection guide</a></p>
  </details>;
}
