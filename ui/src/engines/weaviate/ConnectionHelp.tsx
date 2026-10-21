// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
import { Link } from 'react-router-dom';
export function ConnectionHelp({ base, tlsMode }: { base: string; tlsMode: string | undefined }) {
  return <details><summary className="small">How to connect to Weaviate</summary>
    <p className="small">REST uses port 8080; client gRPC uses port 50051. Configure the advertised member endpoint and a scoped application API key from <Link to={`${base}/weaviate`}>Weaviate access</Link>. REST requests authenticate with <code>Authorization: Bearer &lt;API-key&gt;</code>. Do not embed keys in URLs or use the administrative key in applications.</p>
    <p className="small">{tlsMode === 'disabled' ? 'Client TLS is disabled: keep connections on an isolated private network.' : 'Trust the cluster CA and keep hostname verification enabled for HTTPS REST and TLS gRPC.'} Client TLS does not encrypt internal gossip, data or Raft transports. Private endpoints require access to the isolated cluster network.</p>
  </details>;
}
