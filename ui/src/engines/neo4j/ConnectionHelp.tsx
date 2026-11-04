// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';
import { CodeLine } from '@/components/ui/Primitives';
import { boltURI } from './spec';

export function ConnectionHelp({ connection }: { connection: Schemas['ConnectionSummary'] | undefined }) {
  return <details><summary className="small">How to connect (Bolt / Neo4j drivers)</summary>
    <CodeLine value={boltURI(connection?.hosts?.[0])} />
    <p className="small">Use neo4j+s://HOST:7687 with your Neo4j driver. Bolt requires TLS and verifies the certificate and hostname. Reveal database credentials above to download ca.pem, then install this cluster CA in your driver's or operating system's trusted certificate store. Do not disable certificate verification or substitute neo4j+ssc to bypass trust.</p>
    <p className="small">Private hosts require network access to the cluster. For public access, use bolt+s://PUBLIC_HOST:7687 for a direct singleton connection with the trusted cluster CA: routing discovery can advertise private hosts. Only Bolt port 7687 is exposed; HTTPS Query API and Browser remain private. Community has one instance with no failover, and every user has full administrative privileges.</p>
  </details>;
}
