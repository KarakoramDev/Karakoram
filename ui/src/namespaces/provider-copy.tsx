// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Namespace } from '../api/hooks';
import type { Schemas } from '../api/client';

export function namespaceBackend(namespaces: Namespace[] | undefined, name: string): string | undefined {
  return namespaces?.find((namespace) => namespace.name === name)?.backend;
}

export function namespaceBackendInfo(namespaces: Namespace[] | undefined, name: string, backends: Schemas['BackendInfo'][] | undefined): Schemas['BackendInfo'] | undefined {
  const kind = namespaceBackend(namespaces, name);
  return backends?.find((backend) => backend.kind === kind);
}

export const namespaceHint = 'The project or environment that will host the cluster. Backup storages and name uniqueness are per namespace.';

export function clusterNameHint(backend?: string): string {
  return backend === 'zerops'
    ? '2–12 lowercase letters and digits; becomes the Zerops hostname prefix (e.g. ordersm1, ordersm2).'
    : '2–12 lowercase letters and digits; used as the member service name prefix (e.g. ordersm1, ordersm2).';
}

export function clustersDescription(namespace?: string, allNamespaces = false): string {
  return namespace ? `Database clusters managed in namespace ${namespace}.` : allNamespaces ? 'Database clusters managed across every namespace you can see.' : 'Database clusters managed in your namespaces.';
}

export function BillingNote({ backend }: { backend?: string }) {
  return <p className="small muted">{backend === 'zerops'
    ? <>These are aggregate autoscaling bounds, not a price estimate or the initial allocation. Billing follows <a href="https://zerops.io/pricing" target="_blank" rel="noreferrer">Zerops pricing</a> for runtime CPU/RAM, runtime disks, data volumes and other enabled resources; backup storage is additional.</>
    : <>These are aggregate resource bounds, not a price estimate or the initial allocation. Infrastructure costs follow your provider’s own pricing; Karakoram does not estimate billing. Backup storage is additional.</>}</p>;
}
