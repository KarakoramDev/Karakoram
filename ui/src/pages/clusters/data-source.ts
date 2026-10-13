// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '../../api/client';

type DataSource = Schemas['DataSource'];

/**
 * Wizard-side shape of a data source: the source namespace is always
 * explicit, even when it equals the target namespace. The target can
 * change after the source was picked (the Basics step stays editable), and
 * a source without its own namespace would silently start to mean "the
 * same-named cluster in the new target".
 */
export function dataSourceFor(namespace: string, cluster: string, point: Pick<DataSource, 'backup' | 'pitrTime'> = {}): DataSource {
  return { namespace, cluster, ...(point.backup ? { backup: point.backup } : {}), ...(point.pitrTime ? { pitrTime: point.pitrTime } : {}) };
}

/** `namespace/cluster` when the source is cross-namespace, bare `cluster` otherwise. */
export function dataSourceLabel(ds: DataSource, targetNamespace: string): string {
  const ns = ds.namespace || targetNamespace;
  return ns !== targetNamespace ? `${ns}/${ds.cluster}` : ds.cluster;
}

/**
 * Wire shape for the create request. The API defaults the source namespace
 * to the target namespace, so it is only sent when it differs; the explicit
 * value stays in wizard state until this point.
 */
export function wireDataSource(ds: DataSource, targetNamespace: string): DataSource {
  const { namespace, ...rest } = ds;
  return namespace && namespace !== targetNamespace ? { ...rest, namespace } : rest;
}
