// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '../../api/client';
import { resourcesSummary } from '../../pages/clusters/cluster-utils';

/** Shared by create Review and Overview so master allocation cannot disappear from either. */
export function elasticResourceRows(spec: Schemas['ClusterSpec']): [string, string][] {
  const rows: [string, string][] = [['Data node resources', `${spec.components.engine.replicas} × ${resourcesSummary(spec.components.engine.resources)}`]];
  const master = spec.components.extra?.master;
  if (master) rows.push(['Dedicated master resources', `${master.replicas} × ${resourcesSummary(master.resources)}`]);
  return rows;
}
