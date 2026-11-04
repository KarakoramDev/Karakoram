// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { EngineUI, MonitoringPanel } from '../types.ts';
import { API_PORT, openbaoChangeReview, specSummary, validateRestore, validateSpec } from './spec.ts';

/** Canonical series are `openbao.<name>` from the provider catalog (internal/engine/openbao/metrics.go). */
export const openbaoMonitoringPanels: MonitoringPanel[] = [
  { title: 'Server up, initialized & unsealed', names: ['openbao.up', 'openbao.initialized', 'openbao.sealed'], unit: 'state', yMax: 1, format: (value) => value > 0 ? 'Yes' : 'No', description: 'sealed is 1 while a member is sealed; expect 0 on healthy members.' },
  { title: 'Active node', names: ['openbao.active'], unit: 'active', yMax: 1, format: (value) => value > 0 ? 'Active' : 'Standby' },
  { title: 'Raft committed & applied index', names: ['openbao.raftCommittedIndex', 'openbao.raftAppliedIndex'], unit: 'index' },
  { title: 'Raft leader last contact', names: ['openbao.raftLeaderLastContactSeconds'], unit: 's' },
  { title: 'Autopilot health & failure tolerance', names: ['openbao.autopilotHealthy', 'openbao.autopilotFailureTolerance'], unit: 'count', description: 'healthy is 1 when every voter is healthy; tolerance is the number of voters that can fail without losing quorum.' },
  { title: 'Request rate', names: ['openbao.requests'], unit: 'requests/s' },
  { title: 'Leases', names: ['openbao.leases'], unit: 'count' },
  { title: 'Runtime memory', names: ['openbao.memoryBytes'], unit: 'bytes' },
];

export const openbaoMetadata: EngineUI = {
  label: 'OpenBao',
  description: 'Secrets manager with integrated Raft storage, Karakoram-held unseal keys and dynamic database credentials.',
  publicPort: API_PORT,
  publicPortLabel: 'HTTPS API',
  validateSpec,
  validateRestore,
  changeReview: openbaoChangeReview,
  monitoringPanels: openbaoMonitoringPanels,
  topologySummary: (cluster) => specSummary(cluster.spec),
};
