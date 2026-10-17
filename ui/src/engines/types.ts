// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { ComponentType, ReactNode } from 'react';
import type { Schemas } from '../api/client';
import type { Action, Resource } from '../auth/permissions';

export interface WizardSectionProps {
  namespace?: string;
  provider: Schemas['Provider'];
  spec: Schemas['ClusterSpec'];
  onChange: (spec: Schemas['ClusterSpec']) => void;
}

export interface MonitoringPanel {
  title: string;
  names: string[];
  unit?: string;
  yMax?: number;
  format?: (value: number) => string;
  description?: string;
}

export interface TopologyEditorProps {
  cluster: Schemas['Cluster'];
  provider: Schemas['Provider'] | undefined;
  /** False while the user lacks update permission or the cluster is deleting/suspended. */
  editable: boolean;
  can: (action: Action, resource?: Resource) => boolean;
  /** True while a submitted patch is in flight. */
  busy: boolean;
  /** Sends an RFC 7396 merge patch of the spec through the cluster update mutation; rejects on failure (already reported to the user). */
  submit: (patch: Record<string, unknown>, idempotencyKey: string) => Promise<Schemas['Cluster']>;
}

export interface ComponentValidation {
  topology: string[];
  resources: string[];
}

export interface EngineChangeReview {
  buildPatch: (current: Schemas['ClusterSpec'], draft: Schemas['ClusterSpec']) => Record<string, unknown>;
  /** Draft is the persisted spec with only the outgoing merge patch applied, not form defaults. */
  disruptions: (current: Schemas['ClusterSpec'], draft: Schemas['ClusterSpec']) => string[];
}

export interface EngineUI {
  label: string;
  description?: string;
  /** Show replica-set names and replication lag in the overview; defaults to false. */
  replicaSetDetails?: boolean;
  publicPort?: number | ((spec: Schemas['ClusterSpec']) => number);
  /** Protocol label displayed beside the public port. */
  publicPortLabel?: string | ((spec: Schemas['ClusterSpec']) => string);
  engineConfigLabel?: string;
  engineConfigPlaceholder?: string;
  /** Additional create-form errors, displayed in the wizard's Basics step. */
  validateSpec?: (spec: Schemas['ClusterSpec']) => string[];
  /** Rendered in the wizard's Basics step below the preset choice. */
  wizardSection?: ComponentType<WizardSectionProps>;
  /** Replaces the generic per-component node-count editor in the wizard's Topology step. */
  topologySection?: ComponentType<WizardSectionProps>;
  /** Replaces the generic per-component resource editors in the wizard's Resources step. */
  resourcesSection?: ComponentType<WizardSectionProps>;
  /** Replaces the generic replica/resource validation of the Topology and Resources steps. */
  validateComponents?: (spec: Schemas['ClusterSpec'], topology: Schemas['ProviderTopology'] | undefined) => ComponentValidation;
  /** Replaces the generic spec editor of the Topology & scaling tab (the replace-member card stays generic). */
  topologyEditor?: ComponentType<TopologyEditorProps>;
  /** Shared Apply dialog: patch construction and consequence copy for this engine. */
  changeReview?: EngineChangeReview;
  /** Topology label of a draft spec (wizard sidebar and review). */
  specSummary?: (spec: Schemas['ClusterSpec']) => string;
  /** Extra detail tabs; `capability` hides the tab when the provider does not describe it. */
  detailTabs?: { id: string; label: string; element: ReactNode; capability?: string }[];
  monitoringPanels?: MonitoringPanel[];
  topologySummary?: (cluster: Schemas['Cluster']) => string;
}
