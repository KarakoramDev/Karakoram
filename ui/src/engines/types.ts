// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { ComponentType, ReactNode } from 'react';
import type { Schemas } from '../api/client';

export interface WizardSectionProps {
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
}

export interface EngineUI {
  label: string;
  description?: string;
  publicPort?: number | ((spec: Schemas['ClusterSpec']) => number);
  /** Protocol label displayed beside the public port. */
  publicPortLabel?: string | ((spec: Schemas['ClusterSpec']) => string);
  engineConfigLabel?: string;
  engineConfigPlaceholder?: string;
  /** Additional create-form errors, displayed in the wizard's Basics step. */
  validateSpec?: (spec: Schemas['ClusterSpec']) => string[];
  wizardSection?: ComponentType<WizardSectionProps>;
  detailTabs?: { id: string; label: string; element: ReactNode }[];
  monitoringPanels?: MonitoringPanel[];
  topologySummary?: (cluster: Schemas['Cluster']) => string;
}
