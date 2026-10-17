// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { resourceMinimums, validateResources } from '@/pages/clusters/wizard/resources';
import type { EngineUI } from '../types';
import { postgresMetadata } from './metadata';
import { PostgresAccessTab } from './AccessTab';
import { TopologyEditor } from './TopologyEditor';
import { OptionsSection, ResourcesSection, TopologySection } from './WizardSections';
import { validateComponents } from './spec';

export const postgresUI: EngineUI = {
  ...postgresMetadata, wizardSection: OptionsSection, topologySection: TopologySection, resourcesSection: ResourcesSection, topologyEditor: TopologyEditor,
  validateComponents: (spec, topology) => validateComponents(spec, topology, (r, stateless, name) => validateResources(r, undefined, stateless, resourceMinimums(spec.provider, topology, name))),
  detailTabs: [{ id: 'postgres', label: 'Postgres access', element: <PostgresAccessTab />, capability: 'postgresAccess' }],
};
