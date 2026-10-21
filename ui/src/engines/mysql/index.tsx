// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { resourceMinimums, validateResources } from '@/pages/clusters/wizard/resources';
import type { EngineUI } from '../types';
import { mysqlMetadata } from './metadata';
import { MySQLAccessTab } from './AccessTab';
import { TopologyEditor } from './TopologyEditor';
import { OptionsSection, ResourcesSection, TopologySection } from './WizardSections';
import { validateComponents } from './spec';
export const mysqlUI: EngineUI = {
  ...mysqlMetadata, wizardSection: OptionsSection, topologySection: TopologySection, resourcesSection: ResourcesSection, topologyEditor: TopologyEditor,
  validateComponents: (spec, topology) => validateComponents(spec, topology, (r, stateless, name) => validateResources(r, undefined, stateless, resourceMinimums(spec.provider, topology, name))),
  detailTabs: [{ id: 'mysql', label: 'MySQL access', element: <MySQLAccessTab />, capability: 'mysqlAccess' }],
};
