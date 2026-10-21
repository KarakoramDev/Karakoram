// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { resourceMinimums, validateResources } from '@/pages/clusters/wizard/resources';
import type { EngineUI } from '../types';
import { CertificatesTab } from './CertificatesTab';
import { NamespacesTab } from './NamespacesTab';
import { TopologyEditor } from './TopologyEditor';
import { WebUITab } from './WebUITab';
import { OptionsSection, ResourcesSection, TopologySection } from './WizardSections';
import { specSummary, temporalChangeReview, validateComponents, validateSpec } from './spec';

export const temporalUI: EngineUI = {
  label: 'Temporal',
  description: 'Temporal workflow server with PostgreSQL persistence from a bound Karakoram cluster.',
  engineConfigUnsupported: true,
  validateSpec,
  wizardSection: OptionsSection,
  topologySection: TopologySection,
  resourcesSection: ResourcesSection,
  validateComponents: (spec, topology) => validateComponents(spec, topology, (resources, stateless, name) => validateResources(resources, undefined, stateless, resourceMinimums(spec.provider, topology, name))),
  specSummary,
  changeReview: temporalChangeReview,
  topologyEditor: TopologyEditor,
  detailTabs: [
    { id: 'namespaces', label: 'Namespaces', element: <NamespacesTab />, capability: 'temporalAccess' },
    { id: 'certificates', label: 'Client certificates', element: <CertificatesTab />, capability: 'temporalAccess' },
    { id: 'web-ui', label: 'Web UI', element: <WebUITab />, capability: 'temporalAccess' },
  ],
  topologySummary: (cluster) => specSummary(cluster.spec),
};
