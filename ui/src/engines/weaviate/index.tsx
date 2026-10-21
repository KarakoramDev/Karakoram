// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
import type { EngineUI } from '../types';
import { weaviateMetadata } from './metadata';
import WizardSection from './WizardSection';
import AccessTab from './AccessTab';
export const weaviateUI: EngineUI = {
  ...weaviateMetadata,
  wizardSection: WizardSection,
  detailTabs: [{ id: 'weaviate', label: 'Weaviate access', capability: 'weaviateAccess', element: <AccessTab /> }],
};
