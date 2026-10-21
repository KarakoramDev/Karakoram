// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { neo4jMetadata } from './index';
import { WizardSection } from './WizardSection';
import { UsersTab } from './UsersTab';
import type { EngineUI } from '../types';

export const neo4jUI: EngineUI = {
  ...neo4jMetadata,
  wizardSection: WizardSection,
  detailTabs: [{ id: 'neo4j-users', label: 'Neo4j users', element: <UsersTab />, capability: 'neo4jAccess' }],
};
