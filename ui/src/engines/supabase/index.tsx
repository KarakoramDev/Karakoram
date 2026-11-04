// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { TopologyTab } from '@/pages/clusters/detail/TopologyTab';
import type { EngineUI } from '../types';
import { postgresUI } from '../postgres';
import { supabaseMetadata } from '../postgres/metadata';
import { SupabaseOptions } from './WizardSection';
import { FunctionsTab } from './FunctionsTab';

export const supabaseUI: EngineUI = {
  ...postgresUI, ...supabaseMetadata, wizardSection: SupabaseOptions,
  detailTabs: [
    // Bindings are sourced only from plain postgres clusters (ADR 0045).
    ...(postgresUI.detailTabs ?? []).filter((tab) => tab.id !== 'bindings'),
    { id: 'components', label: 'Components', element: <TopologyTab /> },
    { id: 'functions', label: 'Functions & SMTP', element: <FunctionsTab /> },
  ],
};
