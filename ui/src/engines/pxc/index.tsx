// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { EngineUI } from '../types';
import { mysqlUI } from '../mysql';
import { pxcMetadata } from './metadata';
import { RecoveryTab } from './RecoveryTab';
export const pxcUI: EngineUI = {
  ...mysqlUI, ...pxcMetadata,
  detailTabs: [...(mysqlUI.detailTabs ?? []), { id: 'recovery', label: 'Recovery', element: <RecoveryTab /> }],
};
