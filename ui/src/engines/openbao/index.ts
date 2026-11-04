// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { createElement, lazy, Suspense } from 'react';
import type { EngineUI, RestoreSectionProps, TopologyEditorProps, WizardSectionProps } from '../types';
import { openbaoMetadata } from './metadata.ts';

const Wizard = lazy(() => import('./WizardSection'));
const Tab = lazy(() => import('./OpenBaoTab'));
const Editor = lazy(() => import('./TopologyEditor').then((module) => ({ default: module.TopologyEditor })));
const Restore = lazy(() => import('./RestoreShares'));

export const openbaoUI: EngineUI = {
  ...openbaoMetadata,
  wizardSection: (props: WizardSectionProps) => createElement(Suspense, { fallback: 'Loading OpenBao configuration…' }, createElement(Wizard, props)),
  topologyEditor: (props: TopologyEditorProps) => createElement(Suspense, { fallback: 'Loading OpenBao editor…' }, createElement(Editor, props)),
  restoreSection: (props: RestoreSectionProps) => createElement(Suspense, { fallback: 'Loading OpenBao restore options…' }, createElement(Restore, props)),
  detailTabs: [
    { id: 'openbao', label: 'OpenBao', element: createElement(Suspense, { fallback: 'Loading OpenBao…' }, createElement(Tab)) },
  ],
};
