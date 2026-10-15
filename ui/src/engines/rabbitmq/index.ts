// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import { createElement, lazy, Suspense } from 'react';
import type { EngineUI, WizardSectionProps } from '../types';
import { validateRabbitMQSpec } from './helpers.ts';

const Wizard = lazy(() => import('./WizardSection'));
const Access = lazy(() => import('./AccessTab'));
const QuorumRecovery = lazy(() => import('./QuorumRecovery'));

export const rabbitmqUI: EngineUI = {
  label: 'RabbitMQ',
  description: 'RabbitMQ clusters with quorum queues and definitions backups.',
  wizardSection: (props: WizardSectionProps) => createElement(Suspense, { fallback: 'Loading RabbitMQ configuration…' }, createElement(Wizard, props)),
  detailTabs: [
    { id: 'access', label: 'Access', element: createElement(Suspense, { fallback: 'Loading RabbitMQ access…' }, createElement(Access)) },
    { id: 'queue-recovery', label: 'Queue recovery', element: createElement(Suspense, { fallback: 'Loading queue recovery…' }, createElement(QuorumRecovery)) },
  ],
  publicPort: (spec) => spec.tls?.mode === 'disabled' ? 5672 : 5671,
  publicPortLabel: (spec) => spec.tls?.mode === 'disabled' ? 'AMQP' : 'AMQPS',
  engineConfigLabel: 'rabbitmq.conf overrides',
  engineConfigPlaceholder: '# Additional RabbitMQ settings\nconsumer_timeout = 1800000',
  validateSpec: validateRabbitMQSpec,
  monitoringPanels: [
    { title: 'Connections, channels & consumers', names: ['rabbitmq.connections', 'rabbitmq.channels', 'rabbitmq.consumers'], unit: 'count' },
    { title: 'Queues', names: ['rabbitmq.queues'], unit: 'count' },
    { title: 'Messages ready & unacknowledged', names: ['rabbitmq.messagesReady', 'rabbitmq.messagesUnacked'], unit: 'messages' },
    { title: 'Publish & delivery rate', names: ['rabbitmq.published', 'rabbitmq.delivered'], unit: 'messages/s' },
    { title: 'Resident memory & high watermark', names: ['rabbitmq.residentMemoryBytes', 'rabbitmq.memoryLimitBytes'], unit: 'bytes' },
    { title: 'Disk free & alarm limit', names: ['rabbitmq.diskFreeBytes', 'rabbitmq.diskFreeLimitBytes'], unit: 'bytes' },
    { title: 'Memory & disk alarms', names: ['rabbitmq.memoryAlarm', 'rabbitmq.diskAlarm'], unit: 'alarm', yMax: 1, format: (value) => value > 0 ? 'Active' : 'Clear' },
    { title: 'Broker availability', names: ['rabbitmq.up'], unit: 'up', yMax: 1 },
    { title: 'Running cluster members', names: ['rabbitmq.clusterMembersRunning'], unit: 'nodes' },
    { title: 'Broker uptime', names: ['rabbitmq.uptimeSeconds'], unit: 's' },
  ],
  topologySummary: (cluster) => `${cluster.spec.components.engine.replicas}-node RabbitMQ cluster`,
};
