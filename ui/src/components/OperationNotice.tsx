// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { ReactNode } from 'react';
import { Link } from 'react-router-dom';
import type { Schemas } from '@/api/client';
import { operationPath, operationTargetLink } from './operations';

export function OperationNotice({ operationId, children }: { operationId?: string; children: ReactNode }) {
  return <>{children}{operationId ? <> <Link to={operationPath(operationId)}>View operation</Link></> : null}</>;
}

export function OperationTarget({ operation }: { operation: Pick<Schemas['Operation'], 'namespace' | 'cluster' | 'target'> }) {
  const link = operationTargetLink(operation);
  const label = `${operation.target.kind}: ${operation.target.name}`;
  return link ? <Link to={link}>{label}</Link> : <span>{label}</span>;
}
