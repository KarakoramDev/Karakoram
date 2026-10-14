// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Provider } from '../../../api/hooks';
import { tabAvailable } from '../../../engines/index.ts';

/** Catalog loading is not evidence that a capability-gated tab is unsupported. */
export function detailTabState(provider: Provider | undefined, tab: string, providersPending: boolean): 'available' | 'loading' | 'unsupported' {
  if (tabAvailable(provider, tab)) return 'available';
  return providersPending ? 'loading' : 'unsupported';
}
