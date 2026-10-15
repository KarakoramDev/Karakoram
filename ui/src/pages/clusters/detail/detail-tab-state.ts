// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Provider } from '../../../api/hooks';
import { tabAvailable } from '../../../engines/index.ts';

/** A pending or failed catalog is not evidence that a capability-gated tab is unsupported. */
export function detailTabState(provider: Provider | undefined, tab: string, providersSuccess: boolean): 'available' | 'loading' | 'unsupported' {
  if (tabAvailable(provider, tab)) return 'available';
  return providersSuccess ? 'unsupported' : 'loading';
}
