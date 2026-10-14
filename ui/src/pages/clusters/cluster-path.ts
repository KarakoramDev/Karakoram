// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

/** Route prefix of a cluster's detail pages. */
export function clusterPath(namespace: string, name: string): string {
  return `/clusters/${encodeURIComponent(namespace)}/${encodeURIComponent(name)}`;
}
