// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

/** Read the complete authorized namespace collection before exposing selector choices. */
export async function fetchAllNamespaces<T>(fetchPage: (cursor?: string) => Promise<{ items: T[]; nextCursor?: string }>): Promise<{ items: T[] }> {
  const items: T[] = [];
  let cursor: string | undefined;
  do {
    const page = await fetchPage(cursor);
    items.push(...page.items);
    cursor = page.nextCursor;
  } while (cursor);
  return { items };
}
