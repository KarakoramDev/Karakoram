// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

/** All records are needed to identify the newest successful backup, regardless of list ordering. */
export async function fetchBackupInventory<T>(fetchPage: (cursor?: string) => Promise<{ items: T[]; nextCursor?: string }>): Promise<{ items: T[] }> {
  const items: T[] = [];
  let cursor: string | undefined;
  do {
    const page = await fetchPage(cursor);
    items.push(...page.items);
    cursor = page.nextCursor;
  } while (cursor);
  return { items };
}
