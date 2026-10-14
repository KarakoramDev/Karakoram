// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

/** A transport failure does not prove that an unsafe request failed to commit. */
export async function deleteWithReconciliation(remove: () => Promise<unknown>, read: () => Promise<unknown>, isNotFound: (error: unknown) => boolean): Promise<void> {
  try { await remove(); return; } catch (error) {
    if (isNotFound(error)) return;
    if (error && typeof error === 'object' && 'status' in error && typeof error.status === 'number' && error.status < 500) throw error;
    try { await read(); } catch (readError) {
      if (isNotFound(readError)) return;
      throw new Error('Connection lost. Deletion outcome is not yet known; reconnect and retry to check whether the storage still exists.');
    }
    throw new Error('The deletion request lost its connection. The storage still exists; you can retry deletion.');
  }
}
