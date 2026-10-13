// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

/** Karakoram mark: two ridgelines, the taller peak catching light. */
export function Logo({ size = 28 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" aria-hidden>
      <rect width="32" height="32" rx="7" fill="#223558" />
      <path d="M4 25 L12 9 L17 18 L20 13 L28 25 Z" fill="#e8eef7" />
      <path d="M12 9 L14.5 14 L12 13 L9.5 14 Z" fill="#7fb3ff" />
    </svg>
  );
}
