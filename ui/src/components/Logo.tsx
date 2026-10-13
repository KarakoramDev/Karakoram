// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

/**
 * Karakoram mark ("Ridge K"): the K2 pyramid with a K carved into it as
 * ridgelines. The body follows currentColor; the summit cap uses
 * --logo-cap (teal on light surfaces, glacier on the dark sidebar).
 */
export function Logo({ size = 28 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="currentColor" aria-hidden>
      <path d="M1 30 9 15.1V30Z" />
      <path style={{ fill: 'var(--logo-cap, #0b6b74)' }} d="M16 2 19.9 9.2 12 18.7V9.5Z" />
      <path d="M14 20.9 21.4 12.1 31 30h-7.9Z" />
      <path d="M12 23.1 18.9 30H12Z" />
    </svg>
  );
}
