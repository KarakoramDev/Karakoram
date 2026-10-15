-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 The Karakoram Authors

-- Bound legacy abandoned reservations without shortening successful replays.
UPDATE idempotency_requests
SET expires = MIN(expires, (unixepoch('now') + 300) * 1000000000)
WHERE response = '';
