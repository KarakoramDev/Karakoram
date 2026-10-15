-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 The Karakoram Authors

-- Legacy unkeyed body hashes permit offline password guessing. Their keyed
-- replacements cannot be reconstructed without the original request, so discard
-- both pending reservations and completed replays at this one-time cutover.
DELETE FROM idempotency_requests;
