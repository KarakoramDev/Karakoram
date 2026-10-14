-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 The Karakoram Authors

CREATE TABLE idempotency_requests (
    identity TEXT PRIMARY KEY,
    body_hash TEXT NOT NULL,
    owner TEXT NOT NULL,
    response TEXT NOT NULL DEFAULT '',
    expires INTEGER NOT NULL
);
CREATE INDEX idempotency_expiry ON idempotency_requests(expires);
