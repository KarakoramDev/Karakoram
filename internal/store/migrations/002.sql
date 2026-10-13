-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 The Karakoram Authors

-- Operation URLs use a globally unique ID, even across clusters.
CREATE UNIQUE INDEX operation_identity ON records(key) WHERE kind = 'operation';
-- A bearer credential must resolve to exactly one identity.
CREATE UNIQUE INDEX token_identity ON records(kind, hash)
    WHERE kind IN ('api-token', 'agent-token', 'session') AND hash <> '';
