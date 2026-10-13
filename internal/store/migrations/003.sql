-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 The Karakoram Authors

-- An old generation has no known requester. Do not invent historical actors.
ALTER TABLE clusters ADD COLUMN requested_by TEXT NOT NULL DEFAULT '';
ALTER TABLE events ADD COLUMN actor TEXT NOT NULL DEFAULT '';
ALTER TABLE events ADD COLUMN resource TEXT NOT NULL DEFAULT '';

-- Existing cluster events unambiguously refer to the clusters RBAC resource.
UPDATE events SET data = json_set(CAST(data AS TEXT), '$.resource', 'clusters', '$.target', cluster)
    WHERE cluster <> '';
UPDATE events SET actor = COALESCE(json_extract(CAST(data AS TEXT), '$.actor'), ''),
    resource = COALESCE(json_extract(CAST(data AS TEXT), '$.resource'), '');
CREATE INDEX events_actor ON events(actor, id);
CREATE INDEX events_resource ON events(resource, id);
