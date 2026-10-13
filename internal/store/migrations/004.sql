-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 The Karakoram Authors

CREATE TABLE namespaces (name TEXT PRIMARY KEY, data TEXT NOT NULL, updated_at TEXT NOT NULL);

ALTER TABLE clusters RENAME TO clusters_legacy;
CREATE TABLE clusters (namespace TEXT NOT NULL DEFAULT 'default', name TEXT NOT NULL, spec BLOB NOT NULL, status BLOB NOT NULL, internal BLOB NOT NULL, generation INTEGER NOT NULL, deleting INTEGER NOT NULL DEFAULT 0, delete_options BLOB NOT NULL, created INTEGER NOT NULL, updated INTEGER NOT NULL, requested_by TEXT NOT NULL DEFAULT '', PRIMARY KEY(namespace,name));
INSERT INTO clusters SELECT 'default', name, spec, status, internal, generation, deleting, delete_options, created, updated, requested_by FROM clusters_legacy;
DROP TABLE clusters_legacy;

ALTER TABLE events ADD COLUMN namespace TEXT NOT NULL DEFAULT 'default';
-- Pre-namespace non-cluster audit entries describe installation-wide resources.
UPDATE events SET namespace = '' WHERE cluster = '';
CREATE INDEX events_namespace_cluster ON events(namespace,cluster,id);
ALTER TABLE metrics ADD COLUMN namespace TEXT NOT NULL DEFAULT 'default';
CREATE INDEX metrics_namespace_range ON metrics(namespace,cluster,member,metric,timestamp);
