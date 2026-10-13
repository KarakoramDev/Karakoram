-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 The Karakoram Authors

CREATE TABLE records (
 kind TEXT NOT NULL, scope TEXT NOT NULL DEFAULT '', key TEXT NOT NULL,
 data BLOB NOT NULL, secret TEXT NOT NULL DEFAULT '', hash TEXT NOT NULL DEFAULT '',
 phase TEXT NOT NULL DEFAULT '', created INTEGER NOT NULL, updated INTEGER NOT NULL,
 PRIMARY KEY(kind,scope,key)
);
CREATE INDEX records_order ON records(kind,scope,created,key);
CREATE INDEX records_phase ON records(kind,scope,phase,created);
CREATE INDEX records_hash ON records(kind,hash);
CREATE TABLE clusters (name TEXT PRIMARY KEY, spec BLOB NOT NULL, status BLOB NOT NULL, internal BLOB NOT NULL, generation INTEGER NOT NULL, deleting INTEGER NOT NULL DEFAULT 0, delete_options BLOB NOT NULL, created INTEGER NOT NULL, updated INTEGER NOT NULL);
CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, cluster TEXT NOT NULL, type TEXT NOT NULL, data BLOB NOT NULL, created INTEGER NOT NULL);
CREATE INDEX events_cluster ON events(cluster,id);
CREATE TABLE metrics (id INTEGER PRIMARY KEY AUTOINCREMENT, cluster TEXT NOT NULL, member TEXT NOT NULL, metric TEXT NOT NULL, timestamp INTEGER NOT NULL, data BLOB NOT NULL);
CREATE INDEX metrics_range ON metrics(cluster,member,metric,timestamp);
CREATE TABLE policies (position INTEGER PRIMARY KEY, data BLOB NOT NULL);
