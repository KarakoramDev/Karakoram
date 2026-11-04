CREATE TABLE postgres_bindings (
 id TEXT PRIMARY KEY,
 incarnation TEXT NOT NULL,
 source_namespace TEXT NOT NULL,
 source_cluster TEXT NOT NULL,
 source_uid TEXT NOT NULL,
 consumer_kind TEXT NOT NULL,
 consumer_namespace TEXT NOT NULL,
 consumer_name TEXT NOT NULL,
 databases TEXT NOT NULL CHECK(json_valid(databases)),
 extensions TEXT NOT NULL CHECK(json_valid(extensions)),
 role_name TEXT NOT NULL,
 min_major INTEGER NOT NULL,
 max_major INTEGER NOT NULL,
 state TEXT NOT NULL,
 credential_generation INTEGER NOT NULL,
 created INTEGER NOT NULL,
 updated INTEGER NOT NULL,
 UNIQUE(consumer_kind,consumer_namespace,consumer_name,source_namespace,source_cluster,source_uid)
);
CREATE INDEX postgres_bindings_source ON postgres_bindings(source_namespace,source_cluster,source_uid);
CREATE UNIQUE INDEX postgres_bindings_role ON postgres_bindings(source_namespace,source_cluster,source_uid,role_name);
