-- SPDX-License-Identifier: Apache-2.0
-- Copyright 2026 The Karakoram Authors
-- PostgREST's upstream default exposes this schema even without pg_graphql.
-- The optional extension migration supplies its GraphQL wrapper separately.
CREATE SCHEMA IF NOT EXISTS graphql_public AUTHORIZATION supabase_admin;
REVOKE ALL ON SCHEMA graphql_public FROM PUBLIC;
GRANT USAGE ON SCHEMA graphql_public TO anon, authenticated, service_role;
