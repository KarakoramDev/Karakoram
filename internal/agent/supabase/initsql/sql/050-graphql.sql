-- This migration exists only when pg_graphql was explicitly selected and installed.
CREATE SCHEMA IF NOT EXISTS graphql_public AUTHORIZATION supabase_admin;
REVOKE ALL ON SCHEMA graphql_public FROM PUBLIC;
CREATE OR REPLACE FUNCTION graphql_public.graphql(
  "operationName" text DEFAULT NULL, query text DEFAULT NULL,
  variables jsonb DEFAULT NULL, extensions jsonb DEFAULT NULL)
RETURNS jsonb LANGUAGE sql AS $$
  SELECT graphql.resolve(query := query, variables := coalesce(variables, '{}'),
    "operationName" := "operationName", extensions := extensions);
$$;
ALTER FUNCTION graphql_public.graphql(text, text, jsonb, jsonb) OWNER TO supabase_admin;
GRANT USAGE ON SCHEMA graphql, graphql_public TO anon, authenticated, service_role;
REVOKE ALL ON FUNCTION graphql_public.graphql(text, text, jsonb, jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION graphql_public.graphql(text, text, jsonb, jsonb) TO anon, authenticated, service_role;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA graphql TO anon, authenticated, service_role;
