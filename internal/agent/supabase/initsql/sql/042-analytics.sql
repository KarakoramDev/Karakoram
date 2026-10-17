CREATE SCHEMA IF NOT EXISTS _analytics AUTHORIZATION supabase_admin;
ALTER SCHEMA _analytics OWNER TO supabase_admin;
REVOKE ALL ON SCHEMA _analytics FROM PUBLIC;
