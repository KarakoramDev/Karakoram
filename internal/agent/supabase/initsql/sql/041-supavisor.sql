CREATE SCHEMA IF NOT EXISTS _supavisor AUTHORIZATION supabase_admin;
ALTER SCHEMA _supavisor OWNER TO supabase_admin;
REVOKE ALL ON SCHEMA _supavisor FROM PUBLIC;
