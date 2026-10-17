-- Curated from Supabase PostgreSQL 17 bootstrap; application tables remain service-owned.
DO $bootstrap$
DECLARE item text;
BEGIN
  FOREACH item IN ARRAY ARRAY['supabase_admin','anon','authenticated','service_role','authenticator','supabase_auth_admin','dashboard_user','supabase_read_only_user'] LOOP
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = item) THEN
      EXECUTE format('CREATE ROLE %I', item);
    END IF;
  END LOOP;
END
$bootstrap$;
ALTER ROLE supabase_admin LOGIN SUPERUSER CREATEDB CREATEROLE REPLICATION BYPASSRLS;
ALTER ROLE supabase_admin SET search_path = public, extensions;
ALTER ROLE anon NOLOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER ROLE authenticated NOLOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER ROLE service_role NOLOGIN INHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION BYPASSRLS;
ALTER ROLE authenticator LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER ROLE supabase_auth_admin LOGIN NOINHERIT NOSUPERUSER NOCREATEDB CREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER ROLE dashboard_user NOLOGIN NOSUPERUSER CREATEDB CREATEROLE REPLICATION NOBYPASSRLS;
ALTER ROLE supabase_read_only_user LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION BYPASSRLS;
ALTER ROLE supabase_read_only_user SET default_transaction_read_only = on;
GRANT pg_read_all_data TO supabase_read_only_user;
GRANT anon, authenticated, service_role TO authenticator;
REVOKE supabase_admin FROM authenticator;
ALTER ROLE anon SET statement_timeout = '3s';
ALTER ROLE authenticated SET statement_timeout = '8s';
CREATE SCHEMA IF NOT EXISTS extensions AUTHORIZATION supabase_admin;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA extensions FROM PUBLIC;
GRANT USAGE ON SCHEMA public, extensions TO anon, authenticated, service_role;
-- API DML is deliberate; callers must enable RLS and install policies on exposed tables.
-- TRUNCATE is not granted to client roles because it bypasses row policies.
ALTER DEFAULT PRIVILEGES FOR ROLE postgres, supabase_admin IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE, REFERENCES ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres, supabase_admin IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres, supabase_admin IN SCHEMA public REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE postgres, supabase_admin IN SCHEMA public GRANT EXECUTE ON FUNCTIONS TO anon, authenticated, service_role;
