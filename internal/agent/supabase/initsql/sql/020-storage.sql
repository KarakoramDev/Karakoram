DO $bootstrap$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'supabase_storage_admin') THEN
    CREATE ROLE supabase_storage_admin;
  END IF;
END $bootstrap$;
ALTER ROLE supabase_storage_admin LOGIN NOINHERIT NOSUPERUSER NOCREATEDB CREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER ROLE supabase_storage_admin SET search_path = storage, extensions;
DO $bootstrap$ BEGIN
  EXECUTE format('GRANT CREATE ON DATABASE %I TO supabase_storage_admin', current_database());
END $bootstrap$;
GRANT anon, authenticated, service_role TO supabase_storage_admin;
CREATE SCHEMA IF NOT EXISTS storage AUTHORIZATION supabase_storage_admin;
ALTER SCHEMA storage OWNER TO supabase_storage_admin;
REVOKE ALL ON SCHEMA storage FROM PUBLIC;
GRANT USAGE ON SCHEMA storage TO anon, authenticated, service_role;
GRANT USAGE ON SCHEMA auth, extensions TO supabase_storage_admin;
GRANT EXECUTE ON FUNCTION auth.uid(), auth.role(), auth.email(), auth.jwt() TO supabase_storage_admin;
GRANT REFERENCES ON ALL TABLES IN SCHEMA auth TO supabase_storage_admin;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_auth_admin IN SCHEMA auth GRANT REFERENCES ON TABLES TO supabase_storage_admin;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_storage_admin IN SCHEMA storage
  GRANT SELECT, INSERT, UPDATE, DELETE, REFERENCES ON TABLES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_storage_admin IN SCHEMA storage GRANT USAGE, SELECT ON SEQUENCES TO anon, authenticated, service_role;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_storage_admin IN SCHEMA storage REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC;
ALTER DEFAULT PRIVILEGES FOR ROLE supabase_storage_admin IN SCHEMA storage GRANT EXECUTE ON FUNCTIONS TO anon, authenticated, service_role;
