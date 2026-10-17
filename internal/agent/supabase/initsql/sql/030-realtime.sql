DO $bootstrap$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'supabase_replication_admin') THEN
    CREATE ROLE supabase_replication_admin;
  END IF;
END $bootstrap$;
ALTER ROLE supabase_replication_admin LOGIN REPLICATION NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
CREATE SCHEMA IF NOT EXISTS realtime AUTHORIZATION supabase_admin;
CREATE SCHEMA IF NOT EXISTS _realtime AUTHORIZATION supabase_admin;
REVOKE ALL ON SCHEMA realtime, _realtime FROM PUBLIC;
GRANT USAGE ON SCHEMA realtime TO anon, authenticated, service_role;
DO $bootstrap$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'supabase_realtime') THEN
    CREATE PUBLICATION supabase_realtime;
  END IF;
END $bootstrap$;
-- Tables are added explicitly by their owners; never publish all tenant data.
