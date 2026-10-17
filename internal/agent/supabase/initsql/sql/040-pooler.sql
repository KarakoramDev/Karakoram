DO $bootstrap$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pgbouncer') THEN
    CREATE ROLE pgbouncer;
  END IF;
END $bootstrap$;
ALTER ROLE pgbouncer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE SCHEMA IF NOT EXISTS pgbouncer AUTHORIZATION supabase_admin;
REVOKE ALL ON SCHEMA pgbouncer FROM PUBLIC;
GRANT USAGE ON SCHEMA pgbouncer TO pgbouncer;
CREATE OR REPLACE FUNCTION pgbouncer.get_auth(p_usename text)
RETURNS TABLE(username text, password text)
LANGUAGE sql SECURITY DEFINER SET search_path = '' AS $$
  SELECT rolname::text, CASE WHEN rolvaliduntil < now() THEN NULL ELSE rolpassword::text END
  FROM pg_catalog.pg_authid WHERE rolname = p_usename AND rolcanlogin;
$$;
ALTER FUNCTION pgbouncer.get_auth(text) OWNER TO supabase_admin;
REVOKE ALL ON FUNCTION pgbouncer.get_auth(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION pgbouncer.get_auth(text) TO pgbouncer;
