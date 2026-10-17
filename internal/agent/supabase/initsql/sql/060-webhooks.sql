-- Port of the pinned upstream webhook overlay. Enabled only with pg_net.
DO $bootstrap$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'supabase_functions_admin') THEN
    CREATE ROLE supabase_functions_admin;
  END IF;
END $bootstrap$;
ALTER ROLE supabase_functions_admin LOGIN NOINHERIT NOSUPERUSER NOCREATEDB CREATEROLE NOREPLICATION NOBYPASSRLS;
ALTER ROLE supabase_functions_admin SET search_path = supabase_functions;
CREATE SCHEMA IF NOT EXISTS supabase_functions AUTHORIZATION supabase_functions_admin;
REVOKE ALL ON SCHEMA supabase_functions FROM PUBLIC;
GRANT USAGE ON SCHEMA supabase_functions TO anon, authenticated, service_role;
GRANT USAGE ON SCHEMA extensions TO supabase_functions_admin;
CREATE TABLE IF NOT EXISTS supabase_functions.migrations (
  version text PRIMARY KEY, inserted_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS supabase_functions.hooks (
  id bigserial PRIMARY KEY, hook_table_id integer NOT NULL, hook_name text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(), request_id bigint);
CREATE INDEX IF NOT EXISTS supabase_functions_hooks_request_id_idx ON supabase_functions.hooks(request_id);
CREATE INDEX IF NOT EXISTS supabase_functions_hooks_h_table_id_h_name_idx ON supabase_functions.hooks(hook_table_id, hook_name);
ALTER TABLE supabase_functions.migrations OWNER TO supabase_functions_admin;
ALTER TABLE supabase_functions.hooks OWNER TO supabase_functions_admin;
CREATE OR REPLACE FUNCTION supabase_functions.http_request()
RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER SET search_path = supabase_functions AS $function$
DECLARE
  request_id bigint;
  payload jsonb;
  url text := TG_ARGV[0];
  method text := TG_ARGV[1];
  headers jsonb := coalesce(nullif(TG_ARGV[2], 'null')::jsonb, '{"Content-Type":"application/json"}'::jsonb);
  params jsonb := coalesce(nullif(TG_ARGV[3], 'null')::jsonb, '{}'::jsonb);
  timeout_ms integer := coalesce(nullif(TG_ARGV[4], 'null')::integer, 1000);
BEGIN
  IF url IS NULL OR url = 'null' THEN RAISE EXCEPTION 'url argument is missing'; END IF;
  IF method IS NULL OR method = 'null' THEN RAISE EXCEPTION 'method argument is missing'; END IF;
  CASE method
    WHEN 'GET' THEN
      SELECT net.http_get(url, params, headers, timeout_ms) INTO request_id;
    WHEN 'POST' THEN
      payload := jsonb_build_object('old_record', OLD, 'record', NEW,
        'type', TG_OP, 'table', TG_TABLE_NAME, 'schema', TG_TABLE_SCHEMA);
      SELECT net.http_post(url, payload, params, headers, timeout_ms) INTO request_id;
    ELSE RAISE EXCEPTION 'method argument % is invalid', method;
  END CASE;
  INSERT INTO supabase_functions.hooks(hook_table_id, hook_name, request_id)
    VALUES (TG_RELID, TG_NAME, request_id);
  RETURN NEW;
END $function$;
ALTER FUNCTION supabase_functions.http_request() OWNER TO supabase_functions_admin;
REVOKE ALL ON FUNCTION supabase_functions.http_request() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION supabase_functions.http_request() TO anon, authenticated, service_role;
GRANT SELECT ON supabase_functions.hooks TO service_role;
-- Shared grant implementation is superuser-owned and unavailable to API roles.
-- An extension recreation event restores precisely the same pg_net permissions.
CREATE OR REPLACE FUNCTION extensions.configure_pg_net_access()
RETURNS void LANGUAGE plpgsql SET search_path = pg_catalog AS $function$
BEGIN
  IF EXISTS (SELECT FROM pg_extension WHERE extname = 'pg_net') THEN
    GRANT USAGE ON SCHEMA net TO supabase_functions_admin, anon, authenticated, service_role;
    ALTER FUNCTION net.http_get(text, jsonb, jsonb, integer) SECURITY DEFINER;
    ALTER FUNCTION net.http_post(text, jsonb, jsonb, jsonb, integer) SECURITY DEFINER;
    ALTER FUNCTION net.http_get(text, jsonb, jsonb, integer) SET search_path = net;
    ALTER FUNCTION net.http_post(text, jsonb, jsonb, jsonb, integer) SET search_path = net;
    REVOKE ALL ON FUNCTION net.http_get(text, jsonb, jsonb, integer), net.http_post(text, jsonb, jsonb, jsonb, integer) FROM PUBLIC;
    GRANT EXECUTE ON FUNCTION net.http_get(text, jsonb, jsonb, integer), net.http_post(text, jsonb, jsonb, jsonb, integer)
      TO supabase_functions_admin, anon, authenticated, service_role;
  END IF;
END $function$;
REVOKE ALL ON FUNCTION extensions.configure_pg_net_access() FROM PUBLIC;
SELECT extensions.configure_pg_net_access();
CREATE OR REPLACE FUNCTION extensions.grant_pg_net_access()
RETURNS event_trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $function$
BEGIN
  IF EXISTS (SELECT FROM pg_event_trigger_ddl_commands() AS command
    JOIN pg_extension AS extension ON extension.oid = command.objid
    WHERE extension.extname = 'pg_net') THEN
    PERFORM extensions.configure_pg_net_access();
  END IF;
END $function$;
REVOKE ALL ON FUNCTION extensions.grant_pg_net_access() FROM PUBLIC;
DO $bootstrap$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_event_trigger WHERE evtname = 'issue_pg_net_access') THEN
    CREATE EVENT TRIGGER issue_pg_net_access ON ddl_command_end WHEN TAG IN ('CREATE EXTENSION')
      EXECUTE FUNCTION extensions.grant_pg_net_access();
  END IF;
END $bootstrap$;
INSERT INTO supabase_functions.migrations(version) VALUES ('initial'), ('20210809183423_update_grants') ON CONFLICT DO NOTHING;
