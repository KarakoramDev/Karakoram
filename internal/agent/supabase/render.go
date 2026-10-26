// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/gateway"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

type layout struct {
	binary, dir, release, migrate string
}

// All archive-layout knowledge lives here; source-built launchers exec their
// bundled runtime directly and preserve foreground ownership and signals.
func artifactLayout(s sbspec.Spec) layout {
	root := s.InstallDir
	switch s.Component {
	case "gateway":
		return layout{binary: filepath.Join(root, "supabase-gateway"), dir: root}
	case "auth":
		return layout{binary: filepath.Join(root, "auth"), dir: root}
	case "rest":
		return layout{binary: filepath.Join(root, "postgrest"), dir: root}
	case "vector":
		return layout{binary: filepath.Join(root, "vector-x86_64-unknown-linux-gnu/bin/vector"), dir: root}
	case "realtime", "pooler", "analytics":
		app := map[string]string{"realtime": "realtime", "pooler": "supavisor", "analytics": "logflare"}[s.Component]
		return layout{binary: filepath.Join(root, "bin/start"), dir: filepath.Join(root, "app"), release: filepath.Join(root, "app/bin", app), migrate: filepath.Join(root, "app/bin/migrate")}
	case "functions":
		return layout{binary: filepath.Join(root, "bin/edge-runtime"), dir: root}
	default:
		return layout{binary: filepath.Join(root, "bin/start"), dir: root}
	}
}

// Render returns a native executable and owner-only file contents. It performs
// no I/O and is shared by the production runner and integration harness.
func Render(s sbspec.Spec, dataDir string) (agent.ProcessSpec, map[string][]byte, error) {
	l := artifactLayout(s)
	p := agent.ProcessSpec{Binary: l.binary, Dir: l.dir}
	files := map[string][]byte{}
	env := map[string]string{"NODE_ENV": "production", "LANG": "C.UTF-8"}
	set := func(values map[string]string) {
		for k, v := range values {
			env[k] = v
		}
	}
	boolstr := strconv.FormatBool
	num := strconv.Itoa
	dir := filepath.Join(dataDir, "karakoram")
	env["HOME"] = dir
	dbhost := "127.0.0.1"
	dbport := "5432"
	// Supavisor's wildcard session listener owns port 5432; its database
	// forwarder uses a separate private loopback port to avoid that collision.
	if s.Component == "pooler" {
		dbport = "15432"
	}
	databaseQuery := func() string {
		// Native BEAM bundles consume KARAKORAM_DB_CA_FILE; URL SSL options
		// must not override their explicit verify-peer/cacertfile configuration.
		if s.Component == "pooler" || s.Component == "analytics" {
			return ""
		}
		q := url.Values{"sslmode": {"disable"}}
		if s.DatabaseTLS {
			q.Set("sslmode", "verify-ca")
			q.Set("sslrootcert", filepath.Join(dir, "supabase/ca.pem"))
		}
		return q.Encode()
	}
	roleURL := func(scheme, role, database string) string {
		u := url.URL{Scheme: scheme, User: url.UserPassword(role, s.DBRoles[role]), Host: dbhost + ":" + dbport, Path: "/" + database, RawQuery: databaseQuery()}
		return u.String()
	}
	jwt := s.JWTSecret
	if s.JWKS != "" {
		jwt = s.JWKS
	}
	schemas := s.REST.Schemas
	if len(schemas) == 0 {
		schemas = []string{"public", "graphql_public"}
	}
	maxrows := s.REST.MaxRows
	if maxrows == 0 {
		maxrows = 1000
	}
	expiry := s.Auth.JWTExpiry
	if expiry == 0 {
		expiry = 3600
	}
	upstream := func(name string) string {
		if address := s.Upstreams[name]; address != "" {
			return "http://" + address
		}
		return ""
	}
	pgEnv := map[string]string{"POSTGRES_HOST": dbhost, "POSTGRES_PORT": dbport, "POSTGRES_DB": s.Database}
	// Native listeners must accept the backend's private address family.
	// IPv6 wildcard listeners also retain IPv4 loopback health probes.
	serviceHost := "0.0.0.0"
	if s.DatabaseIPv6 {
		serviceHost = "::"
	}
	switch s.Component {
	case "gateway":
		opaque := map[string]gateway.OpaqueKey{}
		if s.PublishableKey != "" {
			opaque[s.PublishableKey] = gateway.OpaqueKey{JWT: s.AnonAsymmetricJWT}
		}
		if s.SecretKey != "" {
			opaque[s.SecretKey] = gateway.OpaqueKey{JWT: s.ServiceAsymmetricJWT, Admin: true}
		}
		// Only routable enabled components are part of Envoy's upstream vocabulary.
		routes := map[string]string{}
		for _, name := range sbspec.GatewayUpstreams {
			if address := s.Upstreams[name]; address != "" {
				routes[name] = address
			}
		}
		config, err := gateway.Render(gateway.Input{ListenAddress: serviceHost, Upstreams: routes, AnonKey: s.AnonKey, ServiceRoleKey: s.ServiceRoleKey, OpaqueKeys: opaque, DashboardUsername: s.Studio.DashboardUsername, DashboardPassword: s.Studio.DashboardPassword, PublicURL: s.APIExternalURL, GraphQL: s.GraphQL, CORS: gateway.CORS{Origins: s.CORSOrigins}})
		if err != nil {
			return p, nil, err
		}
		files["supabase/envoy.json"] = config
		p.Args = []string{"-c", filepath.Join(dir, "supabase/envoy.json"), "--concurrency", "1", "--log-level", "warning"}
	case "auth":
		set(map[string]string{"GOTRUE_API_HOST": "0.0.0.0", "GOTRUE_API_PORT": "9999", "API_EXTERNAL_URL": s.APIExternalURL, "GOTRUE_DB_DRIVER": "postgres", "GOTRUE_DB_DATABASE_URL": roleURL("postgres", "supabase_auth_admin", s.Database), "GOTRUE_DB_MIGRATIONS_PATH": filepath.Join(s.InstallDir, "migrations"), "GOTRUE_SITE_URL": s.SiteURL, "GOTRUE_URI_ALLOW_LIST": strings.Join(s.Auth.AdditionalRedirectURLs, ","), "GOTRUE_DISABLE_SIGNUP": boolstr(s.Auth.DisableSignup), "GOTRUE_JWT_ADMIN_ROLES": "service_role", "GOTRUE_JWT_AUD": "authenticated", "GOTRUE_JWT_DEFAULT_GROUP_NAME": "authenticated", "GOTRUE_JWT_EXP": num(expiry), "GOTRUE_JWT_SECRET": s.JWTSecret, "GOTRUE_JWT_ISSUER": s.APIExternalURL, "GOTRUE_EXTERNAL_EMAIL_ENABLED": "true", "GOTRUE_EXTERNAL_ANONYMOUS_USERS_ENABLED": "false", "GOTRUE_MAILER_AUTOCONFIRM": boolstr(s.Auth.Autoconfirm), "GOTRUE_SMTP_ADMIN_EMAIL": s.Auth.SMTP.AdminEmail, "GOTRUE_SMTP_HOST": s.Auth.SMTP.Host, "GOTRUE_SMTP_PORT": num(s.Auth.SMTP.Port), "GOTRUE_SMTP_USER": s.Auth.SMTP.User, "GOTRUE_SMTP_PASS": s.Auth.SMTP.Password, "GOTRUE_SMTP_SENDER_NAME": s.Auth.SMTP.SenderName, "GOTRUE_EXTERNAL_PHONE_ENABLED": "false", "GOTRUE_SMS_AUTOCONFIRM": "false"})
		for _, name := range []string{"INVITE", "CONFIRMATION", "RECOVERY", "EMAIL_CHANGE"} {
			env["GOTRUE_MAILER_URLPATHS_"+name] = "/auth/v1/verify"
		}
		if s.JWTKeys != "" {
			env["GOTRUE_JWT_KEYS"] = s.JWTKeys
		}
	case "rest":
		set(map[string]string{"PGRST_SERVER_HOST": serviceHost, "PGRST_SERVER_PORT": "3000", "PGRST_DB_URI": roleURL("postgres", "authenticator", s.Database), "PGRST_DB_SCHEMAS": strings.Join(schemas, ","), "PGRST_DB_MAX_ROWS": num(maxrows), "PGRST_DB_EXTRA_SEARCH_PATH": "public,extensions", "PGRST_DB_ANON_ROLE": "anon", "PGRST_ADMIN_SERVER_PORT": "3001", "PGRST_ADMIN_SERVER_HOST": "127.0.0.1", "PGRST_JWT_SECRET": jwt, "PGRST_DB_USE_LEGACY_GUCS": "false", "PGRST_APP_SETTINGS_JWT_EXP": num(expiry)})
	case "realtime":
		set(map[string]string{"PORT": "4000", "DB_HOST": dbhost, "DB_PORT": dbport, "DB_USER": "supabase_admin", "DB_PASSWORD": s.DBRoles["supabase_admin"], "DB_NAME": s.Database, "DB_AFTER_CONNECT_QUERY": "SET search_path TO _realtime", "DB_ENC_KEY": s.RealtimeDBEncKey, "API_JWT_SECRET": s.JWTSecret, "SECRET_KEY_BASE": s.SecretKeyBase, "METRICS_JWT_SECRET": s.JWTSecret, "ERL_AFLAGS": "-proto_dist inet_tcp +S 2:2 +SDcpu 1 +SDio 1", "RLIMIT_NOFILE": "10000", "APP_NAME": "realtime", "SEED_SELF_HOST": "true", "RUN_JANITOR": "true", "DISABLE_HEALTHCHECK_LOGGING": "true"})
		// Only singleton Realtime is admitted: the native release has no
		// configured inter-member distribution network. Its default POSTGRES
		// discovery adds a linked DB failure path without connecting peers.
		// An explicit empty value makes the pinned runtime build no topologies.
		env["CLUSTER_STRATEGIES"] = ""
		// Realtime's internal RPC listener is bound to loopback and must never be reachable.
		env["GEN_RPC_SOCKET_IP"] = "127.0.0.1"
		env["API_JWT_SECRET"] = s.RealtimeAdminSecret
		env["DB_SSL"] = boolstr(s.DatabaseTLS)
		if s.DatabaseTLS {
			env["DB_SSL_CA_CERT"] = filepath.Join(dir, "supabase/ca.pem")
		}
		if s.JWKS != "" {
			env["API_JWT_JWKS"] = s.JWKS
		}
	case "storage":
		size := s.Storage.FileSizeLimit
		if size == 0 {
			size = 52428800
		}
		protocol := "http"
		if s.Storage.TLS {
			protocol = "https"
		}
		set(map[string]string{"HOST": serviceHost, "PORT": "5000", "ANON_KEY": s.AnonKey, "SERVICE_KEY": s.ServiceRoleKey, "POSTGREST_URL": upstream("rest"), "AUTH_JWT_SECRET": s.JWTSecret, "DATABASE_URL": roleURL("postgres", "supabase_storage_admin", s.Database), "STORAGE_PUBLIC_URL": s.APIExternalURL, "REQUEST_ALLOW_X_FORWARDED_PATH": "true", "FILE_SIZE_LIMIT": strconv.FormatInt(size, 10), "STORAGE_BACKEND": "s3", "GLOBAL_S3_BUCKET": s.Storage.Bucket, "GLOBAL_S3_ENDPOINT": s.Storage.Endpoint, "GLOBAL_S3_PROTOCOL": protocol, "GLOBAL_S3_FORCE_PATH_STYLE": boolstr(s.Storage.PathStyle), "AWS_ACCESS_KEY_ID": s.Storage.AccessKey, "AWS_SECRET_ACCESS_KEY": s.Storage.SecretKey, "TENANT_ID": s.Storage.TenantID, "REGION": s.Storage.Region, "ENABLE_IMAGE_TRANSFORMATION": boolstr(s.Storage.ImageTransformation), "IMGPROXY_URL": upstream("imgproxy"), "S3_PROTOCOL_ACCESS_KEY_ID": s.Storage.ProtocolAccessKey, "S3_PROTOCOL_ACCESS_KEY_SECRET": s.Storage.ProtocolSecretKey})
		if s.JWKS != "" {
			env["JWT_JWKS"] = s.JWKS
		}
	case "imgproxy":
		set(map[string]string{"IMGPROXY_BIND": ":5001", "IMGPROXY_USE_ETAG": "true", "IMGPROXY_AUTO_WEBP": "true", "IMGPROXY_MAX_SRC_RESOLUTION": "16.8"})
	case "meta":
		set(map[string]string{"PG_META_HOST": "127.0.0.1", "PG_META_PORT": "8081", "PG_META_DB_HOST": dbhost, "PG_META_DB_PORT": dbport, "PG_META_DB_NAME": s.Database, "PG_META_DB_USER": "postgres", "PG_META_DB_PASSWORD": s.DBRoles["postgres"], "CRYPTO_KEY": s.Studio.MetaCryptoKey})
		env["PG_META_DB_URL"] = roleURL("postgresql", "postgres", s.Database)
	case "studio":
		set(pgEnv)
		// The pinned Studio builds and encrypts metadata connection URLs by
		// appending POSTGRES_DB verbatim; it has no separate TLS option.
		database := url.URL{Path: s.Database, RawQuery: databaseQuery()}
		env["POSTGRES_DB"] = database.String()
		set(map[string]string{"HOSTNAME": serviceHost, "PORT": "3000", "STUDIO_PG_META_URL": upstream("meta"), "POSTGRES_PASSWORD": s.DBRoles["postgres"], "POSTGRES_USER_READ_WRITE": "postgres", "PG_META_CRYPTO_KEY": s.Studio.MetaCryptoKey, "PGRST_DB_SCHEMAS": strings.Join(schemas, ","), "PGRST_DB_MAX_ROWS": num(maxrows), "PGRST_DB_EXTRA_SEARCH_PATH": "public", "DEFAULT_ORGANIZATION_NAME": s.Studio.Organization, "DEFAULT_PROJECT_NAME": s.Studio.Project, "SUPABASE_URL": s.GatewayURL, "SUPABASE_PUBLIC_URL": s.APIExternalURL, "SUPABASE_ANON_KEY": s.AnonKey, "SUPABASE_SERVICE_KEY": s.ServiceRoleKey, "AUTH_JWT_SECRET": s.JWTSecret, "SUPABASE_PUBLISHABLE_KEY": s.PublishableKey, "SUPABASE_SECRET_KEY": s.SecretKey, "ENABLED_FEATURES_LOGS_ALL": boolstr(s.Upstreams["analytics"] != ""), "SNIPPETS_MANAGEMENT_FOLDER": filepath.Join(dir, "snippets"), "EDGE_FUNCTIONS_MANAGEMENT_FOLDER": filepath.Join(dir, "functions")})
		env["HOSTNAME"] = "127.0.0.1"
		// The loopback bridge adds the service credential on Studio's private hop.
		env["STUDIO_PG_META_URL"] = "http://" + studioMetaBridgeAddress
		if s.Upstreams["analytics"] != "" {
			env["LOGFLARE_URL"] = upstream("analytics")
			env["LOGFLARE_PRIVATE_ACCESS_TOKEN"] = s.LogflarePrivateToken
		}
	case "functions":
		publishable, _ := json.Marshal(map[string]string{"default": s.PublishableKey})
		secret, _ := json.Marshal(map[string]string{"default": s.SecretKey})
		set(map[string]string{"JWT_SECRET": s.JWTSecret, "SUPABASE_URL": s.GatewayURL, "SUPABASE_PUBLIC_URL": s.APIExternalURL, "SUPABASE_ANON_KEY": s.AnonKey, "SUPABASE_SERVICE_ROLE_KEY": s.ServiceRoleKey, "SUPABASE_PUBLISHABLE_KEYS": string(publishable), "SUPABASE_SECRET_KEYS": string(secret), "SUPABASE_DB_URL": roleURL("postgresql", "postgres", s.Database), "VERIFY_JWT": boolstr(s.VerifyJWT), "FUNCTIONS_ROOT": filepath.Join(dir, "functions"), "DENO_DIR": filepath.Join(dir, "deno-cache")})
		if s.JWKS != "" {
			env["SUPABASE_JWKS"] = s.JWKS
		}
		p.Args = []string{"start", "--user-worker-request-idle-timeout", "150000", "--main-service", filepath.Join(s.InstallDir, "app/main")}
		files["functions/deno.jsonc"] = []byte("{\"imports\":{}}\n")
		for name, entries := range s.Functions {
			for file, source := range entries {
				files[filepath.Join("functions", name, file)] = []byte(source)
			}
		}
	case "pooler":
		pool := s.Pooler.DefaultPoolSize
		if pool == 0 {
			pool = 20
		}
		max := s.Pooler.MaxClientConnections
		if max == 0 {
			max = 100
		}
		dbpool := s.Pooler.DBPoolSize
		if dbpool == 0 {
			dbpool = 5
		}
		set(pgEnv)
		set(map[string]string{"PORT": "4000", "POSTGRES_PASSWORD": s.DBRoles["pgbouncer"], "DATABASE_URL": roleURL("ecto", "supabase_admin", "_supabase"), "CLUSTER_POSTGRES": "true", "SECRET_KEY_BASE": s.SecretKeyBase, "VAULT_ENC_KEY": s.VaultEncKey, "API_JWT_SECRET": s.JWTSecret, "METRICS_JWT_SECRET": s.JWTSecret, "REGION": "local", "ERL_AFLAGS": "-proto_dist inet_tcp +S 2:2 +SDcpu 1 +SDio 1", "POOLER_TENANT_ID": s.Pooler.TenantID, "POOLER_DEFAULT_POOL_SIZE": num(pool), "POOLER_MAX_CLIENT_CONN": num(max), "POOLER_POOL_MODE": "transaction", "DB_POOL_SIZE": num(dbpool)})
		env["NODE_IP"] = "127.0.0.1"
		files["supabase/pooler.exs"] = []byte(poolerSeed)
		if s.DatabaseTLS {
			env["KARAKORAM_DB_CA_FILE"] = filepath.Join(dir, "supabase/ca.pem")
			env["GLOBAL_UPSTREAM_CA_PATH"] = filepath.Join(dir, "supabase/ca.pem")
		}
	case "analytics":
		set(map[string]string{"PORT": "4000", "LOGFLARE_NODE_HOST": "127.0.0.1", "DB_USERNAME": "supabase_admin", "DB_DATABASE": "_supabase", "DB_HOSTNAME": dbhost, "DB_PORT": dbport, "DB_PASSWORD": s.DBRoles["supabase_admin"], "DB_SCHEMA": "_analytics", "LOGFLARE_SINGLE_TENANT": "true", "LOGFLARE_SUPABASE_MODE": "true", "LOGFLARE_PUBLIC_ACCESS_TOKEN": s.LogflarePublicToken, "LOGFLARE_PRIVATE_ACCESS_TOKEN": s.LogflarePrivateToken, "LOGFLARE_FEATURE_FLAG_OVERRIDE": "multibackend=true", "POSTGRES_BACKEND_URL": roleURL("postgresql", "supabase_admin", "_supabase"), "POSTGRES_BACKEND_SCHEMA": "_analytics", "ERL_AFLAGS": "-proto_dist inet_tcp +S 2:2 +SDcpu 1 +SDio 1"})
		env["DB_URL"] = roleURL("ecto", "supabase_admin", "_supabase")
		if s.DatabaseTLS {
			env["KARAKORAM_DB_CA_FILE"] = filepath.Join(dir, "supabase/ca.pem")
		}
	case "vector":
		if s.Upstreams["analytics"] == "" || s.LogflarePublicToken == "" {
			return p, nil, errors.New("vector requires analytics upstream and ingestion token")
		}
		config, err := renderVector(s, dir)
		if err != nil {
			return p, nil, err
		}
		files["supabase/vector.json"] = config
		p.Args = []string{"--config", filepath.Join(dir, "supabase/vector.json")}
	}
	switch s.Component {
	case "realtime", "pooler", "analytics":
		// The pinned releases force distribution on in env.sh. Keep their
		// local runtime semantics, but never trust the public artifact cookie
		// or expose distribution/epmd on the member's private network.
		// All three already receive this random, sealed per-cluster password.
		password := s.DBRoles["supabase_admin"]
		if password == "" {
			return p, nil, errors.New("missing BEAM distribution secret material")
		}
		cookie := hmac.New(sha256.New, []byte(password))
		cookie.Write([]byte("karakoram/erlang-cookie/v1\x00" + s.Component))
		env["RELEASE_COOKIE"] = hex.EncodeToString(cookie.Sum(nil))
		env["ERL_AFLAGS"] += " -kernel inet_dist_use_interface {127,0,0,1}"
		env["ERL_EPMD_ADDRESS"] = "127.0.0.1"
	}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if strings.ContainsRune(env[key], 0) {
			return p, nil, errors.New("invalid component environment")
		}
		p.Env = append(p.Env, key+"="+env[key])
	}
	// The distribution credential belongs only in the child environment,
	// never in the diagnostic environment dump.
	delete(env, "RELEASE_COOKIE")
	encoded, _ := json.Marshal(env)
	files["supabase/environment.json"] = encoded
	return p, files, nil
}

const poolerSeed = `{:ok, _} = Application.ensure_all_started(:supavisor)
{:ok, version} = case Supavisor.Repo.query!("select version()") do
  %{rows: [[ver]]} -> Supavisor.Helpers.parse_pg_version(ver)
end
params = %{
 "external_id" => System.fetch_env!("POOLER_TENANT_ID"),
 "db_host" => System.fetch_env!("POSTGRES_HOST"),
 "db_port" => System.fetch_env!("POSTGRES_PORT"),
 "db_database" => System.fetch_env!("POSTGRES_DB"),
 "require_user" => false,
 "upstream_ssl" => not is_nil(System.get_env("KARAKORAM_DB_CA_FILE")),
 "upstream_verify" => if(System.get_env("KARAKORAM_DB_CA_FILE"), do: "peer", else: "none"),
 "upstream_tls_ca" => case System.get_env("KARAKORAM_DB_CA_FILE") do
   nil -> nil
   path ->
     {:ok, der} = Supavisor.Helpers.cert_to_bin(File.read!(path))
     der
 end,
 "auth_query" => "SELECT * FROM pgbouncer.get_auth($1)",
 "default_max_clients" => System.fetch_env!("POOLER_MAX_CLIENT_CONN"),
 "default_pool_size" => System.fetch_env!("POOLER_DEFAULT_POOL_SIZE"),
 "default_parameter_status" => %{"server_version" => version},
 "users" => [%{"db_user" => "pgbouncer", "db_password" => System.fetch_env!("POSTGRES_PASSWORD"), "mode_type" => "transaction", "pool_size" => System.fetch_env!("POOLER_DEFAULT_POOL_SIZE"), "is_manager" => true}]
}
case Supavisor.Tenants.get_tenant_by_external_id(params["external_id"]) do
 nil -> {:ok, _} = Supavisor.Tenants.create_tenant(params)
 tenant -> {:ok, _} = Supavisor.Tenants.update_tenant(tenant, params)
end
`
