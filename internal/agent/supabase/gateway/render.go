// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package gateway renders a self-contained Envoy 1.39 bootstrap. The result
// contains credentials and must be written atomically with mode 0600. No access
// logger is installed; the admin listener is loopback-only for host discovery.
// Routing and translation follow Supabase's pinned upstream Envoy configuration,
// with explicit disabled-route denies, header-over-query precedence, preserved
// Storage SigV4, and unlimited websocket duration.
//
// The compatibility reference is supabase/supabase commit
// bc27d2661edd09a5d0b8d76ce33da6d4071a756e, volumes/api/envoy templates.
// Intentional hardenings relative to those templates are documented above.
// The caller terminates TLS before the listener and provides the trusted public
// origin; incoming forwarding headers never select that origin.
//
// Construct Input from the enabled component discovery addresses and credentials
// retrieved from SecretStore, then Render and hand the private bootstrap to the
// native Envoy process. GraphQL must remain false until its SQL wrapper exists.
package gateway

import (
	"crypto/sha1"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

type object = map[string]any

// AdminPort is local to the gateway member and must never be exposed by infra.
const AdminPort = 8001

// OpaqueKey maps one configured opaque credential to its pre-signed internal
// ES256 role JWT. Admin grants REST OpenAPI root and postgres-meta access.
type OpaqueKey struct {
	JWT   string
	Admin bool
}

// CORS defines exact allowed origins (or a single '*'), allowed methods and
// headers, exposed response headers and credential support. Empty Origins allows
// any origin, matching upstream. Wildcard origins cannot use credentials.
type CORS struct {
	Origins          []string
	Methods          []string
	Headers          []string
	ExposeHeaders    []string
	AllowCredentials bool
}

// Input is the private gateway configuration. Upstreams enables components by
// name (auth, rest, storage, realtime, meta, studio, functions), each with a
// private host:port, including bracketed IPv6. Omitted components return 404 on
// their reserved routes even when Studio is enabled. GraphQL additionally needs
// a deployed pg_graphql wrapper. PublicURL is the external HTTP(S) base URL;
// upstream forwarded headers are derived here, never trusted from clients.
type Input struct {
	ListenAddress     string
	ListenPort        uint16
	Upstreams         map[string]string
	AnonKey           string
	ServiceRoleKey    string
	OpaqueKeys        map[string]OpaqueKey
	DashboardUsername string
	DashboardPassword string
	PublicURL         string
	CORS              CORS
	GraphQL           bool
	RealtimeHost      string
}

//go:embed translate.lua
var translation string

// Render returns deterministic, indented JSON accepted as an Envoy bootstrap.
// It performs no filesystem, network, clock, or random operations.
func Render(in Input) ([]byte, error) {
	if in.AnonKey == "" || in.ServiceRoleKey == "" || in.AnonKey == in.ServiceRoleKey {
		return nil, errors.New("distinct legacy anon and service keys are required")
	}
	u, err := url.Parse(in.PublicURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, errors.New("public URL must be an HTTP(S) origin")
	}
	if in.ListenAddress == "" {
		in.ListenAddress = "0.0.0.0"
	}
	if net.ParseIP(in.ListenAddress) == nil {
		return nil, errors.New("listen address must be an IP address")
	}
	if in.ListenPort == 0 {
		in.ListenPort = 8000
	}
	if in.RealtimeHost == "" {
		in.RealtimeHost = "realtime-dev.supabase-realtime"
	}
	if strings.ContainsAny(in.RealtimeHost, "\r\n\t /:") {
		return nil, errors.New("invalid realtime tenant host")
	}
	names := make([]string, 0, len(in.Upstreams))
	for name := range in.Upstreams {
		if !slices.Contains(sbspec.GatewayUpstreams, name) {
			return nil, fmt.Errorf("unknown gateway component %q", name)
		}
		names = append(names, name)
	}
	sort.Strings(names)
	var clusters []any
	for _, name := range names {
		host, port, err := net.SplitHostPort(in.Upstreams[name])
		if err != nil || host == "" {
			return nil, fmt.Errorf("invalid %s upstream address", name)
		}
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 {
			return nil, fmt.Errorf("invalid %s upstream port", name)
		}
		// Bound negative/empty-answer convergence without trusting potentially long
		// discovery TTLs. A short failure backoff also avoids five-second NXDOMAIN
		// stalls while sequentially created services acquire discovery records.
		// Only transport failures eject hosts; an application's HTTP 5xx must
		// remain visible. At most half the pool is ejected, so the default 50%
		// panic threshold is not crossed and a singleton is never ejected.
		clusters = append(clusters, object{
			"name": name, "type": "STRICT_DNS", "connect_timeout": "5s",
			"dns_refresh_rate": "1s", "respect_dns_ttl": false,
			"dns_failure_refresh_rate": object{"base_interval": "0.250s", "max_interval": "1s"},
			"lb_policy":                "ROUND_ROBIN",
			"outlier_detection": object{
				"split_external_local_origin_errors": true,
				"consecutive_local_origin_failure":   1, "enforcing_consecutive_local_origin_failure": 100,
				"consecutive_5xx": 0, "enforcing_consecutive_5xx": 0,
				"consecutive_gateway_failure": 0, "enforcing_consecutive_gateway_failure": 0,
				"enforcing_success_rate": 0, "enforcing_local_origin_success_rate": 0,
				"interval": "1s", "base_ejection_time": "5s", "max_ejection_time": "30s",
				"max_ejection_percent": 50, "always_eject_one_host": false,
			},
			"load_assignment": object{"cluster_name": name, "endpoints": []any{object{"lb_endpoints": []any{object{"endpoint": object{"address": socket(host, n)}}}}}},
		})
	}
	if in.Upstreams["studio"] != "" && (in.DashboardUsername == "" || in.DashboardPassword == "" || strings.ContainsAny(in.DashboardUsername, ":\r\n")) {
		return nil, errors.New("dashboard basic-auth credentials are required for Studio")
	}
	rawKeys := []string{in.AnonKey, in.ServiceRoleKey}
	adminKeys := []string{in.ServiceRoleKey}
	opaqueNames := make([]string, 0, len(in.OpaqueKeys))
	for key, mapping := range in.OpaqueKeys {
		if (!strings.HasPrefix(key, "sb_publishable_") && !strings.HasPrefix(key, "sb_secret_")) || mapping.JWT == "" {
			return nil, errors.New("opaque keys require an upstream prefix and internal JWT")
		}
		if key == in.AnonKey || key == in.ServiceRoleKey {
			return nil, errors.New("opaque and legacy credentials must be distinct")
		}
		opaqueNames = append(opaqueNames, key)
	}
	sort.Strings(opaqueNames)
	for _, key := range opaqueNames {
		rawKeys = append(rawKeys, key)
		if in.OpaqueKeys[key].Admin {
			adminKeys = append(adminKeys, key)
		}
	}
	allow := rbac(nil)
	secured := rbac(rawKeys)
	admin := rbac(adminKeys)
	disabledBasic := object{"@type": "type.googleapis.com/envoy.config.route.v3.FilterConfig", "disabled": true}
	var routes []any
	direct := func(prefix string, status int) {
		routes = append(routes, object{"name": "deny", "match": object{"prefix": prefix}, "direct_response": object{"status": status, "body": object{"inline_string": map[int]string{403: "Access is forbidden.", 404: "Not found"}[status]}}, "typed_per_filter_config": object{"envoy.filters.http.basic_auth": disabledBasic, "envoy.filters.http.rbac": allow}})
	}
	for _, p := range []string{"/realtime/v1/api/tenants", "/realtime/v1/api/openapi", "/api/mcp", "/mcp"} {
		direct(p, 403)
	}
	// Reject Phoenix-equivalent path ambiguities only on Realtime. Storage
	// must retain its byte-for-byte SigV4 path, including escaped slashes.
	direct("/realtime", 403)
	routes[len(routes)-1].(object)["match"] = object{"safe_regex": object{"regex": `(?i)/realtime/v1/.*(//|%2f|%5c|%25|\\|/(\.|%2e){1,2}(/|$)).*`}}
	add := func(name, component, path, rewrite, policy string, exact bool) {
		match := object{"prefix": path}
		if exact {
			match = object{"path": path}
		}
		filters := object{"envoy.filters.http.basic_auth": disabledBasic, "envoy.filters.http.rbac": allow}
		if policy == "key" {
			filters["envoy.filters.http.rbac"] = secured
		}
		if policy == "admin" {
			filters["envoy.filters.http.rbac"] = admin
		}
		r := object{"name": name, "match": match, "typed_per_filter_config": filters}
		if in.Upstreams[component] == "" {
			r["name"] = "deny"
			filters["envoy.filters.http.rbac"] = allow
			r["direct_response"] = object{"status": 404}
			routes = append(routes, r)
			return
		}
		// Envoy 1.39 supports reset-before-request: never replay a request
		// after its headers were sent (Storage/REST writes may not be idempotent).
		// The policy applies before an upgrade, not to an established websocket.
		action := object{"cluster": component, "prefix_rewrite": rewrite, "timeout": "30s",
			"retry_policy": object{
				"retry_on": "connect-failure,refused-stream,reset-before-request", "num_retries": 2,
				"retry_host_predicate": []any{object{
					"name":         "envoy.retry_host_predicates.previous_hosts",
					"typed_config": object{"@type": "type.googleapis.com/envoy.extensions.retry.host.previous_hosts.v3.PreviousHostsPredicate"},
				}},
				"host_selection_retry_max_attempts": 3,
			},
		}
		if component == "functions" {
			action["timeout"] = "410s"
			action["idle_timeout"] = "160s"
		}
		if component == "storage" {
			action["timeout"] = "0s"
			action["idle_timeout"] = "0s"
		}
		if component == "realtime" {
			action["host_rewrite_literal"] = in.RealtimeHost
			if name == "realtime-ws" {
				action["timeout"] = "0s"
				action["idle_timeout"] = "0s"
			}
		}
		r["route"] = action
		r["request_headers_to_add"] = []any{header("x-forwarded-prefix", strings.TrimSuffix(path, "/"))}
		if name == "graphql" {
			r["request_headers_to_add"] = []any{header("x-forwarded-prefix", path), header("content-profile", "graphql_public"), header("accept-profile", "graphql_public")}
		}
		if component == "studio" {
			delete(filters, "envoy.filters.http.basic_auth")
			r["request_headers_to_add"] = append(r["request_headers_to_add"].([]any), header("apikey", in.ServiceRoleKey))
			r["request_headers_to_remove"] = []string{"authorization", "proxy-authorization"}
		}
		routes = append(routes, r)
	}
	for _, p := range []string{"verify", "callback", "authorize", ".well-known/jwks.json", ".well-known/oauth-authorization-server", ".well-known/openid-configuration", "sso/saml/acs", "sso/saml/metadata"} {
		add("auth-open", "auth", "/auth/v1/"+p, "/"+p, "open", false)
	}
	add("auth-open", "auth", "/.well-known/oauth-authorization-server/auth/v1", "/.well-known/oauth-authorization-server", "open", true)
	add("auth", "auth", "/auth/v1/", "/", "key", false)
	add("rest-root", "rest", "/rest/v1/", "/", "admin", true)
	add("rest", "rest", "/rest/v1/", "/", "key", false)
	if in.GraphQL {
		add("graphql", "rest", "/graphql/v1", "/rpc/graphql", "key", false)
	} else {
		direct("/graphql/v1", 404)
	}
	add("realtime-api", "realtime", "/realtime/v1/api/ping", "/api/ping", "key", true)
	add("realtime-api", "realtime", "/realtime/v1/api/broadcast", "/api/broadcast", "key", true)
	add("realtime-api", "realtime", "/realtime/v1/api/broadcast/", "/api/broadcast/", "key", false)
	broadcast := routes[len(routes)-1].(object)
	broadcast["match"] = object{"safe_regex": object{"regex": `/realtime/v1/api/broadcast/[^/]+/events/[^/]+`}}
	if action, ok := broadcast["route"].(object); ok {
		delete(action, "prefix_rewrite")
		action["regex_rewrite"] = object{"pattern": object{"regex": `^/realtime/v1/(.*)$`}, "substitution": `/\1`}
	}
	add("realtime-ws", "realtime", "/realtime/v1/websocket", "/socket/websocket", "key", true)
	add("realtime-ws", "realtime", "/realtime/v1/longpoll", "/socket/longpoll", "key", true)
	add("realtime-api", "realtime", "/realtime/v1/healthcheck", "/healthcheck", "key", true)
	direct("/realtime", 403)
	add("storage", "storage", "/storage/v1/", "/", "open", false)
	add("functions", "functions", "/functions/v1/", "/", "open", false)
	add("meta", "meta", "/pg/", "/", "admin", false)
	// Reserve slashless API roots so they can never be mistaken for dashboard pages.
	for _, p := range []string{"/auth/v1", "/rest/v1", "/realtime/v1", "/storage/v1", "/functions/v1", "/pg"} {
		direct(p, 404)
	}
	add("studio", "studio", "/", "/", "open", false)
	origins := in.CORS.Origins
	if len(origins) == 0 {
		origins = []string{"*"}
	}
	if err := sbspec.ValidateCORSOrigins(origins); err != nil {
		return nil, err
	}
	var originMatches []any
	for _, origin := range origins {
		if origin == "*" {
			if in.CORS.AllowCredentials {
				return nil, errors.New("credentialed CORS cannot use wildcard origin")
			}
			originMatches = append(originMatches, object{"safe_regex": object{"regex": ".*"}})
		} else {
			originMatches = append(originMatches, object{"exact": origin})
		}
	}
	methods := in.CORS.Methods
	if len(methods) == 0 {
		methods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"}
	}
	headers := in.CORS.Headers
	if len(headers) == 0 {
		headers = []string{"*"}
	}
	exposed := in.CORS.ExposeHeaders
	if len(exposed) == 0 {
		exposed = []string{"*"}
	}
	cors := object{"@type": "type.googleapis.com/envoy.extensions.filters.http.cors.v3.CorsPolicy", "allow_origin_string_match": originMatches, "allow_methods": strings.Join(methods, ","), "allow_headers": strings.Join(headers, ","), "expose_headers": strings.Join(exposed, ","), "allow_credentials": in.CORS.AllowCredentials, "max_age": "3600"}
	var lua strings.Builder
	lua.WriteString("local known = {\n")
	for _, key := range rawKeys {
		fmt.Fprintf(&lua, "[%s]=true,\n", luaQuote(key))
	}
	lua.WriteString("}\nlocal mapped = {\n")
	for _, key := range opaqueNames {
		fmt.Fprintf(&lua, "[%s]=%s,\n", luaQuote(key), luaQuote(in.OpaqueKeys[key].JWT))
	}
	lua.WriteString("}\n")
	lua.WriteString(translation)
	publicPort := u.Port()
	if publicPort == "" {
		publicPort = map[string]string{"http": "80", "https": "443"}[u.Scheme]
	}
	prelude := "local public_host = " + luaQuote(u.Host) + "\nlocal public_port = " + luaQuote(publicPort) + "\nlocal public_scheme = " + luaQuote(u.Scheme) + "\n"
	filters := []any{filter("cors", "envoy.extensions.filters.http.cors.v3.Cors", object{})}
	if in.Upstreams["studio"] != "" {
		// Htpasswd SHA is Envoy's supported format. Install this filter only
		// when Studio exists; Envoy rejects an empty htpasswd username.
		digest := sha1.Sum([]byte(in.DashboardPassword))
		users := in.DashboardUsername + ":{SHA}" + base64.StdEncoding.EncodeToString(digest[:]) + "\n"
		filters = append(filters, filter("basic_auth", "envoy.extensions.filters.http.basic_auth.v3.BasicAuth", object{"users": object{"inline_string": users}}))
	} else {
		for _, item := range routes {
			delete(item.(object)["typed_per_filter_config"].(object), "envoy.filters.http.basic_auth")
		}
	}
	filters = append(filters, filter("lua.authorize", "envoy.extensions.filters.http.lua.v3.Lua", object{"inline_code": "local authorize_only = true\n" + prelude + lua.String()}), filter("rbac", "envoy.extensions.filters.http.rbac.v3.RBAC", object{"rules": object{"action": "ALLOW", "policies": object{}}}), filter("lua.translate", "envoy.extensions.filters.http.lua.v3.Lua", object{"inline_code": "local authorize_only = false\n" + prelude + lua.String()}), filter("router", "envoy.extensions.filters.http.router.v3.Router", object{}))
	hcm := object{"@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager", "stat_prefix": "supabase", "use_remote_address": true, "xff_num_trusted_hops": 0, "normalize_path": false, "merge_slashes": false, "path_with_escaped_slashes_action": "KEEP_UNCHANGED", "common_http_protocol_options": object{"headers_with_underscores_action": "REJECT_REQUEST"}, "upgrade_configs": []any{object{"upgrade_type": "websocket"}}, "route_config": object{"name": "supabase", "virtual_hosts": []any{object{"name": "supabase", "domains": []string{"*"}, "typed_per_filter_config": object{"envoy.filters.http.cors": cors}, "routes": routes}}}, "http_filters": filters}
	listener := object{"name": "supabase", "address": socket(in.ListenAddress, uint64(in.ListenPort)), "per_connection_buffer_limit_bytes": 32768, "filter_chains": []any{object{"filters": []any{object{"name": "envoy.filters.network.http_connection_manager", "typed_config": hcm}}}}}
	if address := net.ParseIP(in.ListenAddress); address.To4() == nil && address.IsUnspecified() {
		listener["address"].(object)["socket_address"].(object)["ipv4_compat"] = true
	}
	return json.MarshalIndent(object{"node": object{"id": "supabase", "cluster": "supabase"}, "admin": object{"address": socket("127.0.0.1", AdminPort)}, "static_resources": object{"listeners": []any{listener}, "clusters": clusters}}, "", "  ")
}
func socket(host string, port uint64) object {
	return object{"socket_address": object{"address": host, "port_value": port}}
}
func header(key, value string) object {
	return object{"header": object{"key": key, "value": value}, "append_action": "OVERWRITE_IF_EXISTS_OR_ADD"}
}
func filter(name, typ string, config object) object {
	config["@type"] = "type.googleapis.com/" + typ
	return object{"name": "envoy.filters.http." + name, "typed_config": config}
}
func rbac(keys []string) object {
	principals := []any{object{"any": true}}
	if keys != nil {
		principals = nil
		for _, key := range keys {
			principals = append(principals, object{"header": object{"name": "apikey", "string_match": object{"exact": key}}})
		}
	}
	return object{"@type": "type.googleapis.com/envoy.extensions.filters.http.rbac.v3.RBACPerRoute", "rbac": object{"rules": object{"action": "ALLOW", "policies": object{"configured_keys": object{"permissions": []any{object{"any": true}}, "principals": principals}}}}}
}
func luaQuote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for i := range len(s) {
		c := s[i]
		if c < ' ' || c > 126 || c == '"' || c == '\\' {
			fmt.Fprintf(&b, "\\%03d", c)
		} else {
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String()
}
