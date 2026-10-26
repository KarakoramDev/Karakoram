// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
)

// Backend names are platform service-discovery identities whose addresses can
// change on restart. Keep the asynchronous DNS cache, but refresh it promptly.
const (
	proxyDNSCacheTTL        = "5000"
	proxyDNSRefreshInterval = "1000"
	proxyDNSResolverQueue   = "128"
)

func proxyWriterReady(p *pgspec.Proxy, backends []pgspec.ProxyBackend) bool {
	if len(p.Servers) == 0 {
		return true // No writer is expected before bootstrap publication.
	}
	for _, backend := range backends {
		if backend.Hostgroup == 10 && backend.Status == "ONLINE" {
			return true
		}
	}
	return false
}

func proxyBackendStatus(ctx context.Context, c *pgx.Conn) ([]pgspec.ProxyBackend, error) {
	rows, err := queryTable(ctx, c, "SELECT hostname,hostgroup_id,status FROM runtime_pgsql_servers")
	if err != nil {
		return nil, err
	}
	backends := make([]pgspec.ProxyBackend, 0, len(rows))
	for _, row := range rows {
		group, _ := strconv.Atoi(row["hostgroup_id"])
		backends = append(backends, pgspec.ProxyBackend{Host: row["hostname"], Hostgroup: group, Status: row["status"]})
	}
	return backends, nil
}

// A recovered backend can remain automatically SHUNNED until the proxy handles
// frontend demand. TCP connects and admin-table reads never exercise that path.
// Use a bounded, read-only writer request, not an administrative ONLINE override.
func proxyProbeWriter(ctx context.Context, s pgspec.Spec) bool {
	if len(s.Proxy.Users) == 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	cfg, err := pgx.ParseConfig("")
	if err != nil {
		return false
	}
	cfg.Host, _, err = net.SplitHostPort(frontendAddress(s.ListenHost))
	if err != nil {
		return false
	}
	cfg.Port, cfg.Database = 5432, "postgres"
	cfg.User, cfg.Password = s.Proxy.Users[0].Username, s.Proxy.Users[0].Password
	cfg.TLSConfig, cfg.Fallbacks = nil, nil
	cfg.RuntimeParams = map[string]string{}
	cfg.ConnectTimeout = time.Second
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	if s.Proxy.ServerCert != nil || s.Proxy.Users[0].RequireTLS {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(s.CAPEM)) {
			return false
		}
		cfg.TLSConfig = &tls.Config{RootCAs: roots, ServerName: s.Member, MinVersion: tls.VersionTLS12}
	}
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return false
	}
	defer c.Close(ctx)
	var readOnly string
	// SHOW retains the default writer hostgroup even with SELECT splitting.
	return c.QueryRow(ctx, "SHOW transaction_read_only").Scan(&readOnly) == nil && readOnly == "off"
}

func proxyConnect(ctx context.Context, p *pgspec.Proxy) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig("")
	if err != nil {
		return nil, err
	}
	cfg.Host = "127.0.0.1"
	cfg.Port = 6132
	cfg.User = p.Admin.Username
	cfg.Password = p.Admin.Password
	cfg.Database = "postgres"
	cfg.TLSConfig = nil
	cfg.Fallbacks = nil
	cfg.ConnectTimeout = 2 * time.Second
	cfg.RuntimeParams = map[string]string{}
	cfg.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	return pgx.ConnectConfig(ctx, cfg)
}

type tableRow map[string]string

func queryTable(ctx context.Context, c *pgx.Conn, sql string) ([]tableRow, error) {
	rows, err := c.Query(ctx, sql, pgx.QueryExecModeSimpleProtocol)
	if err != nil {
		return nil, errors.New("ProxySQL admin query failed")
	}
	defer rows.Close()
	result := []tableRow{}
	fields := rows.FieldDescriptions()
	for rows.Next() {
		values := rows.RawValues()
		row := tableRow{}
		for i, v := range values {
			row[fields[i].Name] = string(v)
		}
		result = append(result, row)
	}
	if err = rows.Err(); err != nil {
		return nil, errors.New("ProxySQL admin result failed")
	}
	return result, nil
}
func boolInt(v bool) string {
	if v {
		return "1"
	}
	return "0"
}
func sameRow(a, b tableRow) bool {
	for k, v := range b {
		if a[k] != v {
			return false
		}
	}
	return true
}
func keyRows(rows []tableRow, key string) map[string]tableRow {
	m := map[string]tableRow{}
	for _, r := range rows {
		m[r[key]] = r
	}
	return m
}
func sortedKeys[T any](m map[string]T) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// proxyTableDiff computes exact row changes without blanket DELETE/reinsert.
// Hostgroup movement by the native role monitor is deliberately not an input
// to the server identity: an unchanged replica must not be forced into writer.
func proxyTableDiff(table, key string, current, desired []tableRow, columns []string) []string {
	old := keyRows(current, key)
	next := keyRows(desired, key)
	result := []string{}
	for _, id := range sortedKeys(old) {
		if _, ok := next[id]; !ok {
			result = append(result, "DELETE FROM "+table+" WHERE "+key+"="+sqlQuote(id))
		}
	}
	for _, id := range sortedKeys(next) {
		row := next[id]
		if prior, ok := old[id]; ok && sameRow(prior, row) {
			continue
		}
		if _, ok := old[id]; ok {
			result = append(result, "DELETE FROM "+table+" WHERE "+key+"="+sqlQuote(id))
		}
		vals := make([]string, len(columns))
		for i, c := range columns {
			vals[i] = sqlQuote(row[c])
		}
		result = append(result, "INSERT INTO "+table+" ("+strings.Join(columns, ",")+") VALUES ("+strings.Join(vals, ",")+")")
	}
	return result
}
func setProxyServerHostgroups(servers, observed []tableRow) {
	for _, server := range servers {
		// Unknown servers must not accept writes before the role monitor
		// has established that they are the primary.
		server["hostgroup_id"] = "20"
		for _, prior := range observed {
			if prior["hostname"] == server["hostname"] {
				server["hostgroup_id"] = prior["hostgroup_id"]
				break
			}
		}
	}
}

// Native monitoring moves servers between hostgroups. Compare stable intent
// across layers without treating that live role decision as configuration drift.
func proxyLayerMatches(key string, current, desired []tableRow, servers bool) bool {
	old := keyRows(current, key)
	next := keyRows(desired, key)
	if len(old) != len(next) {
		return false
	}
	for id, row := range next {
		prior, ok := old[id]
		if !ok {
			return false
		}
		for k, v := range row {
			if servers && k == "hostgroup_id" {
				continue
			}
			if prior[k] != v {
				return false
			}
		}
	}
	return true
}

func proxyPlan(ctx context.Context, c *pgx.Conn, p *pgspec.Proxy, host, caFile string) ([]string, error) {
	if p.BackendTLS && caFile == "" {
		return nil, errors.New("ProxySQL backend TLS requires a CA file")
	}
	statements := []string{}
	type table struct {
		name, key, load string
		columns         []string
		desired         []tableRow
	}
	servers := []tableRow{}
	maxConnections := p.MaxConnections
	if maxConnections == 0 {
		maxConnections = 100
	}
	for _, h := range p.Servers {
		servers = append(servers, tableRow{"hostname": h, "port": "5432", "hostgroup_id": "20", "max_connections": strconv.Itoa(maxConnections), "use_ssl": boolInt(p.BackendTLS)})
	}
	users := []tableRow{}
	for _, u := range p.Users {
		users = append(users, tableRow{"username": u.Username, "password": u.Password, "active": "1", "default_hostgroup": "10", "transaction_persistent": "1", "use_ssl": boolInt(u.RequireTLS)})
	}
	rules := []tableRow{}
	if p.ReaderPort {
		rules = append(rules, tableRow{"rule_id": "1", "active": "1", "proxy_port": "5433", "match_pattern": "", "destination_hostgroup": "20", "apply": "1"})
	}
	if p.ReadWriteSplit {
		rules = append(rules, tableRow{"rule_id": "10", "active": "1", "proxy_port": "5432", "match_pattern": "^SELECT .*FOR UPDATE", "destination_hostgroup": "10", "apply": "1"}, tableRow{"rule_id": "20", "active": "1", "proxy_port": "5432", "match_pattern": "^SELECT", "destination_hostgroup": "20", "apply": "1"})
	}
	for _, t := range []table{
		{"pgsql_servers", "hostname", "SERVERS", []string{"hostname", "port", "hostgroup_id", "max_connections", "use_ssl"}, servers},
		{"pgsql_replication_hostgroups", "writer_hostgroup", "SERVERS", []string{"writer_hostgroup", "reader_hostgroup", "check_type"}, []tableRow{{"writer_hostgroup": "10", "reader_hostgroup": "20", "check_type": "read_only"}}},
		{"pgsql_users", "username", "USERS", []string{"username", "password", "active", "default_hostgroup", "transaction_persistent", "use_ssl"}, users},
		{"pgsql_query_rules", "rule_id", "QUERY RULES", []string{"rule_id", "active", "proxy_port", "match_pattern", "destination_hostgroup", "apply"}, rules},
	} {
		old, err := queryTable(ctx, c, "SELECT "+strings.Join(t.columns, ",")+" FROM "+t.name)
		if err != nil {
			return nil, err
		}
		runtime, err := queryTable(ctx, c, "SELECT "+strings.Join(t.columns, ",")+" FROM runtime_"+t.name)
		if err != nil {
			return nil, err
		}
		disk, err := queryTable(ctx, c, "SELECT "+strings.Join(t.columns, ",")+" FROM disk."+t.name)
		if err != nil {
			return nil, err
		}
		// Role-only changes must not churn configuration. If stable settings
		// do change, preserve the monitor's runtime roles across the LOAD.
		compare := t.desired
		if t.name == "pgsql_servers" {
			setProxyServerHostgroups(compare, old)
		}
		diff := proxyTableDiff(t.name, t.key, old, compare, t.columns)
		runtimeDrift := !proxyLayerMatches(t.key, runtime, compare, t.name == "pgsql_servers")
		diskDrift := !proxyLayerMatches(t.key, disk, compare, t.name == "pgsql_servers")
		if t.name == "pgsql_servers" && (len(diff) > 0 || runtimeDrift) {
			setProxyServerHostgroups(compare, runtime)
			diff = proxyTableDiff(t.name, t.key, old, compare, t.columns)
		}
		statements = append(statements, diff...)
		if len(diff) > 0 || runtimeDrift {
			statements = append(statements, "LOAD PGSQL "+t.load+" TO RUNTIME")
		}
		if len(diff) > 0 || diskDrift {
			statements = append(statements, "SAVE PGSQL "+t.load+" TO DISK")
		}
	}
	old, err := queryTable(ctx, c, "SELECT variable_name,variable_value FROM global_variables")
	if err != nil {
		return nil, err
	}
	runtimeVariables, err := queryTable(ctx, c, "SELECT variable_name,variable_value FROM runtime_global_variables")
	if err != nil {
		return nil, err
	}
	diskVariables, err := queryTable(ctx, c, "SELECT variable_name,variable_value FROM disk.global_variables")
	if err != nil {
		return nil, err
	}
	runtimeMap, diskMap := keyRows(runtimeVariables, "variable_name"), keyRows(diskVariables, "variable_name")
	pgsqlLoaded := false
	vmap := keyRows(old, "variable_name")
	pgsqlSettings := proxyRuntimeSettings(p, caFile)
	for _, group := range []struct {
		name   string
		values map[string]string
	}{
		{"PGSQL", pgsqlSettings},
		// ProxySQL 3.0.11's PostgreSQL shun_and_killall reads the MySQL
		// ping interval when advancing its recovery timestamp.
		{"MYSQL", map[string]string{"mysql-monitor_ping_interval": "1000"}},
		{"ADMIN", map[string]string{"admin-admin_credentials": p.Admin.Username + ":" + p.Admin.Password, "admin-mysql_ifaces": "127.0.0.1:6032", "admin-pgsql_ifaces": "127.0.0.1:6132"}},
	} {
		changed := false
		runtimeDrift, diskDrift := false, false
		for _, key := range sortedKeys(group.values) {
			runtimeDrift = runtimeDrift || runtimeMap[key]["variable_value"] != group.values[key]
			diskDrift = diskDrift || diskMap[key]["variable_value"] != group.values[key]
			if vmap[key]["variable_value"] == group.values[key] {
				continue
			}
			statements = append(statements, "UPDATE global_variables SET variable_value="+sqlQuote(group.values[key])+" WHERE variable_name="+sqlQuote(key))
			changed = true
		}
		if changed || runtimeDrift {
			statements = append(statements, "LOAD "+group.name+" VARIABLES TO RUNTIME")
			pgsqlLoaded = pgsqlLoaded || group.name == "PGSQL"
		}
		if changed || diskDrift {
			statements = append(statements, "SAVE "+group.name+" VARIABLES TO DISK")
		}
	}
	// Listener and thread settings are startup-only. Save them without LOAD,
	// which can reset a non-dynamic value to its currently running setting.
	startupChanged := false
	for key, value := range proxyStartupSettings(p, host) {
		if vmap[key]["variable_value"] == value && !pgsqlLoaded && diskMap[key]["variable_value"] == value {
			continue
		}
		statements = append(statements, "UPDATE global_variables SET variable_value="+sqlQuote(value)+" WHERE variable_name="+sqlQuote(key))
		startupChanged = true
	}
	if startupChanged {
		statements = append(statements, "SAVE PGSQL VARIABLES TO DISK")
	}
	return statements, nil
}
func reconcileProxy(ctx context.Context, c *pgx.Conn, p *pgspec.Proxy, host, caFile string) (int, error) {
	plan, err := proxyPlan(ctx, c, p, host, caFile)
	if err != nil {
		return 0, err
	}
	for _, sql := range plan {
		// ProxySQL's PostgreSQL admin listener accepts one SQL statement per
		// request. In particular, never join LOAD/SAVE commands with semicolons.
		if _, err = c.Exec(ctx, sql, pgx.QueryExecModeSimpleProtocol); err != nil {
			return 0, fmt.Errorf("ProxySQL configuration statement failed")
		}
	}
	return len(plan), nil
}
func proxyRuntimeSettings(p *pgspec.Proxy, caFile string) map[string]string {
	settings := map[string]string{
		"pgsql-authentication_method":         "3",
		"pgsql-monitor_username":              p.Monitor.Username,
		"pgsql-monitor_password":              p.Monitor.Password,
		"pgsql-monitor_dbname":                "postgres",
		"pgsql-monitor_writer_is_also_reader": "false",
		// Bound reconnect amplification, not query/transaction execution.
		"pgsql-connect_retries_on_failure":                "2",
		"pgsql-connect_retries_delay":                     "100",
		"pgsql-connect_timeout_server":                    "1000",
		"pgsql-connect_timeout_server_max":                "3000",
		"pgsql-shun_on_failures":                          "3",
		"pgsql-shun_recovery_time_sec":                    "1",
		"pgsql-throttle_connections_per_sec_to_hostgroup": "20",
		"pgsql-free_connections_pct":                      "10",
		"pgsql-connection_warming":                        "false",
		"pgsql-ping_interval_server_msec":                 "1000",
		"pgsql-ping_timeout_server":                       "500",
		"pgsql-monitor_enabled":                           "true",
		"pgsql-monitor_read_only_interval":                "500",
		"pgsql-monitor_read_only_timeout":                 "500",
		"pgsql-monitor_read_only_max_timeout_count":       "2",
		"pgsql-monitor_ping_interval":                     "1000",
		"pgsql-monitor_ping_timeout":                      "500",
		"pgsql-monitor_ping_max_failures":                 "2",
		"pgsql-monitor_connect_interval":                  "1000",
		"pgsql-monitor_connect_timeout":                   "500",
		"pgsql-monitor_local_dns_cache_ttl":               proxyDNSCacheTTL,
		"pgsql-monitor_local_dns_cache_refresh_interval":  proxyDNSRefreshInterval,
		"pgsql-monitor_local_dns_resolver_queue_maxsize":  proxyDNSResolverQueue,
	}
	if p.BackendTLS {
		// Both the data and monitor libpq connections use this trust source.
		settings["pgsql-ssl_p2s_ca"] = caFile
	}
	return settings
}

func proxyStartupSettings(p *pgspec.Proxy, host string) map[string]string {
	threads := p.Threads
	if threads == 0 {
		threads = 2
	}
	return map[string]string{"pgsql-interfaces": proxyInterfaces(host, p.ReaderPort), "pgsql-threads": strconv.Itoa(threads)}
}

func reconcileProxyWait(ctx context.Context, p *pgspec.Proxy, host, caFile string, previous pgspec.Credentials) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	for {
		c, err := proxyConnect(ctx, p)
		if err != nil && previous.Username != "" && previous != p.Admin {
			auth := *p
			auth.Admin = previous
			c, err = proxyConnect(ctx, &auth)
		}
		if err == nil {
			defer c.Close(ctx)
			rows, err := queryTable(ctx, c, "SELECT variable_name,variable_value FROM runtime_global_variables")
			if err != nil {
				return false, err
			}
			current := keyRows(rows, "variable_name")
			// Existing backend and monitor connections retain their TLS state.
			restart := p.BackendTLS && current["pgsql-ssl_p2s_ca"]["variable_value"] != caFile
			for key, value := range proxyStartupSettings(p, host) {
				if current[key]["variable_value"] != value {
					restart = true
				}
			}
			_, err = reconcileProxy(ctx, c, p, host, caFile)
			return restart, err
		}
		select {
		case <-ctx.Done():
			return false, errors.New("ProxySQL admin did not accept connections before deadline")
		case <-time.After(100 * time.Millisecond):
		}
	}
}
