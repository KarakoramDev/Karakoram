// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

func x509Pool(pem string) *x509.CertPool {
	p := x509.NewCertPool()
	if !p.AppendCertsFromPEM([]byte(pem)) {
		return nil
	}
	return p
}
func tlsConfig(p *x509.CertPool, host string) *tls.Config {
	return &tls.Config{RootCAs: p, ServerName: host, MinVersion: tls.VersionTLS12}
}
func proxyConfigQuote(s string) string { return strconv.Quote(s) }

// RenderProxyConfig produces the cold-start config. The caller writes it 0600;
// credentials come from sealed member configuration, never argv or SQL hashes.
func RenderProxyConfig(s myspec.Spec, configDir, dataDir string) (string, error) {
	if s.Proxy == nil {
		return "", errors.New("proxy configuration unavailable")
	}
	p := s.Proxy
	if err := p.Validate(); err != nil {
		return "", err
	}
	var b strings.Builder
	admin, interfaces := nativeProxyListeners(s)
	// Empty PostgreSQL interfaces are rejected by the pinned ProxySQL build.
	// Keep both unused protocols on owner-only Unix sockets, never default TCP.
	fmt.Fprintf(&b, "datadir=%s\nadmin_variables={\n admin_credentials=%s\n mysql_ifaces=%s\n pgsql_ifaces=%s\n}\npgsql_variables={\n interfaces=%s\n}\n", proxyConfigQuote(dataDir), proxyConfigQuote(p.Admin.Username+":"+p.Admin.Password), proxyConfigQuote(admin), proxyConfigQuote(filepath.Join(dataDir, "pgsql-admin.sock")), proxyConfigQuote(filepath.Join(dataDir, "pgsql-client.sock")))
	if p.ReaderPort {
		interfaces += ";0.0.0.0:3307"
		if s.ListenHost == "::" {
			interfaces += ";[::]:3307"
		}
	}
	healthcheck := "groupreplication"
	if p.Mode == "galera" {
		healthcheck = "galera"
	}
	fmt.Fprintf(&b, "mysql_variables={\n interfaces=%s\n monitor_username=%s\n monitor_password=%s\n monitor_connect_interval=1000\n monitor_ping_interval=1000\n monitor_%s_healthcheck_interval=1000\n monitor_%s_healthcheck_timeout=800\n have_ssl=true\n ssl_p2s_ca=%s\n ssl_p2s_cert=%s\n ssl_p2s_key=%s\n max_connections=%d\n}\n", proxyConfigQuote(interfaces), proxyConfigQuote(p.Monitor.Username), proxyConfigQuote(p.Monitor.Password), healthcheck, healthcheck, proxyConfigQuote(filepath.Join(configDir, "ca.pem")), proxyConfigQuote(filepath.Join(configDir, "server-cert.pem")), proxyConfigQuote(filepath.Join(configDir, "server-key.pem")), p.MaxConnections)
	if p.Mode == "galera" {
		b.WriteString("mysql_galera_hostgroups=(\n {writer_hostgroup=10,backup_writer_hostgroup=11,reader_hostgroup=20,offline_hostgroup=30,active=1,max_writers=1,writer_is_also_reader=2,max_transactions_behind=100}\n)\n")
	} else if s.Topology != myspec.TopologySingle {
		b.WriteString("mysql_group_replication_hostgroups=(\n {writer_hostgroup=10,backup_writer_hostgroup=11,reader_hostgroup=20,offline_hostgroup=30,active=1,max_writers=1,writer_is_also_reader=0,max_transactions_behind=100}\n)\n")
	}
	b.WriteString("mysql_servers=(\n")
	for i, peer := range p.Backends {
		if i > 0 {
			b.WriteString(",\n")
		}
		fmt.Fprintf(&b, " {address=%s,port=%d,hostgroup=10,use_ssl=1,max_connections=%d}", proxyConfigQuote(peer.Host), peer.Port, p.MaxConnections)
		if s.Topology == myspec.TopologySingle && (p.ReadWriteSplit || p.ReaderPort) {
			fmt.Fprintf(&b, ",\n {address=%s,port=%d,hostgroup=20,use_ssl=1,max_connections=%d}", proxyConfigQuote(peer.Host), peer.Port, p.MaxConnections)
		}
	}
	b.WriteString("\n)\nmysql_users=(\n")
	for i, u := range p.Users {
		if i > 0 {
			b.WriteString(",\n")
		}
		ssl := 0
		if u.RequireTLS {
			ssl = 1
		}
		fmt.Fprintf(&b, " {username=%s,password=%s,default_hostgroup=10,active=1,use_ssl=%d,transaction_persistent=1}", proxyConfigQuote(u.Username), proxyConfigQuote(u.Password), ssl)
	}
	b.WriteString("\n)\nmysql_query_rules=(\n")
	rules := proxyRules(p)
	for i, r := range rules {
		if i > 0 {
			b.WriteString(",\n")
		}
		fmt.Fprintf(&b, " {rule_id=%d,active=1,destination_hostgroup=%d,apply=1", r.id, r.group)
		if r.port > 0 {
			fmt.Fprintf(&b, ",proxy_port=%d", r.port)
		}
		if r.pattern != "" {
			fmt.Fprintf(&b, ",match_digest=%s", proxyConfigQuote(r.pattern))
		}
		b.WriteString("}")
	}
	b.WriteString("\n)\n")
	return b.String(), nil
}

type proxyRule struct {
	id, group, port int
	pattern         string
}

func proxyRules(p *myspec.Proxy) []proxyRule {
	var rules []proxyRule
	// Locking SELECTs stay on the writer, including on the reader listener.
	if p.ReadWriteSplit || p.ReaderPort {
		rules = append(rules, proxyRule{10, 10, 0, "^SELECT.*(FOR UPDATE|FOR SHARE|LOCK IN SHARE MODE)"})
	}
	if p.ReaderPort {
		rules = append(rules, proxyRule{20, 20, 3307, ""})
	}
	if p.ReadWriteSplit {
		rules = append(rules, proxyRule{30, 20, 0, "^SELECT"})
	}
	return rules
}
func proxyAdmin(ctx context.Context, s myspec.Spec) (*sql.DB, error) {
	if s.Proxy == nil {
		return nil, errors.New("proxy configuration unavailable")
	}
	cfg := mysqldriver.NewConfig()
	cfg.User = s.Proxy.Admin.Username
	cfg.Passwd = s.Proxy.Admin.Password
	cfg.Net = "tcp"
	cfg.Addr, _ = nativeProxyListeners(myspec.Spec{})
	cfg.Timeout = 2 * time.Second
	cfg.ReadTimeout = 3 * time.Second
	cfg.WriteTimeout = 3 * time.Second
	// ProxySQL admin does not implement prepared statements. Every statement below
	// has no driver parameters, so COM_QUERY is used with interpolation disabled.
	connector, err := mysqldriver.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)
	db.SetMaxOpenConns(1)
	if err = db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// ProxySQL admin speaks SQLite SQL: do not use MySQL backslash escaping.
func proxyQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func ApplyProxyConfig(ctx context.Context, s myspec.Spec) error {
	if s.Proxy == nil {
		return errors.New("proxy configuration unavailable")
	}
	if err := s.Proxy.Validate(); err != nil {
		return err
	}
	db, err := proxyAdmin(ctx, s)
	if err != nil {
		return err
	}
	defer db.Close()
	return applyProxySQL(ctx, db, s)
}

func proxyTLSChanged(previous, current myspec.Spec) bool {
	return previous.Proxy != nil && (previous.CAPEM != current.CAPEM || previous.ServerCert == nil || current.ServerCert == nil || *previous.ServerCert != *current.ServerCert)
}

func proxyTLSStatements(dir string) []string {
	return []string{
		"UPDATE global_variables SET variable_value=" + proxyQuote(filepath.Join(dir, "ca.pem")) + " WHERE variable_name='mysql-ssl_p2s_ca'",
		"UPDATE global_variables SET variable_value=" + proxyQuote(filepath.Join(dir, "server-cert.pem")) + " WHERE variable_name='mysql-ssl_p2s_cert'",
		"UPDATE global_variables SET variable_value=" + proxyQuote(filepath.Join(dir, "server-key.pem")) + " WHERE variable_name='mysql-ssl_p2s_key'",
		"LOAD MYSQL VARIABLES TO RUNTIME",
		"SAVE MYSQL VARIABLES TO DISK",
		"PROXYSQL RELOAD TLS",
	}
}

// ReloadProxyTLS updates backend TLS contexts and then the frontend contexts.
// Existing sessions retain their negotiated TLS state; new sessions use the
// replaced files. ProxySQL 2.3+ supports the frontend reload command.
func ReloadProxyTLS(ctx context.Context, s myspec.Spec, dir string) error {
	db, err := proxyAdmin(ctx, s)
	if err != nil {
		return err
	}
	defer db.Close()
	for _, statement := range proxyTLSStatements(dir) {
		if _, err = db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (e *Runner) ProxyTLSReloadFallback() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.proxyTLSReloadFallback
}
func applyProxySQL(ctx context.Context, db *sql.DB, s myspec.Spec) error {
	p := s.Proxy
	c, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	// Keep each configuration image atomic in the admin SQLite database; runtime
	// loads are ordered only after the entire image has committed.
	if _, err = c.ExecContext(ctx, "BEGIN"); err != nil {
		return err
	}
	defer c.ExecContext(context.Background(), "ROLLBACK")
	statements := []string{"DELETE FROM mysql_group_replication_hostgroups", "DELETE FROM mysql_galera_hostgroups", "DELETE FROM mysql_servers", "DELETE FROM mysql_users", "DELETE FROM mysql_query_rules"}
	if p.Mode == "galera" {
		statements = append(statements, "INSERT INTO mysql_galera_hostgroups(writer_hostgroup,backup_writer_hostgroup,reader_hostgroup,offline_hostgroup,active,max_writers,writer_is_also_reader,max_transactions_behind) VALUES(10,11,20,30,1,1,2,100)")
	} else if s.Topology != myspec.TopologySingle {
		statements = append(statements, "INSERT INTO mysql_group_replication_hostgroups(writer_hostgroup,backup_writer_hostgroup,reader_hostgroup,offline_hostgroup,active,max_writers,writer_is_also_reader,max_transactions_behind) VALUES(10,11,20,30,1,1,0,100)")
	}
	for _, peer := range p.Backends {
		statements = append(statements, fmt.Sprintf("INSERT INTO mysql_servers(hostgroup_id,hostname,port,use_ssl,max_connections) VALUES(10,%s,%d,1,%d)", proxyQuote(peer.Host), peer.Port, p.MaxConnections))
		if s.Topology == myspec.TopologySingle && (p.ReadWriteSplit || p.ReaderPort) {
			statements = append(statements, fmt.Sprintf("INSERT INTO mysql_servers(hostgroup_id,hostname,port,use_ssl,max_connections) VALUES(20,%s,%d,1,%d)", proxyQuote(peer.Host), peer.Port, p.MaxConnections))
		}
	}
	for _, u := range p.Users {
		ssl := 0
		if u.RequireTLS {
			ssl = 1
		}
		statements = append(statements, fmt.Sprintf("INSERT INTO mysql_users(username,password,active,default_hostgroup,use_ssl,transaction_persistent) VALUES(%s,%s,1,10,%d,1)", proxyQuote(u.Username), proxyQuote(u.Password), ssl))
	}
	for _, r := range proxyRules(p) {
		pattern := "NULL"
		if r.pattern != "" {
			pattern = proxyQuote(r.pattern)
		}
		port := "NULL"
		if r.port > 0 {
			port = strconv.Itoa(r.port)
		}
		statements = append(statements, fmt.Sprintf("INSERT INTO mysql_query_rules(rule_id,active,match_digest,proxy_port,destination_hostgroup,apply) VALUES(%d,1,%s,%s,%d,1)", r.id, pattern, port, r.group))
	}
	for key, value := range map[string]string{"mysql-monitor_username": p.Monitor.Username, "mysql-monitor_password": p.Monitor.Password, "mysql-max_connections": strconv.Itoa(p.MaxConnections)} {
		statements = append(statements, "UPDATE global_variables SET variable_value="+proxyQuote(value)+" WHERE variable_name="+proxyQuote(key))
	}
	for _, q := range statements {
		if _, err = c.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	if _, err = c.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	for _, kind := range []string{"MYSQL VARIABLES", "MYSQL SERVERS", "MYSQL USERS", "MYSQL QUERY RULES"} {
		for _, q := range []string{"LOAD " + kind + " TO RUNTIME", "SAVE " + kind + " TO DISK"} {
			if _, err = c.ExecContext(ctx, q); err != nil {
				return err
			}
		}
	}
	return nil
}
func ProxyStatus(ctx context.Context, s myspec.Spec) (myspec.ProxyStatusResponse, error) {
	var result myspec.ProxyStatusResponse
	db, err := proxyAdmin(ctx, s)
	if err != nil {
		return result, err
	}
	defer db.Close()
	rows, err := db.QueryContext(ctx, "SELECT s.hostgroup_id,s.hostname,s.port,s.status,COALESCE(p.ConnUsed,0),COALESCE(p.ConnFree,0) FROM runtime_mysql_servers s LEFT JOIN stats_mysql_connection_pool p ON s.hostgroup_id=p.hostgroup AND s.hostname=p.srv_host AND s.port=p.srv_port ORDER BY s.hostgroup_id,s.hostname,s.port")
	if err != nil {
		return result, err
	}
	defer rows.Close()
	for rows.Next() {
		var b myspec.ProxyBackend
		if err = rows.Scan(&b.Hostgroup, &b.Host, &b.Port, &b.Status, &b.ConnectionsUsed, &b.ConnectionsFree); err != nil {
			return result, err
		}
		result.Backends = append(result.Backends, b)
	}
	if err = rows.Err(); err != nil {
		return result, err
	}
	rows.Close()
	proxyDiagnostics(ctx, db, result.Backends)
	return result, nil
}
func ProxyReady(ctx context.Context, s myspec.Spec) error {
	result, err := ProxyStatus(ctx, s)
	if err != nil {
		return err
	}
	writers := 0
	for _, b := range result.Backends {
		if b.Hostgroup != 10 || b.Status != "ONLINE" {
			continue
		}
		known := false
		for _, p := range s.Proxy.Backends {
			if p.Host == b.Host && p.Port == b.Port {
				known = true
				break
			}
		}
		if !known {
			return errors.New("proxy writer is not configured")
		}
		writers++
	}
	if writers != 1 {
		return errors.New("proxy does not know exactly one online writer")
	}
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", "127.0.0.1:3306")
	if err != nil {
		return err
	}
	return conn.Close()
}
func RegisterProxyRoutes(mux *http.ServeMux, d GroupDeps) {
	mux.HandleFunc("POST /v1/mysql/proxy-status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		result, err := ProxyStatus(ctx, d.GroupSpec())
		if err != nil {
			http.Error(w, "MySQL proxy observation unavailable", http.StatusServiceUnavailable)
			return
		}
		if status, ok := d.(interface{ ProxyTLSReloadFallback() bool }); ok {
			result.TLSReloadFallback = status.ProxyTLSReloadFallback()
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})
}
