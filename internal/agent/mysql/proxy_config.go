// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// proxyTable is the agent-owned projection of a ProxySQL configuration table.
// SQL literals are shared by the comparison and write paths, including NULLs.
type proxyTable struct {
	name, columns string
	rows          []string
}

// ProxySQL mysql_query_rules defaults that affect matching or execution.
// Keep defaults explicit so memory/runtime drift restores the same behavior as
// deleting and reinserting a rule, rather than accepting an altered flag chain.
var proxyRuleDefaults = [...]struct{ column, value string }{
	{"username", "NULL"},
	{"schemaname", "NULL"},
	{"flagIN", "0"},
	{"client_addr", "NULL"},
	{"proxy_addr", "NULL"},
	{"digest", "NULL"},
	{"match_pattern", "NULL"},
	{"negate_match_pattern", "0"},
	{"re_modifiers", "'CASELESS'"},
	{"flagOUT", "NULL"},
	{"replace_pattern", "NULL"},
	{"cache_ttl", "NULL"},
	{"cache_empty_result", "NULL"},
	{"cache_timeout", "NULL"},
	{"reconnect", "NULL"},
	{"timeout", "NULL"},
	{"retries", "NULL"},
	{"delay", "NULL"},
	{"next_query_flagIN", "NULL"},
	{"mirror_flagOUT", "NULL"},
	{"mirror_hostgroup", "NULL"},
	{"error_msg", "NULL"},
	{"OK_msg", "NULL"},
	{"sticky_conn", "NULL"},
	{"multiplex", "NULL"},
	{"gtid_from_hostgroup", "NULL"},
	{"log", "NULL"},
	{"attributes", "''"},
}

func proxyCluster(s myspec.Spec) bool {
	return s.Proxy.Mode == "galera" || s.Topology != myspec.TopologySingle
}

func proxyConfigImage(s myspec.Spec) []proxyTable {
	p := s.Proxy
	groups := "writer_hostgroup,backup_writer_hostgroup,reader_hostgroup,offline_hostgroup,active,max_writers,writer_is_also_reader,max_transactions_behind"
	image := []proxyTable{
		{name: "mysql_group_replication_hostgroups", columns: groups},
		{name: "mysql_galera_hostgroups", columns: groups},
		{name: "mysql_servers", columns: "hostgroup_id,hostname,port,use_ssl,max_connections,status"},
		{name: "mysql_users", columns: "username,password,active,default_hostgroup,use_ssl,transaction_persistent,frontend,backend"},
		{name: "mysql_query_rules", columns: "rule_id,active,match_digest,proxy_port,destination_hostgroup,apply"},
		{name: "global_variables", columns: "variable_name,variable_value"},
	}
	// Rules are agent-owned in full: altered match filters, flag chains or
	// actions must not silently disable the locking-SELECT safeguard.
	var defaultColumns, defaultValues strings.Builder
	for _, field := range proxyRuleDefaults {
		defaultColumns.WriteString(",")
		defaultColumns.WriteString(field.column)
		defaultValues.WriteString(",")
		defaultValues.WriteString(field.value)
	}
	image[4].columns += defaultColumns.String()
	if p.Mode == "galera" {
		image[1].rows = []string{"10,11,20,30,1,1,2,100"}
	} else if proxyCluster(s) {
		image[0].rows = []string{"10,11,20,30,1,1,0,100"}
	}
	group := 10
	if proxyCluster(s) {
		group = 20
	}
	for _, peer := range p.Backends {
		image[2].rows = append(image[2].rows, fmt.Sprintf("%d,%s,%d,1,%d,'ONLINE'", group, proxyQuote(peer.Host), peer.Port, p.MaxConnections))
		if !proxyCluster(s) && (p.ReadWriteSplit || p.ReaderPort) {
			image[2].rows = append(image[2].rows, fmt.Sprintf("20,%s,%d,1,%d,'ONLINE'", proxyQuote(peer.Host), peer.Port, p.MaxConnections))
		}
	}
	for _, u := range p.Users {
		ssl := 0
		if u.RequireTLS {
			ssl = 1
		}
		image[3].rows = append(image[3].rows, fmt.Sprintf("%s,%s,1,10,%d,1,1,1", proxyQuote(u.Username), proxyQuote(u.Password), ssl))
	}
	for _, r := range proxyRules(p) {
		pattern, port := "NULL", "NULL"
		if r.pattern != "" {
			pattern = proxyQuote(r.pattern)
		}
		if r.port > 0 {
			port = strconv.Itoa(r.port)
		}
		image[4].rows = append(image[4].rows, fmt.Sprintf("%d,1,%s,%s,%d,1", r.id, pattern, port, r.group)+defaultValues.String())
	}
	for _, v := range [][2]string{{"mysql-monitor_username", p.Monitor.Username}, {"mysql-monitor_password", p.Monitor.Password}, {"mysql-max_connections", strconv.Itoa(p.MaxConnections)}} {
		image[5].rows = append(image[5].rows, proxyQuote(v[0])+","+proxyQuote(v[1]))
	}
	return image
}

func (t proxyTable) desired() string {
	if len(t.rows) == 0 {
		return "SELECT " + t.columns + " FROM " + t.name + " WHERE 0"
	}
	// VALUES plus a CTE avoids parsing quoted commas in credential literals.
	return "WITH desired(" + t.columns + ") AS (VALUES (" + strings.Join(t.rows, "),(") + ")) SELECT * FROM desired"
}

func proxyProjection(s myspec.Spec, t proxyTable, runtime bool) string {
	columns := t.columns
	if t.name == "mysql_servers" && proxyCluster(s) {
		// The monitor owns placement, status, and duplicate reader membership.
		// An unexpected hostgroup still differs and is repaired.
		columns = strings.Replace(columns, "hostgroup_id", "CASE WHEN hostgroup_id IN (10,11,20,30) THEN 0 ELSE hostgroup_id END", 1)
		columns = strings.Replace(columns, "status", "'ONLINE'", 1)
	} else if t.name == "mysql_servers" {
		// SHUNNED is transient connection-monitor state, not an admin hold.
		columns = strings.Replace(columns, "status", "CASE WHEN status='SHUNNED' THEN 'ONLINE' ELSE status END", 1)
	}
	if t.name == "mysql_users" && runtime && len(s.Proxy.Users) > 0 {
		// mysql-hash_passwords can expose native hashes rather than the input
		// plaintext. Runtime also has separate frontend/backend rows.
		password := "CASE password"
		for _, u := range s.Proxy.Users {
			a := sha1.Sum([]byte(u.Password))
			b := sha1.Sum(a[:])
			password += " WHEN " + proxyQuote(fmt.Sprintf("*%X", b)) + " THEN " + proxyQuote(u.Password)
		}
		password += " ELSE password END"
		columns = strings.Replace(columns, "password", password, 1)
	}
	if t.name == "mysql_users" {
		columns = strings.Replace(columns, "frontend,backend", "MAX(frontend),MAX(backend)", 1)
	}
	return columns
}

func proxyConfigMatches(ctx context.Context, c *sql.Conn, s myspec.Spec, image []proxyTable) (bool, error) {
	for _, t := range image {
		for _, runtime := range []bool{false, true} {
			name := t.name
			if runtime {
				name = "runtime_" + name
			}
			actual := "SELECT DISTINCT " + proxyProjection(s, t, runtime) + " FROM " + name
			if t.name == "global_variables" {
				actual += " WHERE variable_name IN ('mysql-monitor_username','mysql-monitor_password','mysql-max_connections')"
			}
			if t.name == "mysql_users" {
				actual += " GROUP BY username,password,active,default_hostgroup,use_ssl,transaction_persistent"
			}
			desired := "SELECT DISTINCT " + proxyProjection(s, t, false) + " FROM (" + t.desired() + ")"
			if t.name == "mysql_users" {
				desired += " GROUP BY username,password,active,default_hostgroup,use_ssl,transaction_persistent"
			}
			query := "SELECT NOT EXISTS (SELECT * FROM (" + actual + " EXCEPT " + desired + ")) AND NOT EXISTS (SELECT * FROM (" + desired + " EXCEPT " + actual + "))"
			var equal bool
			if err := c.QueryRowContext(ctx, query).Scan(&equal); err != nil {
				return false, err
			}
			if !equal {
				return false, nil
			}
		}
	}
	return true, nil
}
