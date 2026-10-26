// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func optionQuote(s string) string {
	return `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`, "\r", `\r`).Replace(s) + `"`
}
func sqlLiteral(s string) string {
	return "'" + strings.NewReplacer("\\", "\\\\", "'", "''").Replace(s) + "'"
}
func renderConfig(s myspec.Spec, p paths) string {
	expire := 604800
	if s.Backup != nil {
		expire = s.Backup.Binlog.ExpireSeconds
	}
	opts := map[string]string{
		"basedir": s.ServerDir, "datadir": p.data, "socket": p.socket, "pid_file": filepath.Join(p.run, "mysql.pid"), "log_error": filepath.Join(p.root, "mysql-error.log"),
		"bind_address": "0.0.0.0", "port": "3306", "mysqlx": "OFF", "server_id": strconv.FormatUint(uint64(s.Ordinal)+1, 10), "report_host": nativeReportHost(s),
		"gtid_mode": "ON", "enforce_gtid_consistency": "ON", "log_bin": filepath.Join(p.data, "binlog"), "binlog_expire_logs_seconds": strconv.Itoa(expire),
		"ssl_ca": filepath.Join(p.root, "ca.pem"), "ssl_cert": filepath.Join(p.root, "server-cert.pem"), "ssl_key": filepath.Join(p.root, "server-key.pem"), "require_secure_transport": "ON",
		"innodb_buffer_pool_size": strconv.FormatInt(s.BufferPoolBytes, 10), "max_connections": strconv.Itoa(s.MaxConnections), "skip_replica_start": "ON", "persisted_globals_load": "OFF",
		"init_file": filepath.Join(p.root, "startup.sql"),
	}
	host, port := nativeServerListener(s)
	opts["bind_address"] = host
	opts["port"] = strconv.Itoa(port)
	if strings.HasPrefix(s.Version, "8.0.") {
		opts["binlog_format"] = "ROW"
	}
	for k, v := range s.Parameters {
		opts[k] = v
	}
	// SQL must not read runtime credentials or replication private keys through
	// LOAD_FILE, even for a database administrator with unrelated global grants.
	opts["secure_file_priv"] = "NULL"
	opts["innodb_fast_shutdown"] = "1"
	if s.SlowShutdown {
		opts["innodb_fast_shutdown"] = "0"
	}
	if s.Topology == myspec.TopologyGroup {
		for k, v := range RenderGroupOptions(s, p.root) {
			opts[k] = v
		}
		opts["group_replication_start_on_boot"] = "OFF"
		// Fence every process start, including restarts that do not re-key
		// accounts. GR lifts this only when the member joins as primary.
		opts["super_read_only"] = "ON"
	}
	if s.Galera != nil {
		for k, v := range RenderGaleraOptions(s, p.root) {
			opts[k] = v
		}
	}
	keys := make([]string, 0, len(opts))
	for k := range opts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("[mysqld]\n")
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s\n", k, optionQuote(opts[k]))
	}
	if s.Galera != nil {
		b.WriteString(renderGaleraSST(p.root))
	}
	return b.String()
}

// Accounts are local, binlog-disabled and identical on every member before GR.
// Clone replaces them with the donor's identical system accounts. The protected
// startup file also re-keys restored data without knowing the source password.
func renderAccounts(s myspec.Spec) string {
	var b strings.Builder
	if s.Galera != nil {
		b.WriteString("SET SESSION wsrep_on=OFF;\n")
	}
	// This file runs before the server admits network clients. Re-keying a
	// copied secondary must briefly lift its inherited local write fence.
	b.WriteString("SET SESSION sql_log_bin=0;\nSET SESSION sql_mode='';\nSET GLOBAL super_read_only=OFF;\n")
	for _, c := range []myspec.Credentials{s.Superuser, s.Agent, s.BackupUser, s.Monitor, s.Replication, s.Admin} {
		host := "%"
		secure := " REQUIRE SSL"
		if c.Username == myspec.UserRoot || c.Username == myspec.UserAgent || c.Username == myspec.UserBackup {
			host = "localhost"
			secure = ""
		}
		a := sqlLiteral(c.Username) + "@" + sqlLiteral(host)
		fmt.Fprintf(&b, "CREATE USER IF NOT EXISTS %s IDENTIFIED BY %s%s;\nALTER USER %s IDENTIFIED BY %s%s;\n", a, sqlLiteral(c.Password), secure, a, sqlLiteral(c.Password), secure)
	}
	b.WriteString("GRANT SYSTEM_USER ON *.* TO 'kk_agent'@'localhost','kk_backup'@'localhost','kk_monitor'@'%','kk_repl'@'%';\nGRANT ALL PRIVILEGES ON *.* TO 'admin'@'%' WITH GRANT OPTION;\nREVOKE SYSTEM_USER,SHUTDOWN,GROUP_REPLICATION_ADMIN,CLONE_ADMIN,PERSIST_RO_VARIABLES_ADMIN,REPLICATION_SLAVE_ADMIN,BINLOG_ADMIN ON *.* FROM 'admin'@'%';\nREVOKE FILE ON *.* FROM 'admin'@'%';\n")
	b.WriteString("GRANT ALL PRIVILEGES ON *.* TO 'root'@'localhost' WITH GRANT OPTION;\nGRANT ALL PRIVILEGES ON *.* TO 'kk_agent'@'localhost' WITH GRANT OPTION;\nGRANT SYSTEM_USER,SYSTEM_VARIABLES_ADMIN,CONNECTION_ADMIN,BACKUP_ADMIN,CLONE_ADMIN,GROUP_REPLICATION_ADMIN,REPLICATION_SLAVE_ADMIN ON *.* TO 'kk_agent'@'localhost' WITH GRANT OPTION;\nGRANT BACKUP_ADMIN,PROCESS,RELOAD,LOCK TABLES,REPLICATION CLIENT,SYSTEM_VARIABLES_ADMIN ON *.* TO 'kk_backup'@'localhost';\nGRANT SELECT ON performance_schema.log_status TO 'kk_backup'@'localhost';\nGRANT SELECT ON performance_schema.keyring_component_status TO 'kk_backup'@'localhost';\nGRANT REPLICATION CLIENT ON *.* TO 'kk_monitor'@'%';\nGRANT SELECT ON performance_schema.* TO 'kk_monitor'@'%';\nGRANT SELECT ON sys.* TO 'kk_monitor'@'%';\nGRANT REPLICATION SLAVE,BACKUP_ADMIN,CLONE_ADMIN,GROUP_REPLICATION_STREAM,CONNECTION_ADMIN ON *.* TO 'kk_repl'@'%';\n")
	if s.Topology == myspec.TopologyGroup {
		b.WriteString("SET GLOBAL super_read_only=ON;\n")
	}
	return b.String()
}
func renderStartup(s myspec.Spec, p paths, rekey bool) string {
	var b strings.Builder
	if rekey {
		return renderAccounts(s)
	}
	// Privilege policy is independent of password receipts. Enforce it before
	// admitting clients even when an existing/restored account needs no re-key.
	// A fresh SST joiner may not have the donor's admin account yet.
	if s.Galera != nil {
		b.WriteString("SET SESSION wsrep_on=OFF;\n")
	}
	b.WriteString("SET SESSION sql_log_bin=0;\nSET GLOBAL super_read_only=OFF;\nREVOKE IF EXISTS FILE ON *.* FROM 'admin'@'%' IGNORE UNKNOWN USER;\n")
	if s.Topology == myspec.TopologyGroup {
		b.WriteString("SET GLOBAL super_read_only=ON;\n")
	}
	return b.String()
}
