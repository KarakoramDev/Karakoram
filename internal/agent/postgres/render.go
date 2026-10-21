// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"gopkg.in/yaml.v3"
)

type renderedFile struct {
	name, content string
	executable    bool
}
type paths struct{ root, pgdata, socket, etcd, proxy string }

func dataPaths(dir string) paths {
	root := filepath.Join(dir, "karakoram", "postgres")
	return paths{root: root, pgdata: filepath.Join(dir, "postgres"), socket: filepath.Join(root, "socket"), etcd: filepath.Join(dir, "etcd"), proxy: filepath.Join(root, "proxy")}
}
func listenHost(host string) string {
	if host == "" {
		return "0.0.0.0"
	}
	return host
}
func proxyInterfaces(host string, reader bool) string {
	interfaces := net.JoinHostPort(listenHost(host), "5432")
	if reader {
		interfaces += ";" + net.JoinHostPort(listenHost(host), "5433")
	}
	return interfaces
}
func frontendAddress(host string) string {
	if host == "::" {
		return "[::1]:5432"
	}
	return "127.0.0.1:5432"
}
func pgQuote(v string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(v, `\`, `\\`), "'", "''") + "'"
}
func sqlQuote(v string) string   { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }
func sqlIdent(v string) string   { return `"` + strings.ReplaceAll(v, `"`, `""`) + `"` }
func shellQuote(v string) string { return "'" + strings.ReplaceAll(v, "'", `'"'"'`) + "'" }
func pgbackrestCommand(p paths, name string) string {
	return "/usr/bin/pgbackrest --config=" + shellQuote(filepath.Join(p.root, name))
}
func archiveTimeout(s pgspec.Spec) int {
	if s.Backup != nil && s.Backup.ArchiveTimeoutSeconds > 0 {
		return s.Backup.ArchiveTimeoutSeconds
	}
	return 60
}
func parameters(s pgspec.Spec, p paths) map[string]any {
	m := map[string]any{"wal_level": "replica", "wal_log_hints": "on", "hot_standby": "on", "max_wal_senders": 10, "max_replication_slots": 10}
	for k, v := range s.Postgres.Parameters {
		m[k] = v
	}
	for k, v := range map[string]any{"listen_addresses": "*", "port": 5432, "unix_socket_directories": p.socket, "password_encryption": "scram-sha-256", "archive_mode": "on", "archive_command": "/bin/true", "archive_timeout": archiveTimeout(s), "hba_file": filepath.Join(p.root, "pg_hba.conf")} {
		m[k] = v
	}
	if s.ListenHost != "" {
		m["listen_addresses"] = s.ListenHost
	}
	if len(s.Postgres.SharedPreloadLibraries) > 0 {
		m["shared_preload_libraries"] = strings.Join(s.Postgres.SharedPreloadLibraries, ",")
	}
	if s.Citus != nil {
		libraries := []string{"citus"}
		for _, library := range s.Postgres.SharedPreloadLibraries {
			if library != "citus" {
				libraries = append(libraries, library)
			}
		}
		m["shared_preload_libraries"] = strings.Join(libraries, ",")
		m["wal_level"] = "logical"
	}
	if s.Backup != nil {
		m["archive_command"] = pgbackrestCommand(p, "pgbackrest.conf") + " --stanza=" + shellQuote(s.Backup.Repository.Stanza) + " archive-push %p"
	}
	if s.Bootstrap != nil && s.Bootstrap.Method == "restore" && s.Bootstrap.Restore != nil && (s.Backup == nil || s.Backup.Repository.Path != s.Bootstrap.Restore.Source.Path) {
		m["archive_mode"] = "off"
	}
	if s.ServerCert != nil {
		m["ssl"] = "on"
		m["ssl_cert_file"] = filepath.Join(p.root, "server.pem")
		m["ssl_key_file"] = filepath.Join(p.root, "server.key")
		m["ssl_ca_file"] = filepath.Join(p.root, "ca.pem")
	} else {
		m["ssl"] = "off"
	}
	return m
}
func hba(s pgspec.Spec) []string {
	lines := []string{"local all all scram-sha-256", "local replication all scram-sha-256", "host replication " + s.Replication.Username + " 127.0.0.1/32 scram-sha-256", "host replication " + s.Replication.Username + " ::1/128 scram-sha-256"}
	cidrs := s.Postgres.ClientCIDRs
	if len(cidrs) == 0 {
		cidrs = []string{"0.0.0.0/0", "::/0"}
	}
	kind := "host"
	if s.Postgres.RequireTLS {
		kind = "hostssl"
	}
	for _, c := range cidrs {
		if s.Topology == pgspec.TopologyHA || s.ServerCert != nil {
			lines = append(lines, "hostssl replication "+s.Replication.Username+" "+c+" scram-sha-256")
		}
		// Keep remote clients fenced while source storage/system credentials
		// are present. Local management and replica rebuilds must still work.
		if !s.Postgres.RecoveryClientsHeld && (s.Bootstrap == nil || s.Bootstrap.Method != "restore") {
			lines = append(lines, kind+" all all "+c+" scram-sha-256")
		}
	}
	return lines
}
func renderPostgres(s pgspec.Spec, p paths) string {
	m := parameters(s, p)
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s = %s\n", k, pgQuote(fmt.Sprint(m[k])))
	}
	return b.String()
}
func repositoryLines(b *strings.Builder, index int, r pgspec.Repository, p paths, caFile string) {
	line := func(k string, v any) { fmt.Fprintf(b, "repo%d-%s=%v\n", index, k, v) }
	line("type", "s3")
	line("path", r.Path)
	line("s3-endpoint", r.S3.Endpoint)
	line("s3-bucket", r.S3.Bucket)
	line("s3-region", r.S3.Region)
	line("s3-key", r.S3.AccessKey)
	line("s3-key-secret", r.S3.SecretKey)
	if r.S3.Port > 0 {
		line("storage-port", r.S3.Port)
	}
	style := "host"
	if r.S3.PathStyle {
		style = "path"
	}
	line("s3-uri-style", style)
	line("storage-verify-tls", map[bool]string{true: "y", false: "n"}[r.S3.VerifyTLS])
	if r.S3.CAPEM != "" {
		line("storage-ca-file", filepath.Join(p.root, caFile))
	}
	line("retention-full", 9999999)
	line("retention-full-type", "count")
}

func renderRepository(r pgspec.Repository, p paths, caFile, username string) string {
	var b strings.Builder
	line := func(k string, v any) { fmt.Fprintf(&b, "%s=%v\n", k, v) }
	b.WriteString("[global]\n")
	repositoryLines(&b, 1, r, p, caFile)
	line("expire-auto", "n")
	line("log-level-console", "warn")
	line("log-level-file", "off")
	line("lock-path", filepath.Join(p.root, "backup-lock"))
	line("spool-path", filepath.Join(p.root, "backup-spool"))
	fmt.Fprintf(&b, "\n[%s]\n", r.Stanza)
	line("pg1-path", p.pgdata)
	line("pg1-socket-path", p.socket)
	line("pg1-user", username)
	line("pg1-database", "postgres")
	return b.String()
}
func restoreArgs(s pgspec.Spec, p paths) ([]string, error) {
	r := s.Bootstrap.Restore
	if r.TargetTime != "" && r.TargetName != "" {
		return nil, fmt.Errorf("restore target time and name are mutually exclusive")
	}
	target := ""
	if r.TargetTime != "" {
		stamp, err := time.Parse(time.RFC3339Nano, r.TargetTime)
		if err != nil {
			return nil, fmt.Errorf("invalid restore target time: %w", err)
		}
		// PostgreSQL recovery_target_time does not accept RFC 3339's T/Z
		// spelling. Use an explicit UTC offset and database microsecond precision.
		target = stamp.UTC().Format("2006-01-02 15:04:05.999999-07:00")
	}
	args := []string{"--config=" + filepath.Join(p.root, "pgbackrest-restore.conf"), "--stanza=" + r.Source.Stanza, "--pg1-path=" + p.pgdata, "--delta", "--target-action=promote"}
	if s.Backup == nil || s.Backup.Repository.Path != r.Source.Path {
		args = append(args, "--archive-mode=off")
	}
	if r.Set != "" {
		args = append(args, "--set="+r.Set)
	}
	if r.TargetTime != "" {
		args = append(args, "--type=time", "--target="+target)
	} else if r.TargetName != "" {
		args = append(args, "--type=name", "--target="+r.TargetName)
	} else {
		args = append(args, "--type=immediate")
	}
	return append(args, "restore"), nil
}

var localParameters = []string{"listen_addresses", "port", "unix_socket_directories", "hba_file", "ssl_cert_file", "ssl_key_file", "ssl_ca_file", "archive_command"}

func dynamicConfig(s pgspec.Spec, p paths) map[string]any {
	params := parameters(s, p)
	for _, name := range localParameters {
		delete(params, name)
	}
	// A planned handoff should not spend two seconds awaiting each HA cycle.
	// Keep the DCS lease and retry budget: 1 + 2*5 <= 20, without weakening
	// fencing or synchronous commit when the platform kills a runtime abruptly.
	return map[string]any{"ttl": 20, "loop_wait": 1, "retry_timeout": 5, "synchronous_mode": s.Postgres.Synchronous == pgspec.SyncOn || s.Postgres.Synchronous == pgspec.SyncStrict, "synchronous_mode_strict": s.Postgres.Synchronous == pgspec.SyncStrict, "synchronous_node_count": 1, "postgresql": map[string]any{"use_pg_rewind": true, "use_slots": true, "parameters": params}}
}
func localHost(s pgspec.Spec) string {
	for _, p := range s.Peers {
		if p.Name == s.Member {
			return p.Host
		}
	}
	return "127.0.0.1"
}
func renderPatroni(s pgspec.Spec, p paths) (string, error) {
	auth := func(c pgspec.Credentials) map[string]any {
		return map[string]any{"username": c.Username, "password": c.Password, "sslmode": "verify-full", "sslrootcert": filepath.Join(p.root, "ca.pem"), "sslcert": filepath.Join(p.root, "server.pem"), "sslkey": filepath.Join(p.root, "server.key")}
	}
	rest := map[string]any{"listen": net.JoinHostPort(listenHost(s.ListenHost), "8008"), "connect_address": net.JoinHostPort(localHost(s), "8008"), "certfile": filepath.Join(p.root, "server.pem"), "keyfile": filepath.Join(p.root, "server.key"), "cafile": filepath.Join(p.root, "ca.pem"), "verify_client": "optional", "authentication": map[string]string{"username": s.PatroniREST.Username, "password": s.PatroniREST.Password}}
	hosts := []string{}
	for _, v := range s.Peers {
		if v.Etcd != "" {
			hosts = append(hosts, net.JoinHostPort(v.Host, "2379"))
		}
	}
	if len(hosts) == 0 {
		for _, v := range s.Peers {
			hosts = append(hosts, net.JoinHostPort(v.Host, "2379"))
		}
	}
	if s.Etcd != nil && len(s.Etcd.Endpoints) > 0 {
		hosts = append([]string(nil), s.Etcd.Endpoints...)
	}
	bootstrap := map[string]any{"dcs": dynamicConfig(s, p), "initdb": []any{"data-checksums", map[string]string{"encoding": "UTF8"}, map[string]string{"locale": "C.UTF-8"}, map[string]string{"auth-host": "scram-sha-256"}, map[string]string{"auth-local": "scram-sha-256"}}, "post_bootstrap": filepath.Join(p.root, "bootstrap-roles.sh")}
	pg := map[string]any{"listen": net.JoinHostPort(listenHost(s.ListenHost), "5432"), "connect_address": net.JoinHostPort(localHost(s), "5432"), "data_dir": p.pgdata, "bin_dir": s.BinDir, "pgpass": filepath.Join(p.root, "patroni.pgpass"), "authentication": map[string]any{"superuser": auth(s.Superuser), "replication": auth(s.Replication), "rewind": auth(s.Rewind)}, "pg_hba": hba(s), "parameters": parameters(s, p), "remove_data_directory_on_rewind_failure": true, "remove_data_directory_on_diverged_timelines": true, "create_replica_methods": []string{"basebackup"}}
	// Keep Patroni's native initdb/password/bootstrap lifecycle, but consume
	// first-initialization authorization before the real binary can run.
	pg["bin_name"] = map[string]string{"initdb": filepath.Join(p.root, "bootstrap-initdb.sh")}
	// Local management and replication probes use the private socket. A wildcard
	// TCP listen address is not a certificate identity; remote replication still
	// uses the advertised member hostname with verify-full authentication.
	pg["use_unix_socket"] = true
	pg["use_unix_socket_repl"] = true
	if s.Backup != nil {
		command := pgbackrestCommand(p, "pgbackrest.conf") + " --stanza=" + shellQuote(s.Backup.Repository.Stanza)
		pg["recovery_conf"] = map[string]string{"restore_command": command + " archive-get %f %p"}
		if s.Postgres.PgBackRestReplicas {
			pg["create_replica_methods"] = []string{"pgbackrest", "basebackup"}
			pg["pgbackrest"] = map[string]any{"command": command + " --delta --type=standby restore", "keep_data": true, "no_params": true, "no_leader": true}
		}
	}
	if s.Bootstrap != nil && s.Bootstrap.Method == "restore" {
		bootstrap["method"] = "pgbackrest"
		bootstrap["pgbackrest"] = map[string]any{"command": filepath.Join(p.root, "bootstrap-restore.sh"), "keep_existing_recovery_conf": true, "no_params": true}
	}
	m := map[string]any{"scope": s.Scope, "namespace": "/karakoram/", "name": s.Member, "restapi": rest, "etcd3": map[string]any{"hosts": hosts, "protocol": "https", "cacert": filepath.Join(p.root, "ca.pem"), "cert": filepath.Join(p.root, "server.pem"), "key": filepath.Join(p.root, "server.key")}, "bootstrap": bootstrap, "postgresql": pg}
	if s.Citus != nil {
		m["citus"] = map[string]any{"group": s.Citus.Group, "database": s.Citus.Database}
	}
	if s.Retiring {
		m["tags"] = map[string]bool{"nofailover": true, "noloadbalance": true, "clonefrom": false}
	}
	if s.Bootstrap == nil || s.Bootstrap.Method != "restore" && !s.Bootstrap.AllowInitDB || s.Bootstrap.SeedMember != "" && s.Bootstrap.SeedMember != s.Member {
		delete(m, "bootstrap")
	}
	data, err := yaml.Marshal(m)
	return string(data), err
}
func patroniProcess(p paths) agent.ProcessSpec {
	return agent.ProcessSpec{Binary: "/usr/bin/patroni", Args: []string{filepath.Join(p.root, "patroni.yml")}, Env: []string{"LANG=C.UTF-8", "LC_ALL=C.UTF-8"}, Dir: p.root}
}
func etcdProcess(s pgspec.Spec, p paths) agent.ProcessSpec {
	cert := filepath.Join(p.root, "server.pem")
	key := filepath.Join(p.root, "server.key")
	ca := filepath.Join(p.root, "ca.pem")
	return agent.ProcessSpec{Binary: "/usr/bin/etcd", Dir: p.root, Args: []string{"--name=" + s.Member, "--data-dir=" + p.etcd, "--listen-client-urls=https://" + net.JoinHostPort(listenHost(s.ListenHost), "2379"), "--advertise-client-urls=https://" + net.JoinHostPort(localHost(s), "2379"), "--listen-peer-urls=https://" + net.JoinHostPort(listenHost(s.ListenHost), "2380"), "--initial-advertise-peer-urls=https://" + net.JoinHostPort(localHost(s), "2380"), "--initial-cluster=" + s.Etcd.InitialCluster, "--initial-cluster-state=" + s.Etcd.State, "--initial-cluster-token=" + s.Etcd.Token, "--client-cert-auth=true", "--trusted-ca-file=" + ca, "--cert-file=" + cert, "--key-file=" + key, "--peer-client-cert-auth=true", "--peer-trusted-ca-file=" + ca, "--peer-cert-file=" + cert, "--peer-key-file=" + key}}
}
func renderProxy(p *pgspec.Proxy, paths paths, host string) string {
	var config strings.Builder
	fmt.Fprintf(&config, "datadir=%s\nadmin_variables={\n admin_credentials=%s;\n mysql_ifaces=\"127.0.0.1:6032\";\n pgsql_ifaces=\"127.0.0.1:6132\";\n}\nmysql_variables={\n monitor_ping_interval=1000;\n}\npgsql_variables={\n",
		strconv.Quote(paths.proxy), strconv.Quote(p.Admin.Username+":"+p.Admin.Password))
	settings := proxyRuntimeSettings(p, filepath.Join(paths.root, "ca.pem"))
	for name, value := range proxyStartupSettings(p, host) {
		settings[name] = value
	}
	for _, key := range sortedKeys(settings) {
		value := settings[key]
		switch key {
		case "pgsql-interfaces", "pgsql-monitor_username", "pgsql-monitor_password", "pgsql-monitor_dbname", "pgsql-ssl_p2s_ca":
			value = strconv.Quote(value)
		}
		fmt.Fprintf(&config, " %s=%s;\n", strings.TrimPrefix(key, "pgsql-"), value)
	}
	config.WriteString("}\n")
	return config.String()
}
func render(s pgspec.Spec, p paths, statements []string) ([]renderedFile, error) {
	files := []renderedFile{}
	if s.CAPEM != "" {
		files = append(files, renderedFile{"ca.pem", s.CAPEM, false})
	}
	if s.ServerCert != nil {
		files = append(files, renderedFile{"server.pem", s.ServerCert.CertPEM, false}, renderedFile{"server.key", s.ServerCert.KeyPEM, false})
	}
	if s.Role == pgspec.RoleProxy {
		files = append(files, renderedFile{"proxysql.cnf", renderProxy(s.Proxy, p, s.ListenHost), false})
		if s.Proxy.ServerCert != nil {
			files = append(files, renderedFile{"proxy/proxysql-cert.pem", s.Proxy.ServerCert.CertPEM, false}, renderedFile{"proxy/proxysql-key.pem", s.Proxy.ServerCert.KeyPEM, false}, renderedFile{"proxy/proxysql-ca.pem", s.CAPEM, false})
		}
		return files, nil
	}
	if statements == nil {
		var err error
		statements, err = roleStatements(s)
		if err != nil {
			return nil, err
		}
	}
	rolesSQL := strings.Join(statements, ";\n") + ";\n"
	if s.Bootstrap != nil && s.Bootstrap.Method == "restore" {
		// The post-bootstrap psql hook must keep the restore-only SET LOCAL
		// scoped to the same transaction as the recovered identity writes.
		rolesSQL = "BEGIN;\nSET LOCAL synchronous_commit = local;\n" + rolesSQL + "COMMIT;\n"
	}
	files = append(files, renderedFile{"postgresql.conf", renderPostgres(s, p), false}, renderedFile{"pg_hba.conf", strings.Join(hba(s), "\n") + "\n", false}, renderedFile{"roles.sql", rolesSQL, false})
	pass := strings.NewReplacer(`\`, `\\`, ":", `\:`)
	files = append(files, renderedFile{"backup.pgpass", "*:5432:*:" + pass.Replace(s.Superuser.Username) + ":" + pass.Replace(s.Superuser.Password) + "\n", false})
	if s.Backup != nil {
		config := renderRepository(s.Backup.Repository, p, "s3-ca.pem", s.Superuser.Username)
		var extra strings.Builder
		for i, repository := range s.Backup.AdditionalRepositories {
			ca := fmt.Sprintf("s3-ca-%d.pem", i+2)
			repositoryLines(&extra, i+2, repository, p, ca)
			if repository.S3.CAPEM != "" {
				files = append(files, renderedFile{ca, repository.S3.CAPEM, false})
			}
		}
		config = strings.Replace(config, "[global]\n", "[global]\n"+extra.String(), 1)
		files = append(files, renderedFile{"pgbackrest.conf", config, false})
		if s.Backup.Repository.S3.CAPEM != "" {
			files = append(files, renderedFile{"s3-ca.pem", s.Backup.Repository.S3.CAPEM, false})
		}
	}
	if s.Bootstrap != nil && s.Bootstrap.Method == "restore" {
		files = append(files, renderedFile{"pgbackrest-restore.conf", renderRepository(s.Bootstrap.Restore.Source, p, "restore-s3-ca.pem", s.Superuser.Username), false})
		if s.Bootstrap.Restore.Source.S3.CAPEM != "" {
			files = append(files, renderedFile{"restore-s3-ca.pem", s.Bootstrap.Restore.Source.S3.CAPEM, false})
		}
		cmd := "#!/bin/sh\nset -eu\numask 077\nexport PGPASSFILE=" + shellQuote(filepath.Join(p.root, "backup.pgpass")) + "\n/usr/bin/pgbackrest"
		args, err := restoreArgs(s, p)
		if err != nil {
			return nil, err
		}
		for _, a := range args {
			cmd += " " + shellQuote(a)
		}
		marker := filepath.Join(p.root, "bootstrap-generation.json")
		cmd += "\nprintf '%s\\n' " + shellQuote(strconv.Itoa(s.Bootstrap.Generation)) + " > " + shellQuote(marker+".tmp")
		// With a filename, sync uses fsync rather than filesystem-wide syncfs,
		// which restricted runtimes may forbid even though file writes are safe.
		cmd += "\nsync " + shellQuote(marker+".tmp")
		cmd += "\nmv " + shellQuote(marker+".tmp") + " " + shellQuote(marker)
		cmd += "\nsync " + shellQuote(p.root)
		files = append(files, renderedFile{"bootstrap-restore.sh", cmd + "\n", true})
	}
	if usesPatroni(s) {
		y, err := renderPatroni(s, p)
		if err != nil {
			return nil, err
		}
		files = append(files, renderedFile{"patroni.yml", y, false})
		files = append(files, renderedFile{"bootstrap-initdb.sh", renderInitDBGuard(s, p), true})
		pass := strings.NewReplacer(`\`, `\\`, ":", `\:`).Replace(s.Superuser.Password)
		files = append(files, renderedFile{"bootstrap.pgpass", "*:5432:*:" + s.Superuser.Username + ":" + pass + "\n", false})
		marker := filepath.Join(p.root, "initialized.json")
		script := "#!/bin/sh\nset -eu\numask 077\nprintf 'true\\n' > " + shellQuote(marker+".tmp") + "\nsync " + shellQuote(marker+".tmp") + "\nmv " + shellQuote(marker+".tmp") + " " + shellQuote(marker) + "\nsync " + shellQuote(p.root) + "\nexport PGPASSFILE=" + shellQuote(filepath.Join(p.root, "bootstrap.pgpass")) + "\nexec " + shellQuote(filepath.Join(s.BinDir, "psql")) + " -X -h " + shellQuote(p.socket) + " -U " + shellQuote(s.Superuser.Username) + " -d postgres -v ON_ERROR_STOP=1 -f " + shellQuote(filepath.Join(p.root, "roles.sql")) + "\n"
		files = append(files, renderedFile{"bootstrap-roles.sh", script, true})
	}
	return files, nil
}
