// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

func baseDir(n *agent.Node) string { return filepath.Join(n.DataDir(), "karakoram", "neo4j") }
func dataDir(n *agent.Node) string { return filepath.Join(baseDir(n), "data") }
func confDir(n *agent.Node) string { return filepath.Join(baseDir(n), "conf") }

// render derives both normal and recovery configurations from the same managed
// policy. Recovery never inherits a network-facing connector address.
func render(s neo4jspec.Spec, base, stock string, recovery bool, bolt, https int, listenOverride ...string) (map[string][]byte, error) {
	conf := filepath.Join(base, "conf")
	listen, advertised := "0.0.0.0", s.AdvertisedHost
	if len(listenOverride) > 0 && listenOverride[0] != "" {
		listen = listenOverride[0]
	}
	if recovery {
		conf = filepath.Join(base, "recovery-conf")
		listen, advertised = "127.0.0.1", "localhost"
	}
	managed := map[string]string{
		"server.directories.data":                       filepath.Join(base, "data"),
		"server.directories.transaction.logs.root":      filepath.Join(base, "data", "transactions"),
		"server.directories.logs":                       filepath.Join(base, "logs"),
		"server.directories.run":                        filepath.Join(base, "run"),
		"server.directories.import":                     filepath.Join(base, "import"),
		"server.directories.plugins":                    filepath.Join(s.Install.Neo4jHome, "plugins"),
		"server.directories.lib":                        filepath.Join(s.Install.Neo4jHome, "lib"),
		"server.default_listen_address":                 listen,
		"server.default_advertised_address":             advertised,
		"server.bolt.enabled":                           "true",
		"server.bolt.listen_address":                    net.JoinHostPort(listen, strconv.Itoa(bolt)),
		"server.bolt.advertised_address":                net.JoinHostPort(advertised, strconv.Itoa(bolt)),
		"server.bolt.tls_level":                         "REQUIRED",
		"server.http.enabled":                           "false",
		"server.https.enabled":                          "true",
		"server.https.listen_address":                   net.JoinHostPort(listen, strconv.Itoa(https)),
		"server.https.advertised_address":               net.JoinHostPort(advertised, strconv.Itoa(https)),
		"server.memory.heap.initial_size":               fmt.Sprintf("%dm", s.HeapMB),
		"server.memory.heap.max_size":                   fmt.Sprintf("%dm", s.HeapMB),
		"server.memory.pagecache.size":                  fmt.Sprintf("%dm", s.PageCacheMB),
		"dbms.usage_report.enabled":                     "false",
		"server.fleet_discovery.enabled":                "false",
		"dbms.fleet_manager.enabled":                    "false",
		"server.http.x_forward.enabled":                 "false",
		"dbms.security.allow_csv_import_from_file_urls": "false",
		"server.config.strict_validation.enabled":       "true",
		"dbms.security.auth_enabled":                    strconv.FormatBool(!recovery),
	}
	files := map[string][]byte{}
	for _, policy := range []string{"bolt", "https"} {
		prefix := "dbms.ssl.policy." + policy + "."
		certDir := filepath.Join(base, "certificates", policy)
		managed[prefix+"enabled"] = "true"
		managed[prefix+"base_directory"] = certDir
		managed[prefix+"private_key"] = "private.key"
		managed[prefix+"public_certificate"] = "public.crt"
		managed[prefix+"client_auth"] = "NONE"
		for name, value := range map[string]string{"private.key": s.Server.KeyPEM, "public.crt": s.Server.CertPEM, "trusted/ca.crt": s.CAPEM} {
			files[filepath.Join("neo4j", "certificates", policy, name)] = []byte(value)
		}
	}
	// Multi-valued JVM flags must remain stock-version-specific. No other stock
	// settings can override the explicit managed policy.
	var flags []string
	scanner := bufio.NewScanner(strings.NewReader(stock))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "server.jvm.additional=") {
			flags = append(flags, line)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, errors.New("invalid stock Neo4j configuration")
	}
	flags = append(flags, "server.jvm.additional=-XX:+ExitOnOutOfMemoryError")
	for k, v := range s.Settings {
		if _, ok := managed[k]; ok || k == "server.jvm.additional" {
			return nil, errors.New("Neo4j setting overlaps managed configuration")
		}
		if !allowedSetting(k) || strings.ContainsAny(v, "\r\n\x00") {
			return nil, errors.New("invalid Neo4j setting")
		}
		managed[k] = v
	}
	keys := make([]string, 0, len(managed))
	for k := range managed {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out bytes.Buffer
	for _, k := range keys {
		fmt.Fprintf(&out, "%s=%s\n", k, propertyValue(managed[k]))
	}
	for _, line := range flags {
		out.WriteString(line)
		out.WriteByte('\n')
	}
	rel, err := filepath.Rel(filepath.Dir(base), conf)
	if err != nil {
		return nil, err
	}
	files[filepath.Join(rel, "neo4j.conf")] = out.Bytes()
	return files, nil
}

// Only non-security query tuning belongs to the member settings surface.
func allowedSetting(k string) bool {
	switch k {
	case "db.query.default_language", "db.transaction.timeout", "db.transaction.concurrent.maximum", "db.lock.acquisition.timeout", "db.logs.query.enabled", "db.logs.query.threshold", "dbms.cypher.planner":
		return true
	}
	return false
}
func propertyValue(v string) string { return strings.ReplaceAll(v, "\\", "\\\\") }

func writeFiles(n *agent.Node, files map[string][]byte) ([]string, error) {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	sort.Strings(names)
	var changed []string
	for _, name := range names {
		old, err := os.ReadFile(filepath.Join(n.DataDir(), "karakoram", name))
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return changed, err
		}
		if err == nil && bytes.Equal(old, files[name]) {
			continue
		}
		if err := n.WriteFile(name, files[name], 0600); err != nil {
			return changed, err
		}
		changed = append(changed, name)
	}
	return changed, nil
}
