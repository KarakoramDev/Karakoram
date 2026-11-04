// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/pki"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

// TestNativeKeycloak is opt-in qualification using verified, unpacked native
// distributions. It always stops its own processes; no containers or root needed.
func TestNativeKeycloak(t *testing.T) {
	if os.Getenv("KARAKORAM_KEYCLOAK_NATIVE") != "1" {
		t.Skip("set KARAKORAM_KEYCLOAK_NATIVE=1 with KEYCLOAK_HOME, JAVA_HOME and POSTGRES_HOME")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	kc, java, pg := os.Getenv("KEYCLOAK_HOME"), os.Getenv("JAVA_HOME"), os.Getenv("POSTGRES_HOME")
	if kc == "" || java == "" || pg == "" {
		t.Fatal("missing native runtime paths")
	}
	root := t.TempDir()
	data := filepath.Join(root, "pgdata")
	run := func(binary string, args []string, stdin string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, binary, args...)
		cmd.Env = append(os.Environ(), "LD_LIBRARY_PATH="+filepath.Join(pg, "lib"))
		cmd.Stdin = strings.NewReader(stdin)
		out, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatalf("native %s failed: %v (diagnostic: %s)", filepath.Base(binary), e, out)
		}
		return string(out)
	}
	version := run(filepath.Join(pg, "bin/postgres"), []string{"--version"}, "")
	t.Log(strings.TrimSpace(version))
	run(filepath.Join(pg, "bin/initdb"), []string{"-D", data, "--no-locale", "-E", "UTF8", "-A", "trust"}, "")
	write := func(path string, b []byte) {
		t.Helper()
		if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(path, b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	ca, e := pki.NewCA("keycloak native proof", 0)
	if e != nil {
		t.Fatal(e)
	}
	cert, key, e := ca.Issue(pki.CertRequest{CommonName: "localhost", DNSNames: []string{"localhost"}, IPs: []net.IP{net.IPv4(127, 0, 0, 1)}, Server: true})
	if e != nil {
		t.Fatal(e)
	}
	write(filepath.Join(data, "server.crt"), cert)
	write(filepath.Join(data, "server.key"), key)
	listener, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	write(filepath.Join(data, "postgresql.conf"), []byte(fmt.Sprintf("listen_addresses='127.0.0.1'\nport=%d\nunix_socket_directories='%s'\nssl=on\nssl_cert_file='server.crt'\nssl_key_file='server.key'\npassword_encryption='scram-sha-256'\n", port, root)))
	// Administration is local-only; consumer connections require TLS and SCRAM.
	write(filepath.Join(data, "pg_hba.conf"), []byte("local all all trust\nhostssl keycloak binding_role 127.0.0.1/32 scram-sha-256\nhost all all 127.0.0.1/32 reject\n"))
	run(filepath.Join(pg, "bin/pg_ctl"), []string{"-D", data, "-l", filepath.Join(root, "postgres.log"), "-w", "start"}, "")
	defer func() {
		cmd := exec.Command(filepath.Join(pg, "bin/pg_ctl"), "-D", data, "-w", "stop", "-m", "immediate")
		if out, e := cmd.CombinedOutput(); e != nil {
			t.Errorf("PostgreSQL cleanup: %v %s", e, out)
		} else {
			t.Log("PostgreSQL stopped")
		}
	}()
	password, e := secrets.Token()
	if e != nil {
		t.Fatal(e)
	}
	admin, e := secrets.Token()
	if e != nil {
		t.Fatal(e)
	}
	// Exercise both Java-properties escaping and SmallRye expression escaping.
	password += `${DO_NOT_RESOLVE}:=`
	admin += `${DO_NOT_RESOLVE}:=`
	sql := "CREATE ROLE binding_role LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION CONNECTION LIMIT 100 PASSWORD '" + password + "';\nCREATE DATABASE keycloak OWNER binding_role;\n"
	// Keep SQL and its credentials off command arguments and all output.
	cmd := exec.CommandContext(ctx, filepath.Join(pg, "bin/psql"), "-X", "-h", root, "-p", fmt.Sprint(port), "-d", "postgres", "-v", "ON_ERROR_STOP=1")
	cmd.Env = append(os.Environ(), "LD_LIBRARY_PATH="+filepath.Join(pg, "lib"))
	cmd.Stdin = strings.NewReader(sql)
	if e = cmd.Run(); e != nil {
		t.Fatal("creating least-privilege consumer role/database:", e)
	}
	s := validSpec()
	s.Host = "localhost"
	s.Port = port
	s.Password = password
	s.AdminPassword = admin
	s.CAPEM = string(ca.CertPEM)
	process, files, e := Render(s, root)
	if e != nil {
		t.Fatal(e)
	}
	for name, b := range files {
		write(filepath.Join(root, "karakoram", name), b)
	}
	for name := range files {
		st, e := os.Stat(filepath.Join(root, "karakoram", name))
		if e != nil || st.Mode().Perm() != 0600 {
			t.Fatal("configuration permissions", e)
		}
	}
	env := []string{"PATH=/usr/bin:/bin", "LANG=C.UTF-8", "JAVA_HOME=" + java, "JAVA_OPTS_KC_HEAP=-Xms128m -Xmx768m"}
	binary := filepath.Join(kc, "bin/kc.sh")
	build := exec.CommandContext(ctx, binary, "build", "--db=postgres", "--health-enabled=true")
	build.Dir = kc
	build.Env = env
	if out, e := build.CombinedOutput(); e != nil {
		t.Fatalf("optimized build: %v %s", e, out)
	}
	logfile, e := os.OpenFile(filepath.Join(root, "keycloak.log"), os.O_CREATE|os.O_WRONLY, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer logfile.Close()
	start := exec.CommandContext(ctx, binary, process.Args...)
	start.Dir = kc
	start.Env = env
	start.Stdout = logfile
	start.Stderr = logfile
	if e = start.Start(); e != nil {
		t.Fatal(e)
	}
	exited := make(chan error, 1)
	go func() { exited <- start.Wait() }()
	defer func() {
		_ = start.Process.Signal(os.Interrupt)
		select {
		case <-exited:
		case <-time.After(15 * time.Second):
			_ = start.Process.Kill()
			<-exited
		}
		t.Log("Keycloak stopped")
	}()
	for {
		if ready(ctx) == nil {
			break
		}
		select {
		case e := <-exited:
			exited <- e
			b, _ := os.ReadFile(filepath.Join(root, "keycloak.log"))
			t.Fatalf("Keycloak exited: %v %s", e, b)
		case <-ctx.Done():
			b, _ := os.ReadFile(filepath.Join(root, "keycloak.log"))
			t.Fatalf("Keycloak readiness deadline: %s", b)
		case <-time.After(time.Second):
		}
	}
	t.Log("Agent-rendered configuration: health/ready UP; TLS verify-full PostgreSQL connection; owner-only secret files")
	client := http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	form := url.Values{"grant_type": {"password"}, "client_id": {"admin-cli"}, "username": {"admin"}, "password": {admin}}
	req, e := http.NewRequestWithContext(ctx, "POST", "http://127.0.0.1:8080/realms/master/protocol/openid-connect/token", strings.NewReader(form.Encode()))
	if e != nil {
		t.Fatal(e)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	res, e := client.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	defer res.Body.Close()
	var token struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
	}
	if e = json.NewDecoder(io.LimitReader(res.Body, 1<<20)).Decode(&token); e != nil || res.StatusCode != 200 || token.AccessToken == "" {
		t.Fatalf("bootstrap administrator token request failed: HTTP %d, decode %v", res.StatusCode, e)
	}
	t.Log("Bootstrap administrator REST token: HTTP 200; nonempty access_token (not logged)")
	query := "SELECT (NOT rolsuper AND NOT rolcreatedb AND NOT rolcreaterole AND NOT rolreplication) FROM pg_roles WHERE rolname='binding_role'; SELECT ssl FROM pg_stat_ssl JOIN pg_stat_activity USING(pid) WHERE usename='binding_role';"
	proof := run(filepath.Join(pg, "bin/psql"), []string{"-X", "-h", root, "-p", fmt.Sprint(port), "-d", "postgres", "-At", "-c", query}, "")
	if !strings.HasPrefix(proof, "t\n") || strings.Contains(proof, "f") {
		t.Fatal("least-privilege/TLS proof failed", proof)
	}
	t.Log("PostgreSQL role: NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION; all consumer sessions SSL=true")
}
