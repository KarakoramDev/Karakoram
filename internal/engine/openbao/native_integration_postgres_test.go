//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baoapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// nativeSQLTarget replaces only the target provider's agent boundary. Every role
// lookup, creation, grant and deletion is executed against the native server.
// Other provider/access methods are deliberately outside this test's call graph.
type nativeSQLTarget struct {
	engine.Provider
	postgres.Access
	db *pgx.Conn
}

func (p *nativeSQLTarget) GetRole(ctx context.Context, _ *engine.Context, name string) (*postgres.Role, error) {
	r := &postgres.Role{Name: name}
	err := p.db.QueryRow(ctx, `SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolconnlimit FROM pg_roles WHERE rolname=$1`, name).Scan(&r.Login, &r.Superuser, &r.Createdb, &r.Createrole, &r.Replication, &r.BypassRLS, &r.ConnectionLimit)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, engine.ErrNotFound
	}
	return r, err
}

func nativeSQLLiteral(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }

func (p *nativeSQLTarget) CreateRole(ctx context.Context, _ *engine.Context, r postgres.RoleWrite) (string, error) {
	tx, err := p.db.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	attrs := []string{}
	for _, attr := range []struct {
		name string
		on   bool
	}{{"LOGIN", r.Login}, {"SUPERUSER", r.Superuser}, {"CREATEDB", r.Createdb}, {"CREATEROLE", r.Createrole}, {"REPLICATION", r.Replication}, {"BYPASSRLS", r.BypassRLS}} {
		if attr.on {
			attrs = append(attrs, attr.name)
		} else {
			attrs = append(attrs, "NO"+attr.name)
		}
	}
	attrs = append(attrs, "CONNECTION LIMIT "+strconv.Itoa(r.ConnectionLimit))
	if r.Password != nil {
		attrs = append(attrs, "PASSWORD "+nativeSQLLiteral(*r.Password))
	}
	if r.ValidUntil != nil {
		attrs = append(attrs, "VALID UNTIL "+nativeSQLLiteral(*r.ValidUntil))
	}
	name := pgx.Identifier{r.Name}.Sanitize()
	if _, err = tx.Exec(ctx, "CREATE ROLE "+name+" "+strings.Join(attrs, " ")); err != nil {
		var pgerr *pgconn.PgError
		if errors.As(err, &pgerr) && pgerr.Code == "42710" {
			return "", engine.ErrConflict
		}
		return "", err
	}
	admin := map[string]bool{}
	for _, member := range r.MemberOfAdmin {
		admin[member] = true
	}
	for _, member := range r.MemberOf {
		sql := "GRANT " + pgx.Identifier{member}.Sanitize() + " TO " + name
		if admin[member] {
			sql += " WITH ADMIN OPTION"
		}
		if _, err = tx.Exec(ctx, sql); err != nil {
			return "", err
		}
	}
	return "", tx.Commit(ctx)
}

func (p *nativeSQLTarget) DeleteRole(ctx context.Context, _ *engine.Context, name string) error {
	_, err := p.db.Exec(ctx, "DROP ROLE "+pgx.Identifier{name}.Sanitize())
	return err
}

func nativeControlPlanePostgres(t *testing.T, p *Provider, rc *engine.Context, converge func()) {
	t.Helper()
	ctx := t.Context()
	dsn, ca := nativePostgresServer(t)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal("connect native PostgreSQL: ", err)
	}
	t.Cleanup(func() { _ = admin.Close(context.Background()) })
	if _, err = admin.Exec(ctx, `CREATE TABLE public.native_bao_access (value text NOT NULL); INSERT INTO public.native_bao_access VALUES ('seed')`); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cluster := &domain.Cluster{Name: "nativepg", Namespace: rc.Cluster.Namespace, Spec: domain.ClusterSpec{Provider: "postgres"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady}}
	cluster.Spec.TLS.Mode = "requireTLS"
	cluster.Status.Connection.Hosts = []string{u.Host}
	cluster.Status.Members = []domain.MemberStatus{{Name: u.Host, Role: "primary"}}
	inventory, ok := p.options.Store.(interface {
		CreateCluster(context.Context, *domain.Cluster) error
	})
	if !ok {
		t.Fatal("native integration requires a writable target inventory")
	}
	if err = inventory.CreateCluster(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if err = rc.Secrets.SetClusterSecret(ctx, cluster.Ref(), "tls/ca.crt", ca); err != nil {
		t.Fatal(err)
	}
	targetRC := *rc
	targetRC.Cluster = cluster
	target := Target{Provider: &nativeSQLTarget{db: admin}, Context: &targetRC}
	previousTargets := p.options.Targets
	p.options.Targets = func(ctx context.Context, ref domain.ClusterRef) (Target, error) {
		if ref == cluster.Ref() {
			return target, nil
		}
		if previousTargets != nil {
			return previousTargets(ctx, ref)
		}
		return Target{}, engine.ErrNotFound
	}
	t.Cleanup(func() { p.options.Targets = previousTargets })
	request := DatabaseConnectionWrite{Name: "pg", TargetCluster: cluster.Name, Roles: []DatabaseRoleWrite{{Name: "readonly", Access: AccessReadOnly}, {Name: "readwrite", Access: AccessReadWrite}}}
	connection, err := p.CreateDatabaseConnection(ctx, rc, target, request)
	if err != nil {
		t.Fatal("create native PostgreSQL connection: ", err)
	}
	original, err := readConnectionCredential(ctx, rc, "pg")
	if err != nil {
		t.Fatal(err)
	}
	// Sync must roll the target CA into every member before configuring the
	// PostgreSQL plugin and rotating its administrator's password.
	converge()
	connections, err := p.ListDatabaseConnections(ctx, rc)
	if err != nil {
		t.Fatal(err)
	}
	ready := false
	for _, c := range connections {
		if c.Name == connection.Name {
			ready = c.Phase == ConnectionReady
		}
	}
	if !ready {
		t.Fatal("PostgreSQL connection did not reach ready after native convergence")
	}
	client, err := p.connectionClient(ctx, rc)
	if err != nil {
		t.Fatal(err)
	}
	var issued []string
	for _, access := range []string{"readonly", "readwrite"} {
		var response struct {
			Data struct {
				Username string `json:"username"`
				Password string `json:"password"`
			} `json:"data"`
		}
		if err = client.Do(ctx, "GET", "database/creds/pg_"+access, nil, &response); err != nil {
			t.Fatalf("issue %s credential: %v", access, err)
		}
		if response.Data.Username == "" || response.Data.Password == "" {
			t.Fatalf("empty %s credential", access)
		}
		issued = append(issued, response.Data.Username)
		credentialURL := *u
		credentialURL.User = url.UserPassword(response.Data.Username, response.Data.Password)
		db, connectErr := pgx.Connect(ctx, credentialURL.String())
		if connectErr != nil {
			t.Fatalf("connect using %s credential: %v", access, connectErr)
		}
		var seed string
		if err = db.QueryRow(ctx, `SELECT value FROM public.native_bao_access WHERE value='seed'`).Scan(&seed); err != nil || seed != "seed" {
			_ = db.Close(ctx)
			t.Fatalf("%s SELECT failed: %v", access, err)
		}
		_, insertErr := db.Exec(ctx, `INSERT INTO public.native_bao_access VALUES ($1)`, access)
		_ = db.Close(ctx)
		if access == "readonly" {
			var pgerr *pgconn.PgError
			if !errors.As(insertErr, &pgerr) || pgerr.Code != "42501" {
				t.Fatalf("readonly INSERT must fail with insufficient_privilege, got %v", insertErr)
			}
		} else if insertErr != nil {
			t.Fatal("readwrite INSERT failed: ", insertErr)
		}
	}
	oldURL := *u
	oldURL.User = url.UserPassword(original.Username, original.Password)
	old, err := pgx.Connect(ctx, oldURL.String())
	if old != nil {
		_ = old.Close(ctx)
	}
	var authErr *pgconn.PgError
	if !errors.As(err, &authErr) || authErr.Code != "28P01" {
		t.Fatalf("pre-rotation administrator password must fail authentication, got %v", err)
	}
	if err = p.DeleteDatabaseConnection(ctx, rc, target, "pg"); err != nil {
		t.Fatal("delete native PostgreSQL connection: ", err)
	}
	for _, name := range append(issued, original.Username) {
		var exists bool
		if err = admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)`, name).Scan(&exists); err != nil || exists {
			t.Fatalf("PostgreSQL identity remains after deletion: exists=%v err=%v", exists, err)
		}
	}
	for _, path := range []string{"database/config/pg", "database/roles/pg_readonly", "database/roles/pg_readwrite"} {
		if err = client.Do(ctx, "GET", path, nil, nil); !baoapi.IsStatus(err, 404) {
			t.Fatalf("deleted OpenBao configuration %s: expected 404, got %v", path, err)
		}
	}
	connections, err = p.ListDatabaseConnections(ctx, rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range connections {
		if c.Name == "pg" {
			t.Fatal("deleted PostgreSQL connection remains in control-plane inventory")
		}
	}
	converge()
}

func nativePostgresServer(t *testing.T) (string, []byte) {
	t.Helper()
	bin := os.Getenv("KARAKORAM_PG_BIN_DIR")
	if bin == "" {
		t.Skip("set KARAKORAM_PG_BIN_DIR to a PostgreSQL 17 bin directory for the native PostgreSQL scenario")
	}
	for _, name := range []string{"initdb", "pg_ctl"} {
		if _, err := os.Stat(filepath.Join(bin, name)); err != nil {
			t.Fatalf("native PostgreSQL prerequisite %s: %v", name, err)
		}
	}
	// Keep native runtime data under the worktree, not an external /tmp mount.
	dir, err := os.MkdirTemp(".", ".native-openbao-pg-")
	if err != nil {
		t.Fatal(err)
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	ca := nativePostgresTLS(t, dir)
	password := "native-postgres-bootstrap-password"
	pwfile := filepath.Join(dir, "bootstrap-password")
	if err = os.WriteFile(pwfile, []byte(password+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	data := filepath.Join(dir, "data")
	run := func(name string, args ...string) {
		t.Helper()
		output, err := exec.CommandContext(t.Context(), filepath.Join(bin, name), args...).CombinedOutput()
		if err != nil {
			t.Fatalf("native PostgreSQL %s: %v\n%s", name, err, output)
		}
	}
	run("initdb", "-D", data, "-U", "postgres", "--auth-local=trust", "--auth-host=scram-sha-256", "--pwfile="+pwfile, "--no-locale", "--encoding=UTF8")
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	config := fmt.Sprintf("\nlisten_addresses='127.0.0.1'\nport=%d\nunix_socket_directories=''\nssl=on\nssl_cert_file='%s'\nssl_key_file='%s'\npassword_encryption='scram-sha-256'\n", port, strings.ReplaceAll(filepath.Join(dir, "server.crt"), "'", "''"), strings.ReplaceAll(filepath.Join(dir, "server.key"), "'", "''"))
	f, err := os.OpenFile(filepath.Join(data, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteString(config)
	closeErr := f.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("write PostgreSQL TLS configuration: %v %v", err, closeErr)
	}
	if err = os.WriteFile(filepath.Join(data, "pg_hba.conf"), []byte("hostssl all all 127.0.0.1/32 scram-sha-256\nhostnossl all all 127.0.0.1/32 reject\n"), 0600); err != nil {
		t.Fatal(err)
	}
	// Register stop before startup: even a partially failed pg_ctl must not
	// leave a native process behind when the test exits.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		output, stopErr := exec.CommandContext(ctx, filepath.Join(bin, "pg_ctl"), "-D", data, "-m", "immediate", "-w", "stop").CombinedOutput()
		if stopErr != nil {
			t.Logf("native PostgreSQL stop: %v\n%s", stopErr, output)
		}
	})
	run("pg_ctl", "-D", data, "-l", filepath.Join(dir, "postgres.log"), "-w", "start")
	u := &url.URL{Scheme: "postgresql", Host: net.JoinHostPort("localhost", strconv.Itoa(port)), Path: "/postgres", User: url.UserPassword("postgres", password)}
	query := url.Values{"sslmode": {"verify-full"}, "sslrootcert": {filepath.Join(dir, "ca.crt")}, "connect_timeout": {"5"}}
	u.RawQuery = query.Encode()
	return u.String(), ca
}

func nativePostgresTLS(t *testing.T, dir string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "native PostgreSQL test CA"}, NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: ca.NotBefore, NotAfter: ca.NotAfter, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &leafKey.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(leafKey)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	for name, data := range map[string][]byte{"ca.crt": caPEM, "server.crt": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER}), "server.key": pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})} {
		if err = os.WriteFile(filepath.Join(dir, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	return caPEM
}
