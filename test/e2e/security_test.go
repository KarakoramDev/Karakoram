//go:build e2e

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package e2e

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	api "github.com/KarakoramDev/Karakoram/internal/apiclient"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func TestSecurity(t *testing.T) {
	t.Run("UsersRoles", func(t *testing.T) {
		s := newSuite(t, true)
		name := s.create(s.spec(1))
		s.exerciseUsersRoles(name)
	})
	t.Run("SystemRotation", func(t *testing.T) {
		s := newSuite(t, true)
		storage := s.storage()
		spec := s.spec(1)
		spec.Backup = &api.BackupSpec{Storage: ptr(storage)}
		name := s.create(spec)
		// Connection reveal intentionally returns only databaseAdmin
		// (docs/user-guide/users-tls-access.md); internal system users are
		// proven through the management, PBM and exporter paths they serve.
		connection := s.connection(name)
		if len(connection.Credentials) != 1 || connection.Credentials[0].Username != "databaseAdmin" {
			t.Fatalf("revealed credentials=%d want databaseAdmin only", len(connection.Credentials))
		}
		var systems api.SystemUserList
		s.expect(&systems)(s.client.ListSystemUsers(s.ctx, name))
		if len(systems.Items) != 5 {
			t.Fatal("system user inventory incomplete")
		}
		for _, username := range []string{"clusterAdmin", "userAdmin", "clusterMonitor", "backup", "databaseAdmin"} {
			db, old := credential(s.connection(name), "databaseAdmin")
			s.operation(s.client.RotateSystemUser(s.ctx, name, username, &api.RotateSystemUserParams{}))
			s.ready(name)
			_, fresh := credential(s.connection(name), "databaseAdmin")
			if username == "databaseAdmin" {
				if fresh == "" || fresh == old {
					t.Fatalf("%s rotation did not change password", username)
				}
				s.rejectedAuth(name, username, old, db, true)
			} else if fresh != old {
				t.Fatalf("rotating %s changed the databaseAdmin password", username)
			}
			c := s.mongoAs(name, "databaseAdmin", fresh, db)
			_ = c.Disconnect(context.Background())
			// Every rotation must preserve topology administration, userAdmin
			// management, PBM and the independently authenticated exporter.
			s.operation(s.client.RestartCluster(s.ctx, name, &api.RestartClusterParams{}))
			s.ready(name)
			s.exerciseUsersRoles(name)
			s.put(name, "rotation-proof", username)
			s.proof(name, "rotation-proof", username)
			s.backup(name, storage, api.CreateBackupRequestTypeLogical)
			s.assertMetrics(name)
			s.assertPrometheus(name)
		}
	})
	t.Run("TLSExposure", func(t *testing.T) {
		s := newSuite(t, true)
		spec := s.privateSpec(3)
		// Omitted exposure must normalize to private, not merely obey false.
		spec.Exposure = nil
		name := s.create(spec)
		s.assertPublic(name, false)
		s.update(name, func(spec *api.ClusterSpec) {
			spec.Exposure = &api.Exposure{Public: true, IpType: ptr(api.IPV6), Allowlist: ptr([]string{requireCIDR(t)})}
		})
		connection := s.connection(name)
		if connection.CaCertificate == nil || connection.Connection.PublicHosts == nil || len(*connection.Connection.PublicHosts) != 3 {
			t.Fatal("connection details omitted cluster CA/public member endpoints")
		}
		for _, cred := range connection.Credentials {
			if cred.Uri == nil || *cred.Uri == "" {
				t.Fatalf("connection URI missing for %s", cred.Username)
			}
		}
		s.assertMemberTransport(name)
		db, fresh := credential(connection, "databaseAdmin")
		s.rejectedAuth(name, "databaseAdmin", fresh, db, false)
		s.update(name, func(spec *api.ClusterSpec) { spec.Tls.Mode = api.PreferTLS })
		s.acceptConnection(name, true)
		s.acceptConnection(name, false)
		s.update(name, func(spec *api.ClusterSpec) { spec.Tls.Mode = api.Disabled })
		s.acceptConnection(name, false)
		s.update(name, func(spec *api.ClusterSpec) { spec.Tls.Mode = api.RequireTLS })
		s.assertMemberTransport(name)
		s.acceptConnection(name, true)
		s.rejectedAuth(name, "databaseAdmin", fresh, db, false)
		s.assertPublic(name, true)
		// RFC 3849 documentation-only IPv6 cannot be this public workstation's
		// source address. Prove allowlist enforcement, not just spec retention.
		denied := s.update(name, func(spec *api.ClusterSpec) { spec.Exposure.Allowlist = ptr([]string{"2001:db8::/128"}) })
		actualDenied := denied.Spec.Exposure
		if actualDenied == nil || actualDenied.Allowlist == nil || len(*actualDenied.Allowlist) != 1 || (*actualDenied.Allowlist)[0] != "2001:db8::/128" {
			t.Fatal("exposure did not preserve requested deny-only IPv6 allowlist")
		}
		s.probePublicTransport(s.connection(name), os.Getenv("KARAKORAM_E2E_ALLOWLIST_OBSERVATION_ONLY") == "1")
		s.update(name, func(spec *api.ClusterSpec) { spec.Exposure.Allowlist = ptr([]string{requireCIDR(t)}) })
		s.acceptConnection(name, true)
		public := s.connection(name)
		s.update(name, func(spec *api.ClusterSpec) { spec.Exposure.Public = false })
		s.assertPublic(name, false)
		s.rejectedPublicTransport(public)
		s.update(name, func(spec *api.ClusterSpec) { spec.Exposure.Public = true })
		s.assertPublic(name, true)
		s.acceptConnection(name, true)
		actual := s.cluster(name).Spec.Exposure
		if actual.Allowlist == nil || len(*actual.Allowlist) != 1 || (*actual.Allowlist)[0] != requireCIDR(t) {
			t.Fatal("exposure did not preserve requested IPv6 allowlist")
		}
	})
}

func (s *suite) exerciseUsersRoles(name string) {
	s.t.Helper()
	t := s.t
	role := api.DatabaseRole{Database: "e2e", Name: "application", Privileges: []api.Privilege{{Resource: api.PrivilegeResource{Db: ptr("e2e"), Collection: ptr("")}, Actions: []string{"find", "insert", "update", "remove"}}}, Roles: []api.RoleReference{}}
	var createdRole api.DatabaseRole
	s.expect(&createdRole, 201)(s.client.CreateDatabaseRole(s.ctx, name, &api.CreateDatabaseRoleParams{}, role))
	var fetchedRole api.DatabaseRole
	s.expect(&fetchedRole)(s.client.GetDatabaseRole(s.ctx, name, "e2e", role.Name))
	if fetchedRole.Name != role.Name {
		t.Fatal("role read mismatch")
	}
	var roles api.DatabaseRoleList
	s.expect(&roles)(s.client.ListDatabaseRoles(s.ctx, name, &api.ListDatabaseRolesParams{}))
	if len(roles.Items) != 1 {
		t.Fatal("custom role inventory mismatch")
	}
	role.Privileges[0].Actions = append(role.Privileges[0].Actions, "createCollection")
	s.expect(&fetchedRole)(s.client.ReplaceDatabaseRole(s.ctx, name, "e2e", role.Name, &api.ReplaceDatabaseRoleParams{}, api.UpdateDatabaseRoleRequest{Privileges: role.Privileges, Roles: role.Roles}))
	password := s.name() + s.name()
	var user api.DatabaseUser
	s.expect(&user, 201)(s.client.CreateDatabaseUser(s.ctx, name, &api.CreateDatabaseUserParams{}, api.CreateDatabaseUserRequest{Database: "e2e", Username: "application", Password: &password, Roles: []api.RoleReference{{Db: "e2e", Role: role.Name}}}))
	s.expect(&user)(s.client.GetDatabaseUser(s.ctx, name, "e2e", "application"))
	var users api.DatabaseUserList
	s.expect(&users)(s.client.ListDatabaseUsers(s.ctx, name, &api.ListDatabaseUsersParams{}))
	if len(users.Items) != 1 {
		t.Fatal("application user inventory mismatch")
	}
	s.authenticatedWrite(name, "application", password, "e2e")
	readOnly := []api.Privilege{{Resource: role.Privileges[0].Resource, Actions: []string{"find"}}}
	s.expect(&fetchedRole)(s.client.ReplaceDatabaseRole(s.ctx, name, "e2e", role.Name, &api.ReplaceDatabaseRoleParams{}, api.UpdateDatabaseRoleRequest{Privileges: readOnly, Roles: role.Roles}))
	s.rejectedWrite(name, "application", password)
	s.expect(&fetchedRole)(s.client.ReplaceDatabaseRole(s.ctx, name, "e2e", role.Name, &api.ReplaceDatabaseRoleParams{}, api.UpdateDatabaseRoleRequest{Privileges: role.Privileges, Roles: role.Roles}))
	s.authenticatedWrite(name, "application", password, "e2e")
	s.expect(&user)(s.client.ReplaceDatabaseUser(s.ctx, name, "e2e", "application", &api.ReplaceDatabaseUserParams{}, api.UpdateDatabaseUserRequest{Roles: []api.RoleReference{{Db: "e2e", Role: "read"}}}))
	s.rejectedWrite(name, "application", password)
	newPassword := s.name() + s.name()
	s.expect(&user)(s.client.ReplaceDatabaseUser(s.ctx, name, "e2e", "application", &api.ReplaceDatabaseUserParams{}, api.UpdateDatabaseUserRequest{Password: &newPassword, Roles: []api.RoleReference{{Db: "e2e", Role: role.Name}}}))
	s.rejectedAuth(name, "application", password, "e2e", true)
	s.authenticatedWrite(name, "application", newPassword, "e2e")
	s.expect(nil, 204)(s.client.DeleteDatabaseUser(s.ctx, name, "e2e", "application", &api.DeleteDatabaseUserParams{}))
	s.rejectedAuth(name, "application", newPassword, "e2e", true)
	s.expect(nil, 404)(s.client.GetDatabaseUser(s.ctx, name, "e2e", "application"))
	s.expect(nil, 204)(s.client.DeleteDatabaseRole(s.ctx, name, "e2e", role.Name, &api.DeleteDatabaseRoleParams{}))
	s.expect(nil, 404)(s.client.GetDatabaseRole(s.ctx, name, "e2e", role.Name))
}

func (s *suite) assertMemberTransport(name string) {
	s.t.Helper()
	connection := s.connection(name)
	if connection.CaCertificate == nil {
		s.t.Fatal("cluster CA absent")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(*connection.CaCertificate)) {
		s.t.Fatal("invalid cluster CA")
	}
	for _, member := range s.members(name) {
		ctx, cancel := context.WithTimeout(s.ctx, publicRoutingGrace)
		start := time.Now()
		var socket net.Conn
		err := retryPublicProbe(ctx, func(attempt context.Context) error {
			var err error
			socket, err = (&tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}).DialContext(attempt, "tcp", member.PublicHost)
			if err != nil && socket != nil {
				_ = socket.Close()
			}
			return err
		})
		if err != nil {
			cancel()
			s.t.Fatalf("member=%s verified TLS connect failed after %s (routing grace up to %s): %s", member.Name, time.Since(start).Round(time.Second), publicRoutingGrace, s.redact(err.Error()))
		}
		state := socket.(*tls.Conn).ConnectionState()
		_ = socket.Close()
		if len(state.VerifiedChains) == 0 {
			cancel()
			s.t.Fatalf("member=%s server chain was not verified against cluster CA", member.Name)
		}
		client := s.mongoMember(ctx, name, member, "databaseAdmin")
		var config struct {
			Parsed struct {
				Security struct {
					KeyFile         string `bson:"keyFile"`
					ClusterAuthMode string `bson:"clusterAuthMode"`
				} `bson:"security"`
				Net struct {
					TLS struct {
						Mode               string `bson:"mode"`
						CertificateKeyFile string `bson:"certificateKeyFile"`
						CAFile             string `bson:"CAFile"`
					} `bson:"tls"`
				} `bson:"net"`
			} `bson:"parsed"`
		}
		err = retryPublicProbe(ctx, func(attempt context.Context) error {
			return client.Database("admin").RunCommand(attempt, bson.D{{Key: "getCmdLineOpts", Value: 1}}).Decode(&config)
		})
		_ = client.Disconnect(context.Background())
		cancel()
		if err != nil {
			s.t.Fatalf("member=%s cannot inspect active MongoDB configuration after %s (routing grace up to %s): %s", member.Name, time.Since(start).Round(time.Second), publicRoutingGrace, s.redact(err.Error()))
		}
		if config.Parsed.Security.KeyFile == "" || config.Parsed.Security.ClusterAuthMode != "keyFile" ||
			config.Parsed.Net.TLS.Mode != "requireTLS" || config.Parsed.Net.TLS.CertificateKeyFile == "" || config.Parsed.Net.TLS.CAFile == "" {
			s.t.Fatalf("member=%s lacks active internal keyfile/required TLS configuration", member.Name)
		}
		s.t.Logf("member=%s verified server CA chain; active keyfile and requireTLS client/internal traffic", member.Name)
	}
}

func (s *suite) authenticatedWrite(name, user, password, db string) {
	s.t.Helper()
	c := s.connectPublic(name, user, password, db, true)
	defer c.Disconnect(context.Background())
	ctx, cancel := context.WithTimeout(s.ctx, 20*time.Second)
	defer cancel()
	id := s.name()
	_, err := c.Database("e2e").Collection("authproof").InsertOne(ctx, bson.M{"_id": id})
	if err != nil {
		s.t.Fatalf("application write failed: %v", err)
	}
	if err = c.Database("e2e").Collection("authproof").FindOne(ctx, bson.M{"_id": id}).Err(); err != nil {
		s.t.Fatalf("application read failed: %v", err)
	}
}

func (s *suite) rejectedWrite(name, user, password string) {
	s.t.Helper()
	client := s.connectPublic(name, user, password, "e2e", true)
	defer client.Disconnect(context.Background())
	ctx, cancel := context.WithTimeout(s.ctx, 20*time.Second)
	defer cancel()
	_, err := client.Database("e2e").Collection("authproof").InsertOne(ctx, bson.M{"_id": s.name()})
	var command mongo.CommandError
	if !errors.As(err, &command) || command.Code != 13 {
		s.t.Fatalf("restricted application write must fail authorization, got %v", err)
	}
}
func (s *suite) rejectedAuth(name, user, password, db string, tls bool) {
	s.t.Helper()
	ctx, cancel := context.WithTimeout(s.ctx, 12*time.Second)
	defer cancel()
	c, err := s.dial(ctx, name, user, password, db, tls)
	if c != nil {
		_ = c.Disconnect(context.Background())
	}
	if err == nil {
		s.t.Fatalf("unexpected accepted connection user=%s TLS=%t", user, tls)
	}
	if tls && !stringsAuthentication(err) {
		s.t.Fatalf("old/deleted credential did not fail authentication (not an auth error): %s", s.redact(err.Error()))
	}
}
func stringsAuthentication(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "auth")
}
func (s *suite) acceptConnection(name string, tls bool) {
	s.t.Helper()
	c := s.connectPublic(name, "databaseAdmin", "", "", tls)
	_ = c.Disconnect(context.Background())
}
func (s *suite) assertPublic(name string, want bool) {
	s.t.Helper()
	for _, m := range s.members(name) {
		if (m.PublicHost != "") != want {
			s.t.Fatalf("member=%s public routing presence=%t want=%t", m.Name, m.PublicHost != "", want)
		}
	}
	c := s.cluster(name)
	if c.Status.Connection == nil {
		s.t.Fatal("missing connection status")
	}
	present := c.Status.Connection.PublicHosts != nil && len(*c.Status.Connection.PublicHosts) > 0
	if present != want {
		s.t.Fatalf("cluster publicHosts presence=%t want=%t", present, want)
	}
}

func (s *suite) rejectedPublicTransport(connection api.Connection) {
	s.t.Helper()
	s.probePublicTransport(connection, false)
}

func (s *suite) probePublicTransport(connection api.Connection, observeOnly bool) {
	s.t.Helper()
	if connection.CaCertificate == nil || connection.Connection.PublicHosts == nil || len(*connection.Connection.PublicHosts) == 0 {
		s.t.Fatal("public transport probe lacks saved CA/endpoints")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(*connection.CaCertificate)) {
		s.t.Fatal("invalid cluster CA")
	}
	for _, endpoint := range *connection.Connection.PublicHosts {
		ctx, cancel := context.WithTimeout(s.ctx, 12*time.Second)
		socket, err := (&tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}, Config: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}}).DialContext(ctx, "tcp", endpoint)
		if socket != nil {
			_ = socket.Close()
		}
		cancel()
		if observeOnly {
			outcome := "denied"
			if err == nil {
				outcome = "platform did not enforce allowlist: reachable"
			}
			s.t.Logf("public allowlist probe endpoint=%s outcome=%s; observation only, not enforcement proof", endpoint, outcome)
			continue
		}
		if err == nil {
			s.t.Fatal("public MongoDB TLS endpoint remains accessible after access was denied")
		}
	}
}
