// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"unicode"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baoapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq"
)

var connectionName = regexp.MustCompile(`^[a-z][a-z0-9-]{0,30}$`)

type connectionCredential struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Step     string `json:"step"`
	Rotated  bool   `json:"rotated"`
}

func validateConnection(rc *engine.Context, target Target, req DatabaseConnectionWrite) (DatabaseConnectionWrite, error) {
	if !connectionName.MatchString(req.Name) || !clusterName.MatchString(req.TargetCluster) || len(req.Roles) < 1 || len(req.Roles) > 16 {
		return req, engine.Invalid("invalid connection name, target or role count")
	}
	if target.Context == nil || target.Context.Cluster == nil || target.Provider == nil {
		return req, engine.Invalid("connection target is required")
	}
	tc := target.Context.Cluster
	if tc.Name != req.TargetCluster || tc.Ref().Namespace != rc.Cluster.Ref().Namespace {
		return req, engine.Invalid("connection target must be in the same namespace")
	}
	provider := tc.Spec.Provider
	if provider != "postgres" && provider != "psmdb" && provider != "rabbitmq" {
		return req, engine.Invalid("unsupported connection target provider")
	}
	if provider == "psmdb" && tc.Spec.Topology.Type != "replicaSet" {
		return req, engine.Invalid("MongoDB connections require a replica set")
	}
	if tc.Status.Phase != domain.PhaseReady {
		return req, engine.Invalid("connection target must be ready")
	}
	if tc.Spec.TLS.Mode == "disabled" {
		return req, engine.Invalid("connection target requires TLS")
	}
	seen := map[string]bool{}
	req.Roles = append([]DatabaseRoleWrite(nil), req.Roles...)
	for i := range req.Roles {
		r := &req.Roles[i]
		if !connectionName.MatchString(r.Name) || seen[r.Name] || (r.Access != AccessReadOnly && r.Access != AccessReadWrite) {
			return req, engine.Invalid("invalid or duplicate connection role")
		}
		seen[r.Name] = true
		if r.DefaultTTLSeconds == 0 {
			r.DefaultTTLSeconds = 3600
		}
		if r.MaxTTLSeconds == 0 {
			r.MaxTTLSeconds = 86400
		}
		if r.DefaultTTLSeconds < 1 || r.MaxTTLSeconds < r.DefaultTTLSeconds || r.MaxTTLSeconds > 31536000 {
			return req, engine.Invalid("invalid connection role TTL bounds")
		}
		if provider == "rabbitmq" && i > 0 && (r.DefaultTTLSeconds != req.Roles[0].DefaultTTLSeconds || r.MaxTTLSeconds != req.Roles[0].MaxTTLSeconds) {
			return req, engine.Invalid("RabbitMQ roles in one connection must share defaultTtlSeconds and maxTtlSeconds: OpenBao configures leases per mount")
		}
		if strings.IndexFunc(r.Database, unicode.IsControl) >= 0 || len(r.Database) > 255 || provider == "postgres" && r.Database != "" {
			return req, engine.Invalid("invalid role database")
		}
		if provider == "psmdb" && (strings.ContainsAny(r.Database, " /\\.$\"\x00") || len(r.Database) > 63) {
			return req, engine.Invalid("invalid MongoDB role database")
		}
		if provider == "rabbitmq" && r.Database == "" {
			r.Database = "/"
		}
	}
	sort.Slice(req.Roles, func(i, j int) bool { return req.Roles[i].Name < req.Roles[j].Name })
	return req, nil
}

func saveConnections(ctx context.Context, rc *engine.Context, records []DatabaseConnection) error {
	b, e := json.Marshal(records)
	if e != nil {
		return e
	}
	return putSecret(ctx, rc, SecretConnections, b)
}
func saveConnectionCredential(ctx context.Context, rc *engine.Context, name string, c connectionCredential) error {
	b, e := json.Marshal(c)
	if e != nil {
		return e
	}
	return putSecret(ctx, rc, SecretConnectionPrefix+name, b)
}
func readConnectionCredential(ctx context.Context, rc *engine.Context, name string) (connectionCredential, error) {
	var c connectionCredential
	b, e := secret(ctx, rc, SecretConnectionPrefix+name)
	if e != nil {
		return c, e
	}
	if json.Unmarshal(b, &c) != nil {
		return c, engine.ErrUnavailable
	}
	return c, nil
}
func connectionRequest(c DatabaseConnection) DatabaseConnectionWrite {
	r := DatabaseConnectionWrite{Name: c.Name, TargetCluster: c.TargetCluster}
	for _, v := range c.Roles {
		r.Roles = append(r.Roles, v.DatabaseRoleWrite)
	}
	return r
}

func (p *Provider) lockConnections(rc *engine.Context) func() {
	key := rc.Cluster.Ref().Key()
	value, ok := p.connectionsMu.Load(key)
	if !ok {
		value, _ = p.connectionsMu.LoadOrStore(key, &sync.Mutex{})
	}
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

func (p *Provider) CreateDatabaseConnection(ctx context.Context, rc *engine.Context, target Target, req DatabaseConnectionWrite) (DatabaseConnection, error) {
	defer p.lockConnections(rc)()
	req, e := validateConnection(rc, target, req)
	if e != nil {
		return DatabaseConnection{}, e
	}
	records, e := p.ListDatabaseConnections(ctx, rc)
	if e != nil {
		return DatabaseConnection{}, e
	}
	index := -1
	for i, c := range records {
		if c.Name == req.Name {
			index = i
			if !reflect.DeepEqual(connectionRequest(c), req) || c.TargetProvider != target.Context.Cluster.Spec.Provider || c.Phase == ConnectionDeleting {
				return DatabaseConnection{}, engine.ErrConflict
			}
			break
		}
	}
	if index < 0 {
		c := DatabaseConnection{Name: req.Name, TargetCluster: req.TargetCluster, TargetProvider: target.Context.Cluster.Spec.Provider, MountPath: "database", Phase: ConnectionPending, CreatedAt: now(rc)}
		if c.TargetProvider == "rabbitmq" {
			c.MountPath = "rabbitmq-" + c.Name
		}
		for _, r := range req.Roles {
			path := "database/creds/" + c.Name + "_" + r.Name
			if c.TargetProvider == "rabbitmq" {
				path = c.MountPath + "/creds/" + r.Name
			}
			c.Roles = append(c.Roles, DatabaseRole{DatabaseRoleWrite: r, CredentialsPath: path})
		}
		records = append(records, c)
		index = len(records) - 1
		if e = saveConnections(ctx, rc, records); e != nil {
			return c, e
		}
	}
	c := &records[index]
	if c.Phase != ConnectionReady {
		e = p.advanceConnection(ctx, rc, target, c)
		if e == nil {
			e = saveConnections(ctx, rc, records)
		}
	}
	return *c, e
}

func (p *Provider) advanceConnection(ctx context.Context, rc *engine.Context, target Target, c *DatabaseConnection) error {
	cred, e := readConnectionCredential(ctx, rc, c.Name)
	if missing(e) {
		b := make([]byte, 32)
		if _, e = rand.Read(b); e != nil {
			return e
		}
		s, err := loadState(rc.Cluster)
		if err != nil {
			return err
		}
		uid := sha256.Sum256([]byte(rc.Cluster.Ref().Key() + "/" + s.UID + "/" + c.Name))
		// PostgreSQL reserves kk_; use the unreserved karakoram_openbao prefix.
		cred = connectionCredential{Username: "karakoram_openbao_" + hex.EncodeToString(uid[:12]), Password: hex.EncodeToString(b)}
		if e = requireNewTargetIdentity(ctx, target, *c, cred.Username); e != nil {
			return e
		}
		if e = saveConnectionCredential(ctx, rc, c.Name, cred); e != nil {
			return e
		}
	} else if e != nil {
		return e
	}
	if cred.Step == "" {
		if e = createTargetIdentity(ctx, target, *c, cred); e != nil {
			return e
		}
		cred.Step = "identity"
		if e = saveConnectionCredential(ctx, rc, c.Name, cred); e != nil {
			return e
		}
	}
	ready, e := p.connectionTrustReady(ctx, rc)
	if e != nil {
		return e
	}
	if !ready {
		return nil
	}
	client, e := p.connectionClient(ctx, rc)
	if e != nil {
		return e
	}
	if cred.Step == "identity" {
		if e = p.configureConnection(ctx, rc, target, client, *c, cred); e != nil {
			return e
		}
		cred.Step = "configured"
		if e = saveConnectionCredential(ctx, rc, c.Name, cred); e != nil {
			return e
		}
	}
	if cred.Step == "configured" {
		if c.TargetProvider != "rabbitmq" {
			if e = client.Do(ctx, "POST", "database/rotate-root/"+c.Name, nil, nil); e != nil {
				return e
			}
			cred.Rotated = true
		}
		cred.Step = "rotated"
		if e = saveConnectionCredential(ctx, rc, c.Name, cred); e != nil {
			return e
		}
	}
	c.Phase = ConnectionReady
	c.Message = ""
	return nil
}

func (p *Provider) connectionClient(ctx context.Context, rc *engine.Context) (*baoapi.Client, error) {
	s, e := loadState(rc.Cluster)
	if e != nil {
		return nil, e
	}
	token, e := secret(ctx, rc, SecretManagementToken)
	if e != nil {
		return nil, e
	}
	client, _, e := p.activeClient(ctx, rc, s, string(token))
	return client, e
}

// Only a sealed intent may resume an existing identity. Never adopt a name
// already present before this connection's credential was durably allocated.
func requireNewTargetIdentity(ctx context.Context, t Target, c DatabaseConnection, name string) error {
	var err error
	switch c.TargetProvider {
	case "postgres":
		a, ok := t.Provider.(postgres.Access)
		if !ok {
			return engine.ErrNotSupported
		}
		_, err = a.GetRole(ctx, t.Context, name)
	case "psmdb":
		a, ok := t.Provider.(engine.DataPlane)
		if !ok {
			return engine.ErrNotSupported
		}
		_, err = a.GetDatabaseUser(ctx, t.Context, "admin", name)
	case "rabbitmq":
		a, ok := t.Provider.(rabbitmq.Access)
		if !ok {
			return engine.ErrNotSupported
		}
		_, err = a.GetUser(ctx, t.Context, name)
	}
	if errors.Is(err, engine.ErrNotFound) {
		return nil
	}
	if err == nil {
		return engine.ErrConflict
	}
	return fmt.Errorf("%w: target identity lookup failed", engine.ErrUnavailable)
}

func createTargetIdentity(ctx context.Context, t Target, c DatabaseConnection, cred connectionCredential) error {
	var e error
	switch c.TargetProvider {
	case "postgres":
		a, ok := t.Provider.(postgres.Access)
		if !ok {
			return engine.ErrNotSupported
		}
		_, e = a.CreateRole(ctx, t.Context, postgres.RoleWrite{Role: postgres.Role{Name: cred.Username, Login: true, Createrole: true, ConnectionLimit: -1, MemberOf: []string{"pg_read_all_data", "pg_write_all_data"}, MemberOfAdmin: []string{"pg_read_all_data", "pg_write_all_data"}}, Password: &cred.Password})
	case "psmdb":
		a, ok := t.Provider.(engine.DataPlane)
		if !ok {
			return engine.ErrNotSupported
		}
		_, e = a.CreateDatabaseUser(ctx, t.Context, domain.DatabaseUser{Name: cred.Username, Database: "admin", Roles: []domain.RoleRef{{Role: "userAdminAnyDatabase", DB: "admin"}}}, cred.Password)
	case "rabbitmq":
		a, ok := t.Provider.(rabbitmq.Access)
		if !ok {
			return engine.ErrNotSupported
		}
		_, e = a.CreateUser(ctx, t.Context, rabbitmq.UserWrite{Name: cred.Username, Tags: []string{"administrator"}, Password: &cred.Password})
		if e != nil && !errors.Is(e, engine.ErrConflict) {
			break
		}
		for _, r := range c.Roles {
			grant := rabbitmq.Permission{Vhost: r.Database, User: cred.Username, Configure: ".*", Write: ".*", Read: ".*"}
			e = a.CreatePermission(ctx, t.Context, grant)
			if errors.Is(e, engine.ErrConflict) {
				e = a.UpdatePermission(ctx, t.Context, grant)
			}
			if e != nil {
				return engine.ErrUnavailable
			}
		}
	}
	if errors.Is(e, engine.ErrConflict) {
		return nil
	}
	if e != nil {
		return fmt.Errorf("%w: target identity creation failed", engine.ErrUnavailable)
	}
	return nil
}

func deleteTargetIdentity(ctx context.Context, t Target, c DatabaseConnection, cred connectionCredential) error {
	var e error
	switch c.TargetProvider {
	case "postgres":
		a, ok := t.Provider.(postgres.Access)
		if !ok {
			return engine.ErrNotSupported
		}
		e = a.DeleteRole(ctx, t.Context, cred.Username)
	case "psmdb":
		a, ok := t.Provider.(engine.DataPlane)
		if !ok {
			return engine.ErrNotSupported
		}
		e = a.DeleteDatabaseUser(ctx, t.Context, "admin", cred.Username)
	case "rabbitmq":
		a, ok := t.Provider.(rabbitmq.Access)
		if !ok {
			return engine.ErrNotSupported
		}
		e = a.DeleteUser(ctx, t.Context, cred.Username)
	}
	if errors.Is(e, engine.ErrNotFound) {
		return nil
	}
	if e != nil {
		return fmt.Errorf("%w: target identity deletion failed", engine.ErrUnavailable)
	}
	return nil
}

func (p *Provider) trustCAs(ctx context.Context, rc *engine.Context) ([]string, error) {
	records, e := p.ListDatabaseConnections(ctx, rc)
	if e != nil {
		return nil, e
	}
	seen := map[string]bool{}
	out := []string{}
	for _, c := range records {
		ref := domain.ClusterRef{Namespace: rc.Cluster.Ref().Namespace, Name: c.TargetCluster}
		b, e := rc.Secrets.GetClusterSecret(ctx, ref, "tls/ca.crt")
		if e != nil {
			if c.Phase == ConnectionDeleting && missing(e) {
				continue
			}
			return nil, e
		}
		pem := string(b)
		if !seen[pem] {
			seen[pem] = true
			out = append(out, pem)
		}
	}
	sort.Strings(out)
	return out, nil
}

func (p *Provider) reconcileConnections(ctx context.Context, rc *engine.Context) (bool, error) {
	defer p.lockConnections(rc)()
	records, e := p.ListDatabaseConnections(ctx, rc)
	if e != nil {
		return false, e
	}
	for i := range records {
		c := &records[i]
		if c.Phase != ConnectionPending && c.Phase != ConnectionDeleting {
			continue
		}
		if p.options.Targets == nil {
			return true, engine.ErrUnavailable
		}
		t, e := p.options.Targets(ctx, domain.ClusterRef{Namespace: rc.Cluster.Ref().Namespace, Name: c.TargetCluster})
		if c.Phase == ConnectionDeleting {
			if errors.Is(e, engine.ErrNotFound) {
				t = Target{}
			} else if e != nil {
				return true, e
			}
			return true, p.deleteConnection(ctx, rc, t, records, i)
		}
		if e != nil {
			return true, e
		}
		if e = p.advanceConnection(ctx, rc, t, c); e != nil {
			return true, e
		}
		return true, saveConnections(ctx, rc, records)
	}
	return false, nil
}

func connectionHost(host, port string) string {
	if _, _, e := net.SplitHostPort(host); e == nil {
		return host
	}
	return net.JoinHostPort(host, port)
}
func targetCA(ctx context.Context, t Target) (string, error) {
	b, e := secret(ctx, t.Context, "tls/ca.crt")
	return string(b), e
}
func targetHosts(t Target) []string { return t.Context.Cluster.Status.Connection.Hosts }

// configureConnection never exposes target passwords through status or errors;
// baoapi deliberately omits upstream diagnostics which may echo request bodies.
func (p *Provider) configureConnection(ctx context.Context, rc *engine.Context, t Target, client *baoapi.Client, c DatabaseConnection, cred connectionCredential) error {
	var mounts struct {
		Data map[string]any `json:"data"`
	}
	if e := client.Do(ctx, "GET", "sys/mounts", nil, &mounts); e != nil {
		return e
	}
	if _, ok := mounts.Data[c.MountPath+"/"]; !ok {
		kind := "database"
		if c.TargetProvider == "rabbitmq" {
			kind = "rabbitmq"
		}
		if e := client.Do(ctx, "POST", "sys/mounts/"+c.MountPath, map[string]any{"type": kind}, nil); e != nil {
			return e
		}
	}
	hosts := targetHosts(t)
	if len(hosts) == 0 {
		return engine.ErrUnavailable
	}
	ca, e := targetCA(ctx, t)
	if e != nil {
		return e
	}
	body := map[string]any{"username": cred.Username, "password": cred.Password, "verify_connection": true}
	path := "database/config/" + c.Name
	switch c.TargetProvider {
	case "postgres":
		if t.Context.Cluster.Spec.Components.Extra["proxy"].Replicas == 0 {
			for _, member := range t.Context.Cluster.Status.Members {
				if member.Role == "primary" {
					hosts = []string{member.Name}
					break
				}
			}
		}
		body["plugin_name"] = "postgresql-database-plugin"
		body["allowed_roles"] = []string{c.Name + "_*"}
		body["connection_url"] = "postgresql://{{username}}:{{password}}@" + connectionHost(hosts[0], "5432") + "/postgres?sslmode=verify-full"
	case "psmdb":
		members := make([]string, len(hosts))
		for i, h := range hosts {
			members[i] = connectionHost(h, "27017")
		}
		body["plugin_name"] = "mongodb-database-plugin"
		body["allowed_roles"] = []string{c.Name + "_*"}
		body["connection_url"] = "mongodb://{{username}}:{{password}}@" + strings.Join(members, ",") + "/admin?replicaSet=rs0&tls=true"
		body["tls_ca"] = ca
	case "rabbitmq":
		path = c.MountPath + "/config/connection"
		host, _, splitErr := net.SplitHostPort(hosts[0])
		if splitErr != nil {
			host = hosts[0]
		}
		body["connection_uri"] = "https://" + net.JoinHostPort(host, "15671")
	}
	if e = client.Do(ctx, "POST", path, body, nil); e != nil {
		return e
	}
	if c.TargetProvider == "rabbitmq" {
		role := c.Roles[0]
		if e = client.Do(ctx, "POST", c.MountPath+"/config/lease", map[string]any{"ttl": role.DefaultTTLSeconds, "max_ttl": role.MaxTTLSeconds}, nil); e != nil {
			return e
		}
	}
	for _, role := range c.Roles {
		payload := rolePayload(c, role)
		path = "database/roles/" + c.Name + "_" + role.Name
		if c.TargetProvider == "rabbitmq" {
			path = c.MountPath + "/roles/" + role.Name
		}
		if e = client.Do(ctx, "POST", path, payload, nil); e != nil {
			return e
		}
	}
	return nil
}

func rolePayload(c DatabaseConnection, r DatabaseRole) map[string]any {
	payload := map[string]any{"db_name": c.Name, "default_ttl": r.DefaultTTLSeconds, "max_ttl": r.MaxTTLSeconds}
	switch c.TargetProvider {
	case "postgres":
		grants := "pg_read_all_data"
		if r.Access == AccessReadWrite {
			grants += ", pg_write_all_data"
		}
		payload["creation_statements"] = []string{`CREATE ROLE "{{name}}" WITH LOGIN PASSWORD '{{password}}' VALID UNTIL '{{expiration}}'; GRANT ` + grants + ` TO "{{name}}";`}
		payload["revocation_statements"] = []string{`DROP ROLE IF EXISTS "{{name}}";`}
	case "psmdb":
		role := "readAnyDatabase"
		db := "admin"
		if r.Database != "" {
			db = r.Database
			role = "read"
		}
		if r.Access == AccessReadWrite {
			if r.Database == "" {
				role = "readWriteAnyDatabase"
			} else {
				role = "readWrite"
			}
		}
		statement, _ := json.Marshal(map[string]any{"db": "admin", "roles": []map[string]string{{"role": role, "db": db}}})
		payload["creation_statements"] = []string{string(statement)}
	case "rabbitmq":
		configure, write := "", ""
		if r.Access == AccessReadWrite {
			configure = ".*"
			write = ".*"
		}
		vhosts, _ := json.Marshal(map[string]any{r.Database: map[string]string{"configure": configure, "write": write, "read": ".*"}})
		payload["vhosts"] = string(vhosts)
		delete(payload, "db_name")
		delete(payload, "default_ttl")
		delete(payload, "max_ttl")
	}
	return payload
}

func (p *Provider) DeleteDatabaseConnection(ctx context.Context, rc *engine.Context, t Target, name string) error {
	defer p.lockConnections(rc)()
	records, e := p.ListDatabaseConnections(ctx, rc)
	if e != nil {
		return e
	}
	for i := range records {
		if records[i].Name == name {
			return p.deleteConnection(ctx, rc, t, records, i)
		}
	}
	return engine.ErrNotFound
}
func (p *Provider) deleteConnection(ctx context.Context, rc *engine.Context, t Target, records []DatabaseConnection, i int) error {
	c := &records[i]
	if t.Context != nil && (t.Context.Cluster == nil || t.Provider == nil || t.Context.Cluster.Ref() != (domain.ClusterRef{Namespace: rc.Cluster.Ref().Namespace, Name: c.TargetCluster})) {
		return engine.Invalid("incorrect connection target")
	}
	c.Phase = ConnectionDeleting
	if e := saveConnections(ctx, rc, records); e != nil {
		return e
	}
	cred, e := readConnectionCredential(ctx, rc, c.Name)
	if missing(e) {
		return saveConnections(ctx, rc, append(records[:i], records[i+1:]...))
	}
	if e != nil {
		return e
	}
	if cred.Step != "deleted-config" {
		client, e := p.connectionClient(ctx, rc)
		if e != nil {
			return e
		}
		// Revoke synchronously while the plugin still has both its connection and
		// role definitions. Database lease prefixes end at the complete role name;
		// a partial connection-name prefix is not a lease subtree.
		prefixes := []string{c.MountPath + "/creds/"}
		if c.TargetProvider != "rabbitmq" {
			prefixes = nil
			for _, r := range c.Roles {
				prefixes = append(prefixes, "database/creds/"+c.Name+"_"+r.Name)
			}
		}
		for _, prefix := range prefixes {
			if e = client.Do(ctx, "PUT", "sys/leases/revoke-prefix/"+prefix, map[string]bool{"sync": true}, nil); e != nil && !baoapi.IsStatus(e, 404) {
				return e
			}
		}
		for _, r := range c.Roles {
			path := "database/roles/" + c.Name + "_" + r.Name
			if c.TargetProvider == "rabbitmq" {
				path = c.MountPath + "/roles/" + r.Name
			}
			if e = client.Do(ctx, "DELETE", path, nil, nil); e != nil && !baoapi.IsStatus(e, 404) {
				return e
			}
		}
		if c.TargetProvider != "rabbitmq" {
			if e = client.Do(ctx, "DELETE", "database/config/"+c.Name, nil, nil); e != nil && !baoapi.IsStatus(e, 404) {
				return e
			}
		}
		shared := false
		for j, other := range records {
			if j != i && other.MountPath == c.MountPath {
				shared = true
			}
		}
		if !shared {
			if e = client.Do(ctx, "DELETE", "sys/mounts/"+c.MountPath, nil, nil); e != nil && !baoapi.IsStatus(e, 404) {
				return e
			}
		}
		cred.Step = "deleted-config"
		if e = saveConnectionCredential(ctx, rc, c.Name, cred); e != nil {
			return e
		}
	}
	if t.Context != nil {
		if e = deleteTargetIdentity(ctx, t, *c, cred); e != nil {
			return e
		}
	} else {
		connectionCleanupEvent(ctx, rc, "ConnectionTargetMissing", c.Name)
	}
	name := c.Name
	if e = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), SecretConnectionPrefix+name); e != nil {
		return e
	}
	return saveConnections(ctx, rc, append(records[:i], records[i+1:]...))
}

func connectionCleanupEvent(ctx context.Context, rc *engine.Context, reason, name string) {
	if rc.Events != nil {
		_ = rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Reason: reason, Message: "OpenBao connection " + name + " target cleanup skipped"})
	}
}

func (p *Provider) cleanupConnections(ctx context.Context, rc *engine.Context) {
	defer p.lockConnections(rc)()
	records, e := p.ListDatabaseConnections(ctx, rc)
	if e != nil {
		return
	}
	for _, c := range records {
		if p.options.Targets == nil {
			connectionCleanupEvent(ctx, rc, "ConnectionTargetCleanupFailed", c.Name)
			continue
		}
		t, e := p.options.Targets(ctx, domain.ClusterRef{Namespace: rc.Cluster.Ref().Namespace, Name: c.TargetCluster})
		if e != nil {
			connectionCleanupEvent(ctx, rc, "ConnectionTargetCleanupFailed", c.Name)
			continue
		}
		cred, e := readConnectionCredential(ctx, rc, c.Name)
		if e == nil {
			e = deleteTargetIdentity(ctx, t, c, cred)
		}
		if e != nil {
			connectionCleanupEvent(ctx, rc, "ConnectionTargetCleanupFailed", c.Name)
		}
	}
}
