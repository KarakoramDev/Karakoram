// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type accessError struct {
	status  int
	message string
}

func (e *accessError) Error() string     { return e.message }
func invalidAccess(message string) error { return &accessError{http.StatusBadRequest, message} }
func accessFailure(err error) error {
	var safe *accessError
	if errors.As(err, &safe) {
		return safe
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return &accessError{http.StatusNotFound, "PostgreSQL object not found"}
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		switch pg.Code {
		case "42704", "42703", "42P01", "3D000":
			return &accessError{http.StatusNotFound, "PostgreSQL object not found"}
		case "25006":
			return &accessError{http.StatusServiceUnavailable, "PostgreSQL is not currently writable"}
		case "42710", "42P04", "23505", "2BP01", "55006", "55000", "40001", "40P01":
			return &accessError{http.StatusConflict, "PostgreSQL object conflicts with the requested change"}
		case "42501":
			return &accessError{http.StatusForbidden, "PostgreSQL rejected the requested privileges"}
		}
		if strings.HasPrefix(pg.Code, "22") || strings.HasPrefix(pg.Code, "42") {
			return invalidAccess("PostgreSQL rejected the object definition")
		}
	}
	return &accessError{http.StatusServiceUnavailable, "PostgreSQL access operation unavailable"}
}
func validSQLName(name string) bool {
	if name == "" || len(name) > 63 || !utf8.ValidString(name) {
		return false
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}
func managedRole(name string) bool {
	return validSQLName(name) && name != "postgres" && !strings.HasPrefix(name, "pg_") && !strings.HasPrefix(name, "kk_") && !strings.HasPrefix(name, "kb_")
}
func managedDatabase(name string) bool {
	return validSQLName(name) && name != "postgres" && name != "template0" && name != "template1"
}
func allowedMembership(name string) bool {
	return managedRole(name) || name == "pg_read_all_data" || name == "pg_write_all_data" || name == "pg_monitor" || name == "pg_read_all_stats" || name == "pg_signal_backend"
}
func validVerifier(verifier string) bool {
	if !safeValue(verifier, 1024) || !strings.HasPrefix(verifier, "SCRAM-SHA-256$") {
		return false
	}
	pieces := strings.Split(strings.TrimPrefix(verifier, "SCRAM-SHA-256$"), "$")
	if len(pieces) != 2 {
		return false
	}
	iterations, salt, ok := strings.Cut(pieces[0], ":")
	if !ok {
		return false
	}
	count, err := strconv.Atoi(iterations)
	if err != nil || count < 4096 || count > 10000000 {
		return false
	}
	saltBytes, err := base64.StdEncoding.Strict().DecodeString(salt)
	if err != nil || len(saltBytes) < 16 || len(saltBytes) > 64 {
		return false
	}
	stored, server, ok := strings.Cut(pieces[1], ":")
	if !ok {
		return false
	}
	for _, encoded := range []string{stored, server} {
		key, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil || len(key) != 32 {
			return false
		}
	}
	return true
}
func validateRoleWrite(role pgspec.RoleWrite) error {
	if !managedRole(role.Name) && !(role.Binding && bindingRole(role.Name)) {
		return invalidAccess("invalid or protected role name")
	}
	if role.Binding && (role.Superuser || role.Createdb || role.Createrole || role.Replication || role.BypassRLS || len(role.MemberOf) != 0 || role.ConnectionLimit < 1) {
		return invalidAccess("binding roles must be bounded least-privilege roles")
	}
	if role.ConnectionLimit < -1 || role.ConnectionLimit > 1000000 {
		return invalidAccess("invalid role connection limit")
	}
	if len(role.MemberOf) > 64 {
		return invalidAccess("too many role memberships")
	}
	seen := map[string]bool{}
	for _, name := range role.MemberOf {
		if !allowedMembership(name) || name == role.Name || seen[name] {
			return invalidAccess("invalid, privileged or duplicate role membership")
		}
		seen[name] = true
	}
	if role.ValidUntil != nil && *role.ValidUntil != "infinity" {
		if _, err := time.Parse(time.RFC3339, *role.ValidUntil); err != nil {
			return invalidAccess("validUntil must be RFC3339 or infinity")
		}
	}
	if role.Password != nil && !validVerifier(*role.Password) {
		return invalidAccess("role password must be a SCRAM-SHA-256 verifier")
	}
	return nil
}
func roleStatement(operation string, role pgspec.RoleWrite) (string, []any, error) {
	if operation != "create" && operation != "update" {
		return "", nil, invalidAccess("invalid role write operation")
	}
	if err := validateRoleWrite(role); err != nil {
		return "", nil, err
	}
	verb := "CREATE"
	if operation == "update" {
		verb = "ALTER"
	}
	sql := verb + " ROLE " + pgx.Identifier{role.Name}.Sanitize() + " WITH"
	for _, option := range []struct {
		name    string
		enabled bool
	}{{"LOGIN", role.Login}, {"SUPERUSER", role.Superuser}, {"CREATEDB", role.Createdb}, {"CREATEROLE", role.Createrole}, {"REPLICATION", role.Replication}, {"BYPASSRLS", role.BypassRLS}} {
		sql += " "
		if !option.enabled {
			sql += "NO"
		}
		sql += option.name
	}
	sql += " CONNECTION LIMIT " + strconv.Itoa(role.ConnectionLimit)
	until := "infinity"
	if role.ValidUntil != nil {
		until = *role.ValidUntil
	}
	args := []any{pgx.QueryExecModeSimpleProtocol, until}
	sql += " VALID UNTIL $1"
	if role.Password != nil {
		args = append(args, *role.Password)
		sql += " PASSWORD $2"
	}
	return sql, args, nil
}

const roleSelect = `SELECT r.rolname,r.rolcanlogin,r.rolsuper,r.rolcreatedb,r.rolcreaterole,r.rolreplication,r.rolbypassrls,r.rolconnlimit,
 ARRAY(SELECT DISTINCT parent.rolname FROM pg_catalog.pg_auth_members membership JOIN pg_catalog.pg_roles parent ON parent.oid=membership.roleid WHERE membership.member=r.oid ORDER BY parent.rolname),
 CASE WHEN pg_catalog.isfinite(r.rolvaliduntil) THEN pg_catalog.to_char(r.rolvaliduntil AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US"Z"') ELSE NULL END FROM pg_catalog.pg_roles r`

func readRoles(ctx context.Context, c *pgx.Conn, name string) ([]pgspec.Role, error) {
	query := roleSelect
	args := []any{}
	if name != "" {
		query += " WHERE r.rolname=$1"
		args = append(args, name)
	}
	query += " ORDER BY r.rolname"
	rows, err := c.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []pgspec.Role{}
	for rows.Next() {
		var role pgspec.Role
		if err = rows.Scan(&role.Name, &role.Login, &role.Superuser, &role.Createdb, &role.Createrole, &role.Replication, &role.BypassRLS, &role.ConnectionLimit, &role.MemberOf, &role.ValidUntil); err != nil {
			return nil, err
		}
		result = append(result, role)
	}
	return result, rows.Err()
}
func writeRole(ctx context.Context, c *pgx.Conn, operation string, role pgspec.RoleWrite) error {
	if _, _, err := roleStatement(operation, role); err != nil {
		return err
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = writeRoleTx(ctx, tx, operation, role); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func writeRoleTx(ctx context.Context, tx pgx.Tx, operation string, role pgspec.RoleWrite) error {
	sql, args, err := roleStatement(operation, role)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, sql, args...); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `SELECT parent.rolname,grantor.rolname FROM pg_catalog.pg_auth_members membership JOIN pg_catalog.pg_roles parent ON parent.oid=membership.roleid JOIN pg_catalog.pg_roles child ON child.oid=membership.member JOIN pg_catalog.pg_roles grantor ON grantor.oid=membership.grantor WHERE child.rolname=$1`, role.Name)
	if err != nil {
		return err
	}
	type membership struct{ parent, grantor string }
	current := []membership{}
	for rows.Next() {
		var row membership
		if err = rows.Scan(&row.parent, &row.grantor); err != nil {
			rows.Close()
			return err
		}
		current = append(current, row)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	desired := map[string]bool{}
	for _, parent := range role.MemberOf {
		desired[parent] = true
	}
	present := map[string]bool{}
	for _, membership := range current {
		present[membership.parent] = true
		if !desired[membership.parent] {
			sql := "REVOKE " + pgx.Identifier{membership.parent}.Sanitize() + " FROM " + pgx.Identifier{role.Name}.Sanitize() + " GRANTED BY " + pgx.Identifier{membership.grantor}.Sanitize()
			if _, err = tx.Exec(ctx, sql); err != nil {
				return err
			}
		}
	}
	for _, parent := range role.MemberOf {
		if !present[parent] {
			if _, err = tx.Exec(ctx, "GRANT "+pgx.Identifier{parent}.Sanitize()+" TO "+pgx.Identifier{role.Name}.Sanitize()); err != nil {
				return err
			}
		}
	}
	return nil
}

// ROW(d) has the anonymous record type. row_to_json(record) is an exact builtin
// match, so even an old named pg_database overload cannot intercept this call.
const databaseSelect = `SELECT d.datname,pg_catalog.pg_get_userbyid(d.datdba),d.datdba,pg_catalog.pg_encoding_to_char(d.encoding),COALESCE(pg_catalog.row_to_json(ROW(d))->'f1'->>'datlocale',pg_catalog.row_to_json(ROW(d))->'f1'->>'daticulocale',d.datcollate),d.datallowconn FROM pg_catalog.pg_database d`

func readDatabases(ctx context.Context, c *pgx.Conn, name string) ([]pgspec.Database, error) {
	query := databaseSelect
	args := []any{}
	if name != "" {
		query += " WHERE d.datname=$1"
		args = append(args, name)
	}
	query += " ORDER BY d.datname"
	rows, err := c.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []pgspec.Database{}
	for rows.Next() {
		var db pgspec.Database
		if err = rows.Scan(&db.Name, &db.Owner, &db.OwnerOID, &db.Encoding, &db.Locale, &db.AllowConnections); err != nil {
			return nil, err
		}
		result = append(result, db)
	}
	return result, rows.Err()
}
func validateDatabase(db pgspec.Database) error {
	if !managedDatabase(db.Name) {
		return invalidAccess("invalid or protected database name")
	}
	if db.Owner != "" && !validSQLName(db.Owner) {
		return invalidAccess("invalid database owner")
	}
	if !safeValue(db.Encoding, 64) || !safeValue(db.Locale, 128) {
		return invalidAccess("invalid database encoding or locale")
	}
	return nil
}
func createDatabaseStatement(db pgspec.Database) (string, []any, error) {
	if err := validateDatabase(db); err != nil {
		return "", nil, err
	}
	sql := "CREATE DATABASE " + pgx.Identifier{db.Name}.Sanitize() + " WITH ALLOW_CONNECTIONS " + strconv.FormatBool(db.AllowConnections)
	args := []any{pgx.QueryExecModeSimpleProtocol}
	add := func(value string) string { args = append(args, value); return "$" + strconv.Itoa(len(args)-1) }
	if db.Owner != "" {
		sql += " OWNER " + pgx.Identifier{db.Owner}.Sanitize()
	}
	if db.Encoding != "" {
		sql += " ENCODING " + add(db.Encoding)
	}
	if db.Locale != "" {
		locale := add(db.Locale)
		sql += " LC_COLLATE " + locale + " LC_CTYPE " + locale + " TEMPLATE template0"
	}
	return sql, args, nil
}
func updateDatabase(ctx context.Context, c *pgx.Conn, db pgspec.Database) error {
	if err := validateDatabase(db); err != nil {
		return err
	}
	existing, err := readDatabases(ctx, c, db.Name)
	if err != nil {
		return err
	}
	if len(existing) == 0 {
		return pgx.ErrNoRows
	}
	old := existing[0]
	if (db.Encoding != "" && !strings.EqualFold(strings.ReplaceAll(db.Encoding, "-", ""), strings.ReplaceAll(old.Encoding, "-", ""))) || (db.Locale != "" && db.Locale != old.Locale) {
		return invalidAccess("database encoding and locale are immutable")
	}
	tx, err := c.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if db.Owner != "" && db.Owner != old.Owner {
		if _, err = tx.Exec(ctx, "ALTER DATABASE "+pgx.Identifier{db.Name}.Sanitize()+" OWNER TO "+pgx.Identifier{db.Owner}.Sanitize()); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(ctx, "ALTER DATABASE "+pgx.Identifier{db.Name}.Sanitize()+" WITH ALLOW_CONNECTIONS "+strconv.FormatBool(db.AllowConnections)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func grantStatement(operation string, grant pgspec.Grant) (string, error) {
	if operation != "grant" && operation != "revoke" {
		return "", invalidAccess("invalid grant operation")
	}
	if !managedRole(grant.Role) || !validSQLName(grant.Database) || (grant.Schema != "" && !validSQLName(grant.Schema)) {
		return "", invalidAccess("invalid or protected grant object name")
	}
	if len(grant.Privileges) == 0 || len(grant.Privileges) > 3 {
		return "", invalidAccess("invalid privilege count")
	}
	allowed := map[string]bool{"CONNECT": true, "CREATE": true, "TEMP": true, "TEMPORARY": true}
	kind := "DATABASE"
	target := grant.Database
	if grant.Schema != "" {
		allowed = map[string]bool{"USAGE": true, "CREATE": true}
		kind = "SCHEMA"
		target = grant.Schema
	}
	privileges := make([]string, 0, len(grant.Privileges))
	seen := map[string]bool{}
	for _, privilege := range grant.Privileges {
		privilege = strings.ToUpper(privilege)
		if privilege == "TEMPORARY" {
			privilege = "TEMP"
		}
		if !allowed[privilege] || seen[privilege] {
			return "", invalidAccess("invalid or duplicate privilege")
		}
		if operation == "grant" && pgspec.ProtectedSchema(grant.Schema) && privilege != "USAGE" {
			return "", invalidAccess("write privileges on system or engine-managed schemas are forbidden")
		}
		seen[privilege] = true
		privileges = append(privileges, privilege)
	}
	sort.Strings(privileges)
	verb, direction := "GRANT", "TO"
	if operation == "revoke" {
		verb, direction = "REVOKE", "FROM"
	}
	return verb + " " + strings.Join(privileges, ",") + " ON " + kind + " " + pgx.Identifier{target}.Sanitize() + " " + direction + " " + pgx.Identifier{grant.Role}.Sanitize(), nil
}
func databaseConnect(ctx context.Context, c *pgx.Conn, name string) (*pgx.Conn, error) {
	config := c.Config().Copy()
	config.Database = name
	return pgx.ConnectConfig(ctx, config)
}
func readGrants(ctx context.Context, c *pgx.Conn, database string) ([]pgspec.Grant, error) {
	target, err := databaseConnect(ctx, c, database)
	if err != nil {
		return nil, err
	}
	defer target.Close(ctx)
	rows, err := target.Query(ctx, `SELECT COALESCE(role.rolname,'PUBLIC'),''::text,acl.privilege_type FROM pg_catalog.pg_database d CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(d.datacl,pg_catalog.acldefault('d',d.datdba))) acl LEFT JOIN pg_catalog.pg_roles role ON role.oid=acl.grantee WHERE d.datname=pg_catalog.current_database()
 UNION ALL SELECT COALESCE(role.rolname,'PUBLIC'),n.nspname,acl.privilege_type FROM pg_catalog.pg_namespace n CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(n.nspacl,pg_catalog.acldefault('n',n.nspowner))) acl LEFT JOIN pg_catalog.pg_roles role ON role.oid=acl.grantee ORDER BY 1,2,3`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []pgspec.Grant{}
	for rows.Next() {
		var role, schema, privilege string
		if err = rows.Scan(&role, &schema, &privilege); err != nil {
			return nil, err
		}
		if privilege == "TEMPORARY" {
			privilege = "TEMP"
		}
		if len(result) == 0 || result[len(result)-1].Role != role || result[len(result)-1].Schema != schema {
			result = append(result, pgspec.Grant{Role: role, Database: database, Schema: schema, Privileges: []string{}})
		}
		last := &result[len(result)-1]
		last.Privileges = append(last.Privileges, privilege)
	}
	return result, rows.Err()
}
func validateAccessRequest(action string, req pgspec.AccessRequest) error {
	if req.Role != nil {
		req.Role.Binding = req.Binding
	}
	if req.Name != "" && !validSQLName(req.Name) {
		return invalidAccess("invalid PostgreSQL object name")
	}
	switch action {
	case pgspec.ActionUpdateExtensions:
		return validateExtensionNames(req.Extensions, req.AvailableExtensions)
	case pgspec.ActionRoles:
		if req.Binding && req.Operation != "journal" && (req.Operation == "update" || req.Operation == "password" || req.Operation == "delete") {
			return invalidAccess("binding role mutations require a journaled OID-fenced request")
		}
		switch req.Operation {
		case "capabilities":
			return nil
		case "journal":
			if req.RequestID == "" || !safeValue(req.RequestID, 256) || req.Role == nil ||
				(req.RoleOperation != "create" && req.RoleOperation != "update" && !(req.Binding && req.RoleOperation == "delete")) {
				return invalidAccess("invalid journaled role request")
			}
			if req.Binding && req.RoleOperation != "create" {
				if req.ExpectedRoleOID == 0 {
					return invalidAccess("binding role mutations require the original role OID")
				}
				if req.RoleOperation == "delete" {
					if !bindingRole(req.Role.Name) {
						return invalidAccess("invalid binding role name")
					}
					return nil
				}
			}
			return validateRoleWrite(*req.Role)
		case "list":
			return nil
		case "get":
			if req.Name != "" {
				return nil
			}
		case "delete":
			if managedRole(req.Name) || req.Binding && bindingRole(req.Name) {
				return nil
			}
		case "password":
			if req.Role != nil && (managedRole(req.Role.Name) || req.Binding && bindingRole(req.Role.Name)) && req.Role.Password != nil && validVerifier(*req.Role.Password) && (req.Name == "" || req.Name == req.Role.Name) {
				return nil
			}
		case "create", "update":
			if req.Role != nil && (req.Name == "" || req.Name == req.Role.Name) {
				return validateRoleWrite(*req.Role)
			}
		}
	case pgspec.ActionDatabases:
		switch req.Operation {
		case "public-owner":
			if req.Binding && req.Database != nil && managedDatabase(req.Database.Name) && bindingRole(req.Database.Owner) {
				return nil
			}
		case "list":
			return nil
		case "get":
			if req.Name != "" {
				return nil
			}
		case "delete":
			if managedDatabase(req.Name) {
				return nil
			}
		case "create", "update", "extensions":
			if req.Database != nil && (req.Name == "" || req.Name == req.Database.Name) {
				if err := validateExtensionNames(req.Database.Extensions, req.AvailableExtensions); err != nil {
					return err
				}
				return validateDatabase(*req.Database)
			}
		}
	case pgspec.ActionGrants:
		if req.Operation == "list" {
			name := req.Name
			if name == "" && req.Grant != nil {
				name = req.Grant.Database
			}
			if validSQLName(name) {
				return nil
			}
		}
		if req.Grant != nil {
			_, err := grantStatement(req.Operation, *req.Grant)
			return err
		}
	}
	return invalidAccess("invalid PostgreSQL access action")
}
func executeAccess(ctx context.Context, c *pgx.Conn, action string, req pgspec.AccessRequest) (any, error) {
	switch action {
	case pgspec.ActionUpdateExtensions:
		return updateInstalledExtensions(ctx, c, req.Extensions)
	case pgspec.ActionRoles:
		if req.Operation == "capabilities" {
			return pgspec.AccessCapabilities{CredentialReceipts: true}, nil
		}
		if req.Operation == "journal" {
			return journalRole(ctx, c, req), nil
		}
		switch req.Operation {
		case "list", "get":
			roles, err := readRoles(ctx, c, req.Name)
			if err != nil {
				return nil, err
			}
			if req.Operation == "get" {
				if len(roles) == 0 {
					return nil, pgx.ErrNoRows
				}
				return roles[0], nil
			}
			return roles, nil
		case "create", "update":
			return struct{}{}, writeRole(ctx, c, req.Operation, *req.Role)
		case "password":
			_, err := c.Exec(ctx, "ALTER ROLE "+pgx.Identifier{req.Role.Name}.Sanitize()+" PASSWORD $1", pgx.QueryExecModeSimpleProtocol, *req.Role.Password)
			return struct{}{}, err
		case "delete":
			_, err := c.Exec(ctx, "DROP ROLE "+pgx.Identifier{req.Name}.Sanitize())
			return struct{}{}, err
		}
	case pgspec.ActionDatabases:
		switch req.Operation {
		case "public-owner":
			return struct{}{}, ownBindingPublicSchema(ctx, c, *req.Database)
		case "list", "get":
			databases, err := readDatabases(ctx, c, req.Name)
			if err != nil {
				return nil, err
			}
			if req.Operation == "get" {
				if len(databases) == 0 {
					return nil, pgx.ErrNoRows
				}
				return databases[0], nil
			}
			return databases, nil
		case "create":
			if len(req.Database.Extensions) > 0 && !req.Database.AllowConnections {
				return nil, invalidAccess("database extensions require allowConnections")
			}
			sql, args, err := createDatabaseStatement(*req.Database)
			if err != nil {
				return nil, err
			}
			_, err = c.Exec(ctx, sql, args...)
			if err != nil {
				var pg *pgconn.PgError
				if len(req.Database.Extensions) == 0 || !errors.As(err, &pg) || pg.Code != "42P04" {
					return nil, err
				}
				// CREATE DATABASE is not transactional. A retry after extension
				// failure may resume only against the requested owner.
				existing, readErr := readDatabases(ctx, c, req.Database.Name)
				if readErr != nil {
					return nil, readErr
				}
				if len(existing) != 1 || existing[0].Owner != req.Database.Owner {
					return nil, err
				}
			}
			return struct{}{}, createDatabaseExtensions(ctx, c, *req.Database, req.AvailableExtensions)
		case "update":
			return struct{}{}, updateDatabase(ctx, c, *req.Database)
		case "extensions":
			existing, err := readDatabases(ctx, c, req.Database.Name)
			if err != nil {
				return nil, err
			}
			if len(existing) != 1 || existing[0].Owner != req.Database.Owner {
				return nil, &accessError{http.StatusConflict, "database owner does not match extension request"}
			}
			return struct{}{}, createDatabaseExtensions(ctx, c, *req.Database, req.AvailableExtensions)
		case "delete":
			sql := "DROP DATABASE " + pgx.Identifier{req.Name}.Sanitize()
			if req.Force {
				sql += " WITH (FORCE)"
			}
			_, err := c.Exec(ctx, sql)
			return struct{}{}, err
		}
	case pgspec.ActionGrants:
		if req.Operation == "list" {
			name := req.Name
			if name == "" {
				name = req.Grant.Database
			}
			return readGrants(ctx, c, name)
		}
		sql, err := grantStatement(req.Operation, *req.Grant)
		if err != nil {
			return nil, err
		}
		target := c
		if req.Grant.Schema != "" {
			target, err = databaseConnect(ctx, c, req.Grant.Database)
			if err != nil {
				return nil, err
			}
			defer target.Close(ctx)
		}
		_, err = target.Exec(ctx, sql)
		return struct{}{}, err
	}
	return nil, invalidAccess("invalid PostgreSQL access action")
}
func (e *Runner) accessRoutes(mux *http.ServeMux, n *agent.Node) {
	for _, action := range []string{pgspec.ActionRoles, pgspec.ActionDatabases, pgspec.ActionGrants, pgspec.ActionUpdateExtensions} {
		mux.HandleFunc("POST /v1/postgres/"+action, func(w http.ResponseWriter, r *http.Request) {
			var req pgspec.AccessRequest
			if err := decodeAction(w, r, &req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := validateAccessRequest(action, req); err != nil {
				if action == pgspec.ActionRoles && req.Operation == "journal" {
					actionJSON(w, http.StatusOK, accessRejected("invalid_request", err.Error(), http.StatusBadRequest))
					return
				}
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
			defer cancel()
			var result any
			err := n.WithLock(ctx, func(locked *agent.Node) error {
				s, err := decodeSpec(locked.Config())
				if err != nil {
					return err
				}
				if s.Role != pgspec.RoleData {
					return &accessError{http.StatusServiceUnavailable, "access action requires a data member"}
				}
				if usesPatroni(s) {
					if err = patroniRequest(ctx, s, http.MethodGet, "/primary", nil, nil); err != nil {
						return &accessError{http.StatusServiceUnavailable, "access action requires the Patroni primary with its leader lock"}
					}
				}
				c, err := localConnect(ctx, s, dataPaths(locked.DataDir()))
				if err != nil {
					return err
				}
				defer c.Close(ctx)
				var recovery bool
				if err = c.QueryRow(ctx, "SELECT pg_catalog.pg_is_in_recovery()").Scan(&recovery); err != nil {
					return err
				}
				if recovery {
					return &accessError{http.StatusServiceUnavailable, "access action cannot run on a replica"}
				}
				result, err = executeAccess(ctx, c, action, req)
				return err
			})
			if err != nil {
				safe := accessFailure(err).(*accessError)
				if action == pgspec.ActionRoles && req.Operation == "journal" {
					actionJSON(w, http.StatusOK, pgspec.AccessResult{Outcome: "unknown", Code: "execution_unavailable", Message: safe.message})
					return
				}
				http.Error(w, safe.message, safe.status)
				return
			}
			actionJSON(w, http.StatusOK, result)
		})
	}
}
