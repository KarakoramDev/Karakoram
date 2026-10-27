// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	mysqldriver "github.com/go-sql-driver/mysql"
)

// AccessDependencies supplies a pooled utf8mb4 connection as kk_agent over the
// local Unix socket. It must refuse proxy members and lifecycle/recovery states
// in which native access cannot safely run. The caller retains pool ownership.
type AccessDependencies interface {
	LocalAccessDB(context.Context) (*sql.DB, error)
	GaleraAccess() bool
}

func (e *Runner) GaleraAccess() bool {
	s, _ := e.state()
	return s.Galera != nil
}

type accessError struct {
	status  int
	message string
}

func (e *accessError) Error() string     { return e.message }
func invalidAccess(message string) error { return &accessError{http.StatusBadRequest, message} }
func accessFailure(err error) *accessError {
	var safe *accessError
	if errors.As(err, &safe) {
		return safe
	}
	if errors.Is(err, sql.ErrNoRows) {
		return &accessError{404, "MySQL object not found"}
	}
	var native *mysqldriver.MySQLError
	if errors.As(err, &native) {
		switch native.Number {
		case 1007, 1396, 1062, 3579:
			return &accessError{409, "MySQL object conflicts with the requested change"}
		case 1008, 1049, 1141:
			return &accessError{404, "MySQL object not found"}
		case 1044, 1045, 1142, 1227:
			return &accessError{403, "MySQL rejected the requested privileges"}
		case 1290, 3101:
			return &accessError{503, "MySQL is not currently writable"}
		case 1064, 1115, 1253, 1273, 1819:
			return &accessError{400, "MySQL rejected the object definition"}
		}
	}
	return &accessError{503, "MySQL access operation unavailable"}
}

// RegisterAccessRoutes mounts bounded, authenticated-transport administration
// actions. The outer member server is responsible for transport authentication.
func RegisterAccessRoutes(mux *http.ServeMux, deps AccessDependencies) {
	// GET_LOCK is forbidden by PXC strict mode. Serialize the same node-local
	// administration boundary in the agent instead; CP writer routing and sealed
	// operation receipts remain authoritative across writer changes.
	galeraLock := make(chan struct{}, 1)
	for _, action := range []string{myspec.ActionAccessUsers, myspec.ActionAccessDatabases, myspec.ActionAccessGrants, myspec.ActionAccessCapabilities} {
		mux.HandleFunc("POST /v1/mysql/"+action, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Cache-Control", "no-store")
			var req myspec.AccessRequest
			decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&req); err != nil {
				http.Error(w, "invalid MySQL access request", 400)
				return
			}
			if err := decoder.Decode(new(any)); err != io.EOF {
				http.Error(w, "invalid MySQL access request", 400)
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
			defer cancel()
			var result any
			var err error
			if action == myspec.ActionAccessCapabilities {
				result = myspec.AccessCapabilities{CredentialReceipts: true}
			} else {
				var db *sql.DB
				db, err = deps.LocalAccessDB(ctx)
				if err == nil && db == nil {
					err = errors.New("missing local database")
				}
				if err == nil {
					var lock chan struct{}
					if deps.GaleraAccess() {
						lock = galeraLock
					}
					result, err = executeAccess(ctx, db, action, req, lock)
				}
			}
			if err != nil {
				safe := accessFailure(err)
				http.Error(w, safe.message, safe.status)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(result)
		})
	}
}

func executeAccess(ctx context.Context, db *sql.DB, action string, req myspec.AccessRequest, galeraLock chan struct{}) (any, error) {
	if galeraLock != nil {
		lockCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		select {
		case galeraLock <- struct{}{}:
			defer func() { <-galeraLock }()
		case <-lockCtx.Done():
			return nil, &accessError{503, "MySQL access operation unavailable"}
		}
	}
	c, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	// Session variables, prepared statements and advisory locks must never escape
	// into the pool, including cancellation/transport failures during cleanup.
	defer func() { _ = c.Raw(func(any) error { return driver.ErrBadConn }) }()
	var currentUser string
	var readOnly, superReadOnly bool
	if err = c.QueryRowContext(ctx, "SELECT CURRENT_USER(), @@global.read_only, @@global.super_read_only").Scan(&currentUser, &readOnly, &superReadOnly); err != nil {
		return nil, err
	}
	if currentUser != myspec.UserAgent+"@localhost" {
		return nil, &accessError{503, "MySQL access requires the local administration account"}
	}
	if readOnly || superReadOnly {
		return nil, &accessError{503, "MySQL is not currently writable"}
	}
	if _, err = c.ExecContext(ctx, "SET SESSION sql_mode = 'STRICT_ALL_TABLES'"); err != nil {
		return nil, err
	}
	if galeraLock == nil {
		var acquired sql.NullInt64
		if err = c.QueryRowContext(ctx, "SELECT GET_LOCK('karakoram.access', 5)").Scan(&acquired); err != nil {
			return nil, err
		}
		if !acquired.Valid || acquired.Int64 != 1 {
			return nil, &accessError{503, "MySQL access operation unavailable"}
		}
	}
	switch action {
	case myspec.ActionAccessUsers:
		switch req.Operation {
		case "capabilities":
			return myspec.AccessCapabilities{CredentialReceipts: true}, nil
		case "list", "get":
			if req.Name != "" && !myspec.ManagedUser(req.Name) {
				return nil, sql.ErrNoRows
			}
			if req.Operation == "get" && req.Name == "" {
				return nil, invalidAccess("user name required")
			}
			users, err := readUsers(ctx, c, req.Name)
			if err != nil {
				return nil, err
			}
			if req.Operation == "get" {
				if len(users) == 0 {
					return nil, sql.ErrNoRows
				}
				return users[0], nil
			}
			return users, nil
		case "create", "patch", "password", "discard-old":
			return journalUser(ctx, c, req), nil
		case "delete":
			return journalDeleteUser(ctx, c, req), nil
		}
	case myspec.ActionAccessDatabases:
		switch req.Operation {
		case "list", "get":
			if req.Name != "" && !myspec.ManagedDatabase(req.Name) {
				return nil, sql.ErrNoRows
			}
			if req.Operation == "get" && req.Name == "" {
				return nil, invalidAccess("database name required")
			}
			databases, err := readDatabases(ctx, c, req.Name)
			if err != nil {
				return nil, err
			}
			if req.Operation == "get" {
				if len(databases) == 0 {
					return nil, sql.ErrNoRows
				}
				return databases[0], nil
			}
			return databases, nil
		case "create":
			if req.Database == nil {
				return nil, invalidAccess("database definition required")
			}
			stmt, err := databaseStatement(*req.Database)
			if err != nil {
				return nil, err
			}
			_, err = c.ExecContext(ctx, stmt)
			return struct{}{}, err
		case "delete":
			if !myspec.ManagedDatabase(req.Name) {
				return nil, invalidAccess("invalid or protected database name")
			}
			if !req.Force {
				var count int
				if err = c.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?", req.Name).Scan(&count); err != nil {
					return nil, err
				}
				if count > 0 {
					return nil, &accessError{409, "MySQL database is not empty; explicit force confirmation required"}
				}
			}
			_, err = c.ExecContext(ctx, "DROP DATABASE "+quoteIdentifier(req.Name))
			return struct{}{}, err
		}
	case myspec.ActionAccessGrants:
		if req.Operation == "list" {
			return readGrants(ctx, c, req.UserFilter, req.DatabaseFilter)
		}
		if req.Grant == nil {
			return nil, invalidAccess("grant definition required")
		}
		var partialRevokes bool
		if err = c.QueryRowContext(ctx, "SELECT @@global.partial_revokes").Scan(&partialRevokes); err != nil {
			return nil, err
		}
		stmt, err := grantStatement(req.Operation, *req.Grant, partialRevokes)
		if err != nil {
			return nil, err
		}
		// GRANT can otherwise create permissions on nonexistent databases.
		var exists int
		if err = c.QueryRowContext(ctx, "SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = ?", req.Grant.Database).Scan(&exists); err != nil {
			return nil, err
		}
		_, err = c.ExecContext(ctx, stmt)
		return struct{}{}, err
	}
	return nil, invalidAccess("invalid MySQL access action")
}
func readUsers(ctx context.Context, c *sql.Conn, name string) ([]myspec.User, error) {
	rows, err := c.QueryContext(ctx, `SELECT User, Host, max_user_connections, account_locked = 'Y', COALESCE(JSON_CONTAINS_PATH(User_attributes, 'one', '$.additional_password'), 0) FROM mysql.user WHERE Host = '%' AND (? = '' OR User = ?) ORDER BY User`, name, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []myspec.User{}
	for rows.Next() {
		var user myspec.User
		if err = rows.Scan(&user.Name, &user.Host, &user.MaxUserConnections, &user.Locked, &user.PasswordRetained); err != nil {
			return nil, err
		}
		if myspec.ManagedUser(user.Name) {
			out = append(out, user)
		}
	}
	return out, rows.Err()
}
func readDatabases(ctx context.Context, c *sql.Conn, name string) ([]myspec.Database, error) {
	rows, err := c.QueryContext(ctx, "SELECT SCHEMA_NAME, DEFAULT_CHARACTER_SET_NAME, DEFAULT_COLLATION_NAME FROM information_schema.SCHEMATA WHERE (? = '' OR SCHEMA_NAME = ?) ORDER BY SCHEMA_NAME", name, name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []myspec.Database{}
	for rows.Next() {
		var database myspec.Database
		if err = rows.Scan(&database.Name, &database.CharacterSet, &database.Collation); err != nil {
			return nil, err
		}
		if myspec.ManagedDatabase(database.Name) {
			out = append(out, database)
		}
	}
	return out, rows.Err()
}
func readGrants(ctx context.Context, c *sql.Conn, user, database string) ([]myspec.Grant, error) {
	if user == "" && database == "" {
		return nil, invalidAccess("user or database filter required")
	}
	if user != "" && !myspec.ManagedUser(user) || database != "" && !myspec.ManagedDatabase(database) {
		return nil, invalidAccess("invalid or protected grant filter")
	}
	rows, err := c.QueryContext(ctx, "SELECT GRANTEE, TABLE_SCHEMA, PRIVILEGE_TYPE FROM information_schema.SCHEMA_PRIVILEGES ORDER BY GRANTEE, TABLE_SCHEMA, PRIVILEGE_TYPE")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []myspec.Grant{}
	index := map[string]int{}
	for rows.Next() {
		var grantee, schema, privilege string
		if err = rows.Scan(&grantee, &schema, &privilege); err != nil {
			return nil, err
		}
		if !strings.HasSuffix(grantee, "'@'%'") || !strings.HasPrefix(grantee, "'") {
			continue
		}
		name := strings.TrimSuffix(strings.TrimPrefix(grantee, "'"), "'@'%'")
		schema = strings.ReplaceAll(schema, `\_`, "_")
		if !myspec.ManagedUser(name) || !myspec.ManagedDatabase(schema) || user != "" && name != user || database != "" && schema != database {
			continue
		}
		wire := strings.ReplaceAll(privilege, " ", "_")
		allowed := false
		for _, p := range myspec.DatabasePrivileges {
			if wire == p {
				allowed = true
				break
			}
		}
		if !allowed {
			continue
		}
		key := name + "/" + schema
		i, exists := index[key]
		if !exists {
			i = len(out)
			index[key] = i
			out = append(out, myspec.Grant{User: name, Database: schema, Privileges: []string{}})
		}
		out[i].Privileges = append(out[i].Privileges, wire)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].User != out[j].User {
			return out[i].User < out[j].User
		}
		return out[i].Database < out[j].Database
	})
	return out, rows.Err()
}
