// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
)

func decode(r *http.Request, v any) error {
	b, err := io.ReadAll(io.LimitReader(r.Body, 65537))
	if err != nil || len(b) > 65536 || len(bytes.TrimSpace(b)) == 0 || bytes.TrimSpace(b)[0] != '{' {
		return errors.New("request must be a bounded JSON object")
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if d.Decode(v) != nil {
		return errors.New("invalid JSON request")
	}
	var more any
	if d.Decode(&more) != io.EOF {
		return errors.New("request must contain one JSON object")
	}
	return nil
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func safeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, agentapi.ErrorResponse{Error: message})
}
func (e *Engine) Routes(mux *http.ServeMux, n *agent.Node) {
	mux.HandleFunc("POST /v1/neo4j/readiness", func(w http.ResponseWriter, r *http.Request) {
		if decode(r, &struct{}{}) != nil {
			safeError(w, 400, "invalid readiness request")
			return
		}
		writeJSON(w, 200, e.readiness(r.Context(), n))
	})
	mux.HandleFunc("POST /v1/neo4j/users", func(w http.ResponseWriter, r *http.Request) {
		var req neo4jspec.UserRequest
		if decode(r, &req) != nil {
			safeError(w, 400, "invalid user request")
			return
		}
		if _, _, err := userStatement(req); err != nil {
			safeError(w, 400, "invalid user request")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		var result neo4jspec.UserResponse
		err := n.WithLock(ctx, func(n *agent.Node) error { var err error; result, err = e.users(ctx, n, req); return err })
		if err != nil {
			status, message := userErrorResponse(err)
			safeError(w, status, message)
			return
		}
		writeJSON(w, 200, result)
	})
	mux.HandleFunc("POST /v1/neo4j/admin-password", func(w http.ResponseWriter, r *http.Request) {
		var req neo4jspec.AdminPasswordRequest
		if decode(r, &req) != nil || req.Receipt == "" || req.Current == "" || req.Next == "" {
			safeError(w, 400, "invalid admin password request")
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		err := n.WithLock(ctx, func(n *agent.Node) error {
			s, err := decodeSpec(n.Config())
			if err != nil {
				return err
			}
			_, port := e.ports(false)
			return changeAdminPassword(ctx, s, port, req)
		})
		if errors.Is(err, errCredentials) {
			safeError(w, http.StatusConflict, "Neo4j administrator credentials do not authenticate")
			return
		}
		if err != nil {
			safeError(w, 503, "Neo4j administrator password action failed")
			return
		}
		writeJSON(w, 200, struct{}{})
	})
	e.jobRoutes(mux, n)
}

func changeAdminPassword(ctx context.Context, s neo4jspec.Spec, port int, req neo4jspec.AdminPasswordRequest) error {
	if _, err := query(ctx, s, port, req.Next, "system", "SHOW USERS", nil); err == nil {
		return nil
	} else if !errors.Is(err, errCredentials) {
		return err
	}
	_, err := query(ctx, s, port, req.Current, "system", "ALTER USER neo4j SET PASSWORD $p CHANGE NOT REQUIRED", map[string]any{"p": req.Next})
	return err
}

var userName = regexp.MustCompile(neo4jspec.UserNamePattern)

func userStatement(r neo4jspec.UserRequest) (string, map[string]any, error) {
	if r.Op == neo4jspec.UserList {
		if r.Name != "" || r.Password != "" {
			return "", nil, errors.New("invalid user list request")
		}
		return "SHOW USERS YIELD user, passwordChangeRequired RETURN user, passwordChangeRequired", nil, nil
	}
	if !userName.MatchString(r.Name) || strings.EqualFold(r.Name, neo4jspec.AdminUser) {
		return "", nil, errors.New("invalid application user name")
	}
	quoted := "`" + strings.ReplaceAll(r.Name, "`", "``") + "`"
	switch r.Op {
	case neo4jspec.UserGet:
		if r.Password != "" {
			break
		}
		return "SHOW USERS YIELD user, passwordChangeRequired WHERE user = $name RETURN user, passwordChangeRequired", map[string]any{"name": r.Name}, nil
	case neo4jspec.UserCreate, neo4jspec.UserPassword:
		if r.Password == "" {
			break
		}
		verb := "CREATE"
		if r.Op == neo4jspec.UserPassword {
			verb = "ALTER"
		}
		return verb + " USER " + quoted + " SET PASSWORD $p CHANGE NOT REQUIRED", map[string]any{"p": r.Password}, nil
	case neo4jspec.UserDelete:
		if r.Password != "" {
			break
		}
		return "DROP USER " + quoted, nil, nil
	}
	return "", nil, errors.New("invalid user operation")
}
func (e *Engine) users(ctx context.Context, n *agent.Node, r neo4jspec.UserRequest) (neo4jspec.UserResponse, error) {
	var response neo4jspec.UserResponse
	statement, params, err := userStatement(r)
	if err != nil {
		return response, err
	}
	s, err := decodeSpec(n.Config())
	if err != nil {
		return response, err
	}
	_, port := e.ports(false)
	result, err := query(ctx, s, port, s.AdminPassword, "system", statement, params)
	if err != nil {
		return response, err
	}
	if r.Op == neo4jspec.UserList || r.Op == neo4jspec.UserGet {
		for _, row := range result.rows() {
			name, _ := row["user"].(string)
			change, _ := row["passwordChangeRequired"].(bool)
			if name == neo4jspec.AdminUser {
				continue
			}
			u := neo4jspec.User{Name: name, PasswordChangeRequired: change}
			if r.Op == neo4jspec.UserGet {
				response.User = &u
			} else {
				response.Users = append(response.Users, u)
			}
		}
	} else if r.Op != neo4jspec.UserDelete {
		response.User = &neo4jspec.User{Name: r.Name}
	}
	return response, nil
}
func storeBytes(ctx context.Context, path string) int64 {
	var size int64
	_ = filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			return nil
		}
		if d.Type().IsRegular() {
			if info, err := d.Info(); err == nil {
				size += info.Size()
			}
		}
		return nil
	})
	return size
}
func observe(ctx context.Context, s neo4jspec.Spec, port int) (neo4jspec.Readiness, error) {
	var r neo4jspec.Readiness
	var discovery struct {
		Version string `json:"neo4j_version"`
		Edition string `json:"neo4j_edition"`
	}
	if err := request(ctx, s, port, s.AdminPassword, http.MethodGet, "/", nil, &discovery); err != nil {
		return r, err
	}
	r.Version, r.Edition = discovery.Version, discovery.Edition
	result, err := query(ctx, s, port, s.AdminPassword, "neo4j", "RETURN 1 AS ready", nil)
	if err != nil {
		return r, err
	}
	rows := result.rows()
	if len(rows) != 1 || rows[0]["ready"] != float64(1) {
		return r, errors.New("Neo4j query readiness failed")
	}
	r.Ready = true
	return r, nil
}
func (e *Engine) readiness(ctx context.Context, n *agent.Node) neo4jspec.Readiness {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	e.mu.RLock()
	r := neo4jspec.Readiness{ConfigVersion: e.configVersion, ConfigurationError: e.configurationError, AppliedRestartToken: e.appliedToken}
	e.mu.RUnlock()
	states, maintenance := n.Snapshot()
	if r.ConfigVersion == 0 || r.ConfigurationError != "" {
		r.Message = "Neo4j configuration has not been applied"
		return r
	}
	if maintenance || states[neo4jspec.Process].State != "running" {
		r.Message = "Neo4j process is not running"
		return r
	}
	s, err := decodeSpec(n.Config())
	if err != nil {
		r.ConfigurationError = "invalid Neo4j configuration"
		return r
	}
	_, port := e.ports(false)
	live, err := observe(ctx, s, port)
	r.Ready, r.Version, r.Edition = live.Ready, live.Version, live.Edition
	r.CredentialMismatch = errors.Is(err, errCredentials)
	if err != nil {
		r.Message = "Neo4j authenticated readiness failed"
	}
	r.StoreBytes = storeBytes(ctx, filepath.Join(dataDir(n), "databases"))
	return r
}
func (e *Engine) Readiness(ctx context.Context, opts agent.Options, args []string) error {
	if len(args) != 0 {
		return errors.New("Neo4j readiness takes no arguments")
	}
	b, err := os.ReadFile(filepath.Join(opts.DataDir, "karakoram", "config.json"))
	if err != nil {
		return errors.New("Neo4j configuration unavailable")
	}
	var c agentapi.MemberConfig
	if json.Unmarshal(b, &c) != nil {
		return errors.New("invalid Neo4j configuration")
	}
	s, err := decodeSpec(c)
	if err != nil {
		return err
	}
	_, port := e.ports(false)
	_, err = observe(ctx, s, port)
	return err
}
