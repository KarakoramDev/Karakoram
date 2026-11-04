// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"net/http"
	"regexp"
	"strings"
)

// User has full administrative privileges in Community; there are no roles.
type User struct {
	Name                   string `json:"name"`
	PasswordChangeRequired bool   `json:"passwordChangeRequired"`
}
type Access interface {
	ListUsers(context.Context, *engine.Context) ([]User, error)
	GetUser(context.Context, *engine.Context, string) (*User, error)
	CreateUser(context.Context, *engine.Context, string, string) (string, error)
	SetUserPassword(context.Context, *engine.Context, string, string) (string, error)
	DeleteUser(context.Context, *engine.Context, string) error
}

var userName = regexp.MustCompile(neo4jspec.UserNamePattern)

func validateUser(name string) error {
	lower := strings.ToLower(name)
	if !userName.MatchString(name) || lower == "neo4j" || strings.HasPrefix(lower, "neo4j_") || strings.HasPrefix(lower, "system") || strings.HasPrefix(lower, "kk_") || strings.HasPrefix(lower, "karakoram") {
		return engine.Invalid("invalid or reserved Neo4j user name")
	}
	return nil
}

// accessError strips agent diagnostics, which may contain native query inputs.
func accessError(err error) error {
	if err == nil {
		return nil
	}
	var h *agentapi.HTTPError
	if errors.As(err, &h) {
		switch h.StatusCode {
		case http.StatusNotFound:
			return engine.ErrNotFound
		case http.StatusConflict:
			return engine.ErrConflict
		case http.StatusBadRequest, http.StatusUnprocessableEntity:
			return engine.ErrInvalid
		}
	}
	return errors.New("Neo4j user action failed")
}
func users(ctx context.Context, rc *engine.Context, req neo4jspec.UserRequest) (neo4jspec.UserResponse, error) {
	var out neo4jspec.UserResponse
	err := action(ctx, rc, neo4jspec.ActionUsers, req, &out)
	return out, accessError(err)
}
func (*Provider) ListUsers(ctx context.Context, rc *engine.Context) ([]User, error) {
	out, err := users(ctx, rc, neo4jspec.UserRequest{Op: neo4jspec.UserList})
	if err != nil {
		return nil, err
	}
	result := make([]User, 0, len(out.Users))
	for _, u := range out.Users {
		result = append(result, User{Name: u.Name, PasswordChangeRequired: u.PasswordChangeRequired})
	}
	return result, nil
}
func (*Provider) GetUser(ctx context.Context, rc *engine.Context, name string) (*User, error) {
	if err := validateUser(name); err != nil {
		return nil, err
	}
	out, err := users(ctx, rc, neo4jspec.UserRequest{Op: neo4jspec.UserGet, Name: name})
	if err != nil {
		return nil, err
	}
	if out.User == nil {
		return nil, engine.ErrNotFound
	}
	return &User{Name: out.User.Name, PasswordChangeRequired: out.User.PasswordChangeRequired}, nil
}
func writeUser(ctx context.Context, rc *engine.Context, op, name, password string) (string, error) {
	if err := validateUser(name); err != nil {
		return "", err
	}
	generated := ""
	if password == "" {
		var err error
		password, err = secrets.Password()
		if err != nil {
			return "", err
		}
		generated = password
	}
	if _, err := users(ctx, rc, neo4jspec.UserRequest{Op: op, Name: name, Password: password}); err != nil {
		return "", err
	}
	return generated, nil
}
func (*Provider) CreateUser(ctx context.Context, rc *engine.Context, name, password string) (string, error) {
	return writeUser(ctx, rc, neo4jspec.UserCreate, name, password)
}
func (*Provider) SetUserPassword(ctx context.Context, rc *engine.Context, name, password string) (string, error) {
	return writeUser(ctx, rc, neo4jspec.UserPassword, name, password)
}
func (*Provider) DeleteUser(ctx context.Context, rc *engine.Context, name string) error {
	if err := validateUser(name); err != nil {
		return err
	}
	_, err := users(ctx, rc, neo4jspec.UserRequest{Op: neo4jspec.UserDelete, Name: name})
	return err
}

var _ Access = (*Provider)(nil)
