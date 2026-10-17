// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"path/filepath"
	"regexp"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbkeys"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

const credentialsKey = "supabase/credentials"
const functionsKey = "supabase/functions"

var roleNames = []string{"supabase_admin", "authenticator", "supabase_auth_admin", "supabase_storage_admin", "supabase_functions_admin", "pgbouncer", "supabase_replication_admin", "supabase_read_only_user"}

type sealedCredentials struct {
	Keys                sbkeys.Keys       `json:"keys"`
	Roles               map[string]string `json:"roles"`
	VectorToken         string            `json:"vectorToken"`
	RotationID          string            `json:"rotationId,omitempty"`
	RealtimeAdminSecret string            `json:"realtimeAdminSecret"`
}

func missing(err error) bool {
	return errors.Is(err, store.ErrNotFound) || errors.Is(err, engine.ErrNotFound)
}
func clockNow(rc *engine.Context) time.Time {
	if rc.Clock != nil {
		return rc.Clock.Now()
	}
	return time.Now().UTC()
}
func loadCredentials(ctx context.Context, rc *engine.Context) (sealedCredentials, error) {
	var out sealedCredentials
	b, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), credentialsKey)
	if err != nil {
		return out, err
	}
	err = json.Unmarshal(b, &out)
	return out, err
}
func storeCredentials(ctx context.Context, rc *engine.Context, c sealedCredentials) error {
	b, err := json.Marshal(c)
	if err != nil {
		return err
	}
	return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), credentialsKey, b)
}
func ensureCredentials(ctx context.Context, rc *engine.Context) error {
	c, err := loadCredentials(ctx, rc)
	if err == nil {
		if c.RealtimeAdminSecret != "" {
			return nil
		}
		c.RealtimeAdminSecret, err = secrets.Token()
		if err != nil {
			return err
		}
		return storeCredentials(ctx, rc, c)
	}
	if !missing(err) {
		return err
	}
	keys, err := sbkeys.Generate(rand.Reader, clockNow(rc))
	if err != nil {
		return err
	}
	c = sealedCredentials{Keys: keys, Roles: map[string]string{}}
	c.RealtimeAdminSecret, err = secrets.Token()
	if err != nil {
		return err
	}
	c.VectorToken, err = secrets.Token()
	if err != nil {
		return err
	}
	for _, role := range roleNames {
		password, err := secrets.Password()
		if err != nil {
			return err
		}
		c.Roles[role] = password
	}
	return storeCredentials(ctx, rc, c)
}

type Credentials struct {
	APIURL            string `json:"apiUrl"`
	StudioURL         string `json:"studioUrl"`
	AnonKey           string `json:"anonKey"`
	ServiceRoleKey    string `json:"serviceRoleKey"`
	PublishableKey    string `json:"publishableKey"`
	SecretKey         string `json:"secretKey"`
	DashboardUsername string `json:"dashboardUsername"`
	DashboardPassword string `json:"dashboardPassword"`
	JWTSecret         string `json:"jwtSecret"`
}

func (p *Provider) Credentials(ctx context.Context, rc *engine.Context) (Credentials, error) {
	c, err := loadCredentials(ctx, rc)
	if err != nil {
		return Credentials{}, err
	}
	api, err := p.apiURL(ctx, rc)
	if err != nil {
		return Credentials{}, err
	}
	studio := ""
	if rc.Cluster.Spec.Components.Extra["studio"].Replicas > 0 {
		studio = api
	}
	return Credentials{APIURL: api, StudioURL: studio, AnonKey: c.Keys.AnonKey, ServiceRoleKey: c.Keys.ServiceRoleKey, PublishableKey: c.Keys.PublishableKey, SecretKey: c.Keys.SecretKey, DashboardUsername: "supabase", DashboardPassword: c.Keys.DashboardPassword, JWTSecret: c.Keys.JWTSecret}, nil
}
func (p *Provider) SetSMTPPassword(ctx context.Context, rc *engine.Context, password string) error {
	if len(password) > 4096 {
		return engine.Invalid("SMTP password exceeds 4096 bytes")
	}
	return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), "supabase/smtp-password", []byte(password))
}
func loadFunctions(ctx context.Context, rc *engine.Context) (map[string]map[string]string, error) {
	out := map[string]map[string]string{}
	raw, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), functionsKey)
	if missing(err) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	if err = json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}
func (p *Provider) ListFunctions(ctx context.Context, rc *engine.Context) ([]string, error) {
	functions, err := loadFunctions(ctx, rc)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(functions))
	for name := range functions {
		out = append(out, name)
	}
	sort.Strings(out)
	return out, nil
}
func (p *Provider) GetFunction(ctx context.Context, rc *engine.Context, name string) (map[string]string, error) {
	functions, err := loadFunctions(ctx, rc)
	if err != nil {
		return nil, err
	}
	files, ok := functions[name]
	if !ok {
		return nil, engine.ErrNotFound
	}
	return files, nil
}

var functionName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

func (p *Provider) PutFunction(ctx context.Context, rc *engine.Context, name string, files map[string]string) error {
	p.functionsMu.Lock()
	defer p.functionsMu.Unlock()
	if name == "main" || !functionName.MatchString(name) {
		return engine.Invalid("invalid function name")
	}
	if files["index.ts"] == "" {
		return engine.Invalid("function requires index.ts")
	}
	size := 0
	for path, source := range files {
		if !filepath.IsLocal(path) || filepath.Clean(path) != path {
			return engine.Invalid("invalid function source path")
		}
		size += len(source)
	}
	if size > 1<<20 {
		return engine.Invalid("function sources exceed 1 MiB")
	}
	functions, err := loadFunctions(ctx, rc)
	if err != nil {
		return err
	}
	if _, ok := functions[name]; !ok && len(functions) >= 100 {
		return engine.Invalid("cluster already has 100 functions")
	}
	functions[name] = files
	raw, err := json.Marshal(functions)
	if err != nil {
		return err
	}
	return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), functionsKey, raw)
}
func (p *Provider) DeleteFunction(ctx context.Context, rc *engine.Context, name string) error {
	p.functionsMu.Lock()
	defer p.functionsMu.Unlock()
	functions, err := loadFunctions(ctx, rc)
	if err != nil {
		return err
	}
	if _, ok := functions[name]; !ok {
		return engine.ErrNotFound
	}
	delete(functions, name)
	raw, err := json.Marshal(functions)
	if err != nil {
		return err
	}
	return rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), functionsKey, raw)
}
