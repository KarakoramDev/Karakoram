// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

var errWaiting = errors.New("waiting")

func (p *Provider) esConfig(ctx context.Context, rc *engine.Context, s *state) (esapi.Config, error) {
	ca, err := secret(ctx, rc, secretCA)
	if err != nil {
		return esapi.Config{}, err
	}
	pw, err := secret(ctx, rc, secretAdmin)
	if err != nil {
		return esapi.Config{}, err
	}
	cfg := esapi.Config{CAPEM: ca, Username: AdminUser, Password: string(pw)}
	for _, m := range s.esNodes() {
		if m.Stage == stageDeployed || m.Stage == stageJoined || m.Removing {
			cfg.Endpoints = append(cfg.Endpoints, "https://"+m.Name+":9200")
		}
	}
	if len(cfg.Endpoints) == 0 {
		return cfg, errWaiting
	}
	if rc.Dialer != nil {
		cfg.DialContext = rc.Dialer.DialContext
	}
	return cfg, nil
}
func (p *Provider) es(ctx context.Context, rc *engine.Context, s *state) (*esapi.Client, error) {
	cfg, err := p.esConfig(ctx, rc, s)
	if err != nil {
		return nil, err
	}
	return esapi.New(cfg)
}
func nativeRole(r Role) esapi.Role {
	out := esapi.Role{Cluster: r.Cluster, Indices: make([]esapi.IndexPrivileges, len(r.Indices))}
	for i, index := range r.Indices {
		out.Indices[i] = esapi.IndexPrivileges(index)
	}
	if len(r.Applications) > 0 {
		out.Applications = make([]esapi.ApplicationPrivileges, len(r.Applications))
		for i, application := range r.Applications {
			out.Applications[i] = esapi.ApplicationPrivileges(application)
		}
	}
	return out
}
func (p *Provider) ensureSecurity(ctx context.Context, rc *engine.Context, s *state, force bool) error {
	values := map[string]string{}
	for _, key := range []string{secretElastic, secretLogstash, secretAPM} {
		b, err := secret(ctx, rc, key)
		if err != nil {
			return err
		}
		values[key] = string(b)
	}
	token, err := secret(ctx, rc, secretKibanaToken)
	if err != nil && !missing(err) {
		return err
	}
	valid := false
	if len(token) > 0 {
		cfg, e := p.esConfig(ctx, rc, s)
		if e != nil {
			return e
		}
		cfg.Username = ""
		cfg.Password = ""
		cfg.BearerToken = string(token)
		client, e := esapi.New(cfg)
		if e != nil {
			return e
		}
		auth, e := client.Authenticate(ctx)
		client.Close()
		if e == nil {
			valid = auth.Username == "elastic/kibana"
		} else {
			var apiErr *esapi.Error
			if !errors.As(e, &apiErr) || apiErr.Status != http.StatusUnauthorized {
				return e
			}
		}
	}
	c, err := p.es(ctx, rc, s)
	if err != nil {
		return err
	}
	defer c.Close()
	if !valid {
		name, err := secret(ctx, rc, secretKibanaTokenName)
		if err != nil && !missing(err) {
			return err
		}
		if len(name) > 0 {
			if err = c.DeleteServiceToken(ctx, "elastic", "kibana", string(name)); err != nil && !esapi.IsNotFound(err) {
				return err
			}
		}
		random, err := secrets.RandomString(16)
		if err != nil {
			return err
		}
		tokenName := "kk-" + random
		// Persist the name before creating the token so a crash can invalidate
		// the exact service credential rather than leaving an untracked token.
		if err = putSecret(ctx, rc, secretKibanaTokenName, []byte(tokenName)); err != nil {
			return err
		}
		value, err := c.CreateServiceToken(ctx, "elastic", "kibana", tokenName)
		if err != nil {
			return err
		}
		if err = putSecret(ctx, rc, secretKibanaToken, []byte(value)); err != nil {
			return err
		}
		token = []byte(value)
		p.bumpConfig(s)
		cfg, err := p.esConfig(ctx, rc, s)
		if err != nil {
			return err
		}
		cfg.Username, cfg.Password, cfg.BearerToken = "", "", value
		bearer, err := esapi.New(cfg)
		if err != nil {
			return err
		}
		auth, err := bearer.Authenticate(ctx)
		bearer.Close()
		if err != nil {
			return err
		}
		if auth.Username != "elastic/kibana" {
			return errors.New("unexpected identity for Kibana token")
		}
	}
	values[secretKibanaToken] = string(token)
	b, _ := json.Marshal(values)
	hash := digest(b)
	if !force && s.SecurityHash == hash {
		return nil
	}
	if err = c.ChangePassword(ctx, "elastic", values[secretElastic]); err != nil {
		return err
	}
	for _, entry := range []struct {
		role, user, key string
		definition      Role
	}{{LogstashWriterRole, LogstashUser, secretLogstash, LogstashRoleDefinition()}, {APMWriterRole, APMUser, secretAPM, APMRoleDefinition()}} {
		if err = c.PutRole(ctx, entry.role, nativeRole(entry.definition)); err != nil {
			return err
		}
		enabled := true
		if err = c.PutUser(ctx, entry.user, esapi.User{Password: values[entry.key], Roles: []string{entry.role}, Enabled: &enabled}); err != nil {
			return err
		}
	}
	s.SecurityHash = hash
	return nil
}
func (p *Provider) healthy(ctx context.Context, c *esapi.Client) (bool, error) {
	h, err := c.Health(ctx, esapi.HealthOptions{})
	if err != nil {
		return false, err
	}
	return healthAcceptable(ctx, c, h)
}

func healthAcceptable(ctx context.Context, c *esapi.Client, h esapi.Health) (bool, error) {
	if h.RelocatingShards != 0 || h.InitializingShards != 0 {
		return false, nil
	}
	if h.Status == "green" {
		return true, nil
	}
	if h.Status != "yellow" {
		return false, nil
	}
	if h.NumberOfDataNodes < 1 {
		return false, nil
	}
	if h.NumberOfDataNodes > 1 {
		replicas, err := c.MaxIndexReplicas(ctx)
		if err != nil || h.NumberOfDataNodes > replicas {
			return false, err
		}
	}
	primaries, err := c.UnassignedPrimaries(ctx)
	return err == nil && !primaries, err
}
