// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"sort"
	"strings"
)

func envKey(k string) bool {
	if k == "" {
		return false
	}
	for i, r := range k {
		if !(r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || i > 0 && r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}
func validateEnvironment(values map[string]string) error {
	for k, v := range values {
		if !envKey(k) {
			return reject("environment keys must be shell identifiers")
		}
		if strings.ContainsRune(v, 0) {
			return reject("environment values cannot contain NUL")
		}
	}
	return nil
}
func (b *Backend) SetEnvSecrets(ctx context.Context, hostname string, values map[string]string) error {
	return b.setEnv(ctx, hostname, values, true)
}
func (b *Backend) SetEnvVars(ctx context.Context, hostname string, values map[string]string) error {
	return b.setEnv(ctx, hostname, values, false)
}
func (b *Backend) setEnv(ctx context.Context, hostname string, values map[string]string, secret bool) error {
	if err := validateEnvironment(values); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	r, err := b.require(ctx, hostname)
	if err != nil {
		return err
	}
	return b.mergeEnv(ctx, r, values, secret, false)
}
func (b *Backend) mergeEnv(ctx context.Context, r *record, values map[string]string, secret, preserveSecrets bool) error {
	if !secret {
		for k := range values {
			if k == "record" {
				return reject("readable variable key record is reserved for service metadata")
			}
			if !preserveSecrets && contains(r.SecretKeys, k) && k != "KARAKORAM_OWNER" && k != "KARAKORAM_RELAY_OWNER" {
				return reject("refusing to make an existing secret readable")
			}
		}
	}
	v, err := b.readVariable(ctx, "nomad/jobs/"+r.Hostname)
	if err != nil {
		return err
	}
	if v.Items == nil {
		v.Items = map[string]string{}
	}
	secrets := map[string]bool{}
	for _, k := range r.SecretKeys {
		secrets[k] = true
	}
	if r.Vars == nil {
		r.Vars = map[string]string{}
	}
	for k, value := range values {
		v.Items[k] = value
		if secret || preserveSecrets && secrets[k] {
			delete(r.Vars, k)
			secrets[k] = true
		} else {
			delete(secrets, k)
			r.Vars[k] = value
		}
	}
	if len(values) > 0 {
		if err = b.writeVariable(ctx, &v); err != nil {
			return err
		}
	}
	r.SecretKeys = nil
	for k := range secrets {
		r.SecretKeys = append(r.SecretKeys, k)
	}
	sort.Strings(r.SecretKeys)
	return b.save(ctx, r)
}
