// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

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

// encodeEnvironment uses delimiters understood literally by Dokploy's dotenv
// parser. It deliberately refuses values dotenv cannot represent losslessly.
func encodeEnvironment(values map[string]string) (string, error) {
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var out strings.Builder
	for _, k := range keys {
		v := values[k]
		if !envKey(k) {
			return "", reject("environment keys must be shell identifiers")
		}
		if strings.Contains(v, "${{") {
			return "", reject("Dokploy environment interpolation is not allowed")
		}
		if strings.ContainsAny(v, "\x00\r") {
			return "", reject("environment values cannot contain NUL or carriage returns")
		}
		delim := ""
		switch {
		case !strings.Contains(v, "'") && !strings.HasSuffix(v, `\`):
			delim = "'"
		case !strings.Contains(v, "`") && !strings.HasSuffix(v, `\`):
			delim = "`"
		case !strings.Contains(v, "\"") && !strings.Contains(v, `\n`) && !strings.Contains(v, `\r`) && !strings.HasSuffix(v, `\`):
			delim = "\""
		case !strings.ContainsAny(v, "#\n\uFEFF\u0085") && strings.TrimSpace(v) == v && !strings.ContainsAny(v[:1], "'\"`"):
		default:
			return "", reject("environment value cannot be represented losslessly by Dokploy dotenv")
		}
		out.WriteString(k)
		out.WriteByte('=')
		out.WriteString(delim)
		out.WriteString(v)
		out.WriteString(delim)
		out.WriteByte('\n')
	}
	return out.String(), nil
}

// decodeEnvironment reads the encoding above, including physical multiline
// values. The application owns this field; malformed externally edited values
// are rejected instead of silently dropping secrets during a full replacement.
func decodeEnvironment(text string) (map[string]string, error) {
	values := map[string]string{}
	for len(text) > 0 {
		text = strings.TrimLeft(text, " \t\n\r")
		if text == "" {
			break
		}
		eq := strings.IndexByte(text, '=')
		if eq < 1 {
			return nil, reject("application environment has an invalid entry")
		}
		key := text[:eq]
		if !envKey(key) {
			return nil, reject("application environment has an invalid key")
		}
		text = text[eq+1:]
		var value string
		if len(text) > 0 && strings.ContainsRune("'\"`", rune(text[0])) {
			quote := text[0]
			text = text[1:]
			end := strings.IndexByte(text, quote)
			if end < 0 {
				return nil, reject("application environment has an unterminated value")
			}
			value = text[:end]
			text = text[end+1:]
			if quote == '"' {
				value = strings.ReplaceAll(strings.ReplaceAll(value, `\n`, "\n"), `\r`, "\r")
			}
			if len(text) > 0 && text[0] != '\n' && text[0] != '\r' {
				return nil, reject("application environment has an ambiguous quoted value")
			}
		} else {
			end := strings.IndexByte(text, '\n')
			if end < 0 {
				value = text
				text = ""
			} else {
				value = text[:end]
				text = text[end+1:]
			}
			value = strings.TrimSpace(strings.SplitN(value, "#", 2)[0])
		}
		values[key] = value
	}
	return values, nil
}
func (b *Backend) saveEnv(ctx context.Context, id string, values map[string]string) error {
	encoded, err := encodeEnvironment(values)
	if err != nil {
		return err
	}
	return b.c.post(ctx, "application.saveEnvironment", map[string]any{"applicationId": id, "env": encoded, "buildArgs": "", "buildSecrets": "", "createEnvFile": false}, nil)
}
func (b *Backend) SetEnvSecrets(ctx context.Context, hostname string, values map[string]string) error {
	return b.setEnv(ctx, hostname, values, true)
}
func (b *Backend) SetEnvVars(ctx context.Context, hostname string, values map[string]string) error {
	return b.setEnv(ctx, hostname, values, false)
}
func (b *Backend) setEnv(ctx context.Context, hostname string, values map[string]string, secret bool) error {
	if _, err := encodeEnvironment(values); err != nil {
		return err
	}
	if err := b.mu.Lock(ctx); err != nil {
		return err
	}
	defer b.mu.Unlock()
	a, m, err := b.require(ctx, hostname)
	if err != nil {
		return err
	}
	if !secret {
		for k := range values {
			if contains(m.SecretKeys, k) && k != "KARAKORAM_OWNER" && k != "KARAKORAM_RELAY_OWNER" {
				return reject("refusing to make an existing secret readable")
			}
		}
	}
	existing, err := decodeEnvironment(a.Env)
	if err != nil {
		return err
	}
	if m.Vars == nil {
		m.Vars = map[string]string{}
	}
	secretKeys := map[string]bool{}
	for _, k := range m.SecretKeys {
		secretKeys[k] = true
	}
	for k, v := range values {
		existing[k] = v
		if secret {
			delete(m.Vars, k)
			secretKeys[k] = true
		} else {
			m.Vars[k] = v
			delete(secretKeys, k)
		}
	}
	m.SecretKeys = nil
	for k := range secretKeys {
		m.SecretKeys = append(m.SecretKeys, k)
	}
	sort.Strings(m.SecretKeys)
	m.VarKeys = nil
	for k := range m.Vars {
		m.VarKeys = append(m.VarKeys, k)
	}
	sort.Strings(m.VarKeys)
	if err = b.saveEnv(ctx, a.ID, existing); err != nil {
		return err
	}
	return b.update(ctx, a, m, nil)
}
