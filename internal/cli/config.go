// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package cli implements karactl, the Karakoram REST API command-line client.
// Credentials live in ~/.config/karakoram/config.yaml, never in command output.
// Context values are overridden by KARAKORAM_SERVER/KARAKORAM_TOKEN and then
// explicit flags. Resource create/set commands accept API-shaped YAML or JSON
// with -f; '-' reads stdin. Cluster patch files use {spec: {...}} merge patches.
package cli

import (
	"errors"
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
)

// Context is a named server and its API credential.
type Context struct {
	Server  string `yaml:"server"`
	Token   string `yaml:"token"`
	TokenID string `yaml:"tokenID,omitempty"`
}

// Config records the selected context and named connections.
type Config struct {
	CurrentContext string             `yaml:"currentContext"`
	Contexts       map[string]Context `yaml:"contexts"`
}

// ConfigPath returns the default credential file (independent of XDG overrides).
func ConfigPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "karakoram", "config.yaml"), nil
}

// LoadConfig loads saved contexts; an absent file is an empty configuration.
func LoadConfig(path string) (Config, error) {
	c := Config{Contexts: map[string]Context{}}
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return c, nil
	}
	if err != nil {
		return c, err
	}
	if err = yaml.Unmarshal(b, &c); err != nil {
		return c, fmt.Errorf("read config: %w", err)
	}
	if c.Contexts == nil {
		c.Contexts = map[string]Context{}
	}
	return c, nil
}

// SaveConfig atomically saves credentials with owner-only permissions.
func SaveConfig(path string, c Config) error {
	b, err := yaml.Marshal(c)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".config-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
