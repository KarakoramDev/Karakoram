// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// Config contains endpoint configuration plus bootstrap/recovery/backend credentials.
// On Zerops, custom ZEROPS_* variables are reserved; use KARAKORAM_ZEROPS_*.
type Config struct {
	DataDir, PublicListen, InternalListen, PublicURL, AdvertiseURL, Hostname, AdminPassword, ZeropsToken, ZeropsProjectID, ZeropsAPIURL string
	RecoveryAccount, RecoveryPassword                                                                                                   string
}

// ConfigFromEnv loads deployment settings and validates every endpoint before opening the DB.
func ConfigFromEnv() (Config, error) {
	hostname := os.Getenv("hostname")
	if hostname == "" {
		var err error
		hostname, err = os.Hostname()
		if err != nil {
			return Config{}, err
		}
	}
	public := os.Getenv("KARAKORAM_PUBLIC_URL")
	if public == "" {
		public = os.Getenv("zeropsSubdomain")
	}
	c := Config{DataDir: envDefault("KARAKORAM_DATA_DIR", "/mnt/karakoramdata"), PublicListen: envDefault("KARAKORAM_LISTEN", ":8080"), InternalListen: envDefault("KARAKORAM_INTERNAL_LISTEN", ":9443"), PublicURL: public, AdvertiseURL: envDefault("KARAKORAM_ADVERTISE_URL", "https://"+net.JoinHostPort(hostname, "9443")), Hostname: hostname, AdminPassword: os.Getenv("KARAKORAM_ADMIN_PASSWORD"), ZeropsToken: os.Getenv("KARAKORAM_ZEROPS_TOKEN"), ZeropsProjectID: envDefault("KARAKORAM_ZEROPS_PROJECT_ID", os.Getenv("projectId")), ZeropsAPIURL: envDefault("KARAKORAM_ZEROPS_API_URL", "https://api.app-prg1.zerops.io/api/rest/public")}
	c.RecoveryAccount = envDefault("KARAKORAM_RECOVERY_ACCOUNT", "admin")
	c.RecoveryPassword = os.Getenv("KARAKORAM_RECOVERY_PASSWORD")
	return c, c.Validate()
}
func envDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// DatabasePath returns the stable database location used by server and offline administration.
func (c Config) DatabasePath() string { return filepath.Join(c.DataDir, "karakoram.db") }

// Validate rejects missing credentials and endpoints with userinfo/query/fragment components.
func (c Config) Validate() error {
	if c.RecoveryPassword != "" {
		if err := auth.ValidatePassword(c.RecoveryPassword); err != nil {
			return fmt.Errorf("KARAKORAM_RECOVERY_PASSWORD: %w", err)
		}
		if c.RecoveryAccount != "" && domain.ValidateAccountName(c.RecoveryAccount) != nil {
			return errors.New("invalid KARAKORAM_RECOVERY_ACCOUNT")
		}
	}
	if c.DataDir == "" || !filepath.IsAbs(c.DataDir) {
		return errors.New("KARAKORAM_DATA_DIR must be an absolute path")
	}
	if c.Hostname == "" || strings.ContainsAny(c.Hostname, " /\\:@") {
		return errors.New("invalid control plane hostname")
	}
	for _, addr := range []string{c.PublicListen, c.InternalListen} {
		_, port, err := net.SplitHostPort(addr)
		if err != nil || port == "" {
			return errors.New("invalid HTTP listen address")
		}
	}
	for name, raw := range map[string]string{"KARAKORAM_PUBLIC_URL": c.PublicURL, "KARAKORAM_ADVERTISE_URL": c.AdvertiseURL, "KARAKORAM_ZEROPS_API_URL": c.ZeropsAPIURL} {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
			return fmt.Errorf("%s must be an absolute HTTP(S) URL without credentials/query/fragment", name)
		}
		if name == "KARAKORAM_ADVERTISE_URL" && (u.Scheme != "https" || (u.Path != "" && u.Path != "/")) {
			return errors.New("KARAKORAM_ADVERTISE_URL must be an HTTPS origin")
		}
		if name == "KARAKORAM_PUBLIC_URL" && u.Path != "" && u.Path != "/" {
			return errors.New("KARAKORAM_PUBLIC_URL must be an origin")
		}
	}
	if len(os.Getenv("KARAKORAM_MASTER_KEY")) < 32 {
		return errors.New("KARAKORAM_MASTER_KEY must contain at least 32 characters")
	}
	if c.ZeropsToken == "" || c.ZeropsProjectID == "" {
		return errors.New("KARAKORAM_ZEROPS_TOKEN and KARAKORAM_ZEROPS_PROJECT_ID (or projectId) are required")
	}
	return nil
}
