// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package kcspec defines the private Keycloak member configuration.
package kcspec

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
)

const Version = "26.8.1"
const InstallDir = "/opt/karakoram/keycloak-" + Version + "/keycloak-" + Version

// Spec contains only this consumer's credentials, never source administrator credentials.
type Spec struct {
	Version              string `json:"version"`
	Host                 string `json:"host"`
	Port                 int    `json:"port"`
	Database             string `json:"database"`
	Username             string `json:"username"`
	Password             string `json:"password"`
	CAPEM                string `json:"caPEM"`
	Hostname             string `json:"hostname,omitempty"`
	ProxyHeaders         string `json:"proxyHeaders,omitempty"`
	AdminPassword        string `json:"adminPassword"`
	CredentialGeneration int64  `json:"credentialGeneration"`
}

func (s Spec) Validate() error {
	if s.Version != Version || s.Host == "" || strings.ContainsAny(s.Host, "/?#@\r\n") || s.Port < 1 || s.Port > 65535 || s.Database == "" || s.Username == "" || s.Password == "" || s.CAPEM == "" || s.AdminPassword == "" {
		return errors.New("invalid Keycloak member configuration")
	}
	if s.ProxyHeaders != "" && s.ProxyHeaders != "forwarded" && s.ProxyHeaders != "xforwarded" {
		return errors.New("invalid Keycloak proxy headers")
	}
	return nil
}

// JDBCURL enforces hostname verification against the binding CA, without credentials in the URL.
func (s Spec) JDBCURL(caFile string) string {
	// ProxySQL's libpq backend cannot represent mixed per-column result formats.
	// Keep extended queries and prepared statements, but request text for every type.
	q := url.Values{"sslmode": {"verify-full"}, "sslrootcert": {caFile}, "binaryTransfer": {"false"}}
	return "jdbc:postgresql://" + net.JoinHostPort(s.Host, strconv.Itoa(s.Port)) + "/" + url.PathEscape(s.Database) + "?" + q.Encode()
}
