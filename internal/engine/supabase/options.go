// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"bytes"
	"encoding/json"
	"io"
	"net/mail"
	"net/url"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/agent/supabase/initsql"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
)

type Options struct {
	SynchronousMode        string                `json:"synchronousMode,omitempty"`
	Parameters             map[string]string     `json:"parameters,omitempty"`
	Proxy                  postgres.ProxyOptions `json:"proxy,omitempty"`
	SiteURL                string                `json:"siteUrl,omitempty"`
	AdditionalRedirectURLs []string              `json:"additionalRedirectUrls,omitempty"`
	Auth                   AuthOptions           `json:"auth,omitempty"`
	REST                   sbspec.REST           `json:"rest,omitempty"`
	Storage                StorageOptions        `json:"storage,omitempty"`
	Gateway                struct {
		PublicHTTPS bool     `json:"publicHttps"`
		CORSOrigins []string `json:"corsOrigins,omitempty"`
	} `json:"gateway,omitempty"`
	Studio struct {
		Organization string `json:"organization"`
		Project      string `json:"project"`
	} `json:"studio,omitempty"`
	Extensions []string `json:"extensions,omitempty"`
	Functions  struct {
		VerifyJWT *bool `json:"verifyJwt"`
	} `json:"functions,omitempty"`
}
type AuthOptions struct {
	DisableSignup bool `json:"disableSignup"`
	Autoconfirm   bool `json:"autoconfirm"`
	JWTExpiry     int  `json:"jwtExpiry"`
	SMTP          struct {
		Host       string `json:"host"`
		Port       int    `json:"port"`
		User       string `json:"user"`
		AdminEmail string `json:"adminEmail"`
		SenderName string `json:"senderName"`
	} `json:"smtp"`
}
type StorageOptions struct {
	BackendStorage      string `json:"backendStorage"`
	Bucket              string `json:"bucket"`
	FileSizeLimit       int64  `json:"fileSizeLimit"`
	ImageTransformation bool   `json:"imageTransformation"`
}

func postgresOptions(raw json.RawMessage) (json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, engine.Invalid("invalid engineOptions")
		}
	}
	out := map[string]json.RawMessage{}
	for _, key := range []string{"synchronousMode", "parameters", "proxy"} {
		if value, ok := fields[key]; ok {
			out[key] = value
		}
	}
	return json.Marshal(out)
}
func ParseOptions(raw json.RawMessage) (Options, error) {
	var o Options
	if len(raw) > 0 {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return o, engine.Invalid("engineOptions must be an object")
		}
		d := json.NewDecoder(bytes.NewReader(raw))
		d.DisallowUnknownFields()
		if err := d.Decode(&o); err != nil {
			return o, engine.Invalid("invalid Supabase engineOptions: %v", err)
		}
		if err := d.Decode(new(any)); err != io.EOF {
			return o, engine.Invalid("engineOptions must contain one JSON object")
		}
	}
	pg, err := postgresOptions(raw)
	if err != nil {
		return o, err
	}
	if _, err = postgres.ParseOptions(pg); err != nil {
		return o, err
	}
	validURL := func(value string) bool {
		u, e := url.Parse(value)
		return e == nil && u.Host != "" && (u.Scheme == "http" || u.Scheme == "https") && u.User == nil && u.Fragment == ""
	}
	if o.SiteURL != "" && !validURL(o.SiteURL) {
		return o, engine.Invalid("siteUrl must be an HTTP(S) URL without credentials")
	}
	for _, u := range o.AdditionalRedirectURLs {
		if !validURL(u) {
			return o, engine.Invalid("invalid additionalRedirectUrls entry")
		}
	}
	if err := sbspec.ValidateCORSOrigins(o.Gateway.CORSOrigins); err != nil {
		return o, engine.Invalid("invalid gateway.corsOrigins: %v", err)
	}
	if o.Auth.JWTExpiry < 0 || o.Auth.JWTExpiry > 604800 {
		return o, engine.Invalid("auth.jwtExpiry must be between 1 and 604800 seconds")
	}
	if o.Auth.JWTExpiry == 0 {
		o.Auth.JWTExpiry = 3600
	}
	if o.Auth.SMTP.Port < 0 || o.Auth.SMTP.Port > 65535 {
		return o, engine.Invalid("invalid SMTP port")
	}
	if o.Auth.SMTP.Port == 0 {
		o.Auth.SMTP.Port = 587
	}
	if strings.ContainsAny(o.Auth.SMTP.Host, "/\r\n\t @") {
		return o, engine.Invalid("invalid SMTP host")
	}
	if o.Auth.SMTP.AdminEmail != "" {
		if _, err := mail.ParseAddress(o.Auth.SMTP.AdminEmail); err != nil {
			return o, engine.Invalid("invalid SMTP adminEmail")
		}
	}
	if o.REST.MaxRows < 0 {
		return o, engine.Invalid("rest.maxRows cannot be negative")
	}
	if len(o.REST.Schemas) == 0 {
		o.REST.Schemas = []string{"public", "graphql_public"}
	}
	for _, schema := range o.REST.Schemas {
		if !identifier.MatchString(schema) {
			return o, engine.Invalid("invalid REST schema")
		}
	}
	if slices.Contains(o.Extensions, "pg_graphql") && !slices.Contains(o.REST.Schemas, "graphql_public") {
		o.REST.Schemas = append(o.REST.Schemas, "graphql_public")
	}
	if o.Storage.FileSizeLimit < 0 {
		return o, engine.Invalid("storage.fileSizeLimit cannot be negative")
	}
	if err := initsql.Validate(nil, o.Extensions, nil); err != nil {
		return o, engine.Invalid("unsupported extension selection")
	}
	return o, nil
}
