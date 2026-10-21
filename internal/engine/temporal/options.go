// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"bytes"
	"encoding/json"
	"io"
	"net/url"
	"reflect"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/temporal/tspec"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
)

// PostgresOptions identifies the immutable source and bound database names.
type PostgresOptions struct {
	Cluster            string `json:"cluster"`
	Namespace          string `json:"namespace,omitempty"`
	Database           string `json:"database,omitempty"`
	VisibilityDatabase string `json:"visibilityDatabase,omitempty"`
}

// EngineOptions contains non-secret Temporal cluster configuration.
type EngineOptions struct {
	Persistence struct {
		Postgres PostgresOptions `json:"postgres"`
	} `json:"persistence"`
	HistoryShards int `json:"historyShards"`
	Frontend      struct {
		RequireClientAuth bool `json:"requireClientAuth"`
	} `json:"frontend"`
	Namespaces []tspec.Namespace `json:"namespaces,omitempty"`
	UI         struct {
		PublicHTTPS bool         `json:"publicHttps"`
		OIDC        *OIDCOptions `json:"oidc,omitempty"`
	} `json:"ui"`
}

// OIDCOptions identifies the issuer and client; the secret is sealed separately.
type OIDCOptions struct {
	Issuer   string `json:"issuer"`
	ClientID string `json:"clientId"`
}

// ParseOptions rejects unknown fields, including write-only OIDC credentials.
func ParseOptions(raw json.RawMessage) (EngineOptions, error) {
	var o EngineOptions
	o.HistoryShards = 512
	o.Frontend.RequireClientAuth = true
	if len(raw) == 0 {
		return o, nil
	}
	if trimmed := bytes.TrimSpace(raw); len(trimmed) == 0 || trimmed[0] != '{' {
		return o, engine.Invalid("engineOptions must be a JSON object")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&o); err != nil {
		return o, engine.Invalid("invalid Temporal engineOptions (unknown fields and inline secrets are not permitted)")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return o, engine.Invalid("engineOptions must contain one JSON object")
	}
	return o, nil
}

func optionsFor(c *domain.Cluster) (EngineOptions, error) {
	o, e := ParseOptions(c.Spec.EngineOptions)
	if e != nil {
		return o, e
	}
	pg := &o.Persistence.Postgres
	if pg.Namespace == "" {
		pg.Namespace = c.Ref().Namespace
	}
	if pg.Database == "" {
		pg.Database = "temporal_" + strings.ReplaceAll(c.Name, "-", "_")
	}
	if pg.VisibilityDatabase == "" {
		pg.VisibilityDatabase = "temporal_" + strings.ReplaceAll(c.Name, "-", "_") + "_visibility"
	}
	return o, nil
}

func bindingRequest(c *domain.Cluster, o EngineOptions) pgbinding.Request {
	pg := o.Persistence.Postgres
	return pgbinding.Request{
		ConsumerKind:      "temporal",
		ConsumerNamespace: c.Ref().Namespace, ConsumerName: c.Name,
		SourceNamespace: pg.Namespace, SourceCluster: pg.Cluster,
		Databases: []string{pg.Database, pg.VisibilityDatabase},
		MinMajor:  12,
	}
}

func validateOptions(c *domain.Cluster, o EngineOptions) error {
	if err := bindingRequest(c, o).Validate(); err != nil {
		return engine.Invalid("%v", err)
	}
	if o.HistoryShards < 1 || o.HistoryShards > 16384 {
		return engine.Invalid("historyShards must be 1-16384")
	}
	seen := map[string]bool{}
	for _, n := range o.Namespaces {
		if err := n.Validate(); err != nil {
			return engine.Invalid("%v", err)
		}
		if seen[n.Name] {
			return engine.Invalid("duplicate namespace %q", n.Name)
		}
		if n.Name == "default" && n.Retention != "72h" {
			return engine.Invalid("default namespace retention must be 72h")
		}
		seen[n.Name] = true
	}
	if _, err := namespaceRequest(o); err != nil {
		return err
	}
	if oidc := o.UI.OIDC; oidc != nil {
		if !o.UI.PublicHTTPS {
			return engine.Invalid("ui.oidc requires ui.publicHttps for an HTTPS callback origin")
		}
		u, e := url.Parse(oidc.Issuer)
		if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || oidc.ClientID == "" {
			return engine.Invalid("ui.oidc requires an HTTPS issuer and clientId")
		}
	}
	if o.UI.PublicHTTPS && (o.UI.OIDC == nil || c.Spec.Components.Extra[tspec.UI].Replicas == 0) {
		return engine.Invalid("ui.publicHttps requires ui and OIDC configuration")
	}
	return nil
}

func desiredNamespaces(o EngineOptions) []tspec.Namespace {
	out := []tspec.Namespace{{Name: "default", Retention: "72h"}}
	for _, n := range o.Namespaces {
		if n.Name == "default" {
			out[0] = n
		} else {
			out = append(out, n)
		}
	}
	return out
}

// namespaceRequest includes the implicit default in the agent's bounded payload.
func namespaceRequest(o EngineOptions) ([]tspec.Namespace, error) {
	if len(o.Namespaces) > 99 {
		return nil, engine.Invalid("namespaces permits at most 99 declarations")
	}
	desired := desiredNamespaces(o)
	raw, err := json.Marshal(desired)
	if err != nil {
		return nil, err
	}
	if len(raw) > 64<<10 {
		return nil, engine.Invalid("namespaces request including default must not exceed 64 KiB")
	}
	return desired, nil
}

func immutableOptions(old, next *domain.Cluster) error {
	a, e := optionsFor(old)
	if e != nil {
		return e
	}
	b, e := optionsFor(next)
	if e != nil {
		return e
	}
	if !reflect.DeepEqual(a.Persistence, b.Persistence) || a.HistoryShards != b.HistoryShards || old.Spec.Topology != next.Spec.Topology {
		return engine.Invalid("persistence, historyShards and topology are immutable")
	}
	return nil
}
