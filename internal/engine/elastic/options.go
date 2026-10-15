// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/agentspec"
)

// Options contains spec.engineOptions. Elasticsearch does not consume
// spec.engineConfig: the provider must reject non-empty engineConfig rather than
// silently ignore it. User settings belong in these typed options instead.
type Options struct {
	Kibana struct {
		Public            bool           `json:"public"`
		AllowAllAddresses bool           `json:"allowAllAddresses,omitempty"`
		Settings          map[string]any `json:"settings,omitempty"`
	} `json:"kibana"`
	APM struct {
		Public            bool  `json:"public"`
		AllowAllAddresses bool  `json:"allowAllAddresses,omitempty"`
		APIKeys           *bool `json:"apiKeys,omitempty"`
	} `json:"apm"`
	Logstash struct {
		Pipelines       []agentspec.Pipeline `json:"pipelines,omitempty"`
		PersistentQueue bool                 `json:"persistentQueue"`
	} `json:"logstash"`
	Elasticsearch struct {
		Settings map[string]any `json:"settings,omitempty"`
	} `json:"elasticsearch"`
}

// ParseOptions decodes exactly one JSON object, rejects unknown fields, defaults
// APM API-key authentication to true, and validates user-controlled content.
func ParseOptions(raw []byte) (Options, error) {
	return parseOptions(raw, false)
}

// parseStoredOptions tolerates the formerly user-settable base-path rewrite
// setting. Runtime rendering now owns it and always disables rewriting.
func parseStoredOptions(raw []byte) (Options, error) {
	return parseOptions(raw, true)
}

func parseOptions(raw []byte, stored bool) (Options, error) {
	var o Options
	if len(bytes.TrimSpace(raw)) != 0 {
		if bytes.TrimSpace(raw)[0] != '{' {
			return o, errors.New("engineOptions must be a JSON object")
		}
		d := json.NewDecoder(bytes.NewReader(raw))
		d.DisallowUnknownFields()
		if err := d.Decode(&o); err != nil {
			return Options{}, fmt.Errorf("decode engineOptions: %w", err)
		}
		if err := d.Decode(new(any)); err != io.EOF {
			return Options{}, errors.New("engineOptions must contain exactly one JSON object")
		}
	}
	if o.APM.APIKeys == nil {
		enabled := true
		o.APM.APIKeys = &enabled
	}
	if stored {
		delete(o.Kibana.Settings, "server.rewriteBasePath")
		if server, ok := o.Kibana.Settings["server"].(map[string]any); ok {
			delete(server, "rewriteBasePath")
			if len(server) == 0 {
				delete(o.Kibana.Settings, "server")
			}
		}
	}
	if err := o.Validate(); err != nil {
		return Options{}, err
	}
	return o, nil
}

var pipelineID = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

// Validate enforces pipeline bounds and protects provider-managed settings. It
// deliberately does not guess whether other arbitrary setting names are secure.
func (o Options) Validate() error {
	if len(o.Logstash.Pipelines) > 10 {
		return errors.New("logstash permits at most 10 pipelines")
	}
	seen := make(map[string]bool, len(o.Logstash.Pipelines))
	for i, p := range o.Logstash.Pipelines {
		if !pipelineID.MatchString(p.ID) {
			return fmt.Errorf("logstash pipeline %d has an invalid ID", i+1)
		}
		if seen[p.ID] {
			return fmt.Errorf("logstash pipeline %d has a duplicate ID", i+1)
		}
		seen[p.ID] = true
		if strings.TrimSpace(p.Config) == "" || len(p.Config) > 64*1024 {
			return fmt.Errorf("logstash pipeline %d config must be non-empty and at most 64 KiB", i+1)
		}
	}
	if err := validateSettings("elasticsearch", o.Elasticsearch.Settings, esManaged); err != nil {
		return err
	}
	return validateSettings("kibana", o.Kibana.Settings, kibanaManaged)
}

var esManaged = []string{"cluster.name", "node.name", "node.roles", "path.", "network.", "http.port", "transport.port", "discovery.", "cluster.initial_master_nodes", "xpack.security.", "s3.client.", "bootstrap.memory_lock", "readiness."}
var kibanaManaged = []string{"server.host", "server.port", "server.publicBaseUrl", "server.rewriteBasePath", "elasticsearch.", "xpack.security.encryptionKey", "xpack.encryptedSavedObjects.", "xpack.reporting.encryptionKey", "path.data"}

func validateSettings(component string, settings map[string]any, managed []string) error {
	flat, err := FlattenSettings(settings)
	if err != nil {
		return fmt.Errorf("%s settings: %w", component, err)
	}
	for _, key := range sortedKeys(flat) {
		for _, m := range managed {
			blocked := key == m
			if strings.HasSuffix(m, ".") {
				blocked = key == strings.TrimSuffix(m, ".") || strings.HasPrefix(key, m)
			}
			if blocked {
				return fmt.Errorf("%s setting %q is managed by Karakoram", component, key)
			}
		}
	}
	return nil
}

// FlattenSettings converts nested JSON settings to dotted keys. It rejects
// ambiguous dotted/nested duplicates instead of depending on map iteration order.
// Empty maps remain values so managed namespaces cannot bypass validation.
func FlattenSettings(settings map[string]any) (map[string]any, error) {
	out := make(map[string]any)
	var visit func(string, map[string]any) error
	visit = func(prefix string, values map[string]any) error {
		for _, key := range sortedKeys(values) {
			if key == "" {
				return errors.New("setting name must not be empty")
			}
			name := prefix + key
			v := values[key]
			if nested, ok := v.(map[string]any); ok && len(nested) != 0 {
				if err := visit(name+".", nested); err != nil {
					return err
				}
			} else {
				if _, exists := out[name]; exists {
					return fmt.Errorf("setting %q is specified more than once", name)
				}
				out[name] = v
			}
		}
		return nil
	}
	if err := visit("", settings); err != nil {
		return nil, err
	}
	return out, nil
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
