// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package weaviate

import (
	"bytes"
	"encoding/json"
	"io"
	"net/url"
	"slices"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvspec"
)

type inferenceEndpoint struct {
	URL string `json:"url"`
}
type engineOptions struct {
	Modules                  []string                     `json:"modules,omitempty"`
	Inference                map[string]inferenceEndpoint `json:"inference,omitempty"`
	ReplicationMinimumFactor int                          `json:"replicationMinimumFactor"`
	AsyncReplication         bool                         `json:"asyncReplication"`
	QueryDefaultsLimit       int                          `json:"queryDefaultsLimit"`
	DiskWarningPercent       int                          `json:"diskWarningPercent"`
	DiskReadonlyPercent      int                          `json:"diskReadonlyPercent"`
}

func defaultOptions(topology string) engineOptions {
	replication := 1
	if topology == "cluster" {
		replication = 3
	}
	return engineOptions{ReplicationMinimumFactor: replication, AsyncReplication: true, QueryDefaultsLimit: 25, DiskWarningPercent: 80, DiskReadonlyPercent: 90}
}

// parseOptions validates a standalone object with single-member defaults. Use
// parseOptionsWithDefaults when topology defaults have not yet been persisted.
func parseOptions(raw json.RawMessage) (engineOptions, error) {
	return parseOptionsWithDefaults(raw, "single")
}
func parseOptionsWithDefaults(raw json.RawMessage, topology string) (engineOptions, error) {
	o := defaultOptions(topology)
	if len(raw) == 0 {
		return o, nil
	}
	if !strings.HasPrefix(strings.TrimSpace(string(raw)), "{") {
		return o, invalid("engineOptions must be an object")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&o); err != nil {
		return o, invalid("invalid engineOptions: %v", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return o, invalid("engineOptions must contain one JSON object")
	}
	// JSON null must not silently replace typed scalar values with defaults.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return o, invalid("invalid engineOptions: %v", err)
	}
	for key, value := range fields {
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return o, invalid("engineOptions.%s cannot be null", key)
		}
	}
	seen := map[string]bool{}
	for _, m := range o.Modules {
		if !slices.Contains(wvspec.APIBasedModules, m) || seen[m] {
			return o, invalid("unsupported or duplicate module %q; backup-s3 is managed", m)
		}
		seen[m] = true
	}
	for module, endpoint := range o.Inference {
		if InferenceEnv(module) == "" {
			return o, invalid("unsupported inference module %q", module)
		}
		u, err := url.Parse(endpoint.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || strings.Contains(endpoint.URL, "#") {
			return o, invalid("inference %q requires an http/https URL without credentials, query or fragment", module)
		}
	}
	return o, validateOptions(o, 0)
}

// InferenceEnv returns the verified process environment key for a module.
func InferenceEnv(module string) string {
	for _, env := range []string{"TRANSFORMERS_INFERENCE_API", "CLIP_INFERENCE_API", "RERANKER_INFERENCE_API", "BIND_INFERENCE_API", "IMAGE_INFERENCE_API", "MODEL2VEC_INFERENCE_API", "GPT4ALL_INFERENCE_API", "NER_INFERENCE_API", "QNA_INFERENCE_API", "SUM_INFERENCE_API", "SPELLCHECK_INFERENCE_API"} {
		if m, ok := wvspec.InferenceModule(env); ok && m == module {
			return env
		}
	}
	return ""
}
func validateOptions(o engineOptions, replicas int) error {
	if o.ReplicationMinimumFactor < 1 || replicas > 0 && o.ReplicationMinimumFactor > replicas {
		return invalid("replicationMinimumFactor must be between 1 and replicas")
	}
	if o.QueryDefaultsLimit < 1 || o.QueryDefaultsLimit > 10000 {
		return invalid("queryDefaultsLimit must be between 1 and 10000")
	}
	if o.DiskWarningPercent < 1 || o.DiskWarningPercent >= o.DiskReadonlyPercent || o.DiskReadonlyPercent > 95 {
		return invalid("disk percentages must satisfy 1 <= warning < readonly <= 95")
	}
	return nil
}
func optionsDescription() map[string]any {
	inference := map[string]string{}
	for _, m := range []string{"text2vec-transformers", "multi2vec-clip", "reranker-transformers", "multi2vec-bind", "img2vec-neural", "text2vec-model2vec", "text2vec-gpt4all", "ner-transformers", "qna-transformers", "sum-transformers", "text-spellcheck"} {
		inference[m] = InferenceEnv(m)
	}
	return map[string]any{
		"modules":                  map[string]any{"type": "array", "items": slices.Clone(wvspec.APIBasedModules), "managed": []string{"backup-s3"}},
		"inference":                map[string]any{"type": "object", "modules": inference, "urlSchemes": []string{"http", "https"}},
		"replicationMinimumFactor": map[string]any{"type": "integer", "defaultSingle": 1, "defaultCluster": 3, "minimum": 1, "maximum": "replicas"},
		"asyncReplication":         map[string]any{"type": "boolean", "default": true},
		"queryDefaultsLimit":       map[string]any{"type": "integer", "default": 25, "minimum": 1, "maximum": 10000},
		"diskWarningPercent":       map[string]any{"type": "integer", "default": 80, "minimum": 1, "lessThan": "diskReadonlyPercent"},
		"diskReadonlyPercent":      map[string]any{"type": "integer", "default": 90, "maximum": 95},
	}
}
