// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"bytes"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"io"
	"math"
	"strings"
)

type EngineOptions struct {
	HeapMB               *int              `json:"heapMB,omitempty"`
	PageCacheMB          *int              `json:"pageCacheMB,omitempty"`
	AcceptBackupDowntime bool              `json:"acceptBackupDowntime,omitempty"`
	Settings             map[string]string `json:"settings,omitempty"`
}

// settingKeys is deliberately restricted to query behavior and query logging;
// paths, networking, authentication, TLS and JVM configuration are managed.
func settingKeys() []string {
	return []string{"db.transaction.timeout", "db.transaction.concurrent.maximum", "db.lock.acquisition.timeout", "db.logs.query.enabled", "db.logs.query.threshold", "db.query.default_language", "dbms.cypher.planner"}
}
func ParseOptions(raw json.RawMessage) (EngineOptions, error) {
	var o EngineOptions
	if len(bytes.TrimSpace(raw)) == 0 {
		return o, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return o, engine.Invalid("engineOptions must be an object")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&o); err != nil {
		return o, engine.Invalid("invalid Neo4j engineOptions")
	}
	if d.Decode(new(any)) != io.EOF {
		return o, engine.Invalid("invalid Neo4j engineOptions")
	}
	allowed := map[string]bool{}
	for _, k := range settingKeys() {
		allowed[k] = true
	}
	for k, v := range o.Settings {
		if !allowed[k] {
			return o, engine.Invalid("Neo4j setting %q is not allowed", k)
		}
		if strings.ContainsAny(v, "\r\n\x00") {
			return o, engine.Invalid("Neo4j settings must be single-line values")
		}
	}
	return o, nil
}

// MemoryMB derives heap and page cache from minimum RAM in GiB, not the
// autoscaling ceiling. Both defaults round down to a 64 MiB boundary.
func MemoryMB(ramGiB float64) (heap, pageCache int) {
	mb := ramGiB * 1024
	heap = max(256, min(31*1024, int(mb*.4)/64*64))
	pageCache = max(64, int(mb*.25)/64*64)
	return
}
func memory(ram float64, o EngineOptions) (int, int, error) {
	if math.IsNaN(ram) || math.IsInf(ram, 0) || ram < 1 {
		return 0, 0, engine.Invalid("minimum RAM must be at least 1 GiB")
	}
	h, p := MemoryMB(ram)
	if o.HeapMB != nil {
		h = *o.HeapMB
	}
	if o.PageCacheMB != nil {
		p = *o.PageCacheMB
	}
	if h < 256 || h > 31*1024 || p < 64 || float64(h)+float64(p) > ram*1024*.8 {
		return 0, 0, engine.Invalid("heap must be 256..31744 MiB, page cache at least 64 MiB, and their sum at most 80%% of minimum RAM")
	}
	return h, p, nil
}
