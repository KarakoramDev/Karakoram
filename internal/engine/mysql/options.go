// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type EngineOptions struct {
	Parameters       map[string]string       `json:"parameters,omitempty"`
	Proxy            ProxyOptions            `json:"proxy,omitempty"`
	GroupReplication GroupReplicationOptions `json:"groupReplication,omitempty"`
	Binlog           BinlogOptions           `json:"binlog,omitempty"`
}
type ProxyOptions struct {
	ReadWriteSplit bool `json:"readWriteSplit,omitempty"`
	ReaderPort     bool `json:"readerPort,omitempty"`
}
type GroupReplicationOptions struct {
	Consistency                string `json:"consistency,omitempty"`
	UnreachableMajorityTimeout int    `json:"unreachableMajorityTimeout,omitempty"`
}
type BinlogOptions struct {
	ArchiveInterval int `json:"archiveInterval,omitempty"`
	ExpireSeconds   int `json:"expireSeconds,omitempty"`
}

// ParseOptions rejects unknown options and settings owned by the orchestrator.
func ParseOptions(raw json.RawMessage) (EngineOptions, error) {
	o := EngineOptions{GroupReplication: GroupReplicationOptions{Consistency: myspec.ConsistencyBeforeOnPrimaryFailover, UnreachableMajorityTimeout: 30}, Binlog: BinlogOptions{ArchiveInterval: 300, ExpireSeconds: 604800}}
	if len(raw) != 0 {
		b := bytes.TrimSpace(raw)
		if len(b) == 0 || b[0] != '{' {
			return o, engine.Invalid("MySQL engineOptions must be an object")
		}
		d := json.NewDecoder(bytes.NewReader(raw))
		d.DisallowUnknownFields()
		if err := d.Decode(&o); err != nil {
			return o, engine.Invalid("invalid MySQL engineOptions: %v", err)
		}
		if err := d.Decode(new(any)); err != io.EOF {
			return o, engine.Invalid("engineOptions must contain one JSON object")
		}
		// Null nested blocks are not an alternative spelling of defaults.
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return o, engine.Invalid("invalid engineOptions")
		}
		for k, v := range fields {
			if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
				return o, engine.Invalid("engineOptions.%s cannot be null", k)
			}
		}
	}
	if !myspec.ValidConsistency(o.GroupReplication.Consistency) || o.GroupReplication.UnreachableMajorityTimeout < 1 || o.GroupReplication.UnreachableMajorityTimeout > 31536000 {
		return o, engine.Invalid("invalid Group Replication consistency or majority timeout")
	}
	if o.Binlog.ArchiveInterval < 1 || o.Binlog.ArchiveInterval > 86400 || o.Binlog.ExpireSeconds <= o.Binlog.ArchiveInterval || o.Binlog.ExpireSeconds > 2147483647 {
		return o, engine.Invalid("binlog expiry must exceed archive interval (1..86400 seconds)")
	}
	if err := myspec.ValidateParameters(o.Parameters); err != nil {
		return o, engine.Invalid("%v", err)
	}
	return o, nil
}

func restartParameters(parameters map[string]string) map[string]string {
	var result map[string]string
	for name, value := range parameters {
		if myspec.RestartRequiredParameter(name) {
			if result == nil {
				result = map[string]string{}
			}
			result[name] = value
		}
	}
	return result
}

// Dynamic values are live desired state. Startup-only values remain pinned to
// each member until its rolling journal reaches the stopped-runtime checkpoint.
func memberParameters(m *member, desired map[string]string) map[string]string {
	if m.Role != "data" {
		return desired
	}
	if len(m.Parameters) == 0 {
		static := false
		for name := range desired {
			static = static || myspec.RestartRequiredParameter(name)
		}
		if !static {
			return desired
		}
	}
	result := make(map[string]string, len(desired)+len(m.Parameters))
	for name, value := range desired {
		if !myspec.RestartRequiredParameter(name) {
			result[name] = value
		}
	}
	for name, value := range m.Parameters {
		result[name] = value
	}
	return result
}
