// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const recordPrefix = "karakoram/services/"

type storedDeployment struct {
	Runtime                  infra.RuntimeSpec
	Source, Checksum, Binary string
	EphemeralDiskMB          int `json:",omitempty"`
}
type record struct {
	ID, Hostname, Volume, SpreadGroup, Node, VolumeID, VolumeName string
	Kind                                                          infra.ServiceKind
	Resources                                                     infra.Resources
	Min, Max                                                      int
	SecretKeys                                                    []string
	Vars                                                          map[string]string `json:"-"`
	PublicPorts                                                   []infra.PublicPort
	Desired                                                       string
	Deployment                                                    *storedDeployment
	Pending                                                       *operation
	// Import identity is reserved before any volume/environment side effect.
	// The hash covers the original batch, not readable secret values.
	ImportHash string `json:",omitempty"`
	Importing  bool   `json:",omitempty"`
	// Direct allocation restarts share exclusion without replacing Pending,
	// including deployed legacy records that have no pending-operation handle.
	RestartLease *operationLease `json:",omitempty"`
	// Last restoration claim; superseding lifecycle saves must preserve it.
	// Ordinary registrations use Pending.Started for their observation grace.
	RegistrationIssued time.Time `json:",omitempty"`
	Index              uint64    `json:"-"`
}
type variable struct {
	Path, Namespace string
	Items           map[string]string
	ModifyIndex     uint64
}

func query(key, value string) url.Values { return url.Values{key: []string{value}} }
func (b *Backend) readVariable(ctx context.Context, path string) (variable, error) {
	var v variable
	_, err := b.c.get(ctx, "/v1/var/"+path, nil, &v)
	if errors.Is(err, infra.ErrNotFound) {
		return variable{Path: path, Items: map[string]string{}}, nil
	}
	return v, err
}
func (b *Backend) writeVariable(ctx context.Context, v *variable) error {
	var out variable
	err := operationMutation(ctx, func() error {
		return b.c.put(ctx, "/v1/var/"+v.Path, query("cas", strconv.FormatUint(v.ModifyIndex, 10)), map[string]any{"Path": v.Path, "Namespace": b.target.ID, "Items": v.Items}, &out)
	})
	if err == nil {
		v.ModifyIndex = out.ModifyIndex
	}
	return err
}
func (b *Backend) removeVariable(ctx context.Context, path string, index uint64) error {
	err := operationMutation(ctx, func() error {
		return b.c.delete(ctx, "/v1/var/"+path, query("cas", strconv.FormatUint(index, 10)), nil)
	})
	if errors.Is(err, infra.ErrNotFound) {
		return nil
	}
	return err
}
func (b *Backend) load(ctx context.Context, hostname string) (*record, error) {
	if !safeHostname(hostname) {
		return nil, reject("service hostname must be a DNS label")
	}
	v, err := b.readVariable(ctx, recordPrefix+hostname)
	if err != nil {
		return nil, err
	}
	if v.ModifyIndex == 0 {
		return nil, nil
	}
	r := decodeRecord(v, hostname)
	if r == nil {
		return nil, silentFailure("invalid service record")
	}
	return r, nil
}

func decodeRecord(v variable, hostname string) *record {
	var r record
	if json.Unmarshal([]byte(v.Items["record"]), &r) != nil || r.ID == "" || r.Hostname != hostname || (r.Kind != infra.KindRuntime && r.Kind != infra.KindVolume) {
		return nil
	}
	r.Index = v.ModifyIndex
	r.Vars = map[string]string{}
	for k, value := range v.Items {
		if k != "record" {
			r.Vars[k] = value
		}
	}
	return &r
}
func (b *Backend) require(ctx context.Context, hostname string) (*record, error) {
	r, err := b.load(ctx, hostname)
	if err == nil && r == nil {
		err = infra.ErrNotFound
	}
	return r, err
}
func (b *Backend) save(ctx context.Context, r *record) error {
	data, err := json.Marshal(r)
	if err != nil {
		return reject("cannot encode service record")
	}
	items := make(map[string]string, len(r.Vars)+1)
	items["record"] = string(data)
	for k, v := range r.Vars {
		items[k] = v
	}
	v := variable{Path: recordPrefix + r.Hostname, Items: items, ModifyIndex: r.Index}
	if err = b.writeVariable(ctx, &v); err == nil {
		r.Index = v.ModifyIndex
	}
	return err
}
func (b *Backend) records(ctx context.Context, prefix string) ([]*record, bool, error) {
	var vars []variable
	if _, err := b.c.get(ctx, "/v1/vars", query("prefix", recordPrefix+prefix), &vars); err != nil {
		return nil, false, err
	}
	results := make([]struct {
		record     *record
		incomplete bool
	}, len(vars))
	if err := infra.ObserveParallel(ctx, len(vars), func(i int) error {
		v := vars[i]
		hostname, ok := strings.CutPrefix(v.Path, recordPrefix)
		if !ok || !safeHostname(hostname) {
			return nil
		}
		loaded, err := b.readVariable(ctx, v.Path)
		if err != nil {
			return err
		}
		results[i].record = decodeRecord(loaded, hostname)
		results[i].incomplete = results[i].record == nil && loaded.ModifyIndex != 0
		return nil
	}); err != nil {
		return nil, false, err
	}
	out := make([]*record, 0, len(vars))
	incomplete := false
	for _, result := range results {
		if result.record != nil {
			out = append(out, result.record)
		}
		incomplete = incomplete || result.incomplete
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Hostname < out[j].Hostname })
	return out, incomplete, nil
}
func identityKey(k string) bool {
	switch k {
	case "KARAKORAM_OWNER", "KARAKORAM_RELAY_OWNER", "KARAKORAM_CLUSTER", "KARAKORAM_MEMBER", "KARAKORAM_AGENT_TOKEN", "KARAKORAM_DATA_DIR":
		return true
	}
	return false
}
func contains(values []string, k string) bool {
	for _, v := range values {
		if v == k {
			return true
		}
	}
	return false
}
