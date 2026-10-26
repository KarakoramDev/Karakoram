// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package nomad

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"sort"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func importHash(specs []infra.ServiceSpec) string {
	ordered := append([]infra.ServiceSpec(nil), specs...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Hostname < ordered[j].Hostname })
	data, _ := json.Marshal(ordered)
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func resumableImport(r *record, hash string) bool {
	return r.ImportHash == hash && r.Desired == "stopped" && r.Deployment == nil && r.Pending == nil
}

func (b *Backend) reserveImport(ctx context.Context, s infra.ServiceSpec, hash string, records []*record, existing map[string]*record) (*record, error) {
	id, err := newID()
	if err != nil {
		return nil, err
	}
	r := &record{ID: id, Hostname: s.Hostname, Kind: s.Kind, Volume: s.Volume,
		SpreadGroup: s.SpreadGroup, Resources: s.Resources, Min: max(1, s.MinContainers),
		Max: max(1, s.MaxContainers), Vars: maps.Clone(s.Vars), Desired: "stopped",
		ImportHash: hash, Importing: true}
	for key := range s.Env {
		r.SecretKeys = append(r.SecretKeys, key)
	}
	sort.Strings(r.SecretKeys)
	if r.Kind == infra.KindVolume {
		nodes, err := b.nodes(ctx)
		if err != nil {
			return nil, err
		}
		r.Node, err = chooseNode(nodes, records, r.SpreadGroup, s.SpreadReplaces)
		if err != nil {
			return nil, err
		}
		// A reserved, unguessable incarnation attributes an accepted create
		// even when its server-generated volume ID never reaches the caller.
		r.VolumeName = volumeName(b.target.ID, r.Hostname) + "-" + r.ID
	} else if r.Volume != "" {
		volume := existing[r.Volume]
		if volume == nil || volume.Kind != infra.KindVolume {
			return nil, reject("runtime references an absent volume")
		}
		r.Node = volume.Node
	}
	err = b.saveImport(ctx, r)
	if errors.Is(err, infra.ErrConflict) {
		return nil, err
	}
	return r, err
}

// Reconcile only a read-back of the exact attempted record. If the result is
// still unknown, retain the reservation and its original identity for retry;
// never delete a volume/environment on the assumption the CAS did not commit.
func (b *Backend) saveImport(ctx context.Context, r *record) error {
	err := b.save(ctx, r)
	if err == nil {
		return nil
	}
	observe, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	current, readErr := b.load(observe, r.Hostname)
	if readErr == nil && current != nil && current.ID == r.ID && maps.Equal(current.Vars, r.Vars) {
		want, _ := json.Marshal(r)
		got, _ := json.Marshal(current)
		if string(want) == string(got) {
			r.Index = current.Index
			return nil
		}
	}
	return err
}

func (b *Backend) finishImport(ctx context.Context, r *record, spec infra.ServiceSpec) error {
	if r.Kind == infra.KindVolume && r.VolumeID == "" {
		if err := b.createVolume(ctx, r); err != nil {
			return err
		}
		if err := b.saveImport(ctx, r); err != nil {
			return err
		}
	}
	items := maps.Clone(spec.Env)
	if items == nil {
		items = map[string]string{}
	}
	maps.Copy(items, spec.Vars)
	env, err := b.readVariable(ctx, "nomad/jobs/"+r.Hostname)
	if err != nil {
		return err
	}
	if env.ModifyIndex != 0 {
		if !maps.Equal(env.Items, items) {
			return &APIError{Status: 409, Op: "import", Message: "reserved environment differs from original import; refusing adoption"}
		}
	} else if len(items) != 0 {
		env.Items = items
		if err = b.writeVariable(ctx, &env); err != nil {
			observe, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer cancel()
			current, readErr := b.readVariable(observe, env.Path)
			if readErr != nil || current.ModifyIndex == 0 || !maps.Equal(current.Items, items) {
				return errors.Join(err, readErr)
			}
		}
	}
	r.Importing = false
	return b.saveImport(ctx, r)
}
