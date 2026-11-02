// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type coverageCacheDisk struct {
	Inventory      string                      `json:"inventory"`
	LastKey        string                      `json:"lastKey"`
	LastSequence   uint64                      `json:"lastSequence"`
	LastOrderedKey string                      `json:"lastOrderedKey"`
	Have           string                      `json:"have"`
	Base           string                      `json:"base"`
	Response       myspec.PITRCoverageResponse `json:"response"`
	LastCommit     int64                       `json:"lastCommit"`
	Stopped        bool                        `json:"stopped"`
}

func coverageCacheRecord(cache *replayCoverageCache) coverageCacheDisk {
	return coverageCacheDisk{Inventory: cache.inventory, LastKey: cache.lastKey, LastSequence: cache.lastSequence, LastOrderedKey: cache.lastOrderedKey, Have: cache.state.have.String(), Base: cache.state.base.String(), Response: cache.state.response, LastCommit: cache.state.lastCommit, Stopped: cache.state.stopped}
}
func (d coverageCacheDisk) cache() (*replayCoverageCache, error) {
	have, err := parseRecoveryGTIDs(d.Have)
	if err != nil {
		return nil, err
	}
	base, err := parseRecoveryGTIDs(d.Base)
	if err != nil {
		return nil, err
	}
	if !myspec.ValidDigest(d.Inventory) {
		return nil, errors.New("coverage_checkpoint_invalid")
	}
	return &replayCoverageCache{inventory: d.Inventory, lastKey: d.LastKey, lastSequence: d.LastSequence, lastOrderedKey: d.LastOrderedKey, state: &replayCoverageState{have: have, base: base, response: d.Response, lastCommit: d.LastCommit, stopped: d.Stopped}}, nil
}

type coverageVerifiedDisk struct {
	Version    int                 `json:"version"`
	Baseline   myspec.BackupResult `json:"baseline"`
	Cache      *coverageCacheDisk  `json:"cache,omitempty"`
	StartedAt  time.Time           `json:"startedAt"`
	VerifiedAt time.Time           `json:"verifiedAt"`
	Error      string              `json:"error,omitempty"`
}
type coverageWorkDisk struct {
	Version   int                 `json:"version"`
	Baseline  myspec.BackupResult `json:"baseline"`
	Cache     coverageCacheDisk   `json:"cache"`
	StartedAt time.Time           `json:"startedAt"`
	Position  int                 `json:"position"`
	Total     int                 `json:"total"`
}
type coverageRefDisk struct {
	Key      string `json:"key"`
	Sequence uint64 `json:"sequence"`
	Native   bool   `json:"native"`
}
type coverageWork struct {
	coverageWorkDisk
	cache  *replayCoverageCache
	refs   []replayObjectRef
	prefix string
}

func coverageStatePrefix(root, key string) string { return filepath.Join(root, "pitr-coverage-"+key) }
func readCoverageJSON(name string, out any) error {
	raw, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}
func loadCoverageVerified(prefix string) (*coverageVerifiedDisk, error) {
	var out coverageVerifiedDisk
	if err := readCoverageJSON(prefix+".verified.json", &out); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if out.Version != 1 {
		return nil, errors.New("coverage_checkpoint_version")
	}
	return &out, nil
}
func saveCoverageRefs(prefix, inventory string, refs []replayObjectRef) error {
	file, err := os.CreateTemp(filepath.Dir(prefix), ".coverage-refs-")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	encoder := json.NewEncoder(file)
	for _, ref := range refs {
		if err = encoder.Encode(coverageRefDisk{ref.key, ref.sequence, ref.native}); err != nil {
			file.Close()
			return err
		}
	}
	if err = file.Sync(); err != nil {
		file.Close()
		return err
	}
	if err = file.Close(); err != nil {
		return err
	}
	if err = os.Rename(file.Name(), prefix+".refs-"+inventory); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(prefix))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func loadCoverageWork(prefix string) (*coverageWork, error) {
	var disk coverageWorkDisk
	if err := readCoverageJSON(prefix+".work.json", &disk); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if disk.Version != 1 || disk.Position < 0 || disk.Position > disk.Total {
		return nil, errors.New("coverage_checkpoint_invalid")
	}
	cache, err := disk.Cache.cache()
	if err != nil {
		return nil, err
	}
	verified, err := loadCoverageVerified(prefix)
	if err != nil {
		return nil, err
	}
	if verified != nil && verified.Cache != nil && verified.Error == "" && !verified.VerifiedAt.IsZero() && !verified.StartedAt.Before(disk.StartedAt) {
		if err := os.Remove(prefix + ".work.json"); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		if err := os.Remove(prefix + ".refs-" + disk.Cache.Inventory); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
		return nil, nil
	}
	file, err := os.Open(prefix + ".refs-" + disk.Cache.Inventory)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	refs := make([]replayObjectRef, 0, disk.Total)
	for {
		var ref coverageRefDisk
		err := decoder.Decode(&ref)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		refs = append(refs, replayObjectRef{key: ref.Key, sequence: ref.Sequence, native: ref.Native})
	}
	if len(refs) != disk.Total {
		return nil, errors.New("coverage_checkpoint_invalid")
	}
	return &coverageWork{coverageWorkDisk: disk, cache: cache, refs: refs, prefix: prefix}, nil
}
func (w *coverageWork) checkpoint() error {
	w.Cache = coverageCacheRecord(w.cache)
	return durableJSON(w.prefix+".work.json", w.coverageWorkDisk)
}
func (w *coverageWork) complete() error {
	record := coverageCacheRecord(w.cache)
	verified := coverageVerifiedDisk{Version: 1, Baseline: w.Baseline, Cache: &record, StartedAt: w.StartedAt, VerifiedAt: time.Now().UTC()}
	if err := durableJSON(w.prefix+".verified.json", verified); err != nil {
		return err
	}
	if err := os.Remove(w.prefix + ".work.json"); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(w.prefix + ".refs-" + w.cache.inventory); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
