// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

const coverageDurableRoots = 256
const coverageDurableLifetime = 24 * time.Hour

// Invalidate the proof durably before reclaiming an unpersisted failure.
func invalidateCoverageProof(prefix string) error {
	if err := os.Remove(prefix + ".verified.json"); err != nil && !os.IsNotExist(err) {
		return err
	}
	dir, err := os.Open(filepath.Dir(prefix))
	if err != nil {
		return err
	}
	err = dir.Sync()
	dir.Close()
	if err != nil {
		return err
	}
	return nil
}

func removeCoverageState(prefix string) error {
	if err := invalidateCoverageProof(prefix); err != nil {
		return err
	}
	names, err := filepath.Glob(prefix + ".*")
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// Only the inventory named by the durable work header can be resumed. Called
// before the worker starts or after its batch completes, never during publication.
func pruneCoverageInventories(prefix string) error {
	var work coverageWorkDisk
	err := readCoverageJSON(prefix+".work.json", &work)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	keep := ""
	if err == nil {
		keep = prefix + ".refs-" + work.Cache.Inventory
	}
	names, err := filepath.Glob(prefix + ".refs-*")
	if err != nil {
		return err
	}
	for _, name := range names {
		if name != keep {
			if err := os.Remove(name); err != nil && !os.IsNotExist(err) {
				return err
			}
		}
	}
	return nil
}

// Retain recent working roots regardless of the inactive-root cap. Memory LRU
// eviction never removes proofs. Old inactive roots expire after 24 hours;
// beyond the generous inactive cap, retain the most recently written proofs.
func pruneCoverageStore(root string, resident map[string]bool, startup bool) error {
	files, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	roots := map[string]time.Time{}
	for _, file := range files {
		if startup && strings.HasPrefix(file.Name(), ".coverage-refs-") {
			if err := os.Remove(filepath.Join(root, file.Name())); err != nil {
				return err
			}
			continue
		}
		rest, ok := strings.CutPrefix(file.Name(), "pitr-coverage-")
		if !ok {
			continue
		}
		key, _, ok := strings.Cut(rest, ".")
		if !ok || !myspec.ValidDigest(key) {
			continue
		}
		info, err := file.Info()
		if err != nil {
			return err
		}
		if info.ModTime().After(roots[key]) {
			roots[key] = info.ModTime()
		}
	}
	keys := make([]string, 0, len(roots))
	for key := range roots {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		if resident[a] != resident[b] {
			return resident[a]
		}
		if roots[a].Equal(roots[b]) {
			return a < b
		}
		return roots[a].After(roots[b])
	})
	now := time.Now()
	inactive := 0
	for _, key := range keys {
		prefix := coverageStatePrefix(root, key)
		active := resident[key] || now.Sub(roots[key]) < coverageActiveLifetime
		if !active {
			inactive++
		}
		if !active && (now.Sub(roots[key]) > coverageDurableLifetime || inactive > coverageDurableRoots) {
			if err := removeCoverageState(prefix); err != nil {
				return err
			}
			continue
		}
		if startup {
			if err := pruneCoverageInventories(prefix); err != nil {
				return err
			}
		}
	}
	return nil
}
