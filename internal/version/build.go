// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package version

import (
	"debug/elf"
	"fmt"
	"os"
	"sync"
)

var identityOnce sync.Once
var identity string

// BuildIdentity identifies the running executable without hashing a deployment archive.
// It deliberately preserves the deployment artifact's Go build-note / stat fallback.
func BuildIdentity() string {
	identityOnce.Do(func() {
		path, err := os.Executable()
		if err != nil {
			return
		}
		stat, err := os.Stat(path)
		if err != nil {
			return
		}
		identity = fmt.Sprintf("karakoram/%d/%d", stat.Size(), stat.ModTime().UnixNano())
		if binary, err := elf.Open(path); err == nil {
			defer binary.Close()
			if note := binary.Section(".note.go.buildid"); note != nil {
				if id, err := note.Data(); err == nil {
					identity = fmt.Sprintf("karakoram/%d/%x", stat.Size(), id)
				}
			}
		}
	})
	return identity
}
