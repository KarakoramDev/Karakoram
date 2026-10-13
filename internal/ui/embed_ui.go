// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build ui

package ui

import (
	"embed"
	"io/fs"
)

// dist is populated by `npm run build` in ./ui (vite outDir points here).
//
//go:embed all:dist
var dist embed.FS

// Built reports whether the SPA is compiled into this binary.
const Built = true

func assets() fs.FS {
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic("internal/ui: embedded dist directory missing: " + err.Error())
	}
	return sub
}
