// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build !ui

package ui

import "io/fs"

// Built reports whether the SPA is compiled into this binary.
const Built = false

func assets() fs.FS { return nil }
