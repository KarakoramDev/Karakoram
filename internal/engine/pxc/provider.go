// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package pxc composes the shared MySQL lifecycle with Galera replication.
package pxc

import "github.com/KarakoramDev/Karakoram/internal/engine/mysql"

func New(options mysql.Options) *mysql.Core {
	options.Provider = "pxc"
	return mysql.New(options)
}
