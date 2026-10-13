// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store_test

import (
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

var _ engine.ClusterStore = (*store.Store)(nil)
var _ engine.SecretStore = (*store.Store)(nil)
