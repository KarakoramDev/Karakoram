// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import "github.com/KarakoramDev/Karakoram/internal/domain"

// WriteOnlyOptions extracts credentials before validation, persistence and API
// response construction. Implementations validate required creation credentials
// here; omitted update credentials leave existing secrets unchanged.
type WriteOnlyOptions interface {
	ExtractWriteOnlyOptions(old *domain.ClusterSpec, spec *domain.ClusterSpec) (map[string][]byte, error)
}
