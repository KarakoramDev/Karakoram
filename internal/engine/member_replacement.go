// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import "github.com/KarakoramDev/Karakoram/internal/domain"

// MemberReplacementValidator applies a provider's native replacement target
// restrictions before admitting either replacement or relocation.
type MemberReplacementValidator interface {
	ValidateMemberReplacement(*domain.Cluster, string) error
}
