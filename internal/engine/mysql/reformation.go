// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

// reforming distinguishes an active recovery fence from its retained receipt.
// A completed journal must not forbid new member identities or access requests.
func (f formation) reforming() bool {
	return f.Reformation != nil && f.Reformation.State != "formed"
}
