// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package myspec

import (
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

func (u RecoveryUser) Validate() error {
	valid := func(p string) bool {
		return len(p) >= 12 && len(p) <= 1024 && utf8.ValidString(p) && strings.IndexFunc(p, unicode.IsControl) < 0
	}
	if !ManagedUser(u.Name) || !valid(u.Current) || u.Secondary != "" && !valid(u.Secondary) {
		return fmt.Errorf("invalid managed recovery account")
	}
	return nil
}
