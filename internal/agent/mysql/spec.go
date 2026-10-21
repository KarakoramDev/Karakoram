// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func decodeSpec(c agentapi.MemberConfig) (myspec.Spec, error) {
	var s myspec.Spec
	if c.Engine != myspec.Engine {
		return s, errors.New("MySQL engine required")
	}
	d := json.NewDecoder(bytes.NewReader(c.EngineSpec))
	d.DisallowUnknownFields()
	if err := d.Decode(&s); err != nil {
		return s, errors.New("invalid MySQL specification")
	}
	if d.Decode(new(any)) != io.EOF {
		return s, errors.New("trailing MySQL specification")
	}
	if err := s.Validate(); err != nil {
		return s, err
	}
	if c.Member != s.Member || c.Role != s.Role {
		return s, errors.New("MySQL member identity mismatch")
	}
	if s.Role == myspec.RoleData {
		if s.Admin.Username != "admin" {
			return s, errors.New("MySQL admin credentials required")
		}
		if err := s.Admin.Validate(); err != nil {
			return s, err
		}
	}
	for _, p := range c.DesiredProcesses {
		if p != "mysqld" && p != "proxysql" {
			return s, errors.New("invalid MySQL process")
		}
		if (s.Role == myspec.RoleProxy) != (p == "proxysql") {
			return s, errors.New("MySQL process role mismatch")
		}
	}
	return nativeSpec(s), nil
}
