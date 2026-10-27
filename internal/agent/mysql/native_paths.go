//go:build !integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import "github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"

func nativeSpec(s myspec.Spec) myspec.Spec { return s }
func nativeEnv() []string                  { return []string{"PATH=/usr/bin:/bin", "LANG=C.UTF-8", "LC_ALL=C.UTF-8"} }
func nativeListener() (string, int)        { return "0.0.0.0", myspec.PortMySQL }
func nativeServerListener(s myspec.Spec) (string, int) {
	if s.ListenHost == "::" {
		// MySQL's all-addresses wildcard explicitly enables dual-stack sockets.
		return "*", myspec.PortMySQL
	}
	return nativeListener()
}
func nativeProxyBinary() string { return "/usr/bin/proxysql" }
func nativeProxyListeners(s myspec.Spec) (string, string) {
	if s.ListenHost == "::" {
		return "127.0.0.1:6032", "0.0.0.0:3306;[::]:3306"
	}
	return "127.0.0.1:6032", "0.0.0.0:3306"
}
func nativeReportHost(s myspec.Spec) string { return s.Member }
