//go:build !integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import "github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"

func nativeSpec(s myspec.Spec) myspec.Spec           { return s }
func nativeEnv() []string                            { return []string{"PATH=/usr/bin:/bin", "LANG=C.UTF-8", "LC_ALL=C.UTF-8"} }
func nativeListener() (string, int)                  { return "0.0.0.0", myspec.PortMySQL }
func nativeServerListener(myspec.Spec) (string, int) { return nativeListener() }
func nativeProxyBinary() string                      { return "/usr/bin/proxysql" }
func nativeProxyListeners() (string, string)         { return "127.0.0.1:6032", "0.0.0.0:3306" }
func nativeReportHost(s myspec.Spec) string          { return s.Member }
