//go:build integration

// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"os"
	"path/filepath"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// Native integration binaries retain strict wire validation but run extracted
// artifacts without privileged installation or production network listeners.
func nativeSpec(s myspec.Spec) myspec.Spec {
	if bin := os.Getenv("KARAKORAM_MYSQL_BIN_DIR"); bin != "" {
		s.ServerDir = filepath.Dir(bin)
	}
	if dir := os.Getenv("KARAKORAM_MYSQL_XTRABACKUP_DIR"); dir != "" {
		s.XtraBackupDir = dir
	}
	return s
}
func nativeEnv() []string {
	env := []string{"PATH=/usr/bin:/bin", "LANG=C.UTF-8", "LC_ALL=C.UTF-8", "TMPDIR=" + os.TempDir()}
	if lib := os.Getenv("KARAKORAM_MYSQL_LD_LIBRARY_PATH"); lib != "" {
		env = append(env, "LD_LIBRARY_PATH="+lib)
	}
	return env
}
func nativeListener() (string, int) {
	if port, err := strconv.Atoi(os.Getenv("KARAKORAM_MYSQL_PORT")); err == nil && port > 0 && port <= 65535 {
		return "127.0.0.1", port
	}
	return "127.0.0.1", 24800
}
func nativeServerListener(s myspec.Spec) (string, int) {
	for _, peer := range s.Peers {
		if peer.Name == s.Member && peer.Host == "127.0.0.1" {
			return peer.Host, peer.Port
		}
	}
	return nativeListener()
}
func nativeProxyBinary() string {
	if path := os.Getenv("KARAKORAM_PROXYSQL_BIN"); path != "" {
		return path
	}
	return "/usr/bin/proxysql"
}
func nativeProxyListeners(myspec.Spec) (string, string) {
	admin, client := os.Getenv("KARAKORAM_PROXYSQL_ADMIN_ADDR"), os.Getenv("KARAKORAM_PROXYSQL_CLIENT_ADDR")
	if admin == "" {
		admin = "127.0.0.1:6032"
	}
	if client == "" {
		client = "0.0.0.0:3306"
	}
	return admin, client
}
func nativeReportHost(s myspec.Spec) string {
	for _, peer := range s.Peers {
		if peer.Name == s.Member && peer.Host == "127.0.0.1" {
			return peer.Host
		}
	}
	return s.Member
}
