// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

//go:build !linux

package agent

// affinityCPUs is unknown outside Linux; member agents run only on Linux.
func affinityCPUs() int { return 0 }
