// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package cronutil defines the scheduling grammar shared by control-plane policies.
package cronutil

import "github.com/robfig/cron/v3"

// Parser accepts minute-resolution cron schedules and standard descriptors.
func Parser() cron.Parser {
	return cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
}
