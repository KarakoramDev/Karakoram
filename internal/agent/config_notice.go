// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import "time"

type configNoticeResult struct {
	target    string
	unchanged bool
	err       error
}

type configNoticeBackoff struct {
	unchanged int
	next      time.Time
	warned    bool
	target    string
	applied   uint64
}

func (b *configNoticeBackoff) allowed(now time.Time, target string, applied uint64) bool {
	if applied != b.applied {
		b.applied = applied
		b.unchanged, b.next, b.target = 0, time.Time{}, ""
	}
	return target != "" && target != b.target || !now.Before(b.next)
}

func (b *configNoticeBackoff) completed(now time.Time, result configNoticeResult) bool {
	if result.err != nil {
		return false
	}
	if result.target != b.target {
		b.target = result.target
		b.unchanged, b.next = 0, time.Time{}
	}
	if !result.unchanged {
		b.unchanged = 0
		b.next = time.Time{}
		return false
	}
	b.unchanged++
	if b.unchanged < 2 {
		return false
	}
	b.next = now.Add(time.Minute)
	if b.warned {
		return false
	}
	b.warned = true
	return true
}
