// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"testing"
	"time"
)

func TestArchivedSegmentEnd(t *testing.T) {
	for _, tc := range []struct {
		name          string
		size          uint64
		timeline, end uint64
		valid         bool
	}{
		{"00000001000000000000000A", 16 << 20, 1, 11 * 16 << 20, true},
		{"0000000200000000000000FF", 16 << 20, 2, 1 << 32, true},
		{"000000030000000100000003", 64 << 20, 3, 1<<32 | 4*64<<20, true},
		{"000000010000000000000100", 16 << 20, 0, 0, false},
		{"00000001.history", 16 << 20, 0, 0, false},
		{"00000001000000000000000A.partial", 16 << 20, 0, 0, false},
		{"00000001000000000000000A", 17 << 20, 0, 0, false},
		{"00000001000000000000000G", 16 << 20, 0, 0, false},
		{"00000001FFFFFFFF000000FF", 16 << 20, 0, 0, false},
	} {
		timeline, end, ok := archivedSegmentEnd(tc.name, tc.size)
		if ok != tc.valid || ok && (timeline != tc.timeline || end != tc.end) {
			t.Errorf("%s: got timeline=%d end=%X valid=%v", tc.name, timeline, end, ok)
		}
	}
}

func TestWALWatermarkTracksContentNotUploadTime(t *testing.T) {
	var w archiveWatermark
	at := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	const size = 16 << 20
	if got := w.observe("system", 1, walSample{at, 10*size + 100}, "000000010000000000000009", size); got != "" {
		t.Fatal("unarchived content advertised", got)
	}
	// Upload can finish arbitrarily later: it must publish the sampled content
	// time, not a new wall-clock timestamp at upload or observation.
	got := w.observe("system", 1, walSample{at.Add(time.Minute), 11*size + 100}, "00000001000000000000000A", size)
	if got != at.Format(time.RFC3339Nano) {
		t.Fatal("archive used upload/observation time", got)
	}
	// A forced archive_timeout switch puts the new heartbeat in the next segment;
	// archiving it advances to the preceding committed heartbeat sample.
	got = w.observe("system", 1, walSample{at.Add(2 * time.Minute), 12*size + 100}, "00000001000000000000000B", size)
	if got != at.Add(time.Minute).Format(time.RFC3339Nano) {
		t.Fatal("window did not advance after segment switch", got)
	}
}

func TestWALWatermarkTimelineAndSystemIsolation(t *testing.T) {
	var w archiveWatermark
	at := time.Now().UTC()
	const size = 16 << 20
	if got := w.observe("system", 1, walSample{at, 100}, "000000010000000000000000", size); got == "" {
		t.Fatal("missing initial watermark")
	}
	if got := w.observe("system", 2, walSample{at.Add(time.Second), 200}, "000000010000000000000000", size); got != "" {
		t.Fatal("old timeline authorized new primary", got)
	}
	if got := w.observe("system", 2, walSample{}, "000000020000000000000000", size); got != at.Add(time.Second).Format(time.RFC3339Nano) {
		t.Fatal("new timeline did not advance", got)
	}
	if got := w.observe("different-system", 2, walSample{}, "000000020000000000000000", size); got != "" {
		t.Fatal("recovered system reused previous samples", got)
	}
}

func TestWALWatermarkBoundsArchiveOutageMemory(t *testing.T) {
	var w archiveWatermark
	for i := range 11000 {
		w.observe("system", 2, walSample{time.Unix(int64(i+1), 0), uint64(i + 1)}, "000000010000000000000000", 16<<20)
	}
	if len(w.samples) > 10000 {
		t.Fatal("unbounded samples", len(w.samples))
	}
}

func TestParseWALLSN(t *testing.T) {
	if value, ok := parseLSN("A/FFFFFFFF"); !ok || value != 0xAFFFFFFFF {
		t.Fatal(value, ok)
	}
	for _, value := range []string{"", "0", "0/G", "100000000/0", "0/100000000"} {
		if _, ok := parseLSN(value); ok {
			t.Fatal("accepted invalid LSN", value)
		}
	}
}

func TestWALWatermarkRestoreResetRejectsReusedTimeline(t *testing.T) {
	var w archiveWatermark
	at := time.Now().UTC()
	if got := w.observe("same-system", 2, walSample{at, 100}, "000000020000000000000000", 16<<20); got == "" {
		t.Fatal("missing initial sample")
	}
	w.lastSample = at
	w.reset()
	if !w.lastSample.IsZero() {
		t.Fatal("restore reset suppressed first heartbeat")
	}
	if got := w.observe("same-system", 2, walSample{}, "000000020000000000000000", 16<<20); got != "" {
		t.Fatal("repeated recovery reused another lineage's sample", got)
	}
}
