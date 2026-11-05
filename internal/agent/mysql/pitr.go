// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type binlogTransaction struct {
	GTID                    string `json:"gtid"`
	OriginalCommitTimestamp int64  `json:"original_commit_timestamp"`
	EndPosition             uint64 `json:"end_position"`
}
type gtidInterval struct{ lo, hi uint64 }
type recoveryGTIDs map[string][]gtidInterval

var recoveryGTIDTag = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,31}$`)

func parseRecoveryGTIDs(raw string) (recoveryGTIDs, error) {
	out := recoveryGTIDs{}
	if strings.TrimSpace(raw) == "" {
		return out, nil
	}
	for _, block := range strings.Split(raw, ",") {
		parts := strings.Split(strings.TrimSpace(block), ":")
		if len(parts) < 2 || !myspec.ValidUUID(parts[0]) {
			return nil, errors.New("invalid_gtid")
		}
		sid := strings.ToLower(parts[0])
		start := 1
		if _, err := strconv.ParseUint(parts[1], 10, 64); err != nil && !strings.Contains(parts[1], "-") {
			if !recoveryGTIDTag.MatchString(parts[1]) {
				return nil, errors.New("invalid_gtid")
			}
			sid += ":" + parts[1]
			start++
		}
		if start == len(parts) {
			return nil, errors.New("invalid_gtid")
		}
		for _, r := range parts[start:] {
			bounds := strings.Split(r, "-")
			if len(bounds) > 2 {
				return nil, errors.New("invalid_gtid")
			}
			lo, err := strconv.ParseUint(bounds[0], 10, 63)
			if err != nil || lo == 0 {
				return nil, errors.New("invalid_gtid")
			}
			hi := lo
			if len(bounds) == 2 {
				hi, err = strconv.ParseUint(bounds[1], 10, 63)
			}
			if err != nil || hi < lo {
				return nil, errors.New("invalid_gtid")
			}
			out[sid] = append(out[sid], gtidInterval{lo, hi})
		}
	}
	out.normalize()
	return out, nil
}
func (s recoveryGTIDs) normalize() {
	for sid, ranges := range s {
		sort.Slice(ranges, func(i, j int) bool { return ranges[i].lo < ranges[j].lo })
		merged := ranges[:0]
		for _, r := range ranges {
			if len(merged) > 0 && r.lo <= merged[len(merged)-1].hi+1 {
				if r.hi > merged[len(merged)-1].hi {
					merged[len(merged)-1].hi = r.hi
				}
			} else {
				merged = append(merged, r)
			}
		}
		s[sid] = merged
	}
}
func (s recoveryGTIDs) add(other recoveryGTIDs) {
	for sid, rs := range other {
		s[sid] = append(s[sid], rs...)
	}
	s.normalize()
}
func (s recoveryGTIDs) contains(other recoveryGTIDs) bool {
	for sid, rs := range other {
		for _, r := range rs {
			found := false
			for _, h := range s[sid] {
				if h.lo <= r.lo && h.hi >= r.hi {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}
func (s recoveryGTIDs) String() string {
	keys := make([]string, 0, len(s))
	for sid := range s {
		keys = append(keys, sid)
	}
	sort.Strings(keys)
	blocks := make([]string, 0, len(keys))
	for _, sid := range keys {
		b := sid
		for _, r := range s[sid] {
			b += ":" + strconv.FormatUint(r.lo, 10)
			if r.hi != r.lo {
				b += "-" + strconv.FormatUint(r.hi, 10)
			}
		}
		blocks = append(blocks, b)
	}
	return strings.Join(blocks, ",")
}

var binlogGTID = regexp.MustCompile(`GTID_NEXT\s*=\s*'([^']+)'`)
var binlogCommit = regexp.MustCompile(`original_commit_timestamp(?:=|\s+)\s*([0-9]+)`)
var binlogPosition = regexp.MustCompile(`end_log_pos\s+([0-9]+)`)

// Event headers use tabs in native mysqlbinlog output, with spaces in some
// versions/formatters. Match whole header tokens, never SQL or row contents.
func binlogHeader(line, event string) bool {
	if !strings.HasPrefix(line, "#") {
		return false
	}
	for offset := 0; offset < len(line); {
		i := strings.Index(line[offset:], event)
		if i < 0 {
			return false
		}
		i += offset
		end := i + len(event)
		if i > 0 && (line[i-1] == ' ' || line[i-1] == '\t') &&
			(end == len(line) || line[end] == ' ' || line[end] == '\t' || line[end] == '=') {
			return true
		}
		offset = end
	}
	return false
}

func parseBinlogOutput(data []byte) (binlogSidecar, error) {
	return parseBinlogStream(bytes.NewReader(data), nil)
}

// emit permits probes to discard transaction metadata as it is consumed.
// Replay sidecars retain only one small record per committed transaction, never rows.
func parseBinlogStream(r io.Reader, emit func(binlogTransaction) error) (binlogSidecar, error) {
	var out binlogSidecar
	scan := bufio.NewScanner(r)
	scan.Buffer(make([]byte, 4096), 4<<20)
	var tx binlogTransaction
	var position uint64
	previous := false
	inTransaction := false
	queryBoundary := false
	set := recoveryGTIDs{}
	record := func() error {
		g, err := parseRecoveryGTIDs(tx.GTID)
		if err != nil {
			return err
		}
		set.add(g)
		if out.FirstCommitTimestamp == 0 || tx.OriginalCommitTimestamp < out.FirstCommitTimestamp {
			out.FirstCommitTimestamp = tx.OriginalCommitTimestamp
		}
		if tx.OriginalCommitTimestamp > out.LastCommitTimestamp {
			out.LastCommitTimestamp = tx.OriginalCommitTimestamp
		}
		if emit != nil {
			return emit(tx)
		}
		out.Transactions = append(out.Transactions, tx)
		return nil
	}
	finish := func() error {
		if tx.GTID == "" {
			if tx.OriginalCommitTimestamp != 0 {
				return errors.New("missing_transaction_gtid")
			}
			return nil
		}
		if inTransaction || !queryBoundary {
			return errors.New("incomplete_binlog_transaction")
		}
		if tx.OriginalCommitTimestamp == 0 {
			return errors.New("missing_commit_timestamp")
		}
		if err := record(); err != nil {
			return err
		}
		tx = binlogTransaction{}
		queryBoundary = false
		return nil
	}
	var previousBlock strings.Builder
	finishPrevious := func() error {
		if previousBlock.Len() == 0 {
			return nil
		}
		set, err := parseRecoveryGTIDs(previousBlock.String())
		if err != nil {
			return err
		}
		out.PreviousGTIDSet = set.String()
		previousBlock.Reset()
		return nil
	}
	for scan.Scan() {
		if bytes.HasPrefix(scan.Bytes(), []byte("###")) {
			continue
		}
		line := scan.Text()
		// DDL commits do not have an Xid event. Seal their previous Query
		// boundary before consuming the next transaction or rotation header.
		if binlogHeader(line, "GTID") || binlogHeader(line, "Anonymous_GTID") || binlogHeader(line, "Rotate to") {
			if err := finish(); err != nil {
				return out, err
			}
		}
		if m := binlogPosition.FindStringSubmatch(line); m != nil {
			var err error
			position, err = strconv.ParseUint(m[1], 10, 64)
			if err != nil {
				return out, err
			}
			if position >= 4 {
				out.DecodePosition = position
			}
		}
		if tx.GTID != "" && binlogHeader(line, "Query") {
			tx.EndPosition = position
			queryBoundary = true
		}
		if binlogHeader(line, "Previous-GTIDs") {
			previous = true
			continue
		}
		if previous {
			v := strings.TrimSpace(strings.TrimPrefix(line, "#"))
			headerMetadata := binlogCommit.MatchString(line) || strings.HasPrefix(v, "immediate_commit_timestamp=")
			if strings.HasPrefix(line, "# ") && !strings.Contains(v, " ") && v != "[empty]" && !headerMetadata {
				if previousBlock.Len() > 0 {
					previousBlock.WriteByte(',')
				}
				previousBlock.WriteString(strings.TrimSuffix(v, ","))
				continue
			}
			previous = false
			if err := finishPrevious(); err != nil {
				return out, err
			}
		}
		if m := binlogCommit.FindStringSubmatch(line); m != nil {
			// mysqlbinlog prints the original timestamp both in the GTID
			// comment and in a versioned SET before GTID_NEXT. These are
			// repeated fields of one header, not a transaction boundary.
			if tx.GTID != "" {
				if err := finish(); err != nil {
					return out, err
				}
			}
			var err error
			tx.OriginalCommitTimestamp, err = strconv.ParseInt(m[1], 10, 64)
			if err != nil {
				return out, err
			}
		}
		if m := binlogGTID.FindStringSubmatch(line); m != nil && m[1] != "AUTOMATIC" {
			if _, err := parseRecoveryGTIDs(m[1]); err != nil {
				return out, err
			}
			tx.GTID = m[1]
		}
		if tx.GTID != "" && (line == "BEGIN" || strings.HasPrefix(line, "XA START ")) {
			inTransaction = true
		}
		// XA PREPARE is a durable GTID boundary (also used for ONE PHASE).
		// Its later COMMIT/ROLLBACK, if any, has a separate GTID Query.
		if tx.GTID != "" && (binlogHeader(line, "Xid") || binlogHeader(line, "XA PREPARE") || strings.HasPrefix(line, "COMMIT")) {
			if tx.OriginalCommitTimestamp == 0 {
				return out, errors.New("missing_commit_timestamp")
			}
			tx.EndPosition = position
			if err := record(); err != nil {
				return out, err
			}
			inTransaction = false
			queryBoundary = false
			tx = binlogTransaction{}
		}
	}
	if err := scan.Err(); err != nil {
		return out, err
	}
	if err := finishPrevious(); err != nil {
		return out, err
	}
	if err := finish(); err != nil {
		return out, err
	}
	out.GTIDSet = set.String()
	if out.FirstCommitTimestamp != 0 {
		out.FirstEventTime = time.UnixMicro(out.FirstCommitTimestamp).UTC()
		out.LastEventTime = time.UnixMicro(out.LastCommitTimestamp).UTC()
	}
	return out, nil
}

type archivedBinlog struct {
	key  string
	meta binlogSidecar
}

// trustedReplayHistory admits only the physical snapshot's original source.
// A foreign stream's numeric GTIDs cannot authenticate its row history. After
// a writer change, take a fresh full from that writer before extending PITR.
func trustedReplayHistory(logs []archivedBinlog, backup myspec.BackupResult) ([]archivedBinlog, int64, error) {
	if !myspec.ValidUUID(backup.SourceUUID) || !binlogName.MatchString(backup.BinlogFile) || backup.BinlogPosition < 4 {
		return nil, 0, errors.New("backup_binlog_provenance_missing")
	}
	base, err := parseRecoveryGTIDs(backup.GTIDSet)
	if err != nil {
		return nil, 0, err
	}
	// The exact physical anchor proves which baseline GTIDs were logged.
	// Purged-only history in the full need not occur in Previous_gtids.
	for _, l := range logs {
		base, err = snapshotLoggedGTIDs(l, backup, base)
		if err != nil {
			return nil, 0, err
		}
	}
	type orderedLog struct {
		log      archivedBinlog
		sequence uint64
	}
	ordered := make([]orderedLog, 0, len(logs))
	for _, l := range logs {
		anchored, eligible, err := anchorReplayLog(l, backup, base)
		if err != nil {
			return nil, 0, err
		}
		if eligible {
			_, ordinal, _ := strings.Cut(anchored.meta.File, ".")
			sequence, _ := strconv.ParseUint(ordinal, 10, 64)
			ordered = append(ordered, orderedLog{anchored, sequence})
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].sequence == ordered[j].sequence {
			return ordered[i].log.key < ordered[j].log.key
		}
		return ordered[i].sequence < ordered[j].sequence
	})
	have, _ := parseRecoveryGTIDs(backup.GTIDSet)
	last := backup.EndTime.UnixMicro()
	trusted := make([]archivedBinlog, 0, len(ordered))
	for _, item := range ordered {
		l := item.log
		prev, err := parseRecoveryGTIDs(l.meta.PreviousGTIDSet)
		if err != nil {
			return nil, 0, err
		}
		if !have.contains(prev) || !prev.contains(base) {
			break
		}
		set, err := parseRecoveryGTIDs(l.meta.GTIDSet)
		if err != nil {
			return nil, 0, err
		}
		decoded := recoveryGTIDs{}
		novel := make([]binlogTransaction, 0, len(l.meta.Transactions))
		for _, tx := range l.meta.Transactions {
			g, err := parseRecoveryGTIDs(tx.GTID)
			if err != nil || !set.contains(g) || tx.OriginalCommitTimestamp <= 0 {
				return nil, 0, errors.New("invalid_sidecar")
			}
			decoded.add(g)
			if have.contains(g) {
				continue
			}
			novel = append(novel, tx)
			have.add(g)
			if tx.OriginalCommitTimestamp > last {
				last = tx.OriginalCommitTimestamp
			}
		}
		if !decoded.contains(set) || !set.contains(decoded) {
			return nil, 0, errors.New("invalid_sidecar")
		}
		base.add(set)
		if len(novel) > 0 {
			l.meta.Transactions = novel
			trusted = append(trusted, l)
		}
	}
	return trusted, last, nil
}

func selectBinlogs(logs []archivedBinlog, backup myspec.BackupResult, targetGTID string, targetTime string) ([]archivedBinlog, string, error) {
	trusted, last, err := trustedReplayHistory(logs, backup)
	if err != nil {
		return nil, "", err
	}
	desired, err := parseRecoveryGTIDs(targetGTID)
	if err != nil {
		return nil, "", err
	}
	included, _ := parseRecoveryGTIDs(backup.GTIDSet)
	if targetGTID != "" && !desired.contains(included) {
		return nil, "", errors.New("target_before_backup")
	}
	var target time.Time
	if targetTime != "" {
		target, err = time.Parse(time.RFC3339Nano, targetTime)
		if err != nil || target.Before(backup.EndTime) {
			return nil, "", errors.New("invalid_target_time")
		}
		if target.UnixMicro() > last {
			return nil, "", errors.New("archive_gap_or_untrusted_writer_take_fresh_full")
		}
	}
	selected := make([]archivedBinlog, 0, len(trusted))
	cut := false
	for _, l := range trusted {
		needed := false
		for _, tx := range l.meta.Transactions {
			g, _ := parseRecoveryGTIDs(tx.GTID)
			include := targetGTID == "" && targetTime == "" || targetGTID != "" && desired.contains(g) || targetTime != "" && tx.OriginalCommitTimestamp <= target.UnixMicro()
			if !include {
				cut = true
				continue
			}
			if cut {
				return nil, "", errors.New("target_not_replay_closed")
			}
			included.add(g)
			needed = true
		}
		if needed {
			selected = append(selected, l)
		}
	}
	if targetGTID != "" && (!included.contains(desired) || !desired.contains(included)) {
		return nil, "", errors.New("archive_gap_or_untrusted_writer_take_fresh_full")
	}
	return selected, included.String(), nil
}

type replayFile struct {
	Path      string `json:"path"`
	ObjectKey string `json:"objectKey"`
	SHA256    string `json:"sha256"`
	Size      int64  `json:"size"`
}

type replayLogs struct {
	Files         []replayFile `json:"files"`
	StartPosition uint64       `json:"startPosition,omitempty"`
}

func fetchReplayLogs(ctx context.Context, s myspec.Spec, r myspec.Restore, dir string, backup myspec.BackupResult) (replayLogs, string, error) {
	c, closeClient, err := s3Client(r.Source)
	if err != nil {
		return replayLogs{}, "", err
	}
	defer closeClient()
	logs, err := readReplaySidecars(ctx, c, r.Source)
	if err != nil {
		return replayLogs{}, "", err
	}
	selected, target, err := selectBinlogs(logs, backup, r.TargetGTID, r.TargetTime)
	if err != nil {
		return replayLogs{}, "", err
	}
	startPosition := replayStartPosition(selected, backup)
	files := make([]replayFile, 0, len(selected))
	for i, l := range selected {
		name := filepath.Join(dir, fmt.Sprintf("replay-%06d", i))
		if err = downloadFile(ctx, c, r.Source, l.key, name); err != nil {
			return replayLogs{}, "", err
		}
		hash, size, e := hashFile(name)
		if e != nil || size != l.meta.Size || hash != l.meta.SHA256 {
			return replayLogs{}, "", errors.New("archive_checksum")
		}
		files = append(files, replayFile{Path: name, ObjectKey: l.key, SHA256: l.meta.SHA256, Size: l.meta.Size})
	}
	return replayLogs{Files: files, StartPosition: startPosition}, target, nil
}

// snapshotLoggedGTIDs derives the logged part of the full's executed set from
// its exact source/file/position. MySQL Previous_gtids accumulates logged GTIDs,
// not all executed GTIDs: provisioning can leave history only in gtid_purged.
// Without the anchor, retain the conservative requirement for the complete base.
func snapshotLoggedGTIDs(l archivedBinlog, backup myspec.BackupResult, base recoveryGTIDs) (recoveryGTIDs, error) {
	if l.meta.ServerUUID != backup.SourceUUID || l.meta.File != backup.BinlogFile {
		return base, nil
	}
	logged, err := parseRecoveryGTIDs(l.meta.PreviousGTIDSet)
	if err != nil {
		return nil, err
	}
	all, err := parseRecoveryGTIDs(l.meta.GTIDSet)
	if err != nil {
		return nil, err
	}
	decoded := recoveryGTIDs{}
	for _, tx := range l.meta.Transactions {
		g, err := parseRecoveryGTIDs(tx.GTID)
		if err != nil || !all.contains(g) || tx.EndPosition == 0 {
			return nil, errors.New("invalid_sidecar")
		}
		decoded.add(g)
		if tx.EndPosition <= backup.BinlogPosition {
			logged.add(g)
		}
	}
	if !decoded.contains(all) || !all.contains(decoded) {
		return nil, errors.New("invalid_sidecar")
	}
	required := recoveryGTIDs{}
	for sid, ranges := range base {
		for _, r := range ranges {
			for _, l := range logged[sid] {
				lo, hi := max(r.lo, l.lo), min(r.hi, l.hi)
				if lo <= hi {
					required[sid] = append(required[sid], gtidInterval{lo, hi})
				}
			}
		}
	}
	required.normalize()
	return required, nil
}

// anchorReplayLog excludes pre-snapshot original-source bytes, including GTIDs
// absent from the backup due to a previous divergent mapping. GTID subtraction
// alone cannot distinguish such stale aliases from new transactions.
func anchorReplayLog(l archivedBinlog, backup myspec.BackupResult, base recoveryGTIDs) (archivedBinlog, bool, error) {
	if !myspec.ValidUUID(l.meta.ServerUUID) || !binlogName.MatchString(l.meta.File) {
		return l, false, nil
	}
	if l.meta.ServerUUID != backup.SourceUUID {
		return l, false, nil
	}
	prev, err := parseRecoveryGTIDs(l.meta.PreviousGTIDSet)
	if err != nil {
		return l, false, err
	}
	prefix, ordinal, _ := strings.Cut(l.meta.File, ".")
	basePrefix, baseOrdinal, _ := strings.Cut(backup.BinlogFile, ".")
	sequence, err := strconv.ParseUint(ordinal, 10, 64)
	if err != nil {
		return l, false, err
	}
	snapshotSequence, err := strconv.ParseUint(baseOrdinal, 10, 64)
	if err != nil {
		return l, false, err
	}
	if prefix != basePrefix || sequence < snapshotSequence {
		return l, false, nil
	}
	if sequence > snapshotSequence {
		return l, prev.contains(base), nil
	}
	all, err := parseRecoveryGTIDs(l.meta.GTIDSet)
	if err != nil {
		return l, false, err
	}
	tail := recoveryGTIDs{}
	transactions := make([]binlogTransaction, 0, len(l.meta.Transactions))
	for _, tx := range l.meta.Transactions {
		g, err := parseRecoveryGTIDs(tx.GTID)
		if err != nil || !all.contains(g) || tx.EndPosition == 0 {
			return l, false, errors.New("invalid_sidecar")
		}
		if tx.EndPosition <= backup.BinlogPosition {
			continue
		}
		tail.add(g)
		transactions = append(transactions, tx)
	}
	l.meta.PreviousGTIDSet = backup.GTIDSet
	l.meta.GTIDSet = tail.String()
	l.meta.Transactions = transactions
	return l, len(transactions) > 0, nil
}

// mysqlbinlog applies --start-position to its first input file only. Keep the
// original snapshot file first and preserve all other selected file ordering.
func replayStartPosition(selected []archivedBinlog, backup myspec.BackupResult) uint64 {
	for i, l := range selected {
		if l.meta.ServerUUID == backup.SourceUUID && l.meta.File == backup.BinlogFile {
			copy(selected[1:i+1], selected[:i])
			selected[0] = l
			return backup.BinlogPosition
		}
	}
	return 0
}

func verifyReplayFiles(logs replayLogs, dir string) error {
	for _, file := range logs.Files {
		if filepath.Dir(file.Path) != dir || !strings.HasPrefix(filepath.Base(file.Path), "replay-") {
			return errors.New("replay_plan_invalid")
		}
		hash, size, err := hashFile(file.Path)
		if err != nil || hash != file.SHA256 || size != file.Size {
			return errors.New("replay_plan_changed")
		}
	}
	return nil
}
