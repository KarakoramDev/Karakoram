// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/redact"
	mysqldriver "github.com/go-sql-driver/mysql"
)

const groupDiagnosticBytes = 1024

var groupLogRecordStart = regexp.MustCompile(`(?m)^[0-9]{4}-[0-9]{2}-[0-9]{2}[T ][0-9]{2}:[0-9]{2}:[0-9]{2}`)
var groupLogErrorCode = regexp.MustCompile(`\[MY-[0-9]{6}\]`)

// Treat each native message/record atomically. Historical credentials need not
// match the current spec, and a continuation may contain only the secret value.
func sensitiveGroupRecord(text string) bool {
	lower := strings.ToLower(text)
	for _, marker := range []string{"source_", "master_", "password", "passwd", "pwd", "identified", "key", "cert", "-----", "authorization", "bearer"} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	words := strings.Join(strings.Fields(strings.NewReplacer("`", "", "'", "", "\"", "").Replace(lower)), " ")
	return strings.Contains(words, "change replication") || strings.Contains(words, "change master")
}

// Group diagnostics cross into CP events. Redact before bounding: truncating a
// quoted credential first can otherwise leave an unrecognizable secret suffix.
func sanitizeGroupDiagnostic(s myspec.Spec, text string) string {
	if sensitiveGroupRecord(text) {
		return "[sensitive diagnostic omitted]"
	}
	for _, credential := range []myspec.Credentials{s.Superuser, s.Agent, s.BackupUser, s.Replication, s.Monitor, s.Admin} {
		if credential.Password != "" {
			text = strings.ReplaceAll(text, groupQuote(credential.Password), "'[redacted]'")
			text = strings.ReplaceAll(text, credential.Password, "[redacted]")
		}
	}
	omitValue := func(value string) {
		if value == "" {
			return
		}
		text = strings.ReplaceAll(text, value, "[redacted]")
		for _, line := range strings.Split(value, "\n") {
			if len(line) > 16 {
				text = strings.ReplaceAll(text, line, "[redacted]")
			}
		}
	}
	if s.ServerCert != nil {
		omitValue(s.ServerCert.KeyPEM)
	}
	if s.Galera != nil && s.Galera.TrafficCert != nil {
		omitValue(s.Galera.TrafficCert.KeyPEM)
	}
	if s.Backup != nil {
		omitValue(s.Backup.Repository.S3.SecretKey)
	}
	if s.Restore != nil {
		omitValue(s.Restore.Source.S3.SecretKey)
		for _, user := range s.Restore.RecoveryUsers {
			omitValue(user.Current)
			omitValue(user.Secondary)
		}
	}
	if s.Proxy != nil {
		omitValue(s.Proxy.Admin.Password)
		omitValue(s.Proxy.Monitor.Password)
		for _, user := range s.Proxy.Users {
			omitValue(user.Password)
		}
	}
	text = redact.Diagnostic(text)
	var safe strings.Builder
	for _, line := range strings.Split(strings.TrimSpace(text), "\n") {
		// Sensitive records were omitted as a whole before redaction.
		if safe.Len() > 0 {
			safe.WriteString("; ")
		}
		safe.WriteString(line)
	}
	return strings.TrimSpace(strings.Map(func(r rune) rune {
		if r < ' ' || r == 127 {
			return ' '
		}
		return r
	}, safe.String()))
}

func boundGroupDiagnostic(text string, size int) string {
	if len(text) > size {
		return strings.ToValidUTF8(text[:size], "")
	}
	return text
}

// Read only the bounded end of the managed error file, never the datadir or
// credential-bearing startup SQL. Log lines are explicitly labeled historical;
// they are evidence, not proof of which statement failed in this request.
func recentGroupErrors(d GroupDeps) string {
	name := filepath.Join(d.GroupReceiptsDir(), "mysql-error.log")
	info, err := os.Lstat(name)
	if err != nil || !info.Mode().IsRegular() {
		return ""
	}
	f, err := os.Open(name)
	if err != nil {
		return ""
	}
	defer f.Close()
	const window = int64(16 << 10)
	offset := max(int64(0), info.Size()-window)
	data, err := io.ReadAll(io.NewSectionReader(f, offset, window))
	if err != nil {
		return ""
	}
	text := string(data)
	// A bounded window may begin in any continuation, even at a line boundary.
	// Only timestamp-delimited records are eligible; never classify fragments.
	starts := groupLogRecordStart.FindAllStringIndex(text, -1)
	var previous, latest string
	for i, start := range starts {
		end := len(text)
		if i+1 < len(starts) {
			end = starts[i+1][0]
		}
		record := text[start[0]:end]
		if !strings.HasSuffix(record, "\n") || sensitiveGroupRecord(record) {
			continue
		}
		header, _, _ := strings.Cut(record, "\n")
		lower := strings.ToLower(header)
		subsystem := strings.Contains(lower, "[repl]") || strings.Contains(lower, "[gcs]") || strings.Contains(lower, "[xcom]") || strings.Contains(lower, "[recovery]") || strings.Contains(lower, "[group_replication]")
		if !subsystem || !groupLogErrorCode.MatchString(header) || !(strings.Contains(lower, "group_replication") || strings.Contains(lower, "[gcs]") || strings.Contains(lower, "xcom") || strings.Contains(lower, "recovery")) {
			continue
		}
		// Sanitize whole records chronologically, before retaining/reordering.
		safe := sanitizeGroupDiagnostic(d.GroupSpec(), record)
		previous, latest = latest, safe
	}
	if previous == "" {
		return latest
	}
	return latest + "; " + previous
}

func groupActionDiagnostic(d GroupDeps, action string, err error) string {
	var native *mysqldriver.MySQLError
	message := err.Error()
	prefix := "MySQL " + action + " failed: "
	if errors.As(err, &native) {
		prefix += fmt.Sprintf("MySQL %d: ", native.Number)
		message = native.Message
	}
	message = prefix + boundGroupDiagnostic(sanitizeGroupDiagnostic(d.GroupSpec(), message), 512)
	if tail := recentGroupErrors(d); tail != "" {
		message += "; recent GR log (may predate request): " + tail
	}
	return boundGroupDiagnostic(message, groupDiagnosticBytes)
}
