// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/redact"
	mysqldriver "github.com/go-sql-driver/mysql"
)

const groupDiagnosticBytes = 1024

// Group diagnostics cross into CP events. Redact before bounding: truncating a
// quoted credential first can otherwise leave an unrecognizable secret suffix.
func sanitizeGroupDiagnostic(s myspec.Spec, text string) string {
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
	for _, line := range strings.Split(text, "\n") {
		lower := strings.ToLower(line)
		// Never emit SQL credential statements or PEM material, even when a
		// native error quotes them without a recognizable key/value delimiter.
		if strings.Contains(lower, "identified by") || strings.Contains(lower, "private key") || strings.Contains(lower, "authorization") || strings.Contains(lower, "bearer ") || strings.Contains(lower, "start group_replication user=") || strings.Contains(lower, "-----begin") || strings.Contains(lower, "-----end") {
			line = "[sensitive diagnostic omitted]"
		}
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
	if offset > 0 {
		_, text, _ = strings.Cut(text, "\n") // Discard a possibly partial record.
	}
	var previous, latest string
	for _, line := range strings.Split(text, "\n") {
		lower := strings.ToLower(line)
		if strings.Contains(lower, "group_replication") || strings.Contains(lower, "[gcs]") || strings.Contains(lower, "xcom") {
			previous, latest = latest, line
		}
	}
	if latest == "" {
		return ""
	}
	return sanitizeGroupDiagnostic(d.GroupSpec(), strings.TrimSpace(latest+"\n"+previous))
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
