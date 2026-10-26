// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agent"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

var backupActionMu sync.Mutex

// backupDiagnostic retains only bounded native codes and a capacity flag, never
// free text (which can contain signed object URLs, responses and credentials).
type backupDiagnostic struct {
	line        [4096]byte
	used        int
	code        string
	httpStatus  string
	lastHTTP5xx string
	s3Code      string
	capacity    bool
}

var backupErrorCode = regexp.MustCompile(`(?:ERROR|P[0-9]+ ERROR): \[([0-9]{3})\]:`)
var backupHTTPStatus = regexp.MustCompile(`HTTP request failed with ([0-9]{3})(?:[^0-9]|$)`)
var backupS3Code = regexp.MustCompile(`<Code>([A-Za-z0-9]{1,64})</Code>`)
var backupCapacity = regexp.MustCompile(`(?i)quota exceeded|no space left on device|XMinioStorageFull|QuotaExceeded|storage full|disk full`)
var backupHTTPCapacity = regexp.MustCompile(`(?i)HTTP (507)(?:[^0-9]|$)`)
var backupHTTPServiceError = regexp.MustCompile(`(?:^|[ \t])(?:ERROR: \[101\]: |\[ServiceError\] (?:on retry at [0-9]+|on [0-9]+ retries from [0-9]+-[0-9]+)ms: )(?:raised from local-[0-9]{1,10} protocol: )?\[(5[0-9]{2}|408|429)\]([ \t]|$)`)

func (d *backupDiagnostic) Write(data []byte) (int, error) {
	for _, c := range data {
		if c == '\n' {
			d.consume(true)
			d.used = 0
		} else {
			if d.used == len(d.line) {
				d.consume(false)
				// Preserve enough overlap for any accepted code/indicator split
				// across chunks of an oversized physical line.
				d.used = copy(d.line[:128], d.line[len(d.line)-128:])
			}
			d.line[d.used] = c
			d.used++
		}
	}
	return len(data), nil
}

func (d *backupDiagnostic) observeHTTPStatus(status string) {
	d.httpStatus = status
	if status[0] == '5' {
		d.lastHTTP5xx = status
	}
	d.capacity = d.capacity || status == "507"
}

func (d *backupDiagnostic) consume(endOfLine bool) {
	line := d.line[:d.used]
	if match := backupErrorCode.FindSubmatch(line); match != nil {
		d.code = string(match[1])
	}
	// A scan window is not a physical line ending: reject a numeric token
	// ending at its edge until overlap scanning sees a delimiter or real EOF.
	if match := backupHTTPStatus.FindSubmatchIndex(line); match != nil && (endOfLine || match[3] < len(line)) {
		d.observeHTTPStatus(string(line[match[2]:match[3]]))
	}
	// Retryable HTTP responses use ServiceError [101], not the ProtocolError
	// "HTTP request failed" format. Only retain statuses actually emitted.
	if match := backupHTTPServiceError.FindSubmatchIndex(line); match != nil && (endOfLine || match[4] < match[5]) {
		d.observeHTTPStatus(string(line[match[2]:match[3]]))
	}
	if match := backupS3Code.FindSubmatch(line); match != nil {
		// Syntax alone is not a secret boundary: servers or headers can put
		// arbitrary credential-like alphanumerics inside a Code element.
		switch string(match[1]) {
		case "InternalError":
			d.s3Code = "InternalError"
		case "AccessDenied":
			d.s3Code = "AccessDenied"
		case "InvalidAccessKeyId":
			d.s3Code = "InvalidAccessKeyId"
		case "SignatureDoesNotMatch":
			d.s3Code = "SignatureDoesNotMatch"
		case "NoSuchBucket":
			d.s3Code = "NoSuchBucket"
		case "NoSuchUpload":
			d.s3Code = "NoSuchUpload"
		case "NoSuchKey":
			d.s3Code = "NoSuchKey"
		case "SlowDown":
			d.s3Code = "SlowDown"
		case "ServiceUnavailable":
			d.s3Code = "ServiceUnavailable"
		case "XMinioStorageFull":
			d.s3Code = "XMinioStorageFull"
		case "QuotaExceeded":
			d.s3Code = "QuotaExceeded"
		case "EntityTooLarge":
			d.s3Code = "EntityTooLarge"
		case "RequestTimeout":
			d.s3Code = "RequestTimeout"
		default:
			d.s3Code = "unrecognized S3 error"
		}
	}
	if match := backupHTTPCapacity.FindSubmatchIndex(line); match != nil && (endOfLine || match[3] < len(line)) {
		d.observeHTTPStatus("507")
		d.capacity = true
	}
	d.capacity = d.capacity || d.httpStatus == "507" || backupCapacity.Match(line)
}

func (d *backupDiagnostic) failure(err error) string {
	d.consume(true)
	if d.code == "082" {
		return "pgBackRest error [082]: required WAL segment was not archived before the archive timeout; inspect target archiving and repository reachability"
	}
	prefix := "pgBackRest backup"
	if d.code != "" {
		prefix = "pgBackRest error [" + d.code + "]"
	}
	if d.httpStatus != "" || d.s3Code != "" {
		details := ""
		if d.httpStatus != "" {
			details = "HTTP " + d.httpStatus
		}
		if d.lastHTTP5xx != "" && d.lastHTTP5xx != d.httpStatus {
			details += ", last observed HTTP 5xx " + d.lastHTTP5xx
		}
		if d.s3Code != "" {
			if details != "" {
				details += ", "
			}
			if d.s3Code == "unrecognized S3 error" {
				details += d.s3Code
			} else {
				details += "S3 " + d.s3Code
			}
		}
		message := prefix + ": backup repository request failed (" + details + "); "
		if d.capacity {
			return message + "repository storage is full or over quota — free or extend repository storage, then run a new backup"
		}
		if d.s3Code == "NoSuchUpload" {
			return message + "repository multipart upload is missing; check capacity, upload lifecycle and concurrent cleanup, then run a new backup"
		}
		if d.s3Code == "NoSuchKey" {
			return message + "repository object is missing; check capacity, object lifecycle and concurrent cleanup, then run a new backup"
		}
		return message + "check repository credentials, permissions, health and capacity"
	}
	if d.capacity {
		return prefix + ": member backup spool/disk is full — free or extend member storage, then run a new backup"
	}
	if d.code != "" {
		return "pgBackRest error [" + d.code + "]: native backup command failed; inspect member diagnostics"
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		if status, ok := exit.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			return fmt.Sprintf("pgBackRest backup terminated by signal %s; inspect member process and memory events", status.Signal())
		}
		return fmt.Sprintf("pgBackRest backup exited with status %d; no native error code was captured", exit.ExitCode())
	}
	return "pgBackRest backup could not complete; no native error code was captured"
}

var backupLabel = regexp.MustCompile(`^[0-9]{8}-[0-9]{6}F(_[0-9]{8}-[0-9]{6}[DI])?$`)

// durableJSON publishes a complete owner-only receipt before acknowledging work.
func durableJSON(name string, value any) error {
	if err := privateDir(filepath.Dir(name)); err != nil {
		return err
	}
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(name), ".receipt-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), name); err != nil {
		return err
	}
	d, err := os.Open(filepath.Dir(name))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
func receiptPath(p paths, kind, id string) (string, error) {
	if id == "" || len(id) > 256 || !safeValue(id, 256) {
		return "", errors.New("durable operation receipt required")
	}
	sum := sha256.Sum256([]byte(id))
	return filepath.Join(p.root, "receipts", kind, fmt.Sprintf("%x.json", sum)), nil
}
func pgpass(s pgspec.Spec, p paths) error {
	esc := strings.NewReplacer(`\`, `\\`, ":", `\:`)
	text := "*:5432:*:" + esc.Replace(s.Superuser.Username) + ":" + esc.Replace(s.Superuser.Password) + "\n"
	return os.WriteFile(filepath.Join(p.root, "backup.pgpass"), []byte(text), 0600)
}
func backupCommand(ctx context.Context, s pgspec.Spec, p paths, args ...string) ([]byte, error) {
	if err := pgpass(s, p); err != nil {
		return nil, err
	}
	cmd := exec.CommandContext(ctx, "/usr/bin/pgbackrest", args...)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C.UTF-8", "LC_ALL=C.UTF-8", "PGPASSFILE=" + filepath.Join(p.root, "backup.pgpass")}
	// Native diagnostics may include object endpoints. Never return them through
	// HTTP, events or receipts; expose only the command's exit status.
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("pgBackRest command failed: %w", err)
	}
	return out, nil
}
func actionRepository(s pgspec.Spec, p paths, override *pgspec.Repository) (pgspec.Repository, []string, error) {
	var repo pgspec.Repository
	config := "pgbackrest.conf"
	if override != nil {
		repo = *override
		if err := validateRepository(repo); err != nil {
			return repo, nil, err
		}
		config = "pgbackrest-action.conf"
		if err := os.WriteFile(filepath.Join(p.root, config), []byte(renderRepository(repo, p, "action-s3-ca.pem", s.Superuser.Username)), 0600); err != nil {
			return repo, nil, err
		}
		if repo.S3.CAPEM != "" {
			if err := os.WriteFile(filepath.Join(p.root, "action-s3-ca.pem"), []byte(repo.S3.CAPEM), 0600); err != nil {
				return repo, nil, err
			}
		}
	} else if s.Backup != nil {
		repo = s.Backup.Repository
	} else {
		return repo, nil, errors.New("backup repository is not configured")
	}
	return repo, []string{"--config=" + filepath.Join(p.root, config), "--stanza=" + repo.Stanza}, nil
}

type infoStanza struct {
	Name string `json:"name"`
	DB   []struct {
		ID       int         `json:"id"`
		SystemID json.Number `json:"system-id"`
		Version  string      `json:"version"`
	} `json:"db"`
	Archive []struct {
		Min string `json:"min"`
		Max string `json:"max"`
	} `json:"archive"`
	Backup []struct {
		Label      string            `json:"label"`
		Type       string            `json:"type"`
		Prior      string            `json:"prior"`
		Reference  []string          `json:"reference"`
		Annotation map[string]string `json:"annotation"`
		Database   struct {
			ID int `json:"id"`
		} `json:"database"`
		Timestamp struct {
			Start int64 `json:"start"`
			Stop  int64 `json:"stop"`
		} `json:"timestamp"`
		Archive struct {
			Start string `json:"start"`
			Stop  string `json:"stop"`
		} `json:"archive"`
		Info struct {
			Size       int64 `json:"size"`
			Repository struct {
				Size int64 `json:"size"`
			} `json:"repository"`
		} `json:"info"`
	} `json:"backup"`
}

func projectBackupInfo(data []byte, stanza string) (pgspec.BackupInfoResponse, error) {
	var raw []infoStanza
	if err := json.Unmarshal(data, &raw); err != nil {
		return pgspec.BackupInfoResponse{}, fmt.Errorf("decode pgBackRest inventory: %w", err)
	}
	out := pgspec.BackupInfoResponse{Backups: []pgspec.BackupInfo{}}
	for _, s := range raw {
		if s.Name != stanza {
			continue
		}
		if len(s.DB) > 0 {
			out.SystemID = s.DB[len(s.DB)-1].SystemID.String()
			out.Version = s.DB[len(s.DB)-1].Version
		}
		for _, a := range s.Archive {
			if out.ArchiveMin == "" || a.Min < out.ArchiveMin {
				out.ArchiveMin = a.Min
			}
			if a.Max > out.ArchiveMax {
				out.ArchiveMax = a.Max
			}
		}
		for _, b := range s.Backup {
			item := pgspec.BackupInfo{
				Label: b.Label, Type: b.Type, Prior: b.Prior, References: b.Reference,
				StartedAt: time.Unix(b.Timestamp.Start, 0).UTC().Format(time.RFC3339Nano),
				StoppedAt: time.Unix(b.Timestamp.Stop, 0).UTC().Format(time.RFC3339Nano),
				WALStart:  b.Archive.Start, WALStop: b.Archive.Stop,
				SizeBytes: b.Info.Size, RepoBytes: b.Info.Repository.Size,
				Annotation: b.Annotation,
			}
			for _, db := range s.DB {
				if db.ID == b.Database.ID {
					item.SystemID = db.SystemID.String()
					item.Version = db.Version
				}
			}
			out.Backups = append(out.Backups, item)
		}
		return out, nil
	}
	return out, nil
}
func backupInfo(ctx context.Context, s pgspec.Spec, p paths, repo *pgspec.Repository) (pgspec.BackupInfoResponse, error) {
	r, args, err := actionRepository(s, p, repo)
	if err != nil {
		return pgspec.BackupInfoResponse{}, err
	}
	data, err := backupCommand(ctx, s, p, append(args, "--output=json", "info")...)
	if err != nil {
		return pgspec.BackupInfoResponse{}, err
	}
	return projectBackupInfo(data, r.Stanza)
}

type backupReceipt struct {
	RequestHash string                `json:"requestHash"`
	Response    pgspec.BackupResponse `json:"response"`
	PID         int                   `json:"pid,omitempty"`
	Timeline    int                   `json:"timeline,omitempty"`
}

var activeBackups sync.Map

func (e *Runner) backup(ctx context.Context, s pgspec.Spec, p paths, r pgspec.BackupRequest) (pgspec.BackupResponse, error) {
	if r.Type != "full" && r.Type != "diff" && r.Type != "incr" {
		return pgspec.BackupResponse{}, errors.New("backup type must be full, diff or incr")
	}
	name, err := receiptPath(p, "backup", r.Receipt)
	if err != nil {
		return pgspec.BackupResponse{}, err
	}
	digest := fmt.Sprintf("%x", hash(r))
	var receipt backupReceipt
	data, err := os.ReadFile(name)
	if err == nil {
		if err = json.Unmarshal(data, &receipt); err != nil {
			return receipt.Response, errors.New("invalid backup receipt")
		}
		if receipt.RequestHash != digest {
			return receipt.Response, errors.New("backup receipt belongs to another request")
		}
		if receipt.Response.State == "running" {
			if _, ok := activeBackups.Load(name); !ok {
				// A process surviving an agent restart must finish before the receipt can
				// be classified. Never start a replacement session on another timeline.
				if receipt.PID > 0 && syscall.Kill(receipt.PID, 0) == nil {
					return receipt.Response, nil
				}
				inv, e := backupInfo(ctx, s, p, r.Repository)
				if e != nil {
					return receipt.Response, e
				}
				receipt.Response = pgspec.BackupResponse{State: "failed", Message: "backup interrupted before a durable completion receipt"}
				for _, b := range inv.Backups {
					if b.Annotation["karakoram-operation"] == r.Receipt {
						receipt.Response = pgspec.BackupResponse{State: "done", Label: b.Label}
					}
				}
				if err = durableJSON(name, receipt); err != nil {
					return receipt.Response, err
				}
			}
		}
		return receipt.Response, nil
	}
	if !os.IsNotExist(err) {
		return receipt.Response, err
	}
	st := e.status(ctx, s, p)
	if !st.Running || (st.Role != "primary" && st.Role != "standalone") {
		return receipt.Response, errors.New("backup requires the current primary")
	}
	_, args, err := actionRepository(s, p, r.Repository)
	if err != nil {
		return receipt.Response, err
	}
	if err = pgpass(s, p); err != nil {
		return receipt.Response, err
	}
	args = append(args, "--type="+r.Type, "--annotation=karakoram-operation="+r.Receipt, "backup")
	receipt = backupReceipt{RequestHash: digest, Response: pgspec.BackupResponse{State: "running"}, Timeline: st.Timeline}
	if err = durableJSON(name, receipt); err != nil {
		return receipt.Response, err
	}
	cmd := exec.Command("/usr/bin/pgbackrest", args...)
	cmd.Env = []string{"PATH=/usr/bin:/bin", "LANG=C.UTF-8", "LC_ALL=C.UTF-8", "PGPASSFILE=" + filepath.Join(p.root, "backup.pgpass")}
	var diagnostic backupDiagnostic
	cmd.Stdout = &diagnostic
	cmd.Stderr = &diagnostic
	if err = cmd.Start(); err != nil {
		receipt.Response = pgspec.BackupResponse{State: "failed", Message: "pgBackRest backup could not start"}
		return receipt.Response, durableJSON(name, receipt)
	}
	receipt.PID = cmd.Process.Pid
	activeBackups.Store(name, true)
	if err = durableJSON(name, receipt); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		activeBackups.Delete(name)
		return receipt.Response, err
	}
	go func() {
		err := cmd.Wait()
		backupActionMu.Lock()
		defer backupActionMu.Unlock()
		defer activeBackups.Delete(name)
		receipt.Response = pgspec.BackupResponse{State: "failed", Message: diagnostic.failure(err)}
		finish, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		current := e.status(finish, s, p)
		if err == nil {
			receipt.Response.Message = "pgBackRest exited successfully but completion inventory or primary/timeline validation failed"
		}
		if err == nil && current.Running && (current.Role == "primary" || current.Role == "standalone") && current.Timeline == receipt.Timeline {
			inv, infoErr := backupInfo(finish, s, p, r.Repository)
			if infoErr == nil {
				for _, b := range inv.Backups {
					if b.Annotation["karakoram-operation"] == r.Receipt {
						receipt.Response = pgspec.BackupResponse{State: "done", Label: b.Label}
					}
				}
			}
		}
		_ = durableJSON(name, receipt)
	}()
	return receipt.Response, nil
}
func (e *Runner) backupRoutes(mux *http.ServeMux, n *agent.Node) {
	for _, action := range []string{"backup", "backup-info", "expire", "stanza-create", "stanza-upgrade", "restore", "prepare-restore", "finish-restore", "rekey"} {
		mux.HandleFunc("POST /v1/postgres/"+action, func(w http.ResponseWriter, r *http.Request) {
			var raw json.RawMessage
			if err := decodeAction(w, r, &raw); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			var out any = pgspec.ActionResponse{OK: true}
			err := n.WithLock(r.Context(), func(locked *agent.Node) error {
				backupActionMu.Lock()
				defer backupActionMu.Unlock()
				s, err := decodeSpec(locked.Config())
				if err != nil {
					return err
				}
				if s.Role != pgspec.RoleData {
					return errors.New("backup actions require a data member")
				}
				p := dataPaths(locked.DataDir())
				decode := func(v any) error {
					d := json.NewDecoder(strings.NewReader(string(raw)))
					d.DisallowUnknownFields()
					return d.Decode(v)
				}
				switch action {
				case "backup":
					var req pgspec.BackupRequest
					if err = decode(&req); err != nil {
						return err
					}
					response, err := e.backup(r.Context(), s, p, req)
					out = response
					return err
				case "backup-info":
					var req pgspec.RepositoryRequest
					if err = decode(&req); err != nil {
						return err
					}
					response, err := backupInfo(r.Context(), s, p, req.Repository)
					out = response
					return err
				case "expire":
					var req pgspec.ExpireRequest
					if err = decode(&req); err != nil {
						return err
					}
					if !backupLabel.MatchString(req.Set) {
						return errors.New("invalid backup label")
					}
					_, args, err := actionRepository(s, p, req.Repository)
					if err != nil {
						return err
					}
					inventory, err := backupInfo(r.Context(), s, p, req.Repository)
					if err != nil {
						return err
					}
					fullCount := 0
					for _, set := range inventory.Backups {
						if set.Type == "full" && set.Label != req.Set {
							fullCount++
						}
					}
					if fullCount == 0 {
						fullCount = 1
					}
					// Preserve WAL for every surviving full chain, not merely
					// the newest full. Backup-set retention stays explicit.
					args = append(args,
						"--set="+req.Set,
						fmt.Sprintf("--repo1-retention-archive=%d", fullCount),
						"--repo1-retention-archive-type=full", "expire",
					)
					_, err = backupCommand(r.Context(), s, p, args...)
					return err
				case "stanza-create", "stanza-upgrade":
					var req pgspec.RepositoryRequest
					if err = decode(&req); err != nil {
						return err
					}
					_, args, err := actionRepository(s, p, req.Repository)
					if err != nil {
						return err
					}
					_, err = backupCommand(r.Context(), s, p, append(args, action)...)
					return err
				case "rekey":
					var req struct{}
					if err = decode(&req); err != nil {
						return err
					}
					return rekeyRecoveredSystemRoles(r.Context(), s, p)
				case "finish-restore":
					var req struct{}
					if err = decode(&req); err != nil {
						return err
					}
					return finishRestore(r.Context(), locked, s, p)
				case "prepare-restore":
					var req pgspec.PrepareRestoreRequest
					if err = decode(&req); err != nil {
						return err
					}
					e.watermark.reset()
					return prepareRestore(r.Context(), locked, s, p, req)
				case "restore":
					var req pgspec.RestoreRequest
					if err = decode(&req); err != nil {
						return err
					}
					e.watermark.reset()
					return restoreSingle(r.Context(), locked, s, p, req)
				}
				return errors.New("unsupported backup action")
			})
			if err != nil {
				http.Error(w, "PostgreSQL backup action failed", http.StatusConflict)
				return
			}
			actionJSON(w, 200, out)
		})
	}
}
func prepareRestore(ctx context.Context, n *agent.Node, s pgspec.Spec, p paths, r pgspec.PrepareRestoreRequest) error {
	name, err := receiptPath(p, "prepare-restore", r.Receipt)
	if err != nil {
		return err
	}
	if data, err := os.ReadFile(name); err == nil {
		var prior pgspec.PrepareRestoreRequest
		if json.Unmarshal(data, &prior) != nil || prior != r {
			return errors.New("restore preparation receipt mismatch")
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	process := "postgres"
	if s.Topology == pgspec.TopologyHA {
		process = "patroni"
	}
	if err = n.Stop(ctx, process); err != nil {
		return err
	}
	// Paused Patroni can leave PostgreSQL running after its own shutdown.
	if _, err = os.Stat(filepath.Join(p.pgdata, "postmaster.pid")); err == nil {
		if _, err = command(ctx, filepath.Join(s.BinDir, "pg_ctl"), "-D", p.pgdata, "-m", "fast", "-w", "stop"); err != nil {
			return errors.New("PostgreSQL did not stop for restore")
		}
	}
	if r.Wipe {
		if err = os.RemoveAll(p.pgdata); err != nil {
			return err
		}
		if err = privateDir(p.pgdata); err != nil {
			return err
		}
	}
	return durableJSON(name, r)
}
func restoreSingle(ctx context.Context, n *agent.Node, s pgspec.Spec, p paths, r pgspec.RestoreRequest) error {
	if s.Topology != pgspec.TopologySingle {
		return errors.New("direct restore requires single topology")
	}
	name, err := receiptPath(p, "restore", r.Receipt)
	if err != nil {
		return err
	}
	digest := fmt.Sprintf("%x", hash(r))
	if data, err := os.ReadFile(name); err == nil {
		var prior string
		if json.Unmarshal(data, &prior) != nil || prior != digest {
			return errors.New("restore receipt mismatch")
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	states, _ := n.Snapshot()
	if states["postgres"].PID != 0 {
		return errors.New("stop PostgreSQL before restore")
	}
	if err = validateRepository(r.Restore.Source); err != nil {
		return err
	}
	if r.Restore.Set != "" && !backupLabel.MatchString(r.Restore.Set) {
		return errors.New("invalid restore backup label")
	}
	s.Bootstrap = &pgspec.Bootstrap{Method: "restore", Restore: &r.Restore}
	args, err := restoreArgs(s, p)
	if err != nil {
		return err
	}
	if err = os.WriteFile(filepath.Join(p.root, "pgbackrest-restore.conf"), []byte(renderRepository(r.Restore.Source, p, "restore-s3-ca.pem", s.Superuser.Username)), 0600); err != nil {
		return err
	}
	if r.Restore.Source.S3.CAPEM != "" {
		if err = os.WriteFile(filepath.Join(p.root, "restore-s3-ca.pem"), []byte(r.Restore.Source.S3.CAPEM), 0600); err != nil {
			return err
		}
	}
	if _, err = backupCommand(ctx, s, p, args...); err != nil {
		return err
	}
	if err = durableJSON(filepath.Join(p.root, "bootstrap-generation.json"), r.Generation); err != nil {
		return err
	}
	return durableJSON(name, digest)
}

func appliedBootstrapGeneration(p paths) int {
	data, err := os.ReadFile(filepath.Join(p.root, "bootstrap-generation.json"))
	if err != nil {
		return 0
	}
	var generation int
	if json.Unmarshal(data, &generation) != nil {
		return 0
	}
	return generation
}

// cleanupRestoreFiles removes source-repository credentials after the durable
// configuration no longer requests recovery. Future applies cannot recreate them.
func cleanupRestoreFiles(p paths) error {
	for _, name := range []string{"pgbackrest-restore.conf", "restore-s3-ca.pem", "bootstrap-restore.sh"} {
		if err := os.Remove(filepath.Join(p.root, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

func finishRestore(ctx context.Context, n *agent.Node, s pgspec.Spec, p paths) error {
	if s.Backup == nil || (s.Bootstrap != nil && s.Bootstrap.Method == "restore") {
		return errors.New("publish the target repository before enabling clone archiving")
	}
	if err := cleanupRestoreFiles(p); err != nil {
		return err
	}
	// Only the serialized restore cutover may remove action files. Ordinary
	// applies must leave them available to asynchronous target backups.
	for _, name := range []string{"pgbackrest-action.conf", "action-s3-ca.pem"} {
		if err := os.Remove(filepath.Join(p.root, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	connection, err := localConnect(ctx, s, p)
	if err != nil {
		return err
	}
	defer connection.Close(ctx)
	var mode string
	if err = connection.QueryRow(ctx, "SHOW archive_mode").Scan(&mode); err != nil {
		return err
	}
	if mode == "on" {
		return nil
	}
	// pgBackRest --archive-mode=off writes an ALTER SYSTEM override. Merely
	// changing the rendered postgresql.conf would leave that override active.
	if _, err = connection.Exec(ctx, "ALTER SYSTEM RESET archive_mode"); err != nil {
		return errors.New("remove clone bootstrap archive override")
	}
	if s.Topology == pgspec.TopologyHA {
		return patroniRequest(ctx, s, "POST", "/restart", map[string]any{}, nil)
	}
	return n.Action(ctx, "postgres", "restart")
}
