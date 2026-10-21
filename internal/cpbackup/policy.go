// SPDX-License-Identifier: Apache-2.0
package cpbackup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/robfig/cron/v3"
	"strings"
)

var parser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

const PolicyKey = "controlPlaneBackup"

func Defaults(p domain.ControlPlaneBackupPolicy) domain.ControlPlaneBackupPolicy {
	if p.Schedule == "" {
		p.Schedule = "0 3 * * *"
	}
	if p.RetainCount == 0 {
		p.RetainCount = 14
	}
	p.Storage.Namespace = domain.NormalizedNamespace(p.Storage.Namespace)
	return p
}
func Policy(ctx context.Context, st *store.Store) (domain.ControlPlaneBackupPolicy, error) {
	var p domain.ControlPlaneBackupPolicy
	raw, err := st.GetSetting(ctx, PolicyKey)
	if errors.Is(err, store.ErrNotFound) {
		return Defaults(p), nil
	}
	if err != nil {
		return p, err
	}
	err = json.Unmarshal(raw, &p)
	return Defaults(p), err
}
func Validate(ctx context.Context, st *store.Store, p domain.ControlPlaneBackupPolicy) error {
	if len(strings.Fields(p.Schedule)) != 5 {
		return errors.New("schedule must be a five-field UTC cron expression")
	}
	if _, err := parser.Parse(p.Schedule); err != nil {
		return errors.New("schedule must be a valid five-field UTC cron expression")
	}
	if p.RetainCount < 1 || p.RetainCount > 1000 {
		return errors.New("retainCount must be between 1 and 1000")
	}
	b, err := st.GetBackupStorage(ctx, p.Storage.Namespace, p.Storage.Name)
	if errors.Is(err, store.ErrNotFound) {
		return errors.New("storage must reference an existing S3 backup storage")
	}
	if err != nil {
		return fmt.Errorf("load backup storage: %w", err)
	}
	if b.Type != "s3" {
		return errors.New("storage must be type s3")
	}
	return nil
}
