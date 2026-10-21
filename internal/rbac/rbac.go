// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package rbac provides persisted, deny-overrides authorization. Permissions
// include deny rules: consumers must not interpret an allow in isolation.
package rbac

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	"github.com/casbin/casbin/v2/util"
)

const modelText = `[request_definition]
r = sub, res, act, obj
[policy_definition]
p = sub, res, act, obj, eft
[role_definition]
g = _, _
[policy_effect]
e = some(where (p.eft == allow)) && !some(where (p.eft == deny))
[matchers]
m = subjectsMatch(r.sub, p.sub) && globMatch(r.res, p.res) && globMatch(r.act, p.act) && globMatch(r.obj, p.obj)
`

// Enforcer serializes policy replacement with authorization and enumeration.
type Enforcer struct {
	mu    sync.RWMutex
	st    *store.Store
	en    *casbin.Enforcer
	rules []domain.PolicyRule
}

// DefaultPolicy grants the three built-in roles and the bootstrap administrator.
func DefaultPolicy() string {
	var b strings.Builder
	b.WriteString("p, role:admin, *, *, **, allow\ng, user:admin, role:admin\n")
	operator := map[string]bool{}
	for _, r := range []string{"clusters", "backups", "restores", "database-users", "operations", "events", "monitoring"} {
		operator[r] = true
	}
	resources := vocabulary()
	names := make([]string, 0, len(resources))
	for r := range resources {
		names = append(names, r)
	}
	sort.Strings(names)
	for _, r := range names {
		if operator[r] {
			fmt.Fprintf(&b, "p, role:operator, %s, *, **, allow\n", r)
		} else if r == "backup-storages" || r == "providers" || r == "namespaces" {
			fmt.Fprintf(&b, "p, role:operator, %s, read, **, allow\n", r)
		}
		if r != "audit" && r != "cluster-credentials" && r != "accounts" && r != "api-tokens" && r != "rbac" && r != "settings" {
			fmt.Fprintf(&b, "p, role:viewer, %s, read, **, allow\n", r)
		}
	}
	return b.String()
}

// KnownPermission reports whether resource:action belongs to the RBAC
// vocabulary; action "*" matches any action of a known resource.
func KnownPermission(resource, action string) bool {
	actions := vocabulary()[resource]
	return len(actions) > 0 && (action == "*" || actions[action])
}
func vocabulary() map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, r := range api.OperationRBAC {
		if r.Resource == "" {
			continue
		}
		if out[r.Resource] == nil {
			out[r.Resource] = map[string]bool{}
		}
		out[r.Resource][r.Action] = true
	}
	// Export is checked alongside restore/clone permissions, not by a route.
	out["backups"]["export"] = true
	// Data MCP is authorized by the gateway, not an OpenAPI route.
	out["cluster-data"] = map[string]bool{"read": true, "write": true}
	for _, r := range api.AdditionalRBAC {
		if out[r.Resource] == nil {
			out[r.Resource] = map[string]bool{}
		}
		out[r.Resource][r.Action] = true
	}
	return out
}

// normalizeObject preserves legacy cluster globs in the default namespace.
// Namespace rules use bare namespace names/globs; "*" historically meant all.
func normalizeObject(resource, object string) string {
	if object == "*" {
		return "**"
	}
	if resource != "namespaces" && !strings.Contains(object, "/") && !strings.HasPrefix(object, "**") {
		return "default/" + object
	}
	return object
}

func validateObject(object string) error {
	if strings.Count(object, "/") > 1 && !strings.Contains(object, "**") {
		return fmt.Errorf("invalid object glob %q: too many segments", object)
	}
	for segment := range strings.SplitSeq(object, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return fmt.Errorf("invalid object glob %q: invalid segment", object)
		}
	}
	return nil
}
func parse(text string) ([]domain.PolicyRule, error) {
	rd := csv.NewReader(strings.NewReader(text))
	rd.FieldsPerRecord = -1
	rd.TrimLeadingSpace = true
	rd.Comment = '#'
	var rules []domain.PolicyRule
	vocab := vocabulary()
	for {
		row, err := rd.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, domain.InvalidField("policy", domain.ReasonInvalid, "policy CSV is invalid: %v", err)
		}
		for i := range row {
			row[i] = strings.TrimSpace(row[i])
		}
		if len(row) == 0 {
			continue
		}
		line, _ := rd.FieldPos(0)
		invalid := func(message string) error {
			return domain.InvalidField("policy", domain.ReasonInvalid, "policy line %d: %s", line, message)
		}
		switch row[0] {
		case "g":
			if len(row) != 3 || !subject(row[1]) || !strings.HasPrefix(row[2], "role:") || len(row[2]) == 5 {
				return nil, invalid("g rules require g, user/group/role subject, role:NAME")
			}
		case "p":
			if len(row) == 5 {
				row = append(row, "allow")
			}
			if len(row) != 6 || !subject(row[1]) || row[4] == "" || (row[5] != "allow" && row[5] != "deny") {
				return nil, invalid("p rules require p, user/group/role subject, resource, action, object[, allow|deny]")
			}
			row[4] = normalizeObject(row[2], row[4])
			if err := validateObject(row[4]); err != nil {
				return nil, invalid(err.Error())
			}
			for _, pattern := range row[2:5] {
				if _, err := util.GlobMatch("", pattern); err != nil {
					return nil, invalid(fmt.Sprintf("invalid glob %q: %v", pattern, err))
				}
			}
			found := false
			for res, acts := range vocab {
				matches, _ := util.GlobMatch(res, row[2])
				if !matches {
					continue
				}
				for act := range acts {
					matches, _ := util.GlobMatch(act, row[3])
					if matches {
						found = true
					}
				}
			}
			if !found {
				return nil, invalid(fmt.Sprintf("unknown resource/action %q/%q", row[2], row[3]))
			}
		default:
			return nil, invalid(fmt.Sprintf("unknown policy type %q; rules start with p or g", row[0]))
		}
		rules = append(rules, domain.PolicyRule{PType: row[0], Values: append([]string(nil), row[1:]...)})
	}
	return rules, nil
}
func subject(s string) bool {
	for _, p := range []string{"user:", "group:", "role:"} {
		if strings.HasPrefix(s, p) && len(s) > len(p) {
			return true
		}
	}
	return false
}
func build(rules []domain.PolicyRule) (*casbin.Enforcer, error) {
	m, err := model.NewModelFromString(modelText)
	if err != nil {
		return nil, err
	}
	e, err := casbin.NewEnforcer(m)
	if err != nil {
		return nil, err
	}
	e.AddFunction("subjectsMatch", func(args ...interface{}) (interface{}, error) {
		subs, ok := args[0].([]string)
		if !ok {
			return false, nil
		}
		target := args[1].(string)
		for _, s := range subs {
			if s == target {
				return true, nil
			}
			yes, err := e.GetRoleManager().HasLink(s, target)
			if err != nil {
				return false, err
			}
			if yes {
				return true, nil
			}
		}
		return false, nil
	})
	for _, r := range rules {
		var err error
		if r.PType == "g" {
			_, err = e.AddGroupingPolicy(r.Values)
		} else {
			_, err = e.AddPolicy(r.Values)
		}
		if err != nil {
			return nil, err
		}
	}
	return e, nil
}

// New loads and normalizes persisted policies, seeding defaults only when empty.
func New(st *store.Store) (*Enforcer, error) {
	if st == nil {
		return nil, errors.New("store is required")
	}
	ctx := context.Background()
	rules, err := st.ListPolicyRules(ctx)
	if err != nil {
		return nil, err
	}
	persisted := encode(rules)
	text := persisted
	if len(rules) == 0 {
		text = DefaultPolicy()
	}
	// Normalize before validating, just as for API policy replacements.
	rules, err = parse(text)
	if err != nil {
		return nil, err
	}
	en, err := build(rules)
	if err != nil {
		return nil, err
	}
	if normalized := encode(rules); normalized != persisted {
		if err := st.ReplacePolicyRules(ctx, rules); err != nil {
			return nil, err
		}
	}
	return &Enforcer{st: st, en: en, rules: rules}, nil
}

// Can evaluates all identity subjects together so any matching deny wins.
func (e *Enforcer) Can(subjects []string, resource, action, object string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	yes, err := e.en.Enforce(subjects, resource, action, object)
	return err == nil && yes
}

// Permissions returns deduplicated implicit allow and deny permissions for UI use.
func (e *Enforcer) Permissions(subjects []string) []domain.PolicyRule {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var out []domain.PolicyRule
	seen := map[string]bool{}
	for _, s := range subjects {
		rows, err := e.en.GetImplicitPermissionsForUser(s)
		if err != nil {
			continue
		}
		for _, r := range rows {
			r = append([]string(nil), r...)
			r[0] = s
			k := strings.Join(r, "\x00")
			if !seen[k] {
				seen[k] = true
				out = append(out, domain.PolicyRule{PType: "p", Values: r})
			}
		}
	}
	return out
}
func encode(rules []domain.PolicyRule) string {
	var b strings.Builder
	w := csv.NewWriter(&b)
	for _, r := range rules {
		_ = w.Write(append([]string{r.PType}, r.Values...))
	}
	w.Flush()
	return b.String()
}

// Policy returns normalized canonical CSV including explicit effects.
func (e *Enforcer) Policy() string { e.mu.RLock(); defer e.mu.RUnlock(); return encode(e.rules) }

// ReplacePolicy normalizes, validates and persists before swapping the live model.
// At least one existing enabled account or verified OIDC group must retain
// unrestricted administrative rights.
func (e *Enforcer) ReplacePolicy(ctx context.Context, text string) error {
	rules, err := parse(text)
	if err != nil {
		return err
	}
	en, err := build(rules)
	if err != nil {
		return err
	}
	accounts, err := e.st.ListAccounts(ctx)
	if err != nil {
		return err
	}
	sessions, err := e.st.ListSessions(ctx)
	if err != nil {
		return err
	}
	candidates := map[string][]string{}
	for _, a := range accounts {
		if a.Disabled {
			continue
		}
		user := "user:" + a.Name
		subjects := []string{user}
		for _, role := range a.Roles {
			if !strings.HasPrefix(role, "role:") {
				role = "role:" + role
			}
			subjects = append(subjects, role)
		}
		candidates[user] = subjects
		if a.OIDC {
			for _, session := range sessions {
				if session.Owner != a.Name || session.Revoked || !session.ExpiresAt.After(time.Now()) || session.CreatedAt.Before(a.PasswordChangedAt) || session.CreatedAt.Before(a.CreatedAt) {
					continue
				}
				identity := append(append([]string(nil), subjects...), session.Subjects...)
				for _, sub := range session.Subjects {
					if strings.HasPrefix(sub, "group:") {
						candidates[sub] = identity
					}
				}
			}
		}
	}
	admin := false
	for s, subjects := range candidates {
		// The retained subject must itself be granted admin access; account roles
		// and verified group subjects must not introduce a deny for that identity.
		unrestricted := false
		rows, err := en.GetImplicitPermissionsForUser(s)
		if err != nil {
			return err
		}
		for _, r := range rows {
			if (r[1] == "*" || r[1] == "**") && (r[2] == "*" || r[2] == "**") &&
				(r[3] == "**" || r[3] == "*") && r[4] == "allow" {
				unrestricted = true
				break
			}
		}
		if !unrestricted {
			continue
		}
		yes, err := en.Enforce([]string{s}, "*", "*", "*")
		if err != nil || !yes {
			continue
		}
		yes, err = en.Enforce(subjects, "*", "*", "*")
		if err != nil || !yes {
			continue
		}
		denied := false
		for _, subject := range subjects {
			rows, _ := en.GetImplicitPermissionsForUser(subject)
			for _, r := range rows {
				if r[4] == "deny" {
					denied = true
				}
			}
		}
		if !denied {
			admin = true
			break
		}
	}
	if !admin {
		return domain.InvalidField("policy", domain.ReasonProtected, "policy must preserve an unrestricted administrator: retain full admin rights for at least one enabled account or a verified active OIDC group identity, without deny rules")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err = e.st.ReplacePolicyRules(ctx, rules); err != nil {
		return err
	}
	e.en = en
	e.rules = rules
	return nil
}
