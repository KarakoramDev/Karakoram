// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq/rmqspec"
)

const (
	maxStoredQuorumBlockers   = 50
	quorumBlockedRequeueAfter = 30 * time.Second
)

var quorumMemberPattern = regexp.MustCompile(`^rabbit@[a-z][a-z0-9]{1,24}$`)

func validateQuorumBlockers(blockers []rmqspec.QuorumBlocker) error {
	for _, blocker := range blockers {
		for _, identity := range []string{blocker.VHost, blocker.Queue} {
			if identity == "" || len(identity) > 255 || !utf8.ValidString(identity) || strings.IndexFunc(identity, unicode.IsControl) >= 0 {
				return fmt.Errorf("invalid quorum blocker identity")
			}
		}
		switch blocker.Type {
		case "quorum", "stream", "process":
		default:
			return fmt.Errorf("invalid quorum blocker type")
		}
		for _, node := range blocker.Members {
			if !quorumMemberPattern.MatchString(node) {
				return fmt.Errorf("invalid quorum blocker member")
			}
		}
	}
	return nil
}

func quorumBlockerCount(m *member) int {
	return max(m.QuorumBlockerCount, len(m.QuorumBlockers))
}

func warningEvent(ctx context.Context, rc *engine.Context, reason, message string) error {
	if rc.Events == nil {
		return nil
	}
	return rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: reason, Message: message, CreatedAt: now(rc)})
}

func clearQuorumBlockers(ctx context.Context, rc *engine.Context, m *member) error {
	// An empty diagnosis after a failed wait does not mean the rolling change
	// can continue. Announce recovery only at the final pre-drain safety gate.
	if len(m.QuorumBlockers) > 0 && rc.Events != nil && m.ChangeStep == "critical" {
		if err := rc.Events.Record(ctx, domain.Event{
			Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Type: "Normal",
			Reason: "RollingRestartRecovered", Message: fmt.Sprintf("Queue quorum recovered on %s; continuing the rolling change.", m.Name), CreatedAt: now(rc),
		}); err != nil {
			return err
		}
	}
	m.QuorumBlockers = nil
	m.QuorumBlockerCount = 0
	return nil
}

// checkQuorum distinguishes a known safety wait from a failed observation.
// The caller must stay at the safety gate while blocked, without failing reconciliation.
func (p *Provider) checkQuorum(ctx context.Context, rc *engine.Context, m *member) (blocked bool, err error) {
	var out rmqspec.QuorumCriticalResponse
	if err := p.call(ctx, rc, m.Name, "quorum-critical", rmqspec.EmptyRequest{}, &out); err != nil {
		return false, err
	}
	if len(out.Blockers) == 0 {
		return false, clearQuorumBlockers(ctx, rc, m)
	}
	if err := validateQuorumBlockers(out.Blockers); err != nil {
		return false, err
	}
	// The native health check supplies identities but not membership in 4.3.
	// A failed inventory must not hide the already-known restart blockers.
	if client, err := p.adminClient(ctx, rc); err == nil {
		queues, err := client.Queues(ctx)
		client.Close()
		if err == nil {
			for i := range out.Blockers {
				for _, queue := range queues {
					if queue.Vhost == out.Blockers[i].VHost && queue.Name == out.Blockers[i].Queue {
						for _, node := range queue.Members {
							if !quorumMemberPattern.MatchString(node) {
								return false, fmt.Errorf("invalid quorum blocker member")
							}
						}
						out.Blockers[i].Members = slices.Clone(queue.Members)
						break
					}
				}
			}
		}
	}
	slices.SortFunc(out.Blockers, func(a, b rmqspec.QuorumBlocker) int {
		if order := strings.Compare(a.VHost, b.VHost); order != 0 {
			return order
		}
		if order := strings.Compare(a.Queue, b.Queue); order != 0 {
			return order
		}
		return strings.Compare(a.Type, b.Type)
	})
	previous := m.QuorumBlockers
	previousCount := m.QuorumBlockerCount
	m.QuorumBlockerCount = len(out.Blockers)
	m.QuorumBlockers = slices.Clone(out.Blockers[:min(len(out.Blockers), maxStoredQuorumBlockers)])
	for i := range m.QuorumBlockers {
		slices.Sort(m.QuorumBlockers[i].Members)
	}
	message := quorumMessage(m)
	if previousCount != m.QuorumBlockerCount || !slices.EqualFunc(previous, m.QuorumBlockers, func(a, b rmqspec.QuorumBlocker) bool {
		return a.VHost == b.VHost && a.Queue == b.Queue && a.Type == b.Type
	}) {
		if err := warningEvent(ctx, rc, "RollingRestartBlocked", message); err != nil {
			m.QuorumBlockers = previous
			m.QuorumBlockerCount = previousCount
			return false, err
		}
	}
	return true, nil
}

func quorumMessage(m *member) string {
	var message strings.Builder
	fmt.Fprintf(&message, "Rolling restart of %s is blocked by quorum-critical queues/components: ", m.Name)
	for i, queue := range m.QuorumBlockers {
		if i == 10 {
			fmt.Fprintf(&message, "; and %d more", quorumBlockerCount(m)-i)
			break
		}
		if i > 0 {
			message.WriteString(", ")
		}
		fmt.Fprintf(&message, "%q in vhost %q (%s; members: %s)", queue.Queue, queue.VHost, queue.Type, strings.Join(queue.Members, ", "))
	}
	message.WriteString(". Recover offline replicas or explicitly grow quorum queue replicas using the Management API; see cluster detail → Queue recovery. Do not force a restart.")
	return message.String()
}

func projectQuorumStatus(rc *engine.Context, s *internalState, status *domain.ClusterStatus) {
	status.Conditions = slices.DeleteFunc(status.Conditions, func(condition domain.Condition) bool {
		return condition.Type == "QuorumCriticalQueues" || condition.Type == "QuorumReplicaRecovery"
	})
	blocked := false
	for _, m := range ordered(s) {
		if len(m.QuorumBlockers) == 0 {
			continue
		}
		blocked = true
		message := quorumMessage(m)
		status.Conditions = append(status.Conditions,
			domain.Condition{Type: "QuorumCriticalQueues", Status: "True", Reason: "RollingRestartBlocked", Message: message, LastTransitionTime: now(rc)},
			domain.Condition{Type: "QuorumReplicaRecovery", Status: "True", Reason: "ManagementAPI", Message: quorumRecovery(rc, s, m), LastTransitionTime: now(rc)})
		status.Message = message
	}
	if !blocked && strings.HasPrefix(status.Message, "Rolling restart of ") {
		status.Message = "Queue quorum recovered; continuing the rolling change."
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

// Commands run from inside the project, without host-shell or cookie access.
// Values are encoded as URL path segments, JSON and shell arguments separately.
func quorumRecovery(rc *engine.Context, s *internalState, m *member) string {
	scheme, port, tls := "https", "15671", " --cacert ca.pem"
	mode := m.TLSMode
	if mode == "" {
		mode = rc.Cluster.Spec.TLS.Mode
	}
	if mode == "disabled" {
		scheme, port, tls = "http", "15672", ""
	}
	base := scheme + "://" + m.Name + ":" + port + "/api/queues/"
	curl := "curl --fail-with-body --silent --show-error --user admin" + tls
	var result strings.Builder
	fmt.Fprintf(&result, "karactl -n %s clusters ca %s > ca.pem\n", shellQuote(rc.Cluster.Ref().Namespace), shellQuote(rc.Cluster.Name))
	result.WriteString("Run inside this cluster's project. Reveal the admin password in Connection details; curl prompts for it (never put it in history). Export the cluster CA to ca.pem first. Inspect the queue type, members and online majority before adding a replica. Add one at a time; wait for the new replica to become a synchronized voting member before continuing. Never force membership after majority loss.\n")
	for i, queue := range m.QuorumBlockers {
		if i == 10 {
			fmt.Fprintf(&result, "\n%d more blockers: inspect the Management UI queue inventory.\n", quorumBlockerCount(m)-i)
			break
		}
		fmt.Fprintf(&result, "\n%s in vhost %s (%s):\n", queue.Queue, queue.VHost, queue.Type)
		if queue.Type != "quorum" {
			result.WriteString("Recover the existing stream/metadata majority; quorum queue replica additions do not repair this component.\n")
			continue
		}
		path := url.PathEscape(queue.VHost) + "/" + url.PathEscape(queue.Queue)
		fmt.Fprintf(&result, "%s %s\n", curl, shellQuote(base+path))
		if len(queue.Members) == 0 {
			result.WriteString("Queue membership unavailable: recover Management API access before choosing targets.\n")
			continue
		}
		count := len(queue.Members)
		for _, target := range s.runningMembers() {
			if count >= 3 {
				break
			}
			node := "rabbit@" + target.Name
			if slices.Contains(queue.Members, node) {
				continue
			}
			body, _ := json.Marshal(struct {
				Node string `json:"node"`
			}{node})
			fmt.Fprintf(&result, "%s -X POST -H 'Content-Type: application/json' --data %s %s\n", curl, shellQuote(string(body)), shellQuote(base+"quorum/"+path+"/replicas/add"))
			fmt.Fprintf(&result, "# Wait for voting membership and replica catch-up before the next POST:\n%s %s\n", curl, shellQuote(base+"quorum/"+path+"/status"))
			count++
		}
		result.WriteString("Confirm each target is online and not already a member before POST. Inspect membership after each addition; HTTP success alone is not synchronization verification. If every broker is already a replica, recover the offline replicas rather than adding duplicates.\n")
	}
	return result.String()
}

func memberStage(s *internalState, m *member, agentReady bool) string {
	switch m.Stage {
	case "planned":
		return "PLANNED: waiting for platform import"
	case "imported":
		return "IMPORTED: waiting for package deployment"
	case "deployed":
		if !agentReady {
			return "DEPLOYED: waiting for agent and broker readiness"
		}
		if !s.Bootstrapped {
			return "AGENT_READY: bootstrapping first broker"
		}
		return "AGENT_READY: joining RabbitMQ cluster"
	case "resume-deployed":
		return "DEPLOYED: starting retained broker"
	case "resume-revive":
		return "AGENT_READY: reviving retained broker"
	default:
		return strings.ToUpper(m.Stage)
	}
}
