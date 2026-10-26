// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/xdg-go/scram"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestReplacementCleanupRemovesBeforePromotingSevenMemberSet(t *testing.T) {
	p, rc, backend, _ := setup(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rc.Cluster.Spec.Components.Engine.Replicas = 7
	rc.Cluster.Spec.TLS.Mode = "disabled"
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.UID = "replacement-cleanup"
	plan(rc.Cluster, s)
	for _, m := range s.Members {
		m.Stage = "active"
		m.Version = rc.Cluster.Spec.Version
	}
	s.Sets["rs0"].Stage = "ready"
	old := s.Members[0]
	old.Removing = true
	replacement := s.add(rc.Cluster.Name, old.Set, old.Role, old.Shard)
	replacement.Stage = "replacement-caught-up"
	replacement.Version = rc.Cluster.Spec.Version
	s.ReplaceActive = true
	if err := p.ensureSecrets(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	for _, m := range s.Members {
		m.ServiceID, m.VolumeID = m.Name, m.Volume
		backend.services[m.Name] = &infra.Service{ID: m.ServiceID, Hostname: m.Name, Kind: infra.KindRuntime, Status: infra.StatusActive}
		backend.services[m.Volume] = &infra.Service{ID: m.VolumeID, Hostname: m.Volume, Kind: infra.KindVolume, Status: infra.StatusActive}
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}

	server := &replacementMongo{t: t, config: mongoadmin.Config{ID: "rs0", Version: 1}}
	for _, m := range s.Members {
		votes, priority := 1, float64(1)
		if m == replacement {
			votes, priority = 0, 0
		}
		server.config.Members = append(server.config.Members, mongoadmin.ConfigMember{ID: m.Index, Host: m.Name + ":27017", Votes: votes, Priority: priority})
	}
	credentials := map[string]scram.StoredCredentials{}
	for _, user := range []string{"clusterAdmin", "clusterMonitor"} {
		password, err := secret(ctx, rc, "users/"+user)
		if err != nil {
			t.Fatal(err)
		}
		client, err := scram.SHA256.NewClient(user, string(password), "")
		if err != nil {
			t.Fatal(err)
		}
		credentials[user] = client.GetStoredCredentials(scram.KeyFactors{Salt: "replacement-test", Iters: 4096})
	}
	server.auth, err = scram.SHA256.NewServer(func(user string) (scram.StoredCredentials, error) {
		credential, ok := credentials[user]
		if !ok {
			return scram.StoredCredentials{}, fmt.Errorf("unexpected user %q", user)
		}
		return credential, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	rc.Dialer = server

	result, err := p.CancelOperation(ctx, rc, &domain.Operation{Kind: domain.OperationReplaceMember})
	if err != nil || !result.Done {
		t.Fatalf("cleanup: %+v, %v", result, err)
	}
	s, err = load(rc.Cluster)
	if err != nil || s.ReplaceActive || s.find(replacement.Name).Stage != "syncing" {
		t.Fatalf("cleanup did not release replacement: %+v, %v", s, err)
	}
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	s, err = load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if s.find(old.Name).Stage != "removed" || s.find(replacement.Name).Stage != "syncing" {
		t.Fatalf("first Sync did not remove before promotion: %+v", s.Members)
	}
	if !slices.Equal(server.changes, []int{6}) {
		t.Fatalf("first Sync vote changes: %v, want [6]", server.changes)
	}

	// Removal has separate durable route, runtime and volume cleanup steps.
	// None may be starved by promotion, even after the MongoDB member is gone.
	for range 10 {
		if s.find(old.Name) == nil {
			break
		}
		if _, err := p.Sync(ctx, rc); err != nil {
			t.Fatal(err)
		}
		s, err = load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if s.find(replacement.Name).Stage != "syncing" || !slices.Equal(server.changes, []int{6}) {
			t.Fatal("replacement promoted before old-member cleanup finished")
		}
	}
	if s.find(old.Name) != nil {
		t.Fatal("old member cleanup did not finish")
	}
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	s, err = load(rc.Cluster)
	if err != nil || s.find(replacement.Name).Stage != "active" {
		t.Fatalf("next Sync did not promote replacement: %+v, %v", s, err)
	}
	if !slices.Equal(server.changes, []int{6, 7}) {
		t.Fatalf("vote changes: %v, want [6 7]", server.changes)
	}
}

// replacementMongo serves the real driver's handshake/authentication and the
// membership commands used by Sync. It enforces MongoDB's seven-voter ceiling.
type replacementMongo struct {
	t             *testing.T
	mu            sync.Mutex
	auth          *scram.Server
	config        mongoadmin.Config
	changes       []int
	command       func(bson.Raw) bson.M
	observed      *mongoadmin.Status
	handshake     bson.M
	hello         bson.M
	beforeCommand func(bson.Raw)
}

func (s *replacementMongo) DialContext(_ context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	go s.serve(server)
	return client, nil
}

func (s *replacementMongo) serve(conn net.Conn) {
	defer conn.Close()
	conversation := s.auth.NewConversation()
	for {
		header := make([]byte, 16)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		body := make([]byte, int(binary.LittleEndian.Uint32(header))-16)
		if _, err := io.ReadFull(conn, body); err != nil {
			return
		}
		op := binary.LittleEndian.Uint32(header[12:])
		var command bson.Raw
		switch op {
		case 2004: // OP_QUERY is used for the initial handshake.
			end := bytes.IndexByte(body[4:], 0)
			command = bson.Raw(body[4+end+1+8:])
		case 2013: // OP_MSG: flags, single-document section, BSON command.
			command = bson.Raw(body[5:])
		default:
			s.t.Errorf("unexpected MongoDB opcode %d", op)
			return
		}
		elements, err := command.Elements()
		if err != nil {
			s.t.Error(err)
			return
		}
		name := elements[0].Key()
		if s.beforeCommand != nil {
			s.beforeCommand(command)
		}
		response := bson.M{"ok": 1}
		switch name {
		case "isMaster", "ismaster", "hello":
			response = bson.M{"ok": 1, "ismaster": true, "isWritablePrimary": true, "setName": "rs0", "hosts": bson.A{"demom1:27017"}, "primary": "demom1:27017", "minWireVersion": 0, "maxWireVersion": 25, "maxBsonObjectSize": 16777216, "maxMessageSizeBytes": 48000000, "maxWriteBatchSize": 100000, "saslSupportedMechs": bson.A{"SCRAM-SHA-256"}}
			if s.handshake != nil {
				response = s.handshake
			}
			// Explicit writable-primary checks are distinct from discovery:
			// keep handshake authentication/capability negotiation intact.
			if name == "hello" && s.hello != nil {
				response = s.hello
			}
		case "saslStart", "saslContinue":
			_, payload := command.Lookup("payload").Binary()
			answer, err := conversation.Step(string(payload))
			if err != nil {
				s.t.Errorf("SCRAM: %v", err)
				return
			}
			response = bson.M{"ok": 1, "conversationId": 1, "done": conversation.Done(), "payload": bson.Binary{Data: []byte(answer)}}
		case "replSetGetConfig", "replSetGetStatus", "replSetReconfig":
			s.mu.Lock()
			switch name {
			case "replSetGetConfig":
				response["config"] = s.config
			case "replSetGetStatus":
				members := []mongoadmin.Member{}
				for _, m := range s.config.Members {
					state := "SECONDARY"
					if m.Host == "demom1:27017" {
						state = "PRIMARY"
					}
					members = append(members, mongoadmin.Member{ID: m.ID, Name: m.Host, State: state, Health: 1, OptimeDate: time.Now()})
				}
				response["set"], response["members"] = "rs0", members
				if s.observed != nil {
					response["set"], response["members"] = s.observed.Set, s.observed.Members
				}
			case "replSetReconfig":
				var config mongoadmin.Config
				if err := bson.Unmarshal(command.Lookup(name).Document(), &config); err != nil {
					s.t.Error(err)
				}
				votes := 0
				for _, m := range config.Members {
					votes += m.Votes
				}
				s.changes = append(s.changes, votes)
				if votes > 7 {
					response = bson.M{"ok": 0, "code": 93, "errmsg": "at most seven voting members allowed"}
				} else {
					s.config = config
				}
			}
			encoded, err := bson.Marshal(response)
			s.mu.Unlock()
			if err != nil {
				s.t.Error(err)
				return
			}
			if !s.reply(conn, header, op, encoded) {
				return
			}
			continue
		case "ping", "endSessions":
		default:
			if s.command == nil {
				s.t.Errorf("unexpected MongoDB command %q", name)
				return
			}
			response = s.command(command)
		}
		encoded, err := bson.Marshal(response)
		if err != nil {
			s.t.Error(err)
			return
		}
		if !s.reply(conn, header, op, encoded) {
			return
		}
	}
}

func (s *replacementMongo) reply(conn net.Conn, request []byte, op uint32, document []byte) bool {
	prefix := 5
	if op == 2004 {
		prefix = 20
	}
	message := make([]byte, 16+prefix+len(document))
	binary.LittleEndian.PutUint32(message, uint32(len(message)))
	copy(message[8:12], request[4:8])
	if op == 2004 {
		binary.LittleEndian.PutUint32(message[12:], 1) // OP_REPLY
		binary.LittleEndian.PutUint32(message[32:], 1) // numberReturned
	} else {
		binary.LittleEndian.PutUint32(message[12:], 2013) // OP_MSG
	}
	copy(message[16+prefix:], document)
	_, err := conn.Write(message)
	return err == nil
}
