// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/xdg-go/scram"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Exercise localMongo and the real driver encoding: checking the command before
// RunCommand would miss the deadline-derived maxTimeMS appended by the driver.
func TestMongoRouterHealthWireDeadlineHasNoDuplicateFields(t *testing.T) {
	credential := agentapi.Credentials{Username: "clusterMonitor", Password: "test-password"}
	client, err := scram.SHA256.NewClient(credential.Username, credential.Password, "")
	if err != nil {
		t.Fatal(err)
	}
	stored := client.GetStoredCredentials(scram.KeyFactors{Salt: "router-wire-test", Iters: 4096})
	auth, err := scram.SHA256.NewServer(func(string) (scram.StoredCredentials, error) { return stored, nil })
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var workers sync.WaitGroup
	var reads atomic.Int64
	workers.Add(1)
	go func() {
		defer workers.Done()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
				conversation := auth.NewConversation()
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
					command := bson.Raw(body[5:])
					if op == 2004 {
						end := bytes.IndexByte(body[4:], 0)
						command = bson.Raw(body[4+end+1+8:])
					}
					elements, err := command.Elements()
					if err != nil || len(elements) == 0 {
						t.Error("invalid command", err)
						return
					}
					response := bson.M{"ok": 1}
					switch elements[0].Key() {
					case "isMaster", "ismaster", "hello":
						response = bson.M{"ok": 1, "ismaster": true, "msg": "isdbgrid", "minWireVersion": 0, "maxWireVersion": 25, "maxBsonObjectSize": 16777216, "maxMessageSizeBytes": 48000000, "maxWriteBatchSize": 100000, "saslSupportedMechs": bson.A{"SCRAM-SHA-256"}}
					case "saslStart", "saslContinue":
						_, payload := command.Lookup("payload").Binary()
						answer, err := conversation.Step(string(payload))
						if err != nil {
							t.Error(err)
							return
						}
						response = bson.M{"ok": 1, "conversationId": 1, "done": conversation.Done(), "payload": bson.Binary{Data: []byte(answer)}}
					case "find":
						seen := make(map[string]bool, len(elements))
						for _, element := range elements {
							if seen[element.Key()] {
								t.Errorf("duplicate wire field %s", element.Key())
								response = bson.M{"ok": 0, "code": 40413, "codeName": "IDLDuplicateField", "errmsg": "duplicate field"}
							}
							seen[element.Key()] = true
						}
						maxTime, ok := command.Lookup("maxTimeMS").AsInt64OK()
						if !ok || maxTime <= 0 || maxTime > 2000 {
							t.Errorf("unbounded metadata command maxTimeMS: %v", command.Lookup("maxTimeMS"))
						}
						if command.Lookup("find").StringValue() != "shards" || command.Lookup("$db").StringValue() != "config" || command.Lookup("readConcern").Document().Lookup("level").StringValue() != "majority" {
							t.Error("metadata majority probe changed")
						}
						reads.Add(1)
					}
					document, err := bson.Marshal(response)
					if err != nil {
						t.Error(err)
						return
					}
					prefix := 5
					if op == 2004 {
						prefix = 20
					}
					message := make([]byte, 16+prefix+len(document))
					binary.LittleEndian.PutUint32(message, uint32(len(message)))
					copy(message[8:12], header[4:8])
					binary.LittleEndian.PutUint32(message[12:], 2013)
					if op == 2004 {
						binary.LittleEndian.PutUint32(message[12:], 1)
						binary.LittleEndian.PutUint32(message[32:], 1)
					}
					copy(message[16+prefix:], document)
					if _, err := conn.Write(message); err != nil {
						return
					}
				}
			}()
		}
	}()
	a := &nodeAgent{c: agentapi.MemberConfig{Role: agentapi.RoleRouter, Port: listener.Addr().(*net.TCPAddr).Port, TLS: agentapi.TLSConfig{Mode: "disabled"}, SystemUsers: map[string]agentapi.Credentials{"clusterMonitor": credential}}}
	ready, message := a.mongoRouterHealth(t.Context())
	_ = listener.Close()
	workers.Wait()
	if ready == nil || !*ready || message != "" || reads.Load() != 1 {
		t.Fatalf("valid metadata rejected: ready=%v message=%q reads=%d", ready, message, reads.Load())
	}
}
