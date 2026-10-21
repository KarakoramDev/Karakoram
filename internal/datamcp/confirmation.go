package datamcp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type confirmationState struct {
	Binding ConfirmRequest `json:"binding"`
	Expires int64          `json:"expires"`
	Nonce   string         `json:"nonce"`
}

func (g *Gateway) confirm(req *mcp.CallToolRequest, binding ConfirmRequest) (*mcp.CallToolResult, bool, error) {
	ip := req.Session.InitializeParams()
	if ip == nil || ip.Capabilities.Elicitation == nil {
		return nil, false, errors.New("elicitation unavailable")
	}
	if caps := ip.Capabilities.Elicitation; caps.Form == nil && caps.URL != nil {
		return nil, false, errors.New("form elicitation unavailable")
	}
	p := req.Params
	if p.RequestState == "" {
		if len(p.InputResponses) != 0 {
			return nil, false, errors.New("unbound confirmation")
		}
		state := confirmationState{Binding: binding, Expires: g.o.Now().Add(g.o.Limits.CallTimeout).UnixNano(), Nonce: nonce()}
		data, _ := json.Marshal(state)
		mac := hmac.New(sha256.New, g.secret[:])
		mac.Write(data)
		signed := base64.RawURLEncoding.EncodeToString(data) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
		g.mu.Lock()
		g.continuations[state.Nonce] = time.Unix(0, state.Expires)
		g.mu.Unlock()
		return &mcp.CallToolResult{RequestState: signed, InputRequests: mcp.InputRequestMap{"confirm": &mcp.ElicitParams{Mode: "form", Message: binding.Summary, RequestedSchema: &jsonschema.Schema{Type: "object", Properties: map[string]*jsonschema.Schema{"confirm": {Type: "boolean", Description: "Approve this data modification"}}, Required: []string{"confirm"}}}}}, false, nil
	}
	parts := strings.Split(p.RequestState, ".")
	if len(parts) != 2 {
		return nil, false, errors.New("invalid confirmation")
	}
	data, e := base64.RawURLEncoding.DecodeString(parts[0])
	if e != nil {
		return nil, false, e
	}
	sig, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil {
		return nil, false, e
	}
	mac := hmac.New(sha256.New, g.secret[:])
	mac.Write(data)
	if !hmac.Equal(sig, mac.Sum(nil)) {
		return nil, false, errors.New("invalid confirmation signature")
	}
	var state confirmationState
	if json.Unmarshal(data, &state) != nil || state.Binding != binding || g.o.Now().UnixNano() >= state.Expires {
		return nil, false, errors.New("expired or mismatched confirmation")
	}
	g.mu.Lock()
	_, ok := g.continuations[state.Nonce]
	delete(g.continuations, state.Nonce)
	g.mu.Unlock()
	if !ok {
		return nil, false, errors.New("confirmation already used")
	}
	response, ok := p.InputResponses["confirm"].(*mcp.ElicitResult)
	if !ok || response.Action != "accept" || response.Content["confirm"] != true {
		return nil, false, errors.New("confirmation declined")
	}
	return nil, true, nil
}
