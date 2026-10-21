package agentapi

import (
	"errors"
	"net/http"
)

// StreamingClient returns an independent authenticated transport without a whole
// request or response-header deadline: MCP servers may answer only after a tool
// finishes, so the caller's context bounds each request. TLS trust, namespace
// routing and instance pinning are retained. Only requests to this member's
// exact HTTPS origin may carry its bearer.
func (c *Client) StreamingClient() *http.Client {
	t := c.http.Transport.(*http.Transport).Clone()
	t.ResponseHeaderTimeout = 0
	return &http.Client{Transport: &memberTransport{base: c, transport: t}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

type memberTransport struct {
	base      *Client
	transport *http.Transport
}

func (t *memberTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != t.base.base.Scheme || r.URL.Host != t.base.base.Host || r.URL.User != nil {
		return nil, errors.New("streaming request must target the authenticated member origin")
	}
	clone := r.Clone(r.Context())
	if clone.Header == nil {
		clone.Header = make(http.Header)
	}
	clone.Header.Set("Authorization", "Bearer "+t.base.token)
	return t.transport.RoundTrip(clone)
}
func (t *memberTransport) CloseIdleConnections() { t.transport.CloseIdleConnections() }
