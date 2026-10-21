package companion

import (
	"crypto/tls"
	"crypto/x509"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"time"
)

func certificateTrusted(cert agentapi.Certificate, ca []byte, member string, now time.Time) bool {
	pair, err := tls.X509KeyPair([]byte(cert.CertPEM), []byte(cert.KeyPEM))
	if err != nil {
		return false
	}
	leaf, err := x509.ParseCertificate(pair.Certificate[0])
	if err != nil {
		return false
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return false
	}
	_, err = leaf.Verify(x509.VerifyOptions{Roots: roots, DNSName: member, CurrentTime: now})
	return err == nil
}
