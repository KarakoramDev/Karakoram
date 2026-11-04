// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"crypto/x509"
	"net/url"
	"reflect"
	"strings"
)

func httpsAddress(s string) bool {
	u, e := url.Parse(s)
	return e == nil && u.Scheme == "https" && u.Host != "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

// Seal and storage migrations are deliberately unsupported. Credentials may rotate.
func validateSealAndStorage(old, n *Options) error {
	switch n.Seal.Type {
	case "shamir":
		if n.Seal.Transit != nil || n.Seal.AWSKMS != nil {
			return invalid("shamir does not accept auto-unseal options")
		}
	case "transit":
		t := n.Seal.Transit
		if t == nil || n.Seal.AWSKMS != nil || !httpsAddress(t.Address) || strings.TrimSpace(t.KeyName) == "" {
			return invalid("transit requires an https address and keyName")
		}
		if t.MountPath == "" {
			t.MountPath = "transit/"
		}
		if strings.HasPrefix(t.MountPath, "/") || strings.Contains(t.MountPath, "..") {
			return invalid("invalid transit mountPath")
		}
		if t.CAPEM != "" && !x509.NewCertPool().AppendCertsFromPEM([]byte(t.CAPEM)) {
			return invalid("invalid transit CA PEM")
		}
	case "awskms":
		a := n.Seal.AWSKMS
		if a == nil || n.Seal.Transit != nil || strings.TrimSpace(a.Region) == "" || strings.TrimSpace(a.KMSKeyID) == "" {
			return invalid("awskms requires region and kmsKeyId")
		}
		if a.Endpoint != "" && !httpsAddress(a.Endpoint) {
			return invalid("awskms endpoint must use https")
		}
	default:
		return invalid("seal type must be shamir, transit or awskms")
	}
	switch n.Storage.Type {
	case "raft":
		if n.Storage.PostgreSQL != nil {
			return invalid("raft does not accept postgresql options")
		}
	case "postgresql":
		if n.Storage.PostgreSQL == nil || strings.TrimSpace(n.Storage.PostgreSQL.Cluster) == "" {
			return invalid("postgresql storage requires cluster")
		}
	default:
		return invalid("storage type must be raft or postgresql")
	}
	if old != nil {
		a, b := old.Seal, n.Seal
		if a.Transit != nil {
			v := *a.Transit
			v.Token = ""
			if v.MountPath == "" {
				v.MountPath = "transit/"
			}
			a.Transit = &v
		}
		if b.Transit != nil {
			v := *b.Transit
			v.Token = ""
			b.Transit = &v
		}
		if a.AWSKMS != nil {
			v := *a.AWSKMS
			v.AccessKeyID = ""
			v.SecretAccessKey = ""
			a.AWSKMS = &v
		}
		if b.AWSKMS != nil {
			v := *b.AWSKMS
			v.AccessKeyID = ""
			v.SecretAccessKey = ""
			b.AWSKMS = &v
		}
		if !reflect.DeepEqual(a, b) || !reflect.DeepEqual(old.Storage, n.Storage) {
			return invalid("seal and storage are immutable")
		}
	}
	return nil
}
