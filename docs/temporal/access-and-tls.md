<!-- SPDX-License-Identifier: Apache-2.0 -->
<!-- Copyright 2026 The Karakoram Authors -->
# Temporal access and TLS

See the [overview](README.md) for prerequisites and [operations](operations.md) for maintenance. Karakoram project namespaces and Temporal workflow namespaces are separate concepts; `karactl -n NS` selects the former.

## Trust and authorization

Internode traffic always uses mutual TLS with a cluster CA and per-component identities. The frontend always serves TLS on private gRPC port 7233 (HTTP API 7243). `engineOptions.frontend.requireClientAuth` defaults to `true`; setting it to false removes client-certificate authentication, **not** encryption. Generic public frontend exposure is rejected.

Temporal runs with its no-op authorizer and `--allow-no-auth`. A trusted client certificate grants full frontend access, not access restricted to its common name or a workflow namespace. There is **no per-namespace authorization**. Disabling client authentication gives any network-reachable client that access. Private networking and Karakoram management RBAC do not add SDK authorization.

## Issue an SDK client identity

```sh
karactl -n NS temporal client-certificate issue workflows \
  --common-name orders-worker --ttl 720h --out-dir ./client-tls
karactl -n NS temporal client-certificate list workflows
```

Choose exactly one of `--out-dir DIR` or `--stdout`. Directory output writes `<common-name>.crt` and `<common-name>.key` with mode 0600 and `ca.crt` with mode 0644; missing directories are created with mode 0700 and existing output files are never overwritten. `--stdout` emits certificate, private key and CA PEM: protect any redirected output. Serial, expiry, frontend address and TLS server name are printed to stderr. TTL is a Go duration rounded to the nearest hour.

The equivalent API is `POST /api/v1/clusters/{cluster}/temporal/client-certificates?namespace=NS`, body:

```json
{"commonName":"orders-worker","ttlHours":720}
```

It returns **201**, `Cache-Control: no-store`, certificate metadata plus `certificatePem`, `privateKeyPem`, `caPem`, `frontendAddress` and `serverName`. The private key is returned once and not retained. The default lifetime is 720h; bounds are 1–9528 hours (397 days). Common names are DNS-label-like, at most 64 characters. The metadata journal holds at most 4096 certificates.

`GET` on that same route lists metadata only: serial, common name, certificate/CA SHA-256 fingerprints and issue/expiry timestamps. Issuance requires `cluster-credentials:read`; listing requires `clusters:read` on the cluster.
The list API reports server-computed `currentCa` (CLI `CURRENT` yes/no), comparing each issuing CA with the CA used for new client certificates now—old during rotation's trust phase, new once signing switches—even if no certificate has been issued since rotation; false means the certificate will no longer authenticate after pruning.

Configure the SDK with all of:

1. `frontendAddress` as the host:port to dial; application workers need private network reachability to it. The management relay is not an SDK tunnel.
2. TLS `serverName` from the issuance response for hostname verification, rather than assuming the service hostname is the TLS identity.
3. `caPem` as the trusted root and the issued certificate/private-key pair as the client identity when client authentication is required.
4. The intended workflow namespace, for example `orders`.

Do not disable server verification. Store private keys outside source control and logs. OIDC login to the Web UI is not an SDK credential.

## Revocation and rotate-tls

There is no CRL or individual-certificate revocation endpoint. Use `karactl -n NS clusters rotate-tls workflows` to replace the cluster CA and invalidate **all** previously issued client certificates. Rotation includes every component; it cannot target one role/member.

The checkpointed operation rolls in three phases, each using history → matching → frontend → worker → server → ui order (only enabled components):

1. **Trust:** distribute old+new CA trust while retaining old-issued identities.
2. **Issue:** issue new identities while both CAs remain trusted.
3. **Prune:** remove the old CA from trust and finish with the new CA alone.

Old clients may authenticate during the overlap but cease to work after pruning. Arrange replacement client certificates and CA distribution; do not mistake overlap for permanent backward compatibility. Certificate metadata remains in the journal; its CA fingerprint identifies the issuing CA after rotation.

## Web UI and OIDC

The optional `ui` component listens on private HTTP 8080 and connects to the frontend using a client certificate. It is **private by default**, with UI authentication disabled. Anyone able to reach that private UI has its frontend authority.

Public access is backend-managed HTTPS only, with OIDC required. OIDC is supported **only together with public HTTPS**, not on the private HTTP origin. First seal the secret on the existing private cluster:

```sh
karactl -n NS temporal ui-oidc-secret set workflows --from-file oidc-secret
# Or pipe protected secret-manager output:
# secret-manager-command | karactl -n NS temporal ui-oidc-secret set workflows --from-stdin
```

Exactly one input method is required; the CLI removes one trailing LF/CRLF and never prints the secret. API callers use `PUT /api/v1/clusters/{cluster}/temporal/ui/oidc-client-secret?namespace=NS` with `{"clientSecret":"..."}` in a protected request body. The value is write-only, 1–4096 bytes; the response is **204**, `no-store`, and reconciliation is triggered. Secret updates require `clusters:update`.

Then patch options while keeping the existing persistence settings:

```json
{
  "spec": {
    "engineOptions": {
      "ui": {
        "publicHttps": true,
        "oidc": {"issuer": "https://identity.example.com", "clientId": "temporal-ui"}
      }
    }
  }
}
```

Keep at least one UI replica enabled. Backend public HTTPS/subdomain support and the sealed secret are required before the UI can be published. Read the public `ui` endpoint from cluster status and register `<ui-origin>/auth/sso/callback` with the identity provider. Never put `clientSecret` inside `engineOptions`; strict option parsing rejects it. The secret is sealed in control-plane storage and projected into private UI configuration, not status. OIDC gates UI login; it does not add Temporal namespace-level authorization.
