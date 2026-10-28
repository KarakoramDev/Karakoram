# Notifications

Notification channels deliver operational events to generic JSON webhooks, [Slack](https://slack.com/) incoming webhooks, or email (1–20 recipients). Channels are installation-wide configuration: listing/getting requires `settings:read`; create, replace, delete and test require `settings:update` on `*`. Existing roles are not granted new permissions on upgrade.

This Ops feature is **verified offline** (tests without a live provider); delivery semantics below describe the implemented contract.

## Configure channels

```sh
karactl notification-channels list
karactl notification-channels get incidents -o yaml
karactl notification-channels create --name incidents --type webhook \
  --url https://alerts.example.com/karakoram --category backup-failed \
  --category cluster-degraded --namespace production --signing-secret-stdin < signing-secret
karactl notification-channels test incidents
karactl notification-channels replace incidents -f channel.yaml
karactl notification-channels delete incidents --yes
```

`create` and `replace NAME` accept `-f FILE` JSON/YAML (or `-f -` for stdin) instead of body flags. Protect files containing secrets. Flags default `--enabled=true`; pass `--enabled=false` to disable. Repeat `--category` and `--namespace` for filters. Empty categories mean all categories; empty namespaces mean all namespaces **including installation-wide events**. On create/replace, `--namespace` is a repeatable channel filter, not the global API namespace selector.

Slack uses `--type slack --webhook-url-stdin`, reading the entire secret incoming-webhook URL from stdin. Email uses `--type email --smtp-host HOST --smtp-port 587 --security starttls --username USER --password-stdin --from alerts@example.com --to oncall@example.com`; repeat `--to`. Use a protected file body when multiple secret fields must be supplied. Never put secret Slack URLs, SMTP passwords or signing keys in shell arguments/history. If a generic webhook URL includes credentials, supply it in a protected file rather than `--url`.

URLs, signing keys and passwords are write-only and sealed with per-channel/field authenticated identities. Responses contain configured indicators (and only the generic webhook host), never stored secrets. Replacing a channel keeps omitted secrets only when the destination identity is unchanged: URL host, or SMTP host + port + username. Supply new credentials when changing destination. Replacement replaces filters and other configuration; it is not a patch.

In the UI, changing an SMTP host, port or username clears any entered password; changing a webhook host or explicit port clears any entered signing secret. Re-enter credentials for the new destination. Hostname case and surrounding form whitespace do not change identity; webhook path/query edits keep the entered signing secret. Explicit webhook ports remain part of identity, including default ports such as `:443`. Switching channel type clears entered URLs and credentials.

Delivery and test requests load the destination and sealed credentials from a single stored-record snapshot, so concurrent replacement cannot pair one destination with another version's credentials. Concurrent deletion discards pending delivery work without stopping notification processing for other channels.

Default table output summarizes name, type, enabled state, categories, host and delivery status. JSON/YAML modes expose the API's sanitized configuration/status. `list --limit N --cursor CURSOR` follows paginated inventories; JSON/YAML output preserves `nextCursor` for the next request. Slack has no public host field, so its host column is `-`. `test` reports `Delivered` and `Error`; a test is an immediate delivery check, not proof that future deliveries will succeed.

Test webhook payloads use the normal `operation-failed` category with `severity: info` and `reason: NotificationTest`, keeping the published payload schema valid without reporting a real failed operation. A test bypasses the channel's event filters and does not create an incident in the durable event stream.
Each test delivery has a fresh negative, non-durable event ID in both the payload
and `X-Karakoram-Event-Id`. These IDs do not overlap positive durable event IDs;
receivers can apply the same ID-based deduplication without discarding later tests.

## Categories and triggers

| Category | Triggering events |
| --- | --- |
| `backup-failed` | `BackupFailed`, `BackupScheduleFailed` (and failed backup-target operations) |
| `cluster-degraded` | `ClusterDegraded` on entry into Degraded/Error |
| `cluster-recovered` | `ClusterRecovered` on recovery from Degraded/Error |
| `volume-deletion-refused` | `VolumeDeletionRefused`, `OrphanedVolumeDeleteStuck`, `OrphanedVolumeRecordRemoved` (ownership safety refusal, not deletion success) |
| `certificate-expiring` | `CertificateExpiring`: certificates within 30 days of expiry or expired, daily deduplicated; database, internal/server and relay certificates |
| `deployment-retries-exhausted` | `DeploymentRetriesExhausted` |
| `operation-failed` | `OperationFailed` for non-backup operations |
| `members-co-located` | `MembersCoLocated` on co-location transition |
| `backup-drill-failed` | `BackupDrillFailed` |
| `control-plane-backup-failed` | `ControlPlaneBackupFailed` (installation-wide) |
| `agents-outdated` | `AgentsOutdated`, once per cluster and target build |

Certificate inventory is scanned on startup and hourly. Certificates expiring at or before 30 days, including already expired certificates, generate at most one warning per certificate identity per UTC day; this deduplication survives restarts. Coverage includes engine CA, member and agent certificates, supplied TLS certificate conditions, the internal CA/server certificate, relay CA/hub certificates and namespace relay client certificates. Certificate material, private keys and free-form TLS condition messages are never copied into these notifications. Public HTTPS certificates terminated outside Karakoram are not part of this inventory.

## Webhook payload and verification

Generic webhooks POST JSON with this schema (empty namespace/cluster denote installation-wide scope):

```json
{
  "type": "object",
  "required": ["id", "category", "severity", "namespace", "cluster", "reason", "message", "createdAt", "source"],
  "properties": {
    "id": {"type": "integer", "format": "int64"},
    "category": {"type": "string", "enum": ["backup-failed", "cluster-degraded", "cluster-recovered", "volume-deletion-refused", "certificate-expiring", "deployment-retries-exhausted", "operation-failed", "members-co-located", "backup-drill-failed", "control-plane-backup-failed", "agents-outdated"]},
    "severity": {"enum": ["warning", "info"]},
    "namespace": {"type": "string"},
    "cluster": {"type": "string"},
    "reason": {"type": "string"},
    "message": {"type": "string"},
    "createdAt": {"type": "string", "format": "date-time"},
    "source": {"const": "karakoram"}
  }
}
```

Every webhook includes `X-Karakoram-Event-Id`; persist this ID to deduplicate accepted events. With a signing secret configured, `X-Karakoram-Signature: t=<unix>,v1=<hex>` carries HMAC-SHA256 over the exact bytes `<t>.<body>`. Verify before JSON decoding; do not reserialize the body. Example [Python](https://www.python.org/) receiver helper:

```python
import hashlib
import hmac
import time

def verify(raw_body: bytes, header: str, secret: bytes) -> bool:
    try:
        fields = dict(part.split("=", 1) for part in header.split(","))
        timestamp = fields["t"]
        # Bound replay time; also deduplicate X-Karakoram-Event-Id durably.
        if abs(time.time() - int(timestamp)) > 300:
            return False
        expected = hmac.new(secret, timestamp.encode("ascii") + b"." + raw_body,
                            hashlib.sha256).hexdigest()
        return hmac.compare_digest(expected, fields["v1"])
    except (KeyError, ValueError, UnicodeError):
        return False
```

Slack receives an incoming-webhook `{ "text": "..." }` message rather than the generic JSON schema. Email renders the notification as a message delivered using SMTP.

## Delivery and outbound safety

Delivery is **at least once**, not exactly once. A durable cursor and outbox survive control-plane restarts; a crash after remote acceptance but before acknowledgement can duplicate delivery. Workers retry after 30 seconds, doubling to a maximum one hour, and stop after 24 hours. Channels expose `lastDeliveryAt`, `lastErrorAt`, sanitized `lastError`, `pending` and `failed` counters. Investigate failures and destination availability; a successful test does not replay failed deliveries.

Webhook/Slack destinations require HTTPS unless explicitly allowlisted through `KARAKORAM_NOTIFY_ENDPOINT_ALLOWLIST`. Private addresses are refused unless explicitly allowed; metadata and other unsafe addresses remain forbidden. DNS is checked when dialing to prevent rebinding, proxies are disabled, redirects are refused, requests time out after ten seconds, and response draining is bounded to 4 KiB. Prefer public HTTPS endpoints; allowlisting is an administrator-controlled exception, not a way to disable TLS verification.

The allowlist is a comma-separated list of exact `host` or `host:port` entries. Bare hostnames permit ports 80/443 only; specify an explicit port for SMTP or nonstandard HTTP endpoints. Notification allowlisting can admit private/loopback destinations, but never link-local, cloud metadata or CGNAT addresses. Redirects and non-2xx HTTP responses fail delivery.

Email supports required STARTTLS (`--security starttls`, typically port 587) or implicit TLS (`--security tls`, typically port 465). TLS 1.2 or newer and certificate verification are required; PLAIN authentication occurs only over TLS. There is no plaintext SMTP fallback. SMTP destinations are subject to the outbound address guard too.
