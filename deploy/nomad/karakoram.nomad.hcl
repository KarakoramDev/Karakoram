# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
#
# Experimental single-writer control plane. See docs/nomad.md for client rootfs,
# CNI routing/DNS/portmap, ACL, TLS and namespace provisioning prerequisites.
# Supply non-secret settings through a local -var-file; never put credentials
# in that file or this job. The release archive must contain karakoram at root.
# Register this job and its data volume with an operator token, not the
# restricted KARAKORAM_NOMAD_TOKEN supplied to the running control plane.
#
# Required Nomad Variable: nomad/jobs/karakoram, in the job namespace.
# Environment items: KARAKORAM_MASTER_KEY, KARAKORAM_ADMIN_PASSWORD,
# KARAKORAM_NOMAD_TOKEN. TLS items: NOMAD_CA_PEM, NOMAD_CLIENT_CERT_PEM,
# NOMAD_CLIENT_KEY_PEM. Keep the master key separately for disaster recovery.
#
# Stop and await terminal allocations before registering an updated job. The
# lifetime volume flock is an additional process fence, including node-loss
# reconciliation where Nomad's single-writer volume claim alone is insufficient.

variable "namespace" {
  type    = string
  default = "karakoram"
}

variable "datacenters" {
  type    = list(string)
  default = ["*"]
}

variable "node_pool" {
  type    = string
  default = "default"
}

variable "network" {
  type    = string
  default = "karakoram"
}

variable "volume_name" {
  type    = string
  default = "karakoram-data"
}

variable "public_port" {
  type    = number
  default = 0 # No host publication; route the proxy to the allocation IP.
}

variable "artifact_url" {
  type = string
}

variable "artifact_checksum" {
  type = string
}

variable "native_artifact_mirror" {
  type    = string
  default = ""
}

variable "nomad_address" {
  type = string
}

variable "nomad_tls_server_name" {
  type    = string
  default = "server.global.nomad"
}

variable "region" {
  type    = string
  default = ""
}

variable "public_url" {
  type = string
}

variable "public_ipv4" {
  type    = string
  default = ""
}

variable "public_ipv6" {
  type    = string
  default = ""
}

variable "backup_endpoint_allowlist" {
  type    = string
  default = ""
}

variable "cpu_mhz" {
  type    = number
  default = 1000
}

variable "memory_mb" {
  type    = number
  default = 1024
}

job "karakoram" {
  namespace   = var.namespace
  datacenters = var.datacenters
  node_pool   = var.node_pool
  type        = "service"

  meta = {
    "karakoram.kind" = "control-plane"
  }

  group "control-plane" {
    count = 1

    update {
      max_parallel      = 1
      canary            = 0
      health_check      = "task_states"
      min_healthy_time  = "1s"
      healthy_deadline  = "15m"
      progress_deadline = "20m"
      auto_revert       = false
    }

    restart {
      attempts = 2
      interval = "30m"
      delay    = "15s"
      mode     = "fail"
    }

    # The sticky single-writer volume keeps replacements on the volume's node
    # and the entrypoint's lifetime flock serializes writers, so automatic
    # recovery after a failed task or node restart is safe.
    reschedule {
      unlimited      = true
      delay          = "30s"
      delay_function = "exponential"
      max_delay      = "1h"
    }

    disconnect {
      lost_after = "12h"
      replace    = false
      reconcile  = "keep_original"
    }

    network {
      mode = "cni/${var.network}"
      # DNS must be supplied by the operator's CNI plugin, not a host stub.
      # Private 9443 uses only the routed allocation IP, never a host mapping.
      # Opt-in plaintext HTTP: firewall this host port to only the HTTPS proxy/LB.
      # Route to the owning node: this port is not published on every node.
      dynamic "port" {
        for_each = var.public_port == 0 ? [] : [var.public_port]
        labels   = ["public"]
        content {
          static = port.value
          to     = 8080
        }
      }
    }

    # Group registration precedes task startup, so hosts can resolve immediately.
    # Application readiness must not gate namespace member discovery.
    service {
      name         = "karakoram"
      provider     = "nomad"
      address_mode = "alloc"
      # Numeric alloc ports need no declared network label (Nomad 2.0.7).
      port         = "9443"
    }

    volume "data" {
      type            = "host"
      source          = var.volume_name
      read_only       = false
      sticky          = true
      access_mode     = "single-node-single-writer"
      attachment_mode = "file-system"
    }

    task "server" {
      driver       = "exec"
      user         = "root"
      kill_signal  = "SIGTERM"
      kill_timeout = "120s"

      config {
        command  = "/local/karakoram-entrypoint"
        cap_drop = ["all"]
        cap_add  = ["chown", "dac_override", "fowner", "fsetid", "setgid", "setuid", "setpcap", "sys_chroot", "audit_write"]
      }

      resources {
        cpu    = var.cpu_mhz
        memory = var.memory_mb
      }

      volume_mount {
        volume      = "data"
        destination = "/var/lib/karakoram"
        read_only   = false
      }

      artifact {
        source      = var.artifact_url
        destination = "local/artifact"
        options {
          checksum = var.artifact_checksum
          archive  = "tar.gz"
        }
      }

      env {
        hostname                            = "karakoram"
        KARAKORAM_BACKEND                   = "nomad"
        KARAKORAM_DATA_DIR                  = "/var/lib/karakoram"
        KARAKORAM_LISTEN                    = ":8080"
        KARAKORAM_INTERNAL_LISTEN           = ":9443"
        KARAKORAM_ADVERTISE_URL             = "https://karakoram:9443"
        KARAKORAM_PUBLIC_URL                = var.public_url
        KARAKORAM_NOMAD_ADDR                = var.nomad_address
        KARAKORAM_NOMAD_ALLOWED_ADDRS       = var.nomad_address
        KARAKORAM_NOMAD_NAMESPACE           = var.namespace
        KARAKORAM_NOMAD_REGION              = var.region
        KARAKORAM_NOMAD_DATACENTERS         = join(",", var.datacenters)
        KARAKORAM_NOMAD_NODE_POOL           = var.node_pool
        KARAKORAM_NOMAD_NETWORK             = var.network
        KARAKORAM_NOMAD_PUBLIC_IPV4         = var.public_ipv4
        KARAKORAM_NOMAD_PUBLIC_IPV6         = var.public_ipv6
        KARAKORAM_NOMAD_ARTIFACT_URL        = var.artifact_url
        KARAKORAM_ARTIFACT_MIRROR          = var.native_artifact_mirror
        KARAKORAM_NOMAD_CACERT              = "/secrets/nomad-ca.pem"
        KARAKORAM_NOMAD_CLIENT_CERT         = "/secrets/nomad-client-cert.pem"
        KARAKORAM_NOMAD_CLIENT_KEY          = "/secrets/nomad-client-key.pem"
        KARAKORAM_NOMAD_TLS_SERVER_NAME     = var.nomad_tls_server_name
        KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST = var.backup_endpoint_allowlist
      }

      template {
        destination = "local/karakoram-entrypoint"
        perms       = "0755"
        change_mode = "noop"
        data = <<EOF
#!/bin/sh
set -eu
# Nomad 2.0.7 accepts cap_drop/cap_add but root's kernel bounding set still
# needs this explicit re-exec before any mutable setup.
if [ "$#" -eq 0 ]; then
    exec /usr/bin/setpriv '--bounding-set=-all,+chown,+dac_override,+fowner,+fsetid,+setgid,+setuid,+setpcap,+sys_chroot,+audit_write' "$0" bounded
fi
exec 9> '/var/lib/karakoram/.karakoram-writer.lock'
if ! flock -n 9; then
    printf '%s\n' 'waiting for previous writer' >&2
    flock 9
fi
self_wait=0
while ! getent hosts "$NOMAD_JOB_ID" >/dev/null; do
    if [ "$self_wait" -ge 120 ]; then
        printf '%s\n' 'warning: own Nomad service hostname did not resolve within 120 seconds; continuing' >&2
        break
    fi
    sleep 1
    self_wait=$((self_wait + 1))
done
getent group karakoram >/dev/null || groupadd --gid 10001 karakoram
id karakoram >/dev/null 2>&1 || useradd --uid 10001 --gid 10001 --home-dir /var/lib/karakoram --no-create-home --shell /bin/sh karakoram
test "$(id -u karakoram)" = 10001
test "$(id -g karakoram)" = 10001
chown 10001:10001 '/var/lib/karakoram'
chmod 0700 '/var/lib/karakoram'
# TLS templates belong to UID 10001; allow the server to traverse secrets/.
chown 10001:10001 '/secrets'
chmod 0700 '/secrets'
install -d -o 10001 -g 10001 '/opt/karakoram'
install -m 0755 -o 10001 -g 10001 '/local/artifact/karakoram' '/opt/karakoram/karakoram'
# No runtime sudo, including any sudoers inherited from the rootfs.
rm -f /etc/sudoers.d/karakoram
chmod 0755 /usr/bin/sudo
cd '/var/lib/karakoram'
. '/secrets/karakoram.env'
exec /usr/bin/setpriv --reuid=10001 --regid=10001 --init-groups -- '/opt/karakoram/karakoram' 'server'
EOF
      }

      template {
        destination          = "secrets/karakoram.env"
        perms                = "0600"
        change_mode          = "noop"
        error_on_missing_key = true
        data = <<EOF
{{ with nomadVar "nomad/jobs/karakoram" }}{{ range .Tuples }}{{ if regexMatch "^KARAKORAM_" .K }}export {{ .K }}='{{ .V | replaceAll "'" "'\\''" }}'
{{ end }}{{ end }}{{ end }}
EOF
      }

      template {
        destination = "etc/hosts"
        perms       = "0644"
        change_mode = "noop"
        data = <<EOF
127.0.0.1 localhost
::1 localhost ip6-localhost ip6-loopback
{{ range nomadServices }}{{ $name := .Name }}{{ range nomadService $name }}{{ .Address }} {{ $name }}
{{ end }}{{ end }}
EOF
      }

      template {
        destination          = "secrets/nomad-ca.pem"
        perms                = "0600"
        uid                  = 10001
        gid                  = 10001
        change_mode          = "noop"
        error_on_missing_key = true
        data = <<EOF
{{ with nomadVar "nomad/jobs/karakoram" }}{{ .NOMAD_CA_PEM }}{{ end }}
EOF
      }

      template {
        destination          = "secrets/nomad-client-cert.pem"
        perms                = "0600"
        uid                  = 10001
        gid                  = 10001
        change_mode          = "noop"
        error_on_missing_key = true
        data = <<EOF
{{ with nomadVar "nomad/jobs/karakoram" }}{{ .NOMAD_CLIENT_CERT_PEM }}{{ end }}
EOF
      }

      template {
        destination          = "secrets/nomad-client-key.pem"
        perms                = "0600"
        uid                  = 10001
        gid                  = 10001
        change_mode          = "noop"
        error_on_missing_key = true
        data = <<EOF
{{ with nomadVar "nomad/jobs/karakoram" }}{{ .NOMAD_CLIENT_KEY_PEM }}{{ end }}
EOF
      }
    }
  }
}
