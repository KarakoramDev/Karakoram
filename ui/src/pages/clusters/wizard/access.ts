// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

import type { Schemas } from '@/api/client';

function cidrFamily(entry: string): 'IPV4' | 'IPV6' | undefined {
  const parts = entry.split('/');
  if (parts.length !== 2 || !/^\d+$/.test(parts[1]!)) return;
  const address = parts[0]!;
  const prefix = Number(parts[1]);
  const ipv4 = (s: string) => s.split('.').length === 4 && s.split('.').every((octet) => /^(0|[1-9]\d{0,2})$/.test(octet) && Number(octet) <= 255);
  if (ipv4(address)) return prefix <= 32 ? 'IPV4' : undefined;
  if (!address.includes(':') || prefix > 128) return;
  const halves = address.split('::');
  if (halves.length > 2) return;
  const groups = halves.flatMap((half) => half ? half.split(':') : []);
  let count = 0;
  for (let i = 0; i < groups.length; i++) {
    const group = groups[i]!;
    if (/^[a-f\d]{1,4}$/i.test(group)) count++;
    else if (i === groups.length - 1 && ipv4(group)) count += 2;
    else return;
  }
  if (halves.length === 2 ? count < 8 : count === 8) return 'IPV6';
}

export function coversAllAddresses(exposure: Schemas['Exposure'] | undefined): boolean {
  return Boolean(exposure?.public && exposure.allowlist?.some((entry) => cidrFamily(entry) && Number(entry.split('/')[1]) === 0));
}

/** Serialize acknowledgement only for the currently confirmed all-address exposure. */
export function wireExposure(exposure: Schemas['Exposure'], allAddressesConfirmed: boolean): Schemas['Exposure'] {
  return { ...exposure, allowAllAddresses: allAddressesConfirmed && coversAllAddresses(exposure) };
}

export function validateExposure(exposure: Schemas['Exposure'] | undefined, allAddressesConfirmed = false): string[] {
  if (!exposure?.public) return [];
  const entries = exposure.allowlist ?? [];
  const errors: string[] = [];
  if (!entries.length) errors.push('Public exposure requires at least one CIDR in the IP allowlist.');
  const invalid = entries.filter((entry) => !cidrFamily(entry));
  if (invalid.length) errors.push(`Invalid CIDR entries: ${invalid.join(', ')}.`);
  const mismatched = entries.filter((entry) => cidrFamily(entry) && cidrFamily(entry) !== exposure.ipType);
  if (mismatched.length) errors.push(`Use ${exposure.ipType === 'IPV6' ? 'IPv6' : 'IPv4'} CIDRs for the selected address family: ${mismatched.join(', ')}.`);
  if (coversAllAddresses(exposure) && !allAddressesConfirmed) errors.push('Confirm that all internet addresses will be allowed, or restrict the allowlist.');
  return errors;
}

export function tlsErrors(tls: Schemas['TLS'] | undefined, editing = false): Partial<Record<keyof Schemas['TLS'], string>> {
  if (!tls || tls.mode === 'disabled' || tls.ca !== 'userProvided') return {};
  const errors: Partial<Record<keyof Schemas['TLS'], string>> = {};
  if (!editing || tls.caCertificate?.trim() || tls.caPrivateKey?.trim()) {
    if (!tls.caCertificate?.trim()) errors.caCertificate = 'CA certificate PEM is required.';
    if (!tls.caPrivateKey?.trim()) errors.caPrivateKey = 'CA signing private key PEM is required to issue member and agent certificates.';
  }
  if (tls.certificate?.trim() && !tls.privateKey?.trim()) errors.privateKey = 'Provide the matching server private key, or leave both optional server fields empty.';
  if (tls.privateKey?.trim() && !tls.certificate?.trim()) errors.certificate = 'Provide the matching server certificate, or leave both optional server fields empty.';
  return errors;
}

export function validateTLS(tls: Schemas['TLS'] | undefined, editing = false): string[] {
  return Object.values(tlsErrors(tls, editing));
}

/** Empty optional PEM inputs mean omission, not an invalid empty certificate pair. */
export function cleanTLS(tls: Schemas['TLS']): Schemas['TLS'] {
  const result = { ...tls };
  for (const key of ['caCertificate', 'caPrivateKey', 'certificate', 'privateKey'] as const) {
    if (tls.ca !== 'userProvided' || !tls[key]?.trim()) delete result[key];
  }
  return result;
}
