#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
"""Upload locked bytes with SigV4, then verify each anonymous object GET."""
import datetime
import hashlib
import hmac
import json
import os
from pathlib import Path
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT/'dist/native-artifacts'


def upload(filename, payload):
    endpoint = os.environ['S3_ENDPOINT'].rstrip('/')
    bucket = os.environ.get('NATIVE_ARTIFACT_BUCKET','kk-artifacts')
    prefix = os.environ.get('NATIVE_ARTIFACT_PREFIX','native').strip('/')
    path = '/'+bucket+'/'+prefix+'/'+urllib.parse.quote(filename,safe='')
    now = datetime.datetime.now(datetime.timezone.utc)
    stamp = now.strftime('%Y%m%dT%H%M%SZ'); day = stamp[:8]
    host = urllib.parse.urlsplit(endpoint).netloc
    digest = hashlib.sha256(payload).hexdigest()
    signed = 'host;x-amz-content-sha256;x-amz-date'
    headers = f'host:{host}\nx-amz-content-sha256:{digest}\nx-amz-date:{stamp}\n'
    canonical = '\n'.join(['PUT',path,'',headers,signed,digest])
    region = os.environ.get('S3_REGION','us-east-1')
    scope = f'{day}/{region}/s3/aws4_request'
    message = f'AWS4-HMAC-SHA256\n{stamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}'
    key = ('AWS4'+os.environ['S3_SECRET_KEY']).encode()
    for part in (day,region,'s3','aws4_request'):
        key = hmac.new(key,part.encode(),hashlib.sha256).digest()
    signature = hmac.new(key,message.encode(),hashlib.sha256).hexdigest()
    auth = f'AWS4-HMAC-SHA256 Credential={os.environ["S3_ACCESS_KEY"]}/{scope}, SignedHeaders={signed}, Signature={signature}'
    request = urllib.request.Request(endpoint+path,data=payload,method='PUT',headers={'X-Amz-Date':stamp,'X-Amz-Content-Sha256':digest,'Authorization':auth})
    with urllib.request.urlopen(request,timeout=300) as response:
        if response.status!=200: raise RuntimeError('upload failed: '+filename)
    remote = hashlib.sha256()
    with urllib.request.urlopen(endpoint+path,timeout=300) as response:
        while chunk := response.read(1024*1024): remote.update(chunk)
    if remote.hexdigest()!=digest: raise RuntimeError('anonymous mirror verification failed: '+filename)
    print(filename+': anonymous GET SHA256 verified')


def main():
    entries = json.loads((ROOT/'internal/engine/nativeartifacts/lock.json').read_text())
    sums = []
    for artifact in entries:
        file = OUT/artifact['file']
        payload = file.read_bytes()
        if len(payload)!=artifact['size'] or hashlib.sha256(payload).hexdigest()!=artifact['sha256']:
            raise SystemExit('local artifact differs from lock: '+file.name)
        upload(file.name,payload)
        sums.append(artifact['sha256']+'  '+file.name+'\n')
    payload = ''.join(sums).encode()
    (OUT/'SHA256SUMS').write_bytes(payload)
    upload('SHA256SUMS',payload)


if __name__=='__main__': main()
