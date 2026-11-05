#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 The Karakoram Authors
"""Exercise Storage's S3 frontend, resumable uploads and real imgproxy backend."""
import base64
import datetime
import hashlib
import hmac
import struct
import urllib.parse
import urllib.request
import zlib


def qualify(env):
    base = 'http://127.0.0.1:5000'
    bearer = {'Authorization': 'Bearer '+env['SERVICE_KEY']}

    def request(path, data=None, method='GET', headers=None):
        with urllib.request.urlopen(urllib.request.Request(base+path, data=data, method=method,
                                   headers=bearer | (headers or {})), timeout=30) as response:
            return response.status, response.headers, response.read()

    # Sign actual frontend requests independently of the AWS SDK bundled by Storage.
    def signed(method, path, data=None):
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        day = timestamp[:8]
        digest = hashlib.sha256(data or b'').hexdigest()
        headers = {'host':'127.0.0.1:5000', 'x-amz-content-sha256':digest, 'x-amz-date':timestamp}
        names = ';'.join(sorted(headers))
        canonical = '\n'.join((method,path,'',''.join(k+':'+headers[k]+'\n' for k in sorted(headers)),names,digest))
        scope = day+'/'+env['REGION']+'/s3/aws4_request'
        message = 'AWS4-HMAC-SHA256\n'+timestamp+'\n'+scope+'\n'+hashlib.sha256(canonical.encode()).hexdigest()
        key = ('AWS4'+env['S3_PROTOCOL_ACCESS_KEY_SECRET']).encode()
        for part in (day,env['REGION'],'s3','aws4_request'):
            key = hmac.new(key,part.encode(),hashlib.sha256).digest()
        signature = hmac.new(key,message.encode(),hashlib.sha256).hexdigest()
        headers['Authorization'] = 'AWS4-HMAC-SHA256 Credential='+env['S3_PROTOCOL_ACCESS_KEY_ID']+'/'+scope+', SignedHeaders='+names+', Signature='+signature
        return request(path,data,method,headers)

    payload = b'native SigV4 storage healthy'
    assert signed('PUT','/s3/native-smoke/sigv4.txt',payload)[0] == 200
    assert signed('GET','/s3/native-smoke/sigv4.txt')[2] == payload
    print('NATIVE_STORAGE_SIGV4_PUT_GET_OK',flush=True)

    payload = b'native TUS storage healthy'
    metadata = {'bucketName':'native-smoke','objectName':'resumable.txt','contentType':'text/plain','cacheControl':'3600'}
    headers = {'Tus-Resumable':'1.0.0','Upload-Length':str(len(payload)),
               'Upload-Metadata':','.join(k+' '+base64.b64encode(v.encode()).decode() for k,v in metadata.items())}
    status, created, _ = request('/upload/resumable',b'','POST',headers)
    assert status == 201
    location = urllib.parse.urlsplit(created['Location']).path
    assert request(location,method='HEAD',headers={'Tus-Resumable':'1.0.0'})[1]['Upload-Offset'] == '0'
    status, uploaded, _ = request(location,payload,'PATCH',{'Tus-Resumable':'1.0.0','Upload-Offset':'0','Content-Type':'application/offset+octet-stream'})
    assert status == 204 and int(uploaded['Upload-Offset']) == len(payload)
    assert request('/object/native-smoke/resumable.txt')[2] == payload
    print('NATIVE_STORAGE_TUS_CREATE_HEAD_PATCH_DOWNLOAD_OK',flush=True)

    def chunk(kind, data):
        return struct.pack('>I',len(data))+kind+data+struct.pack('>I',zlib.crc32(kind+data))
    png = b'\x89PNG\r\n\x1a\n'+chunk(b'IHDR',struct.pack('>IIBBBBB',2,2,8,2,0,0,0))+chunk(b'IDAT',zlib.compress(b'\x00'+b'\xff\x00\x00'*2+b'\x00'+b'\x00\xff\x00'*2))+chunk(b'IEND',b'')
    assert request('/object/native-smoke/probe.png',png,'POST',{'Content-Type':'image/png'})[0] == 200
    status, _, transformed = request('/render/image/authenticated/native-smoke/probe.png?width=1&height=1&resize=contain&format=origin')
    assert status == 200 and transformed.startswith(b'\x89PNG') and struct.unpack('>II',transformed[16:24]) == (1,1)
    print('NATIVE_STORAGE_IMGPROXY_AUTHENTICATED_TRANSFORM_OK',flush=True)
