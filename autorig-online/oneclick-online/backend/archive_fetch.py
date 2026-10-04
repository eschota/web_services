"""Stream a public HTTPS archive into OneClick's upload spool, never into RAM."""
import asyncio
import ipaddress
from pathlib import Path
import shutil
import socket
from urllib.parse import urljoin, urlsplit
import uuid

from storage import BoundedClient, ROOT, upload_admission


async def check_source(url):
    parsed=urlsplit(url)
    if parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError('Use a public HTTPS ZIP download link')
    try:
        records=await asyncio.to_thread(socket.getaddrinfo,parsed.hostname,parsed.port or 443,type=socket.SOCK_STREAM)
    except socket.gaierror:
        raise ValueError('The archive host is unavailable')
    if not records or any(not ipaddress.ip_address(record[4][0]).is_global for record in records):
        raise ValueError('The archive host must be publicly reachable')


async def fetch(url, max_bytes):
    token=str(uuid.uuid4())
    folder=ROOT/'uploads'/token
    target=folder/'scene.zip'
    part=folder/'scene.zip.part'
    try:
        folder.mkdir(parents=True)
        async with BoundedClient(timeout=300,follow_redirects=False) as client:
            for _ in range(6):
                await check_source(url)
                async with client.stream('GET',url,headers={'Accept-Encoding':'identity'}) as response:
                    if response.status_code in {301,302,303,307,308}:
                        url=urljoin(url,response.headers.get('location',''))
                        continue
                    response.raise_for_status()
                    length=int(response.headers.get('content-length','0'))
                    if length<0 or length>max_bytes:
                        raise ValueError('Archive exceeds the upload limit')
                    upload_admission(length)
                    written=0
                    with part.open('wb') as out:
                        async for block in response.aiter_bytes(65536):
                            if written==0 and not block.startswith((b'PK\x03\x04',b'PK\x05\x06')):
                                raise ValueError('The link did not return a ZIP archive')
                            written+=len(block)
                            if written>max_bytes:
                                raise ValueError('Archive exceeds the upload limit')
                            upload_admission(len(block))
                            out.write(block)
                    if not written or (length and written!=length):
                        raise ValueError('The source archive download was incomplete')
                    part.replace(target)
                    return token
            raise ValueError('Too many archive redirects')
    except BaseException:
        # Only the freshly allocated, verified project-owned UUID directory is removed.
        assert folder.parent.resolve()==(ROOT/'uploads').resolve()
        shutil.rmtree(folder,ignore_errors=True)
        raise
