import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch, AsyncMock
import httpx
import archive_fetch
import storage

class ArchiveTests(unittest.IsolatedAsyncioTestCase):
    async def test_non_zip_cleans_only_new_spool_directory(self):
        with tempfile.TemporaryDirectory(dir='/srv/oneclick/data') as tmp:
            root=Path(tmp)
            class Client(storage.BoundedClient):
                def __init__(self, **kwargs): super().__init__(transport=httpx.MockTransport(lambda _:httpx.Response(200,content=b'not a zip')),**kwargs)
            with patch.object(archive_fetch,'ROOT',root), patch.object(storage,'ROOT',root), patch.object(archive_fetch,'check_source',AsyncMock()),patch.object(archive_fetch,'BoundedClient',Client):
                with self.assertRaises(ValueError): await archive_fetch.fetch('https://files.example/source',100)
            self.assertEqual(list((root/'uploads').iterdir()),[])

    async def test_public_archive_streams_and_keeps_original(self):
        with tempfile.TemporaryDirectory(dir='/srv/oneclick/data') as tmp:
            root=Path(tmp)
            body=b'PK\x03\x04'+b'x'*100000
            class Client(storage.BoundedClient):
                def __init__(self, **kwargs): super().__init__(transport=httpx.MockTransport(lambda _:httpx.Response(200,headers={'content-length':str(len(body))},content=body)),**kwargs)
            with patch.object(archive_fetch,'ROOT',root), patch.object(storage,'ROOT',root), patch.object(archive_fetch,'check_source',AsyncMock()),patch.object(archive_fetch,'BoundedClient',Client):
                token=await archive_fetch.fetch('https://files.example/source',len(body))
            self.assertEqual((root/'uploads'/token/'scene.zip').read_bytes(),body)
            self.assertFalse((root/'uploads'/token/'scene.zip.part').exists())

    async def test_private_source_is_rejected(self):
        with patch.object(archive_fetch.socket,'getaddrinfo',return_value=[(2,1,6,'',('127.0.0.1',443))]):
            with self.assertRaises(ValueError): await archive_fetch.check_source('https://private.example/archive.zip')

if __name__=='__main__': unittest.main()
