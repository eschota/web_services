import asyncio
import os
from pathlib import Path
import tempfile
import time
import tracemalloc
import unittest
from unittest.mock import patch

import httpx
import storage


class Blocks(httpx.AsyncByteStream):
    async def __aiter__(self):
        for _ in range(1024):
            yield b'x' * 65536


class StorageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir='/srv/oneclick/data')
        self.root = Path(self.tmp.name)
        self.patch = patch.multiple(storage, ROOT=self.root, CACHE=self.root/'cache', MIN_FREE=0, MAX_CACHE=128*1024**2, MAX_FILE=128*1024**2)
        self.patch.start()

    async def asyncTearDown(self):
        self.patch.stop()
        self.tmp.cleanup()

    async def test_pressure_protects_active_upload_and_database(self):
        cache=storage.CACHE
        cache.mkdir()
        for name in ['active-task.glb', 'old.zip']:
            p=cache/name
            p.write_bytes(b'x'*100)
            os.utime(p, (time.time()-7200,)*2)
        original=self.root/'uploads'/'original.zip'
        original.parent.mkdir()
        original.write_bytes(b'original')
        db=self.root/'db.sqlite'
        db.write_bytes(b'database')
        with patch.object(storage,'MAX_CACHE',1):
            result=storage.clean(['active-task'])
        self.assertEqual(result['deleted_count'],1)
        self.assertTrue((cache/'active-task.glb').exists())
        self.assertTrue(original.exists())
        self.assertTrue(db.exists())

    async def test_stream_64mb_has_bounded_python_memory_and_atomic_cache(self):
        class Client(storage.BoundedClient):
            def __init__(self, **kwargs):
                super().__init__(transport=httpx.MockTransport(lambda _: httpx.Response(200, headers={'content-length':str(64*1024**2)}, stream=Blocks())), **kwargs)
        target=storage.CACHE/'large.zip'
        tracemalloc.start()
        with patch.object(storage,'BoundedClient',Client):
            await storage.download('https://worker.test/large.zip',target)
        _,peak=tracemalloc.get_traced_memory()
        tracemalloc.stop()
        self.assertEqual(target.stat().st_size,64*1024**2)
        self.assertLess(peak,8*1024**2)
        self.assertEqual(list(storage.CACHE.glob('*.part.*')),[])
        self.assertEqual(len(storage.INFLIGHT),0)
        print('64 MiB streamed, Python peak bytes:',peak)

    async def test_incomplete_body_leaves_no_file_or_lock(self):
        class Client(storage.BoundedClient):
            def __init__(self, **kwargs):
                super().__init__(transport=httpx.MockTransport(lambda _: httpx.Response(200,headers={'content-length':'100'},content=b'bad')), **kwargs)
        target=storage.CACHE/'broken.zip'
        with patch.object(storage,'BoundedClient',Client):
            with self.assertRaises(ValueError):
                await storage.download('https://worker.test/broken',target)
        self.assertFalse(target.exists())
        self.assertEqual(list(storage.CACHE.glob('*.part.*')),[])
        self.assertEqual(len(storage.INFLIGHT),0)

    async def test_buffered_response_cap(self):
        async with storage.BoundedClient(transport=httpx.MockTransport(lambda _:httpx.Response(200,stream=Blocks()))) as client:
            with self.assertRaises(ValueError):
                await client.get('https://worker.test/log')

    async def test_quota_rejection_does_not_corrupt_reservation_counter(self):
        class Client(storage.BoundedClient):
            def __init__(self, **kwargs):
                super().__init__(transport=httpx.MockTransport(lambda _:httpx.Response(200,headers={'content-length':'1024'},content=b'x'*1024)), **kwargs)
        with patch.object(storage,'MAX_CACHE',10), patch.object(storage,'BoundedClient',Client):
            with self.assertRaises(OSError):
                await storage.download('https://worker.test/a',storage.CACHE/'a.zip')
        self.assertEqual(storage.RESERVED,0)
        self.assertEqual(len(storage.INFLIGHT),0)

    async def test_disk_backed_upload_reservations_block_overcommit(self):
        import json
        temp=self.root/'uploads'/'temp'/'session'
        temp.mkdir(parents=True)
        (temp/'metadata.json').write_text(json.dumps({'total_size':1000}))
        with patch.object(storage,'MAX_UPLOADS',2500):
            with self.assertRaises(OSError):
                storage.upload_admission(1000)
        self.assertTrue((temp/'metadata.json').exists())


if __name__=='__main__':
    unittest.main()
