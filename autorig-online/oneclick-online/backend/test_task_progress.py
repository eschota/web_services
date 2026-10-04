"""Completion authority and bounded log-tail regressions, without real DB/jobs."""
import unittest
from unittest.mock import patch
import httpx
import tasks
from database import Task
from storage import BoundedClient

class DB:
    async def commit(self): pass
    async def refresh(self, _): pass
    async def execute(self, _):
        return type('Result', (), {'rowcount':0})()

class ProgressTests(unittest.IsolatedAsyncioTestCase):
    async def check_state(self, native, expected):
        task=Task(id='11111111-1111-4111-8111-111111111111',status='processing',worker_api='http://worker.test/api-converter-glb',worker_task_id='worker-id',guid='guid',output_log_url='https://public.worker.test/converter/glb/guid/log.log',ready_urls=[],product_entries=[],ready_count=0,total_count=100)
        def handler(request):
            if '/occonvert/status/' in request.url.path:
                if native is None: return httpx.Response(404)
                return httpx.Response(200,json={'status':native,'backend_task_id':task.id})
            if '/model-files/' in request.url.path:
                return httpx.Response(200,json={'folders':{'export/meshes':{'files':[{'name':'model.fbx','rel_path':'export/meshes/model.fbx'}]}}})
            return httpx.Response(206,headers={'content-range':'bytes 0-36/37'},text='Export completed\nTASK_COMPLETE\n')
        class Client(BoundedClient):
            def __init__(self, **kwargs):
                super().__init__(transport=httpx.MockTransport(handler),**kwargs)
        with patch.object(tasks,'BoundedClient',Client):
            await tasks.update_task_progress(DB(),task)
        self.assertEqual(task.status,expected)
        if expected=='done':
            self.assertEqual(task.ready_count,100)
            self.assertIn('https://public.worker.test/converter/glb/guid/export/meshes/model.fbx',task.ready_urls)
        else:
            self.assertLess(task.ready_count,100)

    async def test_max_export_marker_does_not_finish_unity(self):
        await self.check_state('Processing','processing')

    async def test_missing_native_status_never_finishes_from_markers(self):
        await self.check_state(None,'processing')

    async def test_native_completed_collects_final_public_inventory(self):
        await self.check_state('Completed','done')

if __name__=='__main__': unittest.main()
