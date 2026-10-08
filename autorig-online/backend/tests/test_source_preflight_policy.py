import os,sys,unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import AsyncMock,patch
from types import SimpleNamespace
import httpx

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:sys.path.insert(0,str(ROOT))
import tasks
from database import Task
from source_preflight_policy import SourceDisposition,SourcePreflightResult
from workers import WorkerTaskResult

class FakeDb:
    def __init__(self):
        self.commit=AsyncMock();self.refresh=AsyncMock()
        self.execute=AsyncMock(return_value=SimpleNamespace(rowcount=0))
def make_task(attempts=0):
    return Task(id="00000000-0000-0000-0000-000000000099",owner_type="anon",owner_id="anon",input_url="https://example.test/model.glb",input_type="t_pose",status="created",created_at=datetime.utcnow(),source_attempt_count=attempts)
def client_factory(transport):
    def factory(*args,**kwargs):kwargs["transport"]=transport;return httpx.AsyncClient(*args,**kwargs)
    return factory

class SourcePreflightPolicyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        for name, replacement in (
            ("normalize_task_input", lambda *_: SimpleNamespace(changed=False)),
            ("autorig_workload_broker_enabled", lambda: False),
            ("release_task_workload_lease", AsyncMock()),
        ):
            guard = patch.object(tasks, name, replacement, create=True)
            guard.start()
            self.addCleanup(guard.stop)

    async def test_manual_relative_redirect_preserves_query_and_succeeds(self):
        for status in (302,303):
            seen=[]
            def handler(request):
                seen.append(str(request.url))
                if len(seen)==1:return httpx.Response(status,headers={"location":"?download=1"},request=request)
                return httpx.Response(206,content=b"glTF"+b"\0"*12,headers={"content-range":"bytes 0-15/16"},request=request)
            with patch.object(tasks,"worker_http_client",client_factory(httpx.MockTransport(handler))):result=await tasks.preflight_task_source("https://example.test/model.glb?old=1")
            self.assertTrue(result.available);self.assertEqual(seen,["https://example.test/model.glb?old=1","https://example.test/model.glb?download=1"])

    async def test_redirect_never_requests_unlisted_private_or_public_destination(self):
        destinations=("http://127.0.0.1/secret","http://localhost/secret","http://192.168.1.2/x","http://169.254.169.254/latest","https://evil-public.test/model.glb")
        for destination in destinations:
            seen=[]
            transport=httpx.MockTransport(lambda request:(seen.append(str(request.url)) or httpx.Response(302,headers={"location":destination},request=request)))
            with patch.object(tasks,"worker_http_client",client_factory(transport)):
                result=await tasks.preflight_task_source("https://example.test/model.glb")
            self.assertEqual(result.disposition,SourceDisposition.INTERNAL);self.assertEqual(seen,["https://example.test/model.glb"])

    async def test_redirect_loop_and_hop_cap_are_internal(self):
        seen=[]
        def loop(request):
            seen.append(str(request.url));target="/b.glb" if request.url.path.endswith("a.glb") else "/a.glb";return httpx.Response(302,headers={"location":target},request=request)
        with patch.object(tasks,"worker_http_client",client_factory(httpx.MockTransport(loop))):result=await tasks.preflight_task_source("https://example.test/a.glb")
        self.assertEqual(result.disposition,SourceDisposition.INTERNAL);self.assertEqual(len(seen),2)
        seen=[]
        def chain(request):
            seen.append(str(request.url));index=len(seen);return httpx.Response(303,headers={"location":f"/hop-{index}.glb"},request=request)
        with patch.object(tasks,"worker_http_client",client_factory(httpx.MockTransport(chain))):result=await tasks.preflight_task_source("https://example.test/start.glb")
        self.assertEqual(result.disposition,SourceDisposition.INTERNAL);self.assertEqual(len(seen),6)

    async def test_redirect_credentials_and_non_http_are_internal(self):
        for destination in ("https://user:pass@example.test/x.glb","file:///etc/passwd","ftp://example.test/x"):
            seen=[];transport=httpx.MockTransport(lambda request:(seen.append(str(request.url)) or httpx.Response(302,headers={"location":destination},request=request)))
            with patch.object(tasks,"worker_http_client",client_factory(transport)):result=await tasks.preflight_task_source("https://example.test/start.glb")
            self.assertEqual(result.disposition,SourceDisposition.INTERNAL);self.assertEqual(len(seen),1)

    async def test_transport_and_service_failures_never_become_terminal(self):
        failures=(httpx.ConnectError("down"),httpx.ReadTimeout("slow"),httpx.RemoteProtocolError("protocol"),429,500,503)
        for attempts in (0,2,99):
            for failure in failures:
                with self.subTest(attempt=attempts+1,failure=str(failure)):
                    def handler(request):
                        if isinstance(failure,int):return httpx.Response(failure,request=request)
                        raise failure
                    transport=httpx.MockTransport(handler);db=FakeDb();task=make_task(attempts)
                    with patch.object(tasks,"worker_http_client",client_factory(transport)),patch.object(tasks,"send_task_to_worker",AsyncMock()) as send,patch.object(tasks,"quarantine_worker") as quarantine,patch.object(tasks,"_schedule_task_error_notification") as notify:
                        result,error=await tasks.start_task_on_worker(db,task,"https://converter-f13.example/api-converter-glb")
                    self.assertEqual(result.status,"created");self.assertEqual(result.source_attempt_count,attempts+1);self.assertIsNotNone(result.source_next_retry_at);self.assertIsNone(result.worker_api);self.assertIsNone(result.processing_started_at);self.assertIn("retry scheduled",error.lower());send.assert_not_awaited();quarantine.assert_not_called();notify.assert_not_called()
                    expected=(60,300,900,1800)[min(attempts,3)];delay=(result.source_next_retry_at-result.updated_at).total_seconds();self.assertEqual(delay,expected)

    async def test_invalid_payload_is_immediate_and_404_is_bounded_three(self):
        async def run(response,attempts):
            transport=httpx.MockTransport(lambda request:response(request));db=FakeDb();task=make_task(attempts)
            with patch.object(tasks,"worker_http_client",client_factory(transport)),patch.object(tasks,"_schedule_task_error_notification") as notify:
                result,error=await tasks.start_task_on_worker(db,task,"https://worker.test/api-converter-glb")
            return result,error,notify
        bad=lambda request:httpx.Response(200,content=b"not glb",headers={"content-type":"application/octet-stream"},request=request)
        result,error,notify=await run(bad,0);self.assertEqual(result.status,"error");notify.assert_called_once();self.assertIn("not a valid",error)
        notfound=lambda request:httpx.Response(404,request=request)
        result,_,notify=await run(notfound,1);self.assertEqual(result.status,"created");notify.assert_not_called()
        result,_,notify=await run(notfound,2);self.assertEqual(result.status,"error");notify.assert_called_once()

    async def test_transport_mapping_uses_original_url_at_client_boundary(self):
        seen=[]
        def handler(request):seen.append(str(request.url));return httpx.Response(206,content=b"glTF"+b"\0"*12,headers={"content-range":"bytes 0-15/16"},request=request)
        mapping='{"https://example.test":{"origin":"http://127.0.0.1:19099","path_prefix":"/mapped"}}'
        with patch.dict(os.environ,{"AUTORIG_WORKER_TRANSPORTS":mapping}):
            # Keep the real routing client, but supply a mock transport directly.
            original=tasks.worker_http_client
            with patch.object(tasks,"worker_http_client",side_effect=lambda *a,**kw:original(*a,transport=httpx.MockTransport(handler),**kw)):
                result=await tasks.preflight_task_source("https://example.test/model.glb")
        self.assertTrue(result.available);self.assertEqual(seen,["http://127.0.0.1:19099/mapped/model.glb"])

    async def test_successful_recovery_resets_counter_and_dispatches_metadata(self):
        transport=httpx.MockTransport(lambda request:httpx.Response(206,content=b"glTF"+b"\0"*12,headers={"content-range":"bytes 0-15/16"},request=request));db=FakeDb();task=make_task(100)
        worker=WorkerTaskResult(success=True,task_id="worker-1",guid="guid-1",output_urls=[])
        metadata={"title":"Recovered","keywords":["safe"]}
        with patch.object(tasks,"worker_http_client",client_factory(transport)),patch("content_moderation.build_pre_convert_metadata_sync",return_value=metadata),patch.object(tasks,"send_task_to_worker",AsyncMock(return_value=worker)) as send,patch.object(tasks,"persist_validated_worker_viewer_artifacts",AsyncMock()),patch.object(tasks,"quarantine_worker") as quarantine:
            result,error=await tasks.start_task_on_worker(db,task,"https://converter-f13.example/api-converter-glb")
        self.assertIsNone(error);self.assertEqual(result.source_attempt_count,0);self.assertIsNone(result.source_next_retry_at);self.assertEqual(result.status,"processing");self.assertEqual(send.await_args.kwargs["metadata"],metadata);quarantine.assert_not_called()

    async def test_internal_failure_retries_without_user_terminal_error(self):
        db=FakeDb();task=make_task(99)
        with patch.object(tasks,"preflight_task_source",AsyncMock(return_value=SourcePreflightResult(False,"internal",SourceDisposition.INTERNAL))),patch.object(tasks,"_schedule_task_error_notification") as notify:
            result,error=await tasks.start_task_on_worker(db,task,"https://worker.test/api-converter-glb")
        self.assertEqual(result.status,"created");self.assertIsNone(result.error_message);self.assertIn("retry scheduled",error.lower());notify.assert_not_called()

if __name__=="__main__":unittest.main()
