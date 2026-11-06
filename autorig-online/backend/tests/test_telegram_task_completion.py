"""Terminal delivery regression tests; no Telegram/network/customer writes."""
import asyncio, inspect, os, tempfile, unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
import telegram_bot as bot
import telegram_task_completion as delivery

class DB:
    async def __aenter__(self): return self
    async def __aexit__(self,*args): pass
    async def execute(self,*args):
        task=SimpleNamespace(owner_type='anon',status='needs_review',content_rating='safe',guid=None,worker_api=None,
            collection_title=None,collection_member_title=None,collection_index=None,collection_size=None,error_message='bind failed')
        return SimpleNamespace(scalar_one_or_none=lambda:task)

class TerminalDelivery(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        default=Path('/srv/autorig/data/motion_transfer/.work/terminal-notification-tests') if os.name!='nt' else Path(__file__).resolve().parents[3]/'.work/terminal-notification-tests'
        root=Path(os.getenv('AUTORIG_TEST_WORK_ROOT',str(default))); root.mkdir(parents=True,exist_ok=True)
        self.tmp=tempfile.TemporaryDirectory(dir=root); self.video=Path(self.tmp.name)/'fixture.mp4'; self.video.write_bytes(b'fixture')
        self.stack=ExitStack(); self.addCleanup(self.stack.close); self.addCleanup(self.tmp.cleanup)
        self.api=SimpleNamespace(send_video=AsyncMock(return_value=SimpleNamespace(message_id=7)),send_photo=AsyncMock(),send_message=AsyncMock(return_value=SimpleNamespace(message_id=8)))
        self.stack.enter_context(patch('telegram.Bot',return_value=self.api))
        self.stack.enter_context(patch.object(delivery,'AsyncSessionLocal',return_value=DB()))
        helpers={'_get_token':lambda:'fixture-token','_task_url':lambda _: 'https://autorig.online/task?id=fixture',
            '_task_telegram_metrics':AsyncMock(return_value={}), '_format_task_metrics':lambda _: '#fixture',
            'get_broadcast_chat_ids':AsyncMock(return_value=[123]),'private_chats_awaiting_task':AsyncMock(return_value=[]),
            'pop_notification_message_id':AsyncMock(return_value=None),'peek_notification_message_id':AsyncMock(return_value=None),
            'reserve_notification':AsyncMock(return_value=True),'mark_task_done_notification_sent':AsyncMock(),
            '_cleanup_generation_chat':AsyncMock(),'_sanitize_error_for_telegram':lambda x:x,
            '_download_video_from_worker':AsyncMock(return_value=(str(self.video),0,'fixture'))}
        async def retry(factory,**kwargs): return await factory()
        helpers['_send_with_retry']=retry
        for name,value in helpers.items(): self.stack.enter_context(patch.object(bot,name,value))

    def test_public_completion_symbols_exist(self):
        self.assertTrue(inspect.iscoroutinefunction(bot.broadcast_task_done))
        self.assertTrue(inspect.iscoroutinefunction(delivery.broadcast_task_done))
        self.assertTrue(inspect.iscoroutinefunction(delivery._render_v3_preview_now))

    async def test_done_with_video_preserves_review_warning(self):
        await delivery.broadcast_task_done('fixture',extra_html='needs review: deformation')
        self.api.send_video.assert_awaited_once(); self.api.send_photo.assert_not_awaited(); self.api.send_message.assert_not_awaited()
        self.assertIn('needs review',self.api.send_video.call_args.kwargs['caption'])
        self.assertNotIn('Task completed',self.api.send_video.call_args.kwargs['caption'])
        bot.mark_task_done_notification_sent.assert_awaited_once_with('fixture')

    async def test_poster_does_not_satisfy_video_only_completion(self):
        bot._download_video_from_worker.return_value=(None,0,None)
        self.stack.enter_context(patch.object(delivery,'_render_v3_preview_now',new=AsyncMock()))
        self.stack.enter_context(patch.object(delivery,'_wait_v3_preview',new=AsyncMock(return_value=(None,'poster.jpg'))))
        await delivery.broadcast_task_done('fixture')
        await asyncio.sleep(0)
        self.api.send_video.assert_not_awaited(); self.api.send_photo.assert_not_awaited(); self.api.send_message.assert_not_awaited()
        bot.mark_task_done_notification_sent.assert_not_awaited()

    async def test_error_is_not_spliced_into_completion(self):
        await delivery.broadcast_task_error('fixture')
        self.api.send_message.assert_awaited_once()
        self.assertIn('Task failed',self.api.send_message.call_args.kwargs['text'])
        self.assertNotIn('Task completed',self.api.send_message.call_args.kwargs['text'])
        self.api.send_video.assert_not_awaited()

    async def test_failed_video_does_not_send_text_as_completion(self):
        async def failed_video(factory,**kwargs): return None
        with patch.object(bot,'_send_with_retry',new=failed_video): await delivery.broadcast_task_done('fixture')
        self.api.send_message.assert_not_awaited(); self.api.send_photo.assert_not_awaited()
        bot.mark_task_done_notification_sent.assert_not_awaited()

    async def test_missing_delivery_module_does_not_reserve_done(self):
        with patch.dict('sys.modules',{'telegram_task_completion':None}),patch.object(bot,'AsyncSessionLocal') as session:
            with self.assertRaises(ModuleNotFoundError): await bot.reserve_and_broadcast_task_done('fixture')
            session.assert_not_called()

    async def test_startup_statistics_include_review_tasks(self):
        class StatsDB(DB):
            count=0
            async def execute(self,*args):
                self.count+=1
                return SimpleNamespace(all=lambda:[('done',1470),('error',339),('needs_review',239)],scalar=lambda:7)
        with patch.object(bot,'AsyncSessionLocal',return_value=StatsDB()):
            await bot.broadcast_server_startup()
        text=self.api.send_message.call_args.kwargs['text']
        self.assertIn('Needs review: 239',text); self.assertIn('Total: 2048',text)

if __name__=='__main__': unittest.main()
