import ast
import hashlib
import io
import json
import sqlite3
import re
import tempfile
import unittest
from pathlib import Path

from fastapi import FastAPI, HTTPException, UploadFile

import video_variants


class VideoVariantsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory(dir=Path(__file__).parent/'.test-work')
        self.root = Path(self.temporary.name)
        self.database = self.root/'test.db'
        self.calls, self.legacy_calls = [], []
        self.counter = 0
        conn = self.db()
        conn.execute('CREATE TABLE messages (uid TEXT PRIMARY KEY, direction TEXT, author_kind TEXT, '
                     'agent TEXT, project TEXT, kind TEXT, text TEXT, reply_to_uid TEXT, scope TEXT, '
                     'user_id INTEGER, tg_chat_id INTEGER, tg_message_id INTEGER)')
        conn.commit(); conn.close()

        async def tg_call(method, **kwargs):
            self.calls.append((method, kwargs))
            return {'message_id': 123, 'reply_markup': json.loads(kwargs.get('data',{}).get('reply_markup','{}'))}

        async def old_callback(cq):
            self.legacy_calls.append(cq)

        self.ctx={'_handle_callback':old_callback,'normalize_agent':lambda a:a.strip(),
                  'new_uid':self.uid,'MEDIA_DIR':self.root,'PUBLIC_BASE':'https://example.test/dev',
                  'CHAT_ID':-1,'db':self.db,'insert_message':self.insert,'tg_call':tg_call}
        self.app=FastAPI()
        video_variants.install(self.app,self.ctx)
        self.endpoint=next(r.endpoint for r in self.app.routes if r.path=='/dev/api/send_variants')

    async def asyncTearDown(self):
        self.temporary.cleanup()

    def uid(self):
        self.counter+=1
        return f'{self.counter:012x}'

    def db(self):
        conn=sqlite3.connect(self.database)
        conn.row_factory=sqlite3.Row
        return conn

    def insert(self,conn,**kwargs):
        columns=['uid','direction','author_kind','agent','project','kind','text','reply_to_uid',
                 'scope','user_id','tg_chat_id','tg_message_id']
        kwargs.setdefault('uid',self.uid())
        conn.execute('INSERT INTO messages ('+','.join(columns)+') VALUES ('+','.join('?' for _ in columns)+')',
                     [kwargs.get(k) for k in columns])

    def files(self):
        return [UploadFile(filename=label+'.mp4',file=io.BytesIO(('test-'+label).encode())) for label in 'ABC']

    async def send(self):
        return json.loads((await self.endpoint(agent='Own agent',project='Own project',
                                                caption='Three videos',files=self.files())).body)

    async def test_three_native_videos_and_choices_are_in_one_message(self):
        result=await self.send()
        self.assertEqual(len(self.calls),1)
        method,kwargs=self.calls[0]
        self.assertEqual(method,'sendRichMessage')
        blocks=json.loads(kwargs['data']['rich_message'])['blocks']
        videos=[b['video'] for b in blocks if b['type']=='video']
        self.assertEqual([v['media'] for v in videos],['attach://v0','attach://v1','attach://v2'])
        self.assertTrue(all(v['supports_streaming'] for v in videos))
        rows=json.loads(kwargs['data']['reply_markup'])['inline_keyboard']
        self.assertEqual([b['text'] for b in rows[0]],['Выбрать A','Выбрать B','Выбрать C'])
        self.assertEqual(result['state'],'sent')
        self.assertEqual([v['label'] for v in result['variants']],list('ABC'))

    async def test_select_b_uses_parent_agent_and_original_media_digest(self):
        result=await self.send()
        source=Path('/srv/autorig/devbot/app.py')
        if not source.exists():
            self.skipTest('Live callback source is required for the production-runtime integration check')
        tree=ast.parse(source.read_text())
        callback=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef) and n.name=='_handle_callback')
        namespace={**self.ctx,'re':re,'json':json,'HTTPException':HTTPException}
        exec(compile(ast.Module(body=[callback],type_ignores=[]),str(source),'exec'),namespace)
        await namespace['_handle_callback']({'id':'cq1','data':'q:'+result['uid']+':2',
             'agent':'Untrusted other agent','from':{'id':7,'first_name':'Owner'},
             'message':{'chat':{'id':-1},'message_id':123}})
        conn=self.db()
        row=conn.execute("SELECT * FROM messages WHERE direction='in'").fetchone()
        conn.close()
        self.assertEqual((row['agent'],row['project'],row['kind'],row['text'],row['scope']),
                         ('Own agent','Own project','pick','2','direct'))
        selected=result['variants'][int(row['text'])-1]
        self.assertEqual(selected['label'],'B')
        self.assertEqual(selected['sha256'],hashlib.sha256(b'test-B').hexdigest())

    async def test_legacy_callbacks_are_preserved(self):
        cq={'data':'q:abcdef123456:1'}
        await self.ctx['_handle_callback'](cq)
        self.assertEqual(self.legacy_calls,[cq])

    async def test_callbacks_keep_the_existing_public_protocol(self):
        result=await self.send()
        buttons=result['telegram_returned_keyboard']['inline_keyboard'][0]
        self.assertEqual([b['callback_data'] for b in buttons],
                         [f"q:{result['uid']}:{i}" for i in range(1,4)])

    async def test_invalid_count_and_non_video_fail_before_transmission(self):
        for files in [self.files()[:2],self.files()[:2]+[UploadFile(filename='C.jpg',file=io.BytesIO(b'image'))]]:
            with self.assertRaises(HTTPException):
                await self.endpoint(agent='Own agent',project='Own project',caption='',files=files)
        self.assertEqual(self.calls,[])


if __name__=='__main__':
    (Path(__file__).parent/'.test-work').mkdir(exist_ok=True)
    unittest.main()
