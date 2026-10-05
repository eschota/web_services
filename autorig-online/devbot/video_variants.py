"""Three standard Telegram album videos plus a separate A/B/C choice message.

Installed into the existing DEV app without importing its private configuration.
The injected namespace keeps existing storage, bot connection and legacy callbacks.
"""
import hashlib
import json
from pathlib import Path

from fastapi import File, Form, HTTPException, UploadFile
from fastapi.responses import JSONResponse

LIMIT = 48 * 1024 * 1024


def install(app, ctx):
    if ctx.get('_youtube_video_album_choices_installed'):
        return

    @app.post('/dev/api/youtube_video_album_choices')
    async def send_variants(agent: str = Form(...), project: str = Form(...),
                            caption: str = Form(''), files: list[UploadFile] = File(...)):
        name = ctx['normalize_agent'](agent)
        project = project.strip()
        if not project or len(files) != 3:
            raise HTTPException(400, 'A project and exactly three MP4 videos are required')
        if any(Path(f.filename or '').suffix.lower() != '.mp4' for f in files):
            raise HTTPException(400, 'All three files must be MP4 videos')
        uid = ctx['new_uid']()
        media_dir = Path(ctx['MEDIA_DIR'])
        labels = ['A', 'B', 'C']
        descriptors, paths = [], []
        for label, file in zip(labels, files):
            media_uid = ctx['new_uid']()
            path = media_dir / (media_uid + '.mp4')
            digest, size = hashlib.sha256(), 0
            with path.open('wb') as stream:
                while chunk := await file.read(512 * 1024):
                    size += len(chunk)
                    if size > LIMIT:
                        raise HTTPException(413, 'Compress each video below 48 MiB')
                    digest.update(chunk)
                    stream.write(chunk)
            if not size:
                raise HTTPException(400, 'Empty video')
            paths.append(path)
            descriptors.append({'label': label, 'uid': media_uid, 'sha256': digest.hexdigest(),
                                'name': Path(file.filename).name, 'size': size,
                                'url': ctx['PUBLIC_BASE'] + '/api/media/' + media_uid})
        metadata = {'uid': uid, 'agent': name, 'project': project, 'variants': descriptors,
                    'state': 'sending', 'selected': None,'delivery_mode':'standard_video_album'}
        meta_path = media_dir / (uid + '.variants.json')
        meta_path.write_text(json.dumps(metadata, ensure_ascii=False), encoding='utf-8')
        conn = ctx['db']()
        try:
            ctx['insert_message'](conn, uid=uid, direction='out', author_kind='agent',
                                  agent=name, project=project, kind='text', text=caption,
                                  media_path=meta_path.name, media_name=meta_path.name,
                                  media_mime='application/json', scope='direct')
            for descriptor, path in zip(descriptors, paths):
                ctx['insert_message'](conn, uid=descriptor['uid'], direction='out', author_kind='agent',
                                      agent=name, project=project, kind='video', text=descriptor['label'],
                                      media_path=path.name, media_name=descriptor['name'],
                                      media_mime='video/mp4', media_size=descriptor['size'],
                                      reply_to_uid=uid, scope='direct')
            conn.commit()
        finally:
            conn.close()
        media = [{'type':'video','media':f'attach://v{index}','supports_streaming':True,
                  'caption':f'🤖 {name} · 📦 {project}\nВариант {label}\n{caption}'[:1024]}
                 for index,label in enumerate(labels)]
        keyboard = {'inline_keyboard': [[{'text': 'Выбрать ' + label,
                                         'callback_data': f'q:{uid}:{index+1}'}
                                        for index, label in enumerate(labels)]]}
        opened = [path.open('rb') for path in paths]
        try:
            album = await ctx['tg_call']('sendMediaGroup', data={
                'chat_id': ctx['CHAT_ID'], 'media': json.dumps(media, ensure_ascii=False)},
                files={f'v{i}': (descriptor['name'], stream, 'video/mp4')
                       for i, (descriptor, stream) in enumerate(zip(descriptors, opened))}, timeout=300.0)
        finally:
            for stream in opened:
                stream.close()
        if not isinstance(album,list) or len(album)!=3 or any(not m.get('video',{}).get('file_id') for m in album):
            raise HTTPException(502,'Telegram did not confirm three real video attachments; no automatic resend')
        if any(m.get('chat',{}).get('id')!=ctx['CHAT_ID'] for m in album):
            raise HTTPException(502,'Actual recipient differs from configured DEV destination')
        metadata.update(state='album_sent',telegram_album_message_ids=[m['message_id'] for m in album],
                        telegram_media_group_id=album[0].get('media_group_id'),
                        telegram_videos=[{'message_id':m['message_id'],**{k:m['video'].get(k) for k in
                                         ['file_id','file_unique_id','file_size','width','height','duration']}} for m in album])
        meta_path.write_text(json.dumps(metadata,ensure_ascii=False),encoding='utf-8')
        sent=await ctx['tg_call']('sendMessage',data={
            'chat_id':ctx['CHAT_ID'],'text':f'🤖 {name} · 📦 {project}\n{caption}\nВыбор для трёх видео в альбоме: A / B / C.',
            'reply_parameters':json.dumps({'message_id':album[0]['message_id']}),
            'reply_markup':json.dumps(keyboard,ensure_ascii=False)})
        metadata.update({'state': 'sent', 'telegram_message_id': sent.get('message_id'),
                         'telegram_returned_keyboard': sent.get('reply_markup')})
        meta_path.write_text(json.dumps(metadata, ensure_ascii=False), encoding='utf-8')
        conn = ctx['db']()
        try:
            conn.execute('UPDATE messages SET tg_chat_id=?, tg_message_id=? WHERE uid=?',
                         (ctx['CHAT_ID'], sent.get('message_id'), uid))
            for descriptor,message in zip(descriptors,album):
                conn.execute('UPDATE messages SET tg_chat_id=?, tg_message_id=? WHERE uid=?',
                             (ctx['CHAT_ID'],message['message_id'],descriptor['uid']))
            conn.commit()
        finally:
            conn.close()
        return JSONResponse({'ok': True, **metadata})

    ctx['_youtube_video_album_choices_installed'] = True

