"""One-shot sender using the existing service runtime; no service restart or token export."""
import argparse
import asyncio
import json
import re
import sys
import importlib.util
from pathlib import Path

from fastapi import UploadFile

sys.path.insert(0, '/srv/autorig/devbot')
import app as current
spec=importlib.util.spec_from_file_location('youtube_video_variants_staged',Path(__file__).with_name('video_variants.py'))
video_variants=importlib.util.module_from_spec(spec)
spec.loader.exec_module(video_variants)


async def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--request-file',required=True)
    arguments=parser.parse_args()
    args=argparse.Namespace(**json.loads(Path(arguments.request_file).read_text(encoding='utf-8')))
    if not re.fullmatch('[0-9a-f]{12}',args.uid):
        raise ValueError('Invalid request ID')
    meta=Path(current.MEDIA_DIR)/(args.uid+'.variants.json')
    if meta.exists():
        existing=json.loads(meta.read_text(encoding='utf-8'))
        if existing['state']=='sent':
            print(json.dumps({'ok':True,**existing},ensure_ascii=False))
            return
        raise RuntimeError('Unknown prior send outcome; reconcile before retry')
    current.init_storage()
    original_uid=current.new_uid
    first=True

    def request_uid():
        nonlocal first
        if first:
            first=False
            return args.uid
        return original_uid()

    current.new_uid=request_uid
    video_variants.install(current.app,vars(current))
    endpoint=next(r.endpoint for r in current.app.routes if r.path=='/dev/api/youtube_video_album_choices')
    streams=[Path(p).open('rb') for p in args.files]
    try:
        response=await endpoint(agent=args.agent,project=args.project,caption=args.caption,
                                files=[UploadFile(filename=Path(p).name,file=s) for p,s in zip(args.files,streams)])
        print(response.body.decode('utf-8'))
    finally:
        for s in streams:
            s.close()


if __name__=='__main__':
    try:
        asyncio.run(main())
    except Exception as error:
        # Do not print transport exceptions containing the configured bot URL/token.
        detail=str(getattr(error,'detail',''))
        token=getattr(current,'BOT_TOKEN','')
        if token:
            detail=detail.replace(token,'[REDACTED]')
        detail=re.sub(r'bot[0-9]+:[A-Za-z0-9_-]+','bot[REDACTED]',detail)
        print(json.dumps({'ok':False,'error_type':type(error).__name__,
                          'status':getattr(error,'status_code',None),'detail':detail[:400]}))
        raise SystemExit(1)
