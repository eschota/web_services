"""Issue identity grants only to OneClick after the existing AutoRig Google login."""
import base64
import hashlib
import hmac
import json
from pathlib import Path
import re
import time
from urllib.parse import urlencode

from fastapi import Depends, HTTPException
from fastapi.responses import RedirectResponse


def install(app, current_user):
    @app.get('/auth/oneclick')
    async def oneclick_login(state: str, user=Depends(current_user)):
        if not re.fullmatch(r'[A-Za-z0-9_-]{40,100}',state):
            raise HTTPException(400, 'Invalid login state')
        if not user:
            return RedirectResponse('/auth/login?' + urlencode({'next':'/auth/oneclick?' + urlencode({'state':state})}),status_code=302)
        key=Path('/srv/autorig/secrets/oneclick-login.key').read_bytes()
        payload={'iss':'autorig.online','aud':'oneclick3d.xyz','state':state,'exp':int(time.time())+120,'email':user.email,'name':user.name,'picture':user.picture}
        body=base64.urlsafe_b64encode(json.dumps(payload,separators=(',',':')).encode()).rstrip(b'=')
        signature=hmac.new(key,body,hashlib.sha256).hexdigest()
        return RedirectResponse('https://oneclick3d.xyz/auth/callback?' + urlencode({'grant':body.decode()+'.'+signature}),status_code=302,headers={'Cache-Control':'no-store','Referrer-Policy':'no-referrer'})
