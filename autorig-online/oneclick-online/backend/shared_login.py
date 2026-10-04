"""Validate a short-lived AutoRig identity grant and create a separate session."""
import base64
import hashlib
import hmac
import json
from pathlib import Path
import secrets
import time
from urllib.parse import urlencode

from fastapi import Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from auth import create_session, get_or_create_user
from database import get_db


def install(app):
    app.router.routes[:] = [r for r in app.router.routes if getattr(r,'path',None) not in ['/auth/login','/auth/callback']]
    @app.get('/auth/login')
    async def login(next: str='/'):
        if not next.startswith('/') or next.startswith('//') or '\\' in next:
            next='/'
        state=secrets.token_urlsafe(32)
        response=RedirectResponse('https://autorig.online/auth/oneclick?' + urlencode({'state':state}),status_code=302)
        response.set_cookie('oneclick_login_state',state,max_age=300,secure=True,httponly=True,samesite='lax')
        response.set_cookie('auth_next',next,max_age=300,secure=True,httponly=True,samesite='lax')
        response.headers['Cache-Control']='no-store'
        return response

    @app.get('/auth/callback')
    async def callback(request: Request, grant: str, db=Depends(get_db)):
        if len(grant)>8192:
            raise HTTPException(400,'Invalid login grant')
        try:
            body,signature=grant.rsplit('.',1)
            key=Path('/srv/oneclick/secrets/login.key').read_bytes()
            expected=hmac.new(key,body.encode(),hashlib.sha256).hexdigest()
            if not hmac.compare_digest(expected,signature):
                raise ValueError('signature')
            data=json.loads(base64.urlsafe_b64decode(body + '='*((-len(body))%4)))
            state=request.cookies.get('oneclick_login_state','')
            if not state or not hmac.compare_digest(data['state'],state) or data['iss']!='autorig.online' or data['aud']!='oneclick3d.xyz' or not 0 <= data['exp']-time.time() <= 120:
                raise ValueError('state/expiry/audience')
            if not data.get('email'):
                raise ValueError('email')
        except (ValueError,KeyError,TypeError):
            raise HTTPException(400,'Login confirmation expired or invalid')
        user=await get_or_create_user(db,email=data['email'],name=data.get('name'),picture=data.get('picture'))
        session=await create_session(db,user.id)
        next=request.cookies.get('auth_next','/')
        if not next.startswith('/') or next.startswith('//') or '\\' in next:
            next='/'
        response=RedirectResponse(next,status_code=302)
        response.set_cookie('session',session,max_age=30*24*60*60,httponly=True,secure=True,samesite='lax')
        response.delete_cookie('oneclick_login_state')
        response.delete_cookie('auth_next')
        response.headers['Cache-Control']='no-store'
        response.headers['Referrer-Policy']='no-referrer'
        return response
