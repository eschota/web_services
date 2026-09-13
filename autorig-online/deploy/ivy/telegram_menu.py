"""Inspect or configure the existing AutoRig bot menu. Run on WAY; no secrets printed."""
import argparse,json,urllib.request
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('--apply',action='store_true');a=p.parse_args()
values={}
for line in Path('/srv/autorig/secrets/telegram.env').read_text().splitlines():
 if '=' in line and not line.lstrip().startswith('#'):
  key,value=line.split('=',1);values[key.strip()]=value.strip().strip('"').strip("'")
token=next((v for k,v in values.items() if 'TOKEN' in k and ('TELEGRAM' in k or 'BOT' in k)),None)
if not token:raise SystemExit('No configured bot token found')
def call(method,data=None):
 request=urllib.request.Request('https://api.telegram.org/bot'+token+'/'+method,data=json.dumps(data).encode() if data else None,headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(request,timeout=30) as r:answer=json.load(r)
 if not answer.get('ok'):raise RuntimeError(method+' failed')
 return answer['result']
me=call('getMe');menu=call('getChatMenuButton')
print(json.dumps({'username':me.get('username'),'menu':menu}))
if a.apply:
 if menu.get('type')=='web_app' and '/ivy/' not in menu.get('web_app',{}).get('url',''):
  raise SystemExit('Existing Mini App menu belongs to another function; add a dedicated bot command instead')
 backup=Path('/srv/autorig/webgl-assets/ivy/deploy/telegram-menu-before.json');backup.parent.mkdir(parents=True,exist_ok=True)
 if not backup.exists():backup.write_text(json.dumps(menu,indent=2))
 call('setChatMenuButton',{'menu_button':{'type':'web_app','text':'🌿 Ivy Demo','web_app':{'url':'https://autorig.online/ivy/'}}})
 print(json.dumps({'configured':call('getChatMenuButton'),'username':me.get('username')}))
