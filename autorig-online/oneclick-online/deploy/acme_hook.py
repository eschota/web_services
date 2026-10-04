"""Certbot DNS hooks via WAY's authenticated Namecheap gateway; preserves zone."""
import json
import os
from pathlib import Path
import shlex
import sys
import time
import urllib.request

values={}
for line in Path('/etc/qwertystock_domain_api.env').read_text().splitlines():
    if '=' in line and not line.lstrip().startswith('#'):
        key,value=line.split('=',1)
        values[key.strip()]=shlex.split(value)[0] if value.strip() else ''
def call(path,data=None):
    req=urllib.request.Request('http://127.0.0.1:8095'+path,headers={'X-Qwertystock-Domain-Token':values['API_TOKEN'],'Content-Type':'application/json'},data=json.dumps(data).encode() if data is not None else None)
    return json.load(urllib.request.urlopen(req,timeout=60))
domain='oneclick3d.xyz'
name='_acme-challenge'+('.www' if os.environ['CERTBOT_DOMAIN'].startswith('www.') else '')
record={'name':name,'type':'TXT','value':os.environ['CERTBOT_VALIDATION'],'ttl':300}
operation='delete' if '--cleanup' in sys.argv else 'records'
change={operation:[record]}
preview=call('/domains/'+domain+'/records/preview',change)
if operation=='delete' and not preview['ok']:
    sys.exit(0)
assert preview['ok'], 'DNS preview conflicts'
change.update(apply=True,zone_hash=preview['zone_hash'])
result=call('/domains/'+domain+'/records/apply?apply=true',change)
assert result['ok']
print(operation+' '+name+' through verified zone merge')
if operation=='records':
    time.sleep(45)
