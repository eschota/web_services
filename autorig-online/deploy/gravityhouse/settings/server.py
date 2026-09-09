#!/usr/bin/env python3
"""Public, bounded game-control settings. No access to AutoRig data or accounts."""
import datetime,json,math,os,tempfile,threading,time
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
try:import fcntl
except ImportError:fcntl=None

class Conflict(Exception):pass
class Store:
    def __init__(self,directory,schema):
        self.directory=Path(directory);self.directory.mkdir(parents=True,exist_ok=True)
        self.path=self.directory/'settings.json';self.fields={f['key']:f for f in schema['fields']}
        self.defaults={key:f['default'] for key,f in self.fields.items()};self.thread_lock=threading.RLock()
        with self.lock():
            if not self.path.exists():self.atomic(self.path,self.record(self.defaults,1))
    def lock(self):
        class Lock:
            def __enter__(inner):
                self.thread_lock.acquire();inner.file=(self.directory/'settings.lock').open('a+')
                if fcntl:fcntl.flock(inner.file,fcntl.LOCK_EX)
            def __exit__(inner,*args):
                if fcntl:fcntl.flock(inner.file,fcntl.LOCK_UN)
                inner.file.close();self.thread_lock.release()
        return Lock()
    def record(self,settings,revision):return {'schemaVersion':1,'revision':revision,'updatedAt':datetime.datetime.now(datetime.timezone.utc).isoformat(),'settings':dict(settings)}
    def atomic(self,path,data):
        fd,temp=tempfile.mkstemp(prefix='.settings-',dir=self.directory)
        try:
            with os.fdopen(fd,'w',encoding='utf-8',newline='\n') as f:
                json.dump(data,f,ensure_ascii=False,allow_nan=False,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
                if hasattr(os,'fchmod'):os.fchmod(f.fileno(),0o644)
            os.replace(temp,path)
        finally:
            if os.path.exists(temp):os.unlink(temp)
    def read(self):return json.loads(self.path.read_text(encoding='utf-8'))
    def validate(self,settings):
        if not isinstance(settings,dict) or set(settings)!=set(self.fields):raise ValueError('Передайте все поля настроек из текущей схемы.')
        clean={}
        for key,field in self.fields.items():
            value=settings[key]
            if field['type']=='boolean':
                if type(value) is not bool:raise ValueError(key+': требуется переключатель да/нет.')
            elif type(value) not in (int,float) or not math.isfinite(value) or not field['min']<=value<=field['max']:raise ValueError(key+': значение вне допустимого диапазона.')
            clean[key]=value
        return clean
    def save(self,settings,revision):
        clean=self.validate(settings)
        with self.lock():
            current=self.read()
            if type(revision) is not int or revision!=current['revision']:raise Conflict('Настройки на сервере уже изменились. Загрузите актуальные значения.')
            result=self.record(clean,current['revision']+1)
            self.atomic(self.directory/'previous.json',current);self.atomic(self.path,result);return result

class Handler(BaseHTTPRequestHandler):
    store=None
    rate={};rate_lock=threading.Lock()
    def log_message(self,format,*args):pass
    def reply(self,status,data):
        body=json.dumps(data,ensure_ascii=False,allow_nan=False).encode('utf-8')
        self.send_response(status);self.send_header('Content-Type','application/json; charset=utf-8');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(body)));self.end_headers();self.wfile.write(body)
    def do_GET(self):
        if self.path=='/gravityhouse/api/health':self.reply(200,{'status':'ok'});return
        if self.path=='/gravityhouse/api/settings':self.reply(200,self.store.read());return
        self.reply(404,{'error':'Not found'})
    def do_POST(self):
        if self.path not in ('/gravityhouse/api/settings','/gravityhouse/api/settings/reset'):self.reply(404,{'error':'Not found'});return
        origin=self.headers.get('Origin')
        if origin and origin not in ('https://autorig.online','https://www.autorig.online'):self.reply(403,{'error':'Недопустимый источник запроса.'});return
        if self.headers.get('Content-Type','').split(';')[0]!='application/json':self.reply(415,{'error':'Нужен application/json.'});return
        try:
            size=int(self.headers.get('Content-Length','0'))
            if size<=0 or size>8192:raise ValueError('Недопустимый размер запроса.')
            ip=self.headers.get('X-Real-IP',self.client_address[0]);now=time.monotonic()
            with self.rate_lock:
                recent=[t for t in self.rate.get(ip,[]) if now-t<60]
                if len(recent)>=60:self.reply(429,{'error':'Слишком частое сохранение. Подождите немного.'});return
                if len(self.rate)>4096:self.rate.clear()
                self.rate[ip]=recent+[now]
            self.connection.settimeout(10);body=json.loads(self.rfile.read(size))
            if not isinstance(body,dict):raise ValueError('Ожидается JSON-объект.')
            values=self.store.defaults if self.path.endswith('/reset') else body.get('settings')
            self.reply(200,self.store.save(values,body.get('revision')))
        except Conflict as e:self.reply(409,{'error':str(e)})
        except (ValueError,KeyError,TypeError) as e:self.reply(400,{'error':str(e)})
        except Exception:self.reply(500,{'error':'Не удалось сохранить настройки.'})

def main():
    schema=json.loads(Path(__file__).with_name('schema.json').read_text(encoding='utf-8'))
    Handler.store=Store(os.environ.get('SETTINGS_DIRECTORY','/srv/autorig/webgl-assets/gravityhouse/data'),schema)
    ThreadingHTTPServer(('127.0.0.1',int(os.environ.get('PORT','8263'))),Handler).serve_forever()
if __name__=='__main__':main()
