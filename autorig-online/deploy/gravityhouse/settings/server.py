#!/usr/bin/env python3
"""Public, bounded game-control settings and shared level documents. No access to AutoRig data or accounts."""
import datetime,hmac,json,math,os,re,tempfile,threading,time
from pathlib import Path
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
try:import fcntl
except ImportError:fcntl=None

class Conflict(Exception):pass
class NotFound(Exception):pass
class Store:
    def __init__(self,directory,schema,filename='settings.json'):
        self.directory=Path(directory);self.directory.mkdir(parents=True,exist_ok=True)
        if filename not in ('settings.json','graphics-settings.json'):raise ValueError('Invalid store namespace')
        self.path=self.directory/filename;self.fields={f['key']:f for f in schema['fields']}
        self.lock_path=self.directory/('settings.lock' if filename=='settings.json' else 'graphics-settings.lock')
        self.previous_path=self.directory/('previous.json' if filename=='settings.json' else 'graphics-previous.json')
        self.defaults={key:f['default'] for key,f in self.fields.items()};self.thread_lock=threading.RLock()
        with self.lock():
            if not self.path.exists():self.atomic(self.path,self.record(self.defaults,1))
            elif filename=='graphics-settings.json':
                current=self.read();migrated={k:current['settings'].get(k,v) for k,v in self.defaults.items()}
                self.validate(migrated)
                if migrated!=current['settings']:
                    self.atomic(self.previous_path,current);self.atomic(self.path,self.record(migrated,current['revision']+1))
    def lock(self):
        class Lock:
            def __enter__(inner):
                self.thread_lock.acquire();inner.file=self.lock_path.open('a+')
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
            if field.get('optionValues') and value not in field['optionValues']:raise ValueError(key+': неизвестный вариант.')
            if field['type']!='boolean' and field.get('step')==1 and value!=int(value):raise ValueError(key+': требуется целое число.')
            clean[key]=value
        return clean
    def save(self,settings,revision):
        clean=self.validate(settings)
        with self.lock():
            current=self.read()
            if type(revision) is not int or revision!=current['revision']:raise Conflict('Настройки на сервере уже изменились. Загрузите актуальные значения.')
            result=self.record(clean,current['revision']+1)
            self.atomic(self.previous_path,current);self.atomic(self.path,result);return result

LEVEL_ID=re.compile(r'^[A-Za-z0-9_-]{1,64}$')
DRAFT_ID=re.compile(r'^[A-Za-z0-9_-]{8,64}$')
LEVEL_FORMAT='little-tilt.level'
LEVEL_BYTES=1048576
LEVEL_HISTORY=20
DRAFTS_PER_LEVEL=200
class Forbidden(Exception):pass
class LevelStore:
    """Level documents authored in the game's own editor, under data/levels.

    Two namespaces with different write policies:
    * `<id>.json` — the official level every player loads in Game mode and the game repository tracks in git.
      Publishing requires the developer write token (X-Level-Token); each accepted publish keeps the replaced
      record in history/<id>-<revision>.json (last LEVEL_HISTORY) so a shared level can be restored.
    * `drafts/<id>/<draftId>.json` — public player drafts written by the in-game editor (autosave), never read by
      Game mode and never auto-promoted; a developer publishes a draft explicitly. Oldest drafts are evicted
      beyond DRAFTS_PER_LEVEL per level.
    The server bounds size and shape; the game validates plant recipes on load."""
    def __init__(self,directory,token=None):
        self.directory=Path(directory)/'levels';self.history=self.directory/'history';self.drafts=self.directory/'drafts'
        self.history.mkdir(parents=True,exist_ok=True);self.drafts.mkdir(parents=True,exist_ok=True)
        self.lock_path=self.directory/'levels.lock';self.thread_lock=threading.RLock();self.token=token or None
    def lock(self):
        class Lock:
            def __enter__(inner):
                self.thread_lock.acquire();inner.file=self.lock_path.open('a+')
                if fcntl:fcntl.flock(inner.file,fcntl.LOCK_EX)
            def __exit__(inner,*args):
                if fcntl:fcntl.flock(inner.file,fcntl.LOCK_UN)
                inner.file.close();self.thread_lock.release()
        return Lock()
    def atomic(self,path,data):
        path.parent.mkdir(parents=True,exist_ok=True);fd,temp=tempfile.mkstemp(prefix='.level-',dir=path.parent)
        try:
            with os.fdopen(fd,'w',encoding='utf-8',newline='\n') as f:
                json.dump(data,f,ensure_ascii=False,allow_nan=False,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
                if hasattr(os,'fchmod'):os.fchmod(f.fileno(),0o644)
            os.replace(temp,path)
        finally:
            if os.path.exists(temp):os.unlink(temp)
    @staticmethod
    def check_id(level_id):
        if type(level_id) is not str or not LEVEL_ID.fullmatch(level_id):raise ValueError('Недопустимый идентификатор уровня.')
        return level_id
    @staticmethod
    def check_draft_id(draft_id):
        if type(draft_id) is not str or not DRAFT_ID.fullmatch(draft_id):raise ValueError('Недопустимый идентификатор черновика.')
        return draft_id
    @staticmethod
    def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
    @staticmethod
    def load(path):return json.loads(path.read_text(encoding='utf-8'))
    def path(self,level_id):return self.directory/(self.check_id(level_id)+'.json')
    def record(self,level_id,document,revision):return {'schemaVersion':1,'levelId':level_id,'revision':revision,'updatedAt':self.now(),'document':document}
    def read(self,level_id):
        path=self.path(level_id)
        return self.load(path) if path.exists() else None
    def require(self,level_id):
        record=self.read(level_id)
        if record is None:raise NotFound('Уровень ещё не опубликован на сервере.')
        return record
    @staticmethod
    def summary(record):
        document=record.get('document') or {};ivy=document.get('ivy') if isinstance(document.get('ivy'),dict) else {}
        plants=ivy.get('plants') if isinstance(ivy.get('plants'),list) else []
        item={'levelId':record['levelId'],'revision':record['revision'],'updatedAt':record['updatedAt'],'title':document.get('title',''),'plants':len(plants),'bytes':len(json.dumps(document,ensure_ascii=False))}
        if 'draftId' in record:item['draftId']=record['draftId'];item['author']=record.get('author','')
        return item
    def index(self):
        levels=[]
        for path in sorted(self.directory.glob('*.json')):
            if not LEVEL_ID.fullmatch(path.stem):continue
            try:levels.append(self.summary(self.load(path)))
            except (ValueError,KeyError,TypeError):continue
        return {'schemaVersion':1,'levels':levels,'publishing':bool(self.token)}
    def validate(self,level_id,document):
        if not isinstance(document,dict):raise ValueError('Ожидается документ уровня (JSON-объект).')
        if document.get('format')!=LEVEL_FORMAT or document.get('version')!=1:raise ValueError('Неподдерживаемый формат документа уровня.')
        if document.get('levelId')!=level_id:raise ValueError('levelId документа не совпадает с адресом.')
        title=document.get('title','')
        if type(title) is not str or len(title)>120:raise ValueError('title: строка до 120 символов.')
        ivy=document.get('ivy')
        if ivy is not None:
            if not isinstance(ivy,dict) or not isinstance(ivy.get('plants'),list):raise ValueError('ivy: ожидается объект со списком plants.')
            if len(ivy['plants'])>1000:raise ValueError('ivy: не более 1000 растений.')
            if not all(isinstance(p,dict) for p in ivy['plants']):raise ValueError('ivy: каждое растение — объект.')
        layers=document.get('layers',[])
        if not isinstance(layers,list) or len(layers)>64:raise ValueError('layers: список до 64 слоёв.')
        for layer in layers:
            if not isinstance(layer,dict) or type(layer.get('typeId'))is not str or not 0<len(layer['typeId'])<=64:raise ValueError('layers: каждому слою нужен typeId до 64 символов.')
        encoded=json.dumps(document,ensure_ascii=False,allow_nan=False)
        if len(encoded.encode('utf-8'))>LEVEL_BYTES:raise ValueError('Документ уровня превышает 1 МБ.')
        return document
    def authorize(self,token):
        if not self.token:raise Forbidden('Публикация уровней не настроена на сервере.')
        if type(token) is not str or not hmac.compare_digest(token.encode('utf-8'),self.token.encode('utf-8')):raise Forbidden('Неверный токен публикации.')
    def publish(self,level_id,document,revision,token):
        self.authorize(token);path=self.path(level_id);clean=self.validate(level_id,document)
        with self.lock():
            current=self.read(level_id);expected=current['revision'] if current else 0
            if type(revision) is not int or revision!=expected:raise Conflict('Уровень на сервере уже изменился. Загрузите актуальную версию.')
            result=self.record(level_id,clean,expected+1)
            if current:
                self.atomic(self.history/(level_id+'-'+str(current['revision'])+'.json'),current)
                for old in sorted(self.history.glob(level_id+'-*.json'),key=lambda p:int(p.stem.rsplit('-',1)[1]))[:-LEVEL_HISTORY]:old.unlink(missing_ok=True)
            self.atomic(path,result);return result
    def history_list(self,level_id):
        self.check_id(level_id);entries=[]
        for path in sorted(self.history.glob(level_id+'-*.json'),key=lambda p:int(p.stem.rsplit('-',1)[1]),reverse=True):
            try:record=self.load(path);entries.append({'revision':record['revision'],'updatedAt':record['updatedAt'],'bytes':path.stat().st_size})
            except (ValueError,KeyError,TypeError):continue
        return {'levelId':level_id,'history':entries}
    def history_read(self,level_id,revision):
        self.check_id(level_id);path=self.history/(level_id+'-'+str(int(revision))+'.json')
        if not path.exists():raise NotFound('Такой ревизии нет в истории.')
        return self.load(path)
    # --- player drafts -------------------------------------------------------------------------
    def draft_path(self,level_id,draft_id):return self.drafts/self.check_id(level_id)/(self.check_draft_id(draft_id)+'.json')
    def draft_read(self,level_id,draft_id):
        path=self.draft_path(level_id,draft_id)
        return self.load(path) if path.exists() else None
    def draft_require(self,level_id,draft_id):
        record=self.draft_read(level_id,draft_id)
        if record is None:raise NotFound('Черновик не найден.')
        return record
    def draft_index(self,level_id):
        folder=self.drafts/self.check_id(level_id);drafts=[]
        if folder.exists():
            for path in folder.glob('*.json'):
                if not DRAFT_ID.fullmatch(path.stem):continue
                try:drafts.append(self.summary(self.load(path)))
                except (ValueError,KeyError,TypeError):continue
        drafts.sort(key=lambda d:d['updatedAt'],reverse=True)
        return {'levelId':level_id,'drafts':drafts}
    def draft_save(self,level_id,draft_id,document,revision,author=''):
        path=self.draft_path(level_id,draft_id);clean=self.validate(level_id,document)
        if author is None:author=''
        if type(author) is not str or len(author)>60:raise ValueError('author: строка до 60 символов.')
        with self.lock():
            current=self.draft_read(level_id,draft_id);expected=current['revision'] if current else 0
            if type(revision) is not int or revision!=expected:raise Conflict('Черновик на сервере уже изменился. Загрузите актуальную версию.')
            result=dict(self.record(level_id,clean,expected+1),draftId=draft_id,author=author,createdAt=current['createdAt'] if current else self.now())
            self.atomic(path,result)
            others=[p for p in path.parent.glob('*.json') if p!=path and DRAFT_ID.fullmatch(p.stem)]
            if len(others)+1>DRAFTS_PER_LEVEL:
                for old in sorted(others,key=lambda p:p.stat().st_mtime)[:len(others)+1-DRAFTS_PER_LEVEL]:old.unlink(missing_ok=True)
            return result

LEVEL_ROUTE=re.compile(r'^/gravityhouse/api/levels(?:/(?P<id>[A-Za-z0-9_-]{1,64})(?:/(?P<kind>history|drafts)(?:/(?P<sub>[A-Za-z0-9_-]{1,64}))?)?)?$')
class Handler(BaseHTTPRequestHandler):
    store=None;graphics=None;graphics_schema=None;levels=None
    graphics_origins=('https://autorig.online','https://www.autorig.online','http://127.0.0.1:8124','http://localhost:8124')
    def is_graphics(self):return self.path in ('/gravityhouse/api/graphics','/gravityhouse/api/graphics/reset','/gravityhouse/api/graphics/schema')
    def is_levels(self):return self.path.startswith('/gravityhouse/api/levels')
    # Graphics and level routes are called from the web workshop, the WebGL game and local dev pages; controls stay same-origin only.
    def cors_route(self):return self.is_graphics() or self.is_levels()
    rate={};rate_lock=threading.Lock()
    def log_message(self,format,*args):pass
    def reply(self,status,data):
        body=json.dumps(data,ensure_ascii=False,allow_nan=False).encode('utf-8')
        self.send_response(status);self.send_header('Content-Type','application/json; charset=utf-8');self.send_header('Cache-Control','no-store');self.send_header('Content-Length',str(len(body)))
        if self.cors_route() and self.headers.get('Origin') in self.graphics_origins:
            self.send_header('Access-Control-Allow-Origin',self.headers['Origin']);self.send_header('Vary','Origin')
            self.send_header('Access-Control-Allow-Methods','GET, POST, OPTIONS');self.send_header('Access-Control-Allow-Headers','Content-Type, X-Level-Token')
        self.end_headers();self.wfile.write(body)
    def do_OPTIONS(self):
        if not self.cors_route() or self.headers.get('Origin') not in self.graphics_origins:self.reply(403,{'error':'Origin not allowed'});return
        self.reply(200,{'status':'ok'})
    def level_route(self):
        match=LEVEL_ROUTE.fullmatch(self.path)
        if not match or not self.levels:return None
        route=match.groupdict()
        if route['kind']=='history' and route['sub'] is not None and not route['sub'].isdigit():return None
        if route['kind']=='drafts' and route['sub'] is not None and not DRAFT_ID.fullmatch(route['sub']):return None
        return route
    def do_GET(self):
        if self.path=='/gravityhouse/api/health':self.reply(200,{'status':'ok'});return
        if self.path=='/gravityhouse/api/settings':self.reply(200,self.store.read());return
        if self.path=='/gravityhouse/api/graphics':self.reply(200,self.graphics.read());return
        if self.path=='/gravityhouse/api/graphics/schema':self.reply(200,self.graphics_schema);return
        route=self.level_route()
        if route:
            level_id,kind,sub=route['id'],route['kind'],route['sub']
            try:
                if level_id is None:self.reply(200,self.levels.index())
                elif kind is None:self.reply(200,self.levels.require(level_id))
                elif kind=='history':self.reply(200,self.levels.history_list(level_id) if sub is None else self.levels.history_read(level_id,int(sub)))
                else:self.reply(200,self.levels.draft_index(level_id) if sub is None else self.levels.draft_require(level_id,sub))
            except NotFound as e:self.reply(404,{'error':str(e)})
            except (ValueError,KeyError,TypeError) as e:self.reply(400,{'error':str(e)})
            except Exception:self.reply(500,{'error':'Не удалось прочитать уровень.'})
            return
        self.reply(404,{'error':'Not found'})
    def do_POST(self):
        route=self.level_route();level_write=None
        if route and route['id'] and (route['kind'] is None or (route['kind']=='drafts' and route['sub'])):level_write=route
        if level_write is None and self.path not in ('/gravityhouse/api/settings','/gravityhouse/api/settings/reset','/gravityhouse/api/graphics','/gravityhouse/api/graphics/reset'):self.reply(404,{'error':'Not found'});return
        origin=self.headers.get('Origin')
        allowed=self.graphics_origins if self.cors_route() else ('https://autorig.online','https://www.autorig.online')
        if origin and origin not in allowed:self.reply(403,{'error':'Недопустимый источник запроса.'});return
        store=self.graphics if self.is_graphics() else self.store
        if self.headers.get('Content-Type','').split(';')[0]!='application/json':self.reply(415,{'error':'Нужен application/json.'});return
        try:
            size=int(self.headers.get('Content-Length','0'))
            limit=LEVEL_BYTES+65536 if level_write else 131072 if self.is_graphics() else 8192
            if size<=0 or size>limit:raise ValueError('Недопустимый размер запроса.')
            ip=self.headers.get('X-Real-IP',self.client_address[0]);now=time.monotonic()
            with self.rate_lock:
                recent=[t for t in self.rate.get(ip,[]) if now-t<60]
                if len(recent)>=60:self.reply(429,{'error':'Слишком частое сохранение. Подождите немного.'});return
                if len(self.rate)>4096:self.rate.clear()
                self.rate[ip]=recent+[now]
            self.connection.settimeout(10);body=json.loads(self.rfile.read(size))
            if not isinstance(body,dict):raise ValueError('Ожидается JSON-объект.')
            if level_write:
                if level_write['kind']=='drafts':self.reply(200,self.levels.draft_save(level_write['id'],level_write['sub'],body.get('document'),body.get('revision'),body.get('author','')))
                else:self.reply(200,self.levels.publish(level_write['id'],body.get('document'),body.get('revision'),self.headers.get('X-Level-Token')))
                return
            values=store.defaults if self.path.endswith('/reset') else body.get('settings')
            self.reply(200,store.save(values,body.get('revision')))
        except Conflict as e:self.reply(409,{'error':str(e)})
        except Forbidden as e:self.reply(403,{'error':str(e)})
        except (ValueError,KeyError,TypeError) as e:self.reply(400,{'error':str(e)})
        except Exception:self.reply(500,{'error':'Не удалось сохранить уровень.' if level_write else 'Не удалось сохранить настройки.'})

def level_token(directory):
    token=os.environ.get('LEVEL_WRITE_TOKEN','').strip()
    if token:return token
    path=Path(directory)/'level-write-token'
    try:return path.read_text(encoding='utf-8').strip() or None if path.exists() else None
    except OSError:return None
def main():
    data=os.environ.get('SETTINGS_DIRECTORY','/srv/autorig/webgl-assets/gravityhouse/data')
    schema=json.loads(Path(__file__).with_name('schema.json').read_text(encoding='utf-8'))
    Handler.store=Store(data,schema)
    Handler.graphics_schema=json.loads(Path(__file__).with_name('graphics-schema.json').read_text(encoding='utf-8'))
    Handler.graphics=Store(data,Handler.graphics_schema,'graphics-settings.json')
    Handler.levels=LevelStore(data,level_token(data))
    ThreadingHTTPServer(('127.0.0.1',int(os.environ.get('PORT','8263'))),Handler).serve_forever()
if __name__=='__main__':main()
