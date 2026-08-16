# AutoRig.online на новом storage-сервере

Актуально на: **2026-08-16**
Назначение: production runbook и контекст для оператора/AI-агента после переноса AutoRig.online.  
Основной репозиторий: `https://github.com/eschota/web_services.git`  
Converter-репозиторий: `https://github.com/eschota/autorig.online.git`

## Коротко

AutoRig.online перенесён со старого VPS `185.171.83.65` на сервер аналитики
`way.qwertystock.com` (`37.187.57.177`) из-за критического дефицита диска и
потери пользовательских скачиваний после очистки worker-артефактов.

Новый production изолирован в `/srv/autorig`. Он использует отдельные порты
`8200` и `8210`, не занимает старые analytics-порты `8000/8011` и не меняет
MongoDB/PostgreSQL. Основные пользовательские файлы теперь собираются в
durable artifact cache и отдаются nginx с HTTP Range `200/206`.

Текущее состояние после migration hotfix:

- production host: `way-fr` / `37.187.57.177`;
- корневой RAID: `/dev/md3`, 878 ГБ, при последней проверке свободно около
  296 ГБ;
- post-migration runtime hotfix: `ced91f2eba0467b2f090b0e2b8d39012609abd69`;
- migration release tag: `v0.02.004`, post-migration stabilization tag:
  `v0.02.007`;
- backend, Renderfin, Telegram, tunnels и nginx активны;
- Hunyuan pool: F1, F2, F11, F13; туннель F7 снова доступен, но F7 не входит
  в основной Hunyuan pool;
- authoritative DNS для `@` и `www`: `37.187.57.177`, TTL 300;
- старый AutoRig остановлен и временно сохранён только как rollback/reference
  source.

## Зачем был перенос

Старый VPS имел корневой диск около 48 ГБ. Перед переносом использование
достигало 95–97%, свободно оставалось 1.8–2.1 ГБ, а GLB cache вырос примерно до
18 ГБ. Worker’ы периодически удаляют свои результаты, поэтому часть GLB/ZIP в
кэше сайта была последней существующей копией. Простая очистка по возрасту
означала бы потерю пользовательских ригов.

Практические симптомы старой архитектуры:

- готовый ZIP не скачивался после исчезновения worker-копии;
- viewer и preview зависели от доступности конкретного worker’а;
- большой ZIP проксировался через один uvicorn worker;
- диск нельзя было безопасно освободить, не удаляя last-copy deliverables;
- Renderfin, uploads, video cache и основной backend конкурировали за один
  маленький root volume.

Новый сервер выбран из-за RAID-массива на 878 ГБ и большого резерва. Цель
переноса — не только увеличить диск, но и сделать storage host самостоятельной
точкой выдачи артефактов.

## Хосты и DNS

| Роль | SSH / host | IP | Состояние |
|---|---|---:|---|
| Новый production | `ssh way-fr` / `way.qwertystock.com` | `37.187.57.177` | active |
| Старый AutoRig VPS | `ssh autorig-vps` | `185.171.83.65` | AutoRig units stopped |
| Jump-host к старому VPS | `freestock-jakarta` | через SSH config | использовать только если прямой маршрут не работает |

DNS не задаёт порт. Namecheap A-записи `@` и `www` указывают на
`37.187.57.177`; nginx нового сервера направляет запросы на локальные порты.
Все MX/TXT/DMARC и остальные записи должны сохраняться без изменений.

Управление зоной выполняется только через:

```text
https://way.qwertystock.com/domain-api/
```

Сервис слушает `127.0.0.1:8095` и требует заголовок
`X-Qwertystock-Domain-Token`; значение хранится в
`/etc/qwertystock_domain_api.env` и никогда не должно выводиться в лог/чат.

Безопасный порядок DNS-изменения:

1. `GET /domains/autorig.online/records`;
2. `POST /domains/autorig.online/records/preview` с одной merge-safe записью;
3. проверить `conflicts`, `added`, `removed` и сохранить `zone_hash`;
4. `POST /domains/autorig.online/records/apply?apply=true` с тем же изменением,
   `apply=true` и проверенным `zone_hash`;
5. API автоматически создаёт backup полной зоны перед `setHosts`;
6. проверить оба authoritative NS, а не один локальный recursive resolver.

Проверка authoritative DNS:

```powershell
Resolve-DnsName www.autorig.online -Type A -Server dns1.registrar-servers.com
Resolve-DnsName www.autorig.online -Type A -Server dns2.registrar-servers.com
```

15 августа оба authoritative NS возвращали новый IP. Один локальный recursive
resolver кратко продолжал возвращать старый адрес даже после истечения малого
TTL — это не было изменением Namecheap-зоны.

## Соседние сервисы, которые нельзя ломать

AutoRig размещён на сервере аналитики, но не заменяет его сервисы.

| Listener | Назначение |
|---:|---|
| `127.0.0.1:8000` | существующий Node/analytics service |
| `127.0.0.1:8011` | существующий Python/analytics service |
| `127.0.0.1:8095` | qwertystock domain API |
| `127.0.0.1:8200` | AutoRig backend |
| `127.0.0.1:8210` | AutoRig Renderfin |
| `127.0.0.1:27017` | MongoDB |
| `127.0.0.1:5432` | PostgreSQL |

До и после любого AutoRig deploy проверять:

```bash
curl -fsS https://way.qwertystock.com/map >/dev/null
ss -ltn | grep -E ':(8000|8011|8095|8200|8210|27017|5432)[[:space:]]'
```

Нельзя менять существующие analytics, qwertystock, MongoDB, PostgreSQL,
Docker или их nginx server blocks в рамках обычного AutoRig deploy.

## Production layout

```text
/srv/autorig/
├── current -> /srv/autorig/releases/<published-commit-sha>
├── releases/
│   └── <immutable release directories>
├── venv/
├── home/
├── secrets/
├── data/
│   ├── db/
│   │   ├── autorig.db
│   │   └── renderfin.db              # migration/reference snapshot
│   ├── artifact-cache/
│   ├── static/
│   │   ├── tasks/
│   │   └── glb_cache/
│   └── var/
│       ├── uploads/
│       ├── videos/
│       ├── preflight-renders/
│       ├── deliverables/
│       ├── animation-library/
│       ├── animation-fitting/
│       └── renderfin/
│           ├── db/renderfin.db       # активная Renderfin DB
│           └── render/
└── monitoring/
```

Важно: активная Renderfin DB —
`/srv/autorig/data/var/renderfin/db/renderfin.db`. Файл
`/srv/autorig/data/db/renderfin.db` является перенесённым snapshot и не должен
использоваться для live-диагностики очереди.

Static directories внутри release связаны с durable data. Не складывать
runtime-файлы внутрь нового immutable release.

## Secrets

Секреты находятся только в `/srv/autorig/secrets`, mode/owner должны оставаться
ограниченными. Актуальные имена:

```text
backend.env
telegram.env
feature-flags.env
migration-mode.env
renderfin.env
storage-host.env
farm-tunnels.conf
renderfin-hunyuan.json
ai_vision_animal_type_detect.json
ssh/renderfin_farm_tunnel
```

Нельзя:

- коммитить эти файлы;
- печатать их содержимое или process environment;
- копировать секреты обратно в local worktree;
- проверять bearer token через публичный `server-status`.

## Systemd

| Unit | Назначение |
|---|---|
| `autorig-storage.service` | FastAPI backend на `127.0.0.1:8200` |
| `autorig-storage-renderfin.service` | Renderfin API/queue на `127.0.0.1:8210` |
| `autorig-storage-telegram.service` | Telegram bot, callbacks, submit/notifications |
| `autorig-storage-tunnels.service` | SSH tunnels к render/converter farm |
| `autorig.slice` | `MemoryHigh=8G`, `MemoryMax=12G` |
| `nginx.service` | TLS, routing, static и X-Accel delivery |

Renderfin запускает Chromium turntable. Его unit обязан содержать:

```ini
Environment="HOME=/srv/autorig/home"
ReadWritePaths=/srv/autorig/data /srv/autorig/home
```

Без этого Chromium внутри `ProtectSystem=strict` падает до CDP с
`chrome_crashpad_handler: --database is required`.

Проверка:

```bash
systemctl is-active \
  autorig-storage.service \
  autorig-storage-renderfin.service \
  autorig-storage-telegram.service \
  autorig-storage-tunnels.service nginx.service

systemctl show autorig-storage-renderfin.service \
  -p MainPID -p ActiveEnterTimestamp -p Environment -p ReadWritePaths
```

После изменения нескольких компонентов порядок запуска:

1. tunnels;
2. Renderfin;
3. backend;
4. Telegram.

Перезапускать только затронутые units, если полный порядок не нужен.

## Nginx и выдача файлов

Активный site: `/etc/nginx/sites-enabled/autorig.online`.

- `/` и `/api/` → `127.0.0.1:8200`;
- `/renderfin/` → `127.0.0.1:8210`;
- `/renderfin/render/` → локальный Renderfin render directory;
- `/_autorig_artifacts/` → internal X-Accel artifact cache;
- `/_autorig_glb_cache/` → internal legacy viewer GLB cache;
- `/_autorig_task_cache/` → internal recovered ZIP/GLB task cache;
- `/u/` → migrated uploads;
- прямой `/static/tasks/` закрыт 404, доступ выдаётся после backend auth.

Backend сначала проверяет owner/admin/purchase, затем возвращает
`X-Accel-Redirect`. Nginx обслуживает большие файлы и byte ranges без занятия
единственного uvicorn worker’а.

Smoke для Range:

```bash
curl -fsS -D - -o /dev/null -H 'Range: bytes=0-15' \
  'https://autorig.online/api/task/<task-id>/prepared.glb'
```

Ожидается `206`, `Content-Range`, `Content-Length: 16`. Для MP4 дополнительно
проверять tail-range/seek.

## Durable artifact cache

Очередь cache worker хранится в SQLite и переживает рестарт backend.

Контракт:

- задача атомарно ставится в очередь после terminal completion;
- максимум два скачивания глобально и одно с конкретного worker’а;
- retry: 30 секунд, 2, 10 и 30 минут, затем каждые 30 минут в пределах суток;
- скачивание идёт 8-МиБ Range-блоками с resume;
- проверяются `Content-Range`, размер, сигнатура файла и ZIP CRC;
- HTML вместо модели отклоняется;
- разрешён только host назначенного worker’а;
- path traversal запрещён;
- публикация выполняется atomic rename;
- viewer/download работает cache-first и временно использует worker fallback.

Для задачи сохраняются полный worker ZIP, UI-доступные URLs, GLB/FBX,
posters, MP4/MOV, viewer-артефакты и JSON manifest с URL, ролью, относительным
путём, размером, SHA-256, ETag/Last-Modified и временем кэширования.

Task API обратно совместимо публикует:

```text
artifact_cache_status
artifact_cache_file_count
artifact_cache_bytes
artifact_cache_full_until
artifact_cache_error
```

Политика:

- первые 24 часа после завершения не удалять ничего;
- soft cap artifact cache: 250 ГБ;
- обязательный резерв: 120 ГБ;
- uploads/video pressure cleanup: не раньше 72 часов;
- ZIP, основной GLB/FBX, viewer GLB и poster — long-lived deliverables;
- последнюю существующую копию автоматически не удалять;
- если резерв нельзя восстановить без удаления last-copy, остановить создание
  новых задач и отправить Telegram alert;
- `AUTOMATIC_TASK_DB_DELETION=0`.

На проверке 15 августа: artifact cache около 24 ГБ, task cache 2 ГБ, GLB cache
18 ГБ, videos 1.6 ГБ, deliverables 17 ГБ.

## Renderfin и генерация 3D

Пользовательский character-generation pipeline:

```text
изображение / prompt
  -> Flux T-pose/A-pose render
  -> Hunyuan3D image-to-3D
  -> GLB publication
  -> local Chromium turntable MP4
  -> AutoRig task submission
  -> rig + animations + cached deliverables
```

Renderfin state и output находятся в
`/srv/autorig/data/var/renderfin`. Публичная база:
`https://autorig.online/renderfin`.

Hunyuan workers читаются из
`/srv/autorig/secrets/renderfin-hunyuan.json`. В production разрешены F1, F2,
F11 и F13. F7 исключён одновременно из Renderfin pool и основного backend
dispatcher до отдельного ремонта и canary.

Health:

```bash
curl -fsS http://127.0.0.1:8210/renderfin/health
```

Ожидаемые поля: `ok=true`, `hunyuan_path=converter-api`, pool без F7.

Bearer нельзя проверять через `server-status`: этот endpoint не валидирует
credential. Правильный credential smoke — POST заведомо неполного body в
`generate-3d`: `400 invalid_request` означает, что token принят; `401/403` —
token устарел.

### Hotfix после миграции

На новом Linux host были найдены и исправлены две turntable-регрессии:

1. Renderfin unit не передавал writable HOME в Chromium sandbox.
2. После успешного MP4 Node иногда возвращал exit 1 из-за гонки очистки
   временного Chrome profile (`ENOTEMPTY`).

Commit `d49035e31eea9ff3aa815f1662a48c5d74e7caab` добавляет HOME в unit,
дожидается завершения Chrome, повторяет удаление profile и не отбрасывает уже
проверенный MP4 из-за ошибки disposable cleanup.

### Стабилизация 2026-08-16

Массовая недоступность farm оказалась не одновременным падением всех worker'ов,
а каскадным рестартом общего SSH tunnel supervisor: потеря одного соединения
завершала unit и роняла остальные шесть исправных туннелей. Начиная с
`a359849a6e9f67a8aa170c1bcf1fbb8fefe50c5f` каждый туннель имеет собственный
reconnect loop с backoff 5–60 секунд. Проверка принудительным завершением только
F7 подтвердила, что F7 восстановился через 5 секунд, PID F1 не изменился, а
systemd unit не перезапускался.

Renderfin Telegram delivery теперь загружает принадлежащие storage host PNG как
multipart-файлы, а не просит Telegram скачать их по публичному URL. Это устраняет
повторяющийся `WEBPAGE_MEDIA_EMPTY`; path traversal и чужие URL не допускаются.

Оба healthcheck unit используют только live Renderfin DB:
`/srv/autorig/data/var/renderfin/db/renderfin.db`. Snapshot в `data/db` больше не
создаёт ложные stalled alerts. Обычные зелёные heartbeat-строки с историческим
`failed=N` не классифицируются как новые ошибки.

Глобальный nginx resolver заменён с отказавшего OpenDNS `208.67.222.222` на
локальный systemd-resolved `127.0.0.53 valid=300s ipv6=off`. Перед изменением
сохранён `/etc/nginx/nginx.conf.before-autorig-ocsp-20260816`; после `nginx -t`
и reload AutoRig, gallery и `way.qwertystock.com/map` отвечали HTTP 200, а
повторяющийся `ocsp.sectigo.com ... Operation refused` прекратился.

### Converter asset drift после migration smoke

Первый end-to-end smoke выявил общий blocker на F1/F2/F11/F13:

```text
ASSET_PREFLIGHT_FAILED:3dsmax-exporter:3dmax/max_export.ms:size_mismatch
```

Manifest уже ожидал canonical файл из converter `origin/main`, но rollout 11
августа оставил старый `max_export.ms` на всех коробках. 15 августа canonical
asset был разложен на F1/F2/F11/F13 из опубликованного converter commit
`58f939378691ea01e4889f25b43fcf9b93abd716` (`v0.02.008`):

```text
size:   21962
sha256: d23fbd733de7b888e9636a1fa414249bf60e6c39d1b729e0713324796368d4b7
```

На каждом worker создан backup, `runtime_asset_preflight --mode standard`
стал `healthy`, затем процесс, реально владевший портом 7000, был заменён и
проверен по новому PID/start time. Копирование файла без проверки listener PID
не считается deploy.

### Live validation 2026-08-15

После hotfix выполнена проверка на свежем Hunyuan output:

- Renderfin health: `ok=true`, pool F1/F2/F11/F13;
- Hunyuan создал GLB размером 11,181,648 байт;
- GLB и turntable MP4 публично отвечали `206` на `bytes=0-15`;
- четыре Renderfin job, остановленные общей Chrome-регрессией, восстановлены и
  дошли до automatic AutoRig submit;
- прямой payload `type=t_pose, mode=only_rig` на F13 был принят после
  исправления asset preflight;
- worker task `7681ea8c-1a9d-47fd-9aa8-aa6d00dd275d` завершился `Completed`;
- `prepared` появился раньше terminal completion;
- prepared/animations viewer GLB отвечают `206`, содержат skin и 16 animation
  clips;
- четыре full-convert task, ошибочно отклонённые до asset repair, перезапущены
  с теми же task ID без списания кредитов.

Эта проверка покрывает Hunyuan, turntable и humanoid rig/animations. Terminal
статусы долгого full-convert/Unity этапа контролируются отдельно migration
monitor’ом и task API.

## Converter farm: обязательные правила

Converter boxes — Windows. Код:

```text
C:\3d\GLB_Convverter_Git\GLB_Convverter_WebServer
```

Для сложных команд создавать `.ps1`, копировать на абсолютный путь
`C:\Users\user\AppData\Local\Temp\...` и запускать с
`-ExecutionPolicy Bypass -File`. Не бороться с тройным shell quoting.

После deploy:

1. проверить очередь и не рестартовать активный worker;
2. вызвать `/api-converter-glb-restart-server` с локальным admin token;
3. `409` означает, что worker занят — повторить позже, не убивать процесс;
4. найти `Get-NetTCPConnection -State Listen -LocalPort 7000`;
5. проверить `OwningProcess`, `StartTime`, build ID и пустую очередь;
6. проверить public status и выполнить реальный smoke.

Blender background может вернуть exit code 0 при Python traceback. Всегда
проверять ожидаемый artifact/manifest и сохранять stdout tail.

Vertex-PBR Hunyuan bake должен использовать Blender 4.3. Blender 5.1 на этом
шаге наблюдался примерно в 30 раз медленнее.

## Deploy web service

Локальная рабочая копия для миграции:

```text
R:\autorig_migration_worktree
branch: codex/autorig-storage-migration
```

Production deploy выполняется только из опубликованного commit SHA.

Запрещено:

- `git pull` на production;
- deploy из грязного checkout;
- редактирование только по SSH без переноса изменения в Git;
- копирование venv, caches, DB, secrets и runtime в release;
- blind replacement nginx/systemd без `nginx -t` или `systemd-analyze verify`;
- менять старые analytics ports.

Безопасный release:

1. `git fetch origin`, чистый worktree от `origin/main`;
2. scoped tests, `git diff --check`;
3. commit и push в защитную ветку;
4. fast-forward/push `main`;
5. создать `/srv/autorig/releases/<exact-sha>`;
6. передать exact files и сравнить SHA-256 с raw published commit;
7. атомарно переключить `/srv/autorig/current`;
8. поставить изменённые systemd/nginx overlays;
9. restart только нужных units;
10. сверить release symlink, PID/start time, listeners, site и Range.

Минимальная production-проверка:

```bash
readlink -f /srv/autorig/current
systemctl is-active autorig-storage autorig-storage-renderfin \
  autorig-storage-telegram autorig-storage-tunnels nginx
curl -fsS http://127.0.0.1:8200/api/gallery?per_page=1\&sort=date >/dev/null
curl -fsS http://127.0.0.1:8210/renderfin/health
curl -fsS https://autorig.online/gallery >/dev/null
curl -fsS https://way.qwertystock.com/map >/dev/null
df -h /srv/autorig
```

## Database safety

Обе SQLite DB копировать через SQLite backup API, а не обычным `cp` во время
работы. Перед cutover/restore выполнять `PRAGMA quick_check` и сравнивать
ожидаемые counts.

Для read-only диагностики активной Renderfin DB использовать URI mode=ro:

```python
sqlite3.connect(
    "file:/srv/autorig/data/var/renderfin/db/renderfin.db?mode=ro",
    uri=True,
)
```

Нельзя удалять task rows для освобождения места.

## Мониторинг после переноса

Первые трое суток после стабилизации контролирует постоянный stateful monitor.
Файлы:

```text
/srv/autorig/current/autorig-online/deploy/healthcheck/postmigration_monitor.py
/var/lib/autorig-postmigration-monitor/postmigration-72h.json
```

Units:

```text
autorig-storage-postmigration-monitor.timer
autorig-storage-postmigration-monitor.service
```

Monitor проверяет site/map, DNS, services, listeners, disk reserve, обе SQLite
DB (`quick_check`), живую Renderfin queue, farm tunnels/workers, artifact cache,
Unity missing-video errors, YouTube rolling budget, viewer Range, Telegram и
completion email ledger. Новые сигнатуры отправляются один раз, активные
неисправности повторяются до устранения.

Отдельный постоянный healthcheck остаётся включён через
`autorig-storage-healthcheck.timer`; его Renderfin DB path должен совпадать с
путём monitor'а.

### Completion email

Письмо считается успешно отправленным только если в
`task_completion_emails` есть `status=sent`, `attempt_count`, непустой
`provider_message_id` и отсутствует `last_error`. Monitor выполняет отдельный
синтетический probe не чаще одного раза в 12 часов. Проверять только ledger и
сервисные логи без вывода адреса пользователя или API token.

После инцидента 16 августа три последовательных реальных completion email
(`bac403c6…`, `6c3bffd0…`, `c9599804…`) отправились через Resend с первой
попытки и provider message ID. На момент проверки все 13 ledger-записей имели
`sent`, незавершённых email не было.

После нового hotfix проверять, что оба unit читают скрипты через
`/srv/autorig/current`, а не из устаревшего release или migration snapshot.

## Старый VPS и rollback

На `autorig-vps` сохранены:

```text
/root/autorig-online
/var/autorig
```

AutoRig backend, Telegram и Renderfin там остановлены; nginx оставлен для
других функций/контролируемого rollback. Старый root disk уже около 97%, поэтому
нельзя снова включать production traffic без предварительной проверки места.

До появления новых записей на новом сервере был возможен DNS rollback. После
cutover и новых задач безопасная стратегия другая:

- не направлять DNS назад на устаревшую SQLite DB;
- rollback кода/units выполнять на новом host;
- старый VPS держать неизменным минимум семь дней как reference/source;
- если нужен data rollback, сначала остановить writers и сделать согласованный
  reverse delta/SQLite backup.

## При инциденте

### Сайт или Telegram молчит

1. authoritative DNS `@`/`www`;
2. `nginx -t` и `systemctl is-active nginx`;
3. listeners 8200/8210;
4. status/logs backend, Renderfin, Telegram;
5. tunnels;
6. disk/reserve gate;
7. не запускать старый AutoRig параллельно.

### Пользователь не скачивает ZIP/GLB/FBX

1. `/api/task/<id>` и artifact cache fields;
2. owner/purchase authorization;
3. cache manifest и ZIP CRC;
4. internal X-Accel mapping;
5. `Range: bytes=0-15` и tail-range;
6. worker fallback;
7. не удалять cache entry, пока не доказано наличие другой полной копии.

### Генерация 3D застряла

1. Renderfin health и persisted stage clock;
2. tunnel health;
3. Hunyuan worker pool и bearer smoke через `generate-3d`;
4. worker queue/GPU/VRAM;
5. GLB publication с сигнатурой `glTF`;
6. Chromium turntable в service sandbox;
7. automatic submit task и converter preflight;
8. listener PID/build после worker deploy.

### Давление на диск

1. проверить `/dev/md3` и cache sizes;
2. не запускать age-only cleanup;
3. сначала удалять безопасные дубликаты/диагностику/secondary previews старше
   retention window;
4. last-copy deliverables не удалять;
5. при резерве ниже 120 ГБ блокировать новые задачи и alert.

## Что считается успешной проверкой

Логи сами по себе недостаточны. Нужен видимый end-to-end результат:

- Image/Flux/Hunyuan создал валидный GLB;
- GLB доступен публично и отвечает `206`;
- turntable MP4 создан, доступен и поддерживает seek/Range;
- Renderfin создал AutoRig task;
- worker preflight принял task;
- viewer показывает модель до полного окончания;
- rig/animations завершаются;
- ZIP/GLB/FBX скачиваются полностью и tail-range;
- Telegram сообщает корректный terminal status;
- cache queue сохраняет файлы локально.

Нельзя писать «генерация работает», если проверены только Flux/Hunyuan, а
turntable, submit или converter task всё ещё failed.
