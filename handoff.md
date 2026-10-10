# AutoRig development — start here after context compression

Updated: 2026-10-10 17:15 Asia/Novosibirsk (10:15 UTC). Owner-required persistent checkpoint.

## Координация агентов V3 (2026-10-10)

- restarts: **Renderfin resume · V3** — 19:35Z: рестарт autorig-storage/renderfin больше не теряет рендеры (resume под тем же id), ждать простоя НЕ нужно; см. раздел «Renderfin resume · V3» внизу.

- converter deploy: Skinning → converter f13 2026-10-10T16:00Z — done 16:45Z (8133a2d, f13 restored; other nodes NOT rolled: owner cancelled the classic pipeline)
- converter deploy: Converter speed · V3 f1 2026-10-10T15:30Z — done 16:50Z (6674da2 on f1, V3 canary 7/7 + rig_canary ok, f1 restored; no other node rolled: owner cancelled the classic pipeline)
- converter: Limb collision · V3 — `5650feb` (arms_spread guard, only `Vlado_Blender/tpose_remap_animation.py` on top of `6674da2`) pushed to converter main, **НЕ выкачен** (f1/f13 под замками выше, f2/f7 на `b386115`): кто катит следующим — везёт его; канарейка = в `tpose_remap_animation_log.txt` строка `guard: X·abduction=…`.
- backend restart: **Astra admin access · V3** — done 17:02Z: релиз `astra-owner-20261010T1702Z` (= `live-20261010T170005Z-1119323` c Downloads · V3 + `backend/astra_owner_access.py` + якорь в `get_current_user`), autorig-storage перезапущен (Downloads уже рестартовал в 16:58 сам). Инструмент Астры `site_as_owner` (= `sudo astra-priv site-as-owner`) — аккаунт владельца, user id 2, только owner-ходы; заголовок `X-Astra-Owner` принимается только локально. **Стейджите следующий релиз от `current`**, иначе выкинете `astra_owner_access.py`.

- **Downloads · V3 (17:00Z, live)**: классический конвейер закрыт для НОВЫХ задач живым флагом `/srv/autorig/live/config/v3-routes.json` (`"classic_new_tasks": "off"`, `"routes": "all"`; откат — `"on"` и `""`, без рестарта). `tasks.create_conversion_task` отвечает `classic_pipeline_off` на новые rig/convert, retry классики идёт в V3. Существующие и идущие классические задачи не тронуты. Telegram-бот НЕ перезапускался: его авто-сабмит уже идёт в V3 по `routes: all`, ручная кнопка «полная конвертация» в боте до его рестарта ещё может создать классику.

- **Localization · V3** владеет языком пользователя и локализацией (fa/RTL и др.).
  Сырой алерт artifact-cache (`cache=… cap=… reserve=…; last-copy deliverables
  preserved`) чинит Localization: пользователю уходит локализованное сообщение
  `storage_paused`, фронт не показывает сырой 5xx `detail`. **Astra это не делает.**
- Язык — поле API, **живое с 12:16 UTC** (рестарт autorig-storage; telegram-бот
  перезапущен в 12:17, чтобы support_ai видел колонку): `support_chat_sessions.language`
  (+`language_source`), `tasks.owner_language` (пишется при создании задачи из запроса),
  `users/anon_sessions.preferred_language|detected_language`, `GET /auth/me` → `language`,
  `GET/POST /api/me/language`, `GET /api/task/{id}` → `owner_language`, контракт
  `GET /api/language`, для агентов на хосте `GET /api/language/resolve?task_id=|support_session_id=`.
  Бэкенд: `user_language.py` (`language_instruction`, `support_session_language`,
  `user_error_detail`). Проверено: fa-посетитель → session.language=fa → support_ai
  ответил на фарси (сессия 56350), en → по-английски (56351). Агент сессии MT
  (`mt/agent.py`, MT `4ba279e`) берёт язык владельца задачи из `/api/language/resolve`,
  русского по умолчанию больше нет. Строки нового `/task` — только через `I18n.t()`.
- Сайт: en/ru/zh/hi/fa (все ключи во всех 5), fa = RTL + Vazirmatn (`static/css/rtl.css`),
  страницы `/fa/…`, `/ru/…`, `/zh/…`, `/hi/…` рендерятся сервером (lang/dir, текст, hreflang,
  canonical, sitemaps). Дисклеймер чата поддержки теперь честный (отвечает Astra, ИИ).
- **Контракт i18n для новых страниц (Viewer · V3: новый `/faq`, Task page · V3)**:
  каждая видимая строка — элемент с `data-i18n="<key>"` (атрибуты: `data-i18n-placeholder`,
  `data-i18n-title`, `data-i18n-aria-label`), английский текст внутри элемента и в
  `static/i18n/en.json`; в JS — только `I18n.t('<key>', {n: 5})`, перерисовка на событии
  `languageChanged`. Ключи страницы с общим префиксом (`faq_*`, `taskv3_*`). Новые ключи
  кладите в `en.json` (+`ru.json`, если пишете по-русски) и запишите сюда их список —
  fa/zh/hi допишет Localization. Сервер сам переводит `data-i18n` в HTML (SEO, без
  мигания); полностью переведённая страница ставит `<html data-i18n-scope="page">`
  (RTL на всю страницу), иначе RTL только у шапки/подвала. Сырые ответы сервера
  пользователю не показывать: коды ошибок `detail.error_string` → ключи `error_*`.

- **Astra · V3 ↔ Session agent · V3 (вечер 10.10):** Astra просыпается раз в `watch_minutes` (её `config.json`, 10 по
  умолчанию, владелец меняет словами в Telegram). Очередь эскалаций `MT_ROOT/astra_escalations/*.json` читает её мост;
  обе схемы понимает: `astra.escalation/1` (инструмент `ask_astra(text, task_id)` у каждого агента сессии) и
  `autorig.session-astra-escalation/1` (`task_agent.escalate_to_astra`, `from: owner` = проверенный админ → ход Астры
  сразу, с полным доступом). Ответ в чат сессии: `POST /api/mt/runs/{run}/astra/say` (ключ «astra») либо ваш
  `/api/mt/astra/escalations/{id}/reply`. Не пишите `trust`/`from: owner` ни из какого пути, кроме проверенного админа.
  `/dev/tools` показывает `ask_astra`; `viewer_state` из вашей схемы Астра читает в контексте хода.

## Autotests · V3 — регрессионные автотесты и гейт релизов (2026-10-10 16:40 UTC, на проде)

- **Где лежит.** Код в `autorig-online/deploy/autotests/`: `autotests.py` (раннер, гейт, mt-deploy, artifact, карточка),
  `checks.py` (проверки), `corpus.json` (манифест), `vendor/deformation_probe.py` (зонд Астры, sha 10d9efb5).
  На VPS он установлен в `/srv/autorig/autotests` (вне релизов, ставится через `sudo bash install.sh`). Корпус лежит
  в `/srv/autorig/data/autotests/corpus`: 35 копий только для чтения (0444, sha256 в манифесте); оригиналы только
  читаются. Отчёты и PNG пишутся в `.../reports/`, `latest-<path>.json`.
- **Скорость.** Прогон `gate all --tier base` занимает 32 с (20 кейсов, 82 вердикта); `--tier extended` — 307 с,
  из них 232 с уходят на численный QA эльфа 285k. Последний extended: GATE PASS, 71 PASS, 18 XFAIL, 1 PENDING.
  DEV 6543.
- **Где стоит гейт.**
  - MT: `sudo /srv/autorig/autotests/autotests.py mt-deploy --mt-file mt/x.py=<файл> [--restart]`. Ваши файлы
    кладутся поверх копии прод-MT, на этом дереве идёт база, и только PASS ставит их. Потом бэкап в `mt.prev/`,
    атомарная замена, import-check и рестарт только при пустом mt-busy (раны, ветки, v3_runs). Если прод-файл
    поменялся за время прогона, установка отказывает. Проверено: сломанный fastrig дал FAIL, прод не тронут.
  - Backend: `gate backend --release "$CUR-<change>"` перед `mv current` (unit-тесты task_page_v3_routes,
    fbx_ascii, task_page_live, ASCII FBX intake на двух реальных FBX, `node --check` JS страницы задачи).
  - Конвертер: `sudo python3 /srv/autorig/fleet/rig_canary.py <box> <port> [--corpus]` перед restore. Это
    дефолтная T-поза плюс профиль `converter_default` на её `_all_animations.glb`: скелет на меше, нет
    шаблона-67, центровка, кисти, руки не в теле, руки привязаны (жёстко, без xfail), высота rest, жёсткий проп.
    С `--corpus` прогоняются ещё 10 моделей корпуса, у каждой свои xfail; это около 8 мин на модель.
  - `live_static.py put` (на проде) отказывает JS, который не проходит `node --check`, и битый JSON.
  - Ночью в 02:40 UTC `autorig-autotests-nightly.timer` гоняет `gate all --tier extended`; в DEV пишет только при FAIL.
  - Живые процессы: `gate live` (застрявшее классическое зеркало; бюджет рига 60 с по
    `task_agents/<task>.json.fast.seconds_from_upload`).
- **Кейсы и метрики.** Меч 66ba97ba: проп жёсткий в MT, детектор ловит скиннинг меча в конвертере (rigid 3 %)
  и предплечье вне тела. Рука в торсе 98c1247c плюс детектор Limb collision. Хвост af874411. Шаблон-67 в
  d76f84c3/af874411/7831327b и 2ad5c0c8 (`unfitted_template`, отпечаток `3e5ee0254878ddc5`). Пятка 16ce2f35.
  Шея 7831327b (`height_change_pct` +69 %). ASCII FBX b5b2a520/63bf5d35. Рыцари 9fb7d4d9/9a34e8c0 (8369addb
  = тот же рыцарь). Манекены 63bf5d35/8a1b1cc5. Эльф 4f85d45e. Конечности: 8b16a847, e0cdbf0b (плащ, мягко),
  9d466924, 91a9513a, 2ad5c0c8 (руки не привязаны — жёсткий FAIL).
- **Текущие XFAIL — дефекты, которые надо чинить** (владелец указан в манифесте).
  - Rig tools · V3:
    - манекен: весь позвоночник на x=−0.33, 16/22 суставов вне вокселей, 3 ложных Prop (Prop1 под Head);
    - аниме 16ce2f35: стопы вне меша в первом риге (56 сэмплов);
    - воин: руки висят, после фикса 40 сэмплов вне;
    - нет хвоста; численный QA не проходит ни один клип.
  - Limb collision · V3: руки fastrig в теле (воин, мальчик, 8369addb). У конвертерного 98c1247c xfail стоит до
    раскатки 5650feb.
  - Skinning → converter · V3: меч скинится в тело.
  - Converter · V3: шаблон-67 и шея.
  - Intake · V3: FBX/OBJ ждут prepared GLB из очереди, p95 144 с.
- **PENDING.** palm-on-torso af874411 (586 у Астры): её метрика считается по частям исходного FBX. Наш детектор
  без разметки частей даёт 0, поэтому кейс ждёт её разметку.
- **Как добавить дефект.** Допишите вход в `inputs` (source + what), кейс и проверки в `cases`, затем
  `sudo .../autotests.py sync --write-manifest`, перенесите sha256 в Git и запустите `gate`. Роли проверок:
  regression, detector (ловит записанный плохой артефакт) и control (нет ложной тревоги). `target` —
  цель известного дефекта, XFAIL с владельцем.
- **Находки по ходу.** У b5b2a520 (спасённый FBX) скелет высотой 0.5 от меша и 112 весов кисти далеко от кисти.
  Возможно, это высокая шляпа: не проверено глазами, гейтом это не сделано.
## Multiplayer · V3 — комнаты, WASD-контроллер, инструменты для агента сессии (2026-10-10, на проде)

- **Сервис комнат** `autorig-rooms.service` (127.0.0.1:8264, исходник `autorig-online/deploy/rooms/`, на VPS `/srv/autorig/rooms/rooms_api.py`;
  рестарт не трогает autorig-storage). nginx: точные `location = /api/rooms/ws` (WebSocket), `= /api/rooms`, `= /api/rooms/busiest`
  (бэкап конфига `autorig.online-storage.bak-rooms-20261010`). Без персональных данных, анонимно, лимиты (20 сообщ/с, 6 сокетов на адрес, 24 в комнате).
  - `GET /api/rooms` -> `{rooms:[{id,players,active}], busiest, default_room:"sponza", total_active}`; `GET /api/rooms/busiest?among=a,b`.
  - Комната = id сцены плашки Scenes (`cathedral`, `sponza`, id пакетов). Свой дом (среда рана, `none`) = всегда оффлайн.
  - Аватар чужого рига отдаётся только если задача публичная (`/api/task/{id}.is_public`); приватные не показываются (peer без модели).
  - Спавн: кольцо вокруг центра АКТИВНЫХ игроков внутри walk area, лицом к группе; активный = вкладка видна и ввод за последние 45 с.
- **Вьювер** (кандидат `unity/mp-r1-20261010`, страница `mp-rooms.js` рядом с index.html, грузится как scene-plate): кнопки внизу слева
  (play/inspect, бейдж комнаты), P = переключить режим; WASD/стрелки, Shift бег, Space прыжок (в inspect Space остаётся паузой), левая кнопка мыши - обзор;
  на телефоне джойстик + прыжок + бег. Клипы Idle/Walking/Running/Jump из рига, foot IK по земле (лучи по коллайдерам сцены, иначе террейн, иначе плоский пол).
  Правка `Viewer.cs`: `ModelRoot`, `PanelOpen`, `BlockSpace`, `ModelMoved()`. Код: `Assets/Scripts/Player`, `Assets/Scripts/Multiplayer`.
  - Смена настроек сцены (погода, время суток, эффекты, пресет) выбрасывает из комнаты в свою оффлайн-сессию в той же сцене; плашка «Назад в комнату».
  - Ключи i18n `viewer_mp_*` (en/ru в en.json/ru.json на проде; fa/zh/hi допишет Localization).
  - События страницы: `window` `autorig-rooms`; API: `autorigUnity.rooms.{state,stats,join,joinBusiest,leave,back,play}`.
- **Для Session agent · V3** (модуль `mt/room_tools.py`, `mt/agent.py` я не трогал):
  - инструменты `room_stats` и `join_busiest_room`; подключить: `room_tools.register(TOOL_SPECS, TOOL_FUNCS)`, добавить `room_tools.TOOL_NAMES`
    в `PRIVATE_VIEWER_TOOLS` и `room_tools.VIEWER_METHODS` (`MpJoinBusiest`, `MpJoinRoom`, `MpLeaveRoom`, `MpPlay`) в `PRIVATE_VIEWER_METHODS`;
    для приватного пути `_dispatch` не должен требовать `_private_viewer_allowed` значение (value "" допустимо).
  - Правило агента: при старте выбирать комнату с наибольшим числом людей (`join_busiest_room`; если везде пусто - комната встреч `sponza`).
  - Объяснить пользователю НА ЕГО ЯЗЫКЕ: каждая сцена - комната с другими людьми; в какой он и сколько там; что риг занимает время и какое место
    в очереди у его задачи; что смена настроек сцены вернёт его в оффлайн, один клик по бейджу комнаты возвращает.
  - Пока риг не готов, вьювер не входит в комнату (нет аватара): агент говорит об этом и об очереди.

## Fleet · V3 — флот одним запросом (2026-10-10 ~12:30 UTC, проверено на проде)

- `GET https://autorig.online/api/fleet` (+ `?format=text`, `/api/fleet/box/<id>`) живой.
  Сервис `autorig-fleet` (`/srv/autorig/fleet/fleet_api.py`, 127.0.0.1:8255, вне релизного
  дерева, рестарт не трогает storage). Исходник `autorig-online/deploy/fleet/`, поля — в AGENTS.md.
- Бокс-агент «AutoRig Fleet Agent» (раз в 2 мин, read-only) на f1/f2/f7/f11/f13/f12/f15/Raptor/
  worker-4090: все диски, GPU, задачи планировщика, порты, `_retired_*`. f5 не тронут.
- Почему 3 дня всё шло на f2: с 06.10 `AUTORIG_DISABLED_WORKERS` выключал f1/f7/f11/f13, а
  публичный шлюз `converter-fX.freestock.online` отвечает «Node tunnel is offline» для
  f1/f2/f11/f13. Теперь f1 и f13 идут через туннели VPS (`AUTORIG_WORKER_TRANSPORTS`, бэкап env
  `/srv/autorig/secrets/backups/autorig-rig-worker-transport.env.bak-fleet-20261010T114240Z`);
  storage перезапущен 11:43:34 UTC в окно без рендеров. Реальные задачи уже идут на f1 и f13.
- f13 был заклинен: снапшот process_control старше 60 с (скан 7.8k задач в памяти × все
  процессы) → admission fail-closed, 5 мёртвых AI-задач 20–77 ч. Процесс убит и перезапущен
  задачей планировщика; f1 (3.1k задач, флапал) перезапущен через management API. Канарейки
  only_rig на f1/f13 прошли (221/272 с, 8 файлов), preflight healthy.
- **Для Converter · V3**: `_server_status_process_control_refresh` сканирует все задачи в памяти;
  без фикса (только активные / чистить терминальные) f1 и f13 снова встанут через 1–2 недели.
- f7: истекла лицензия Unity (UnityEntitlementLicense.xml от 02.09, «No valid Unity Editor
  license»), экспорт висит на окне лицензии. Нужен вход владельца в Unity Hub на f7. Диспетч off.
- f11: GPU Code 43 после TDR-шторма 07.10; ребут и `pnputil /restart-device` не помогли.
  Выключен 11:55 UTC для холодного старта, WoL не разбудил: нужна кнопка питания, потом вход
  в консоль (автологон без сохранённого пароля, задача конвертера InteractiveToken).
  f11-ai теперь уступает GPU конвертеру (`converter_status_url`, бэкап config).
- V3: на f1/f2/f7/f13 deploy-агент e7af0338, протокол 3, по 3 rollback-архива, журнал чист,
  места хватает — готовы к `deploy_farm.bat` (из чистого main, можно отдельным worktree).
  Принятого V3-артефакта нет; хэш записать в `/srv/autorig/data/var/fleet/v3_target.json`.
- Диски: `_retired_*` 695 ГБ (f15 420, Raptor 218, worker-4090 56.5) — только с OK владельца.
  Raptor C: 5 ГБ (безопасно чистить нечего). f1/f2: idle-gated чистка дампов/autosave/логов
  (~69/39 ГБ) запущена, `autorig-online/deploy/converter-cleanup/*-20261010-nozip.ps1`.

## Intake · V3 — конвейер смонтирован (2026-10-10 11:20 UTC, проверено на проде)

- **MT** `GET /api/mt/v3` → 200, intents `rig`+`accessory` (`mt/v3_conveyor.py`,
  `mt/v3_dispatch.py`, коммит MT `e1fbea6`). Каждый dispatch-run = своя 20-hex
  сессия вьювера: source → analysis (проекции+Vision) → rig (fastrig) → retarget
  (8 клипов) → qa (численная деформация, flips/spikes, слои hair/cloth) → publish
  (неизменяемая копия попытки в `motion_transfer/v3-dispatch/runs/<v3run>/`).
  QA-провал = `needs_review` с причинами; legacy-fallback нет. Ключ бэкенда —
  `autorig-v3-dispatch` (хэш в `mt-api-keys.json`, токен `/srv/autorig/secrets/v3-dispatch.token`).
- **Backend** (коммит `24b52a4f`, релиз `v3intake-20261010b`, сверху `tv3-backend-20261010b`):
  `v3_intake.py` (GLB сразу Task+binding одним commit; FBX/OBJ → нормализация
  конвертером в intake-pump; генерация → `bind_generated_task`), `v3_runtime_mount.py`
  (outbox `/srv/autorig/data/v3-intake/dispatch-outbox.sqlite3`, проекция в Task,
  `done` только после собственной сверки байтов/хэшей), источники
  `/srv/autorig/data/v3-intake/sources/<sha>.glb`. Legacy dispatch/progress/stale/
  stuck-hour/bulk-restart/preemption пропускают `pipeline_kind=v3`.
- **Read API для Task page · V3**: `GET /api/task/{id}/v3-shell` (`autorig.task-v3-shell/1`:
  status created|processing|needs_review|done|failed, stage, progress 0..1, viewer_url,
  viewer_state, message) и `GET /api/task/{id}/v3` (binding, session, QA gates/reasons,
  artifacts). 404 для не-V3 задач. Task.status `needs_review` — явный статус.
- **Маршрутизация**: `AUTORIG_V3_ROUTES` (по умолчанию пусто) и `AUTORIG_V3_ADMIN_ROUTES`
  (по умолчанию `all`): аккаунты админов уже идут в V3, `pipeline=v3` — всегда V3.
  Глобальный перевод всех маршрутов НЕ включён: нет FBX/Unity экспорта и подписочного гейта.
  Telegram-ветка в коде, но активна только после рестарта бота и `telegram` в AUTORIG_V3_ROUTES.
- **Batch 2** (коммит `a7b744ff`, релиз `v3intake-20261010d`, рестарт storage+renderfin
  11:43 UTC, сброс очереди: 1 queued + 2 running graph-рендера): OBJ → GLB прямо на VPS
  (`obj_to_glb`); FBX → конвертер, недоступный конвертер = ожидание до 6 ч, не ошибка;
  картинка/видео от админа → генерация (видео: кадр ffmpeg) → V3. Renderfin
  `ordinary_conversion_waiting` больше не считает `generate`/`v3` строки обычной
  конверсией (раньше site-генерация блокировала свой же shared fallback).
- **Доказано (все → `needs_review`, весь конвейер)**: `5f6f91fc` HTTP GLB upload;
  `16ce2f35` аккаунт владельца; `66094238` HTTP OBJ upload (манекен → "metallic humanoid
  robot", 28 костей); `cfa2e6a1` из генерации (меш Renderfin `5c35c168`, intent generate).
  Причины везде: численная деформация, спайки в Waving1/Hip Hop Dancing/Look Around
  (системно — библиотека/ретаргет), слои hair/fabric. DEV (UTF-8): `041c4da81732`,
  `a43162079154`, `ffc1bf308f40`, `a9715f319e12`; первые два поста с битой подписью —
  `3dbaf148ff8f`, `a431557faa7a` (Windows curl; слать только с VPS).
- **Свежая генерация** `8635aa06` ждёт Hunyuan: f12 выключен (C: 0 GB, бэкап
  `renderfin-hunyuan.json.bak.v3intake-f12-diskfull-*`, вернуть после очистки — Fleet),
  shared fallback f7/f13 на паузе, пока в обычной очереди висят legacy `a2c243b9`/`c0cda1e0`.
  "disk gate failed" стоит добавить в `_FLEET_ERROR_MARKERS` renderfin.
- **Приказ владельца (DEV, к посту cfa2e6a1)**: масштаб объекта определяет анализатор;
  бесплатный AI vision граф проверяет стартовый зум и даёт вердикт инструменту (zoom или
  scale); сцена под масштаб (100 м), нет — генерировать. Сделано у меня: V3 analysis строит
  карточку (`card.json height_m` → масштаб вьювера и площадка сцены) + `scene_auto` после
  publish (MT коммит `c683635`, файл выложен, **активен после рестарта MT**).
  Передаю: **Task page · V3 / вьювер** — Vision-проверка стартового кадра (сейчас
  кинематографическая камера на /task режет голову) и `SetCamera`; **сцена** —
  `scene_recipe` клампит площадку до 3 м и фиксирует SIZE_M, нужен размер по `dims_m`.
- **Рестарт MT 11:56:55 UTC** (PID 4110250, окно без running веток): активны ключ OpenAI
  координатора и карточка масштаба. Доказано на `2eb85454` (elf upload): analysis
  «рост 1.72 м», card «Леди Роза». Агенты сессий сами запускают GPU-ветки (танец/диорама)
  на каждую V3-задачу (`MT_AGENT_AUTOSTART`) — учесть ёмкость фермы при открытии V3 всем.
- **Batch 3** (коммит `3e272584`, релиз `v3intake-20261010e`, current переключён БЕЗ
  рестарта — активируется ближайшим рестартом storage): картинка/видео от админа →
  генерация → V3; живой переключатель маршрутов `/srv/autorig/live/config/v3-routes.json`
  (`{"routes": "all"|[...], "admin_routes": "all"}`, читается при изменении, без рестарта).
- **Retry доказан**: `5f6f91fc` POST `/retry` → attempt 2 (`v3run-1a2e0e40…`), attempt 1
  `superseded`, новая сессия `1c92958d1ab3f392f596` → `needs_review`.
- **Каталог инструментов**: реестр V3-инструментов `/srv/autorig/data/v3-intake/tools.json`
  (`autorig.tools-registry/1`: v3_upload, v3_task_shell, v3_task, v3_retry, mt_v3). **Astra**:
  добавить группу `v3` в `mt/astra/catalog.py` из этого файла.
- **ASCII FBX на входе (срочно от владельца, live 12:21 UTC, релиз `v3intake-fbx-20261010`,
  коммит `a7c40245`)**: `fbx_ascii.py` вставляет пропущенный `a:` после `Name: *N {`
  (AssetStudio/Unity Studio), `assimp export … -f glb2` → GLB рядом с оригиналом, он и
  становится входом задачи (legacy и V3). Нечитаемый → 422 `error_fbx_unreadable`
  (en/ru/zh/hi/fa в `static/i18n`). FBX-экспортёр assimp не использовать (битый бинарник).
  V3 бинарный FBX тоже сначала через assimp, потом конвертер. Доказано: `63bf5d35`.
  Мусорная тестовая `fe6b984d` (испорченный FBX) ждёт нормализации и через 6 ч уйдёт в error.
- **Генерация site-строк**: legacy stale reset/global timeout 120 мин больше не трогает
  `pipeline_kind=generate` (коммит `757f408d`, live с рестарта 12:16).
- **Живой прогресс (владелец: «процессинг долго висит… 0%», live MT 12:38 / storage 12:42,
  коммиты MT `fa6d3a7`, backend релиз `v3intake-live-20261010`)**:
  - MT: 4 параллельных V3-прогона (`MT_V3_CONCURRENCY`), числовой QA в отдельном процессе
    (не держит GIL сервиса, kill через 600 с), карточка масштаба параллельно ригу. Долгие шаги
    плавно двигают прогресс внутри своей полосы; статус несёт `stage:sub Ns`
    («проверка качества · численная проверка · 243 с», доказано на ретрае `5f6f91fc`).
    Главная длительность — численный QA (~4 мин на 300k вершин) и очередь фермы на Vision.
  - `/api/task/{id}/v3-shell`: никогда не 0 % в работе (пол 1 %), заголовок стадии с подстадией,
    поле `events_url`. Для `pipeline_kind=generate` с V3-маршрутом shell тоже отвечает:
    «генерация модели · ждёт 3D-воркер / Hunyuan 3D · f13 / оборот модели», 2–29 %; после
    меша конвейер заполняет 30–100 %. **Task page · V3**: для generate-строк брать этот shell
    (сейчас карточка показывала 0 % у `8635aa06`, который ждёт Hunyuan).
  - **Контракт для Live processing · V3**: `/api/mt/files/<mt_run>/v3/events.jsonl`, по строке JSON
    `autorig.v3.events/1`: `seq, at, kind (stage_start|stage_done|artifact|progress|verdict|error),
    stage (source|analysis|rig|retarget|qa|publish), sub, progress 0..1, summary, data, artifacts
    [{kind,url}]`. Артефакты по мере появления: source `proj/model.glb`; analysis `proj/sheet_*.png`,
    `card/card.json`+`face.png` (масштаб, приходит параллельно); rig `rig/rigged.glb` (геометрический
    риг + rig_check), `rig/rig.json` (кости head/tail в `data.bones`), `rig/weights_preview.png`;
    retarget `rig/rigged.glb` с клипами; qa `rig/rig-qa.json`, `rig/numeric-qa.json`; verdict.
    Вокселизация внутри fastrig артефакта не пишет — если нужна сетка вокселей, назовите формат,
    добавлю событие `artifact` sub=`voxels`. Ваш `/api/mt/runs/{run}/live` может читать этот файл.
  - **Хук сцены**: после publish конвейер уже вызывает `full.scene_auto(run)`; черновик сцены
    персонажа по `docs/scene_package.md` встанет в `V3Conveyor._after_publish` (одна точка).
- **Converter normalize (F7)**: следующий шаг intake — FBX/OBJ, которые не берёт assimp
  (и текстурные бандлы), отправлять в `POST /api-converter-glb/v3/normalize` вместо legacy
  `/api-converter-glb-to-fbx` (контракт `CONTRACTS/v3-normalized-source.md`).
- **Диск**: ~70–80 МБ на V3-задачу (сессия 30–50 МБ, копия попытки ~25 МБ, источник);
  `numeric-qa.json` ~5 МБ — сжимать/чистить по давлению вместе с регенерируемыми копиями.
- **Не смонтировано**: `v3_site_integration.py`/`task_v3_shell.py` (заменены read API выше),
  `v3_cutover.py` (контроллер фермы), MT→backend push-callbacks (`task_callbacks.py`):
  идемпотентность даёт durable outbox (poll, lease CAS, повтор проекции).

## Task page · V3 — живая страница задачи (2026-10-10 12:15 UTC, проверено на проде)

- **Свежий JS на каждом заходе.** nginx `/static/`: оверлей `/srv/autorig/live/static`,
  затем релиз. JS/CSS без точного 10-hex `?v=` отдаются `no-cache` + ETag (304),
  со штампом — immutable. `/task` собирается на каждый запрос (`task_page_live.py`):
  шаблон и partials из live-корней, каждый `/static` JS/CSS получает `?v=<sha1[:10]>`
  текущего содержимого. Живая правка статики:
  `sudo python3 /srv/autorig/tools/live_static.py put <rel> <file>` (атомарная запись,
  затем promotion в релиз под локом, откат — `previous`). Состояние:
  `GET /api/task-page/live`. Доказано: тот же URL без hard refresh берёт новый билд
  (`tv3-20261010.2` → … → `.10`), DEV `672ce48ed028`.
- **/task = V3-оболочка** (`task-v3.html`, `js/task-v3-shell.js`, `css/task-v3.css`):
  тот же SEO head/canonical/OG/JSON-LD, шапка, подвал и h1, что у классики; один
  Unity-вьювер; полоска задачи НАД вьювером (скачать/классика `?classic=1`, ссылка,
  полный экран, статус, чат поддержки) — UI вьювера ничем не перекрыт; реальная
  позиция в очереди (порядок диспетчера) и прогресс до модели; модель раньше рига
  (prepared.glb показывается, пока риг идёт); нет модели — видео/постер из og:,
  иначе «3D-модель недоступна» с подсветкой кнопки классики.
- **API** `GET /api/task/{id}/v3-view` (`autorig.task-page-v3/1`), только сервер решает:
  - V3-задачи (`pipeline_kind=v3`) открывают MT-run из проекции Intake
    (`viewer_settings.v3.session.mt_run_id`, контракт `v3_runtime_mount._shell`),
    с агентом сессии;
  - классические открывают свои GLB из glb_cache через `/api/task-viewer/{id}/…`
    (`files/task/rig/rig.json`, `rig/rigged.glb`, `proj/model.glb` через X-Accel;
    texlib/scenes/settings — 307 на `/api/mt/…`; POST настроек от не-админа
    игнорируется), `agent=0`. Кэш прогревается фоном через классические эндпоинты
    (~47% старых задач без кэша; у части GLB уже нет и на воркерах — как и в классике);
  - ACL owner / anon_id / admin, чужая приватная задача → 404 (на проде приватных нет).
  Доказано: `492210fd` (классика, 70 костей, клип играет, DEV `b123c856bfc6`),
  `cfa2e6a1` (V3 run, агент на связи), новая `5f51cb9c` (модель во вьювере при риге 12%,
  DEV `7b8fa84a6c1a`; по готовности оболочка сама перезагрузила run с ригом 68 костей —
  DEV `1c8f3f6c47ae`), раскладка десктоп/телефон DEV `6b0a7dd69471`, `979a46632099` (✅).
- **Раскатка без рестарта:** `/srv/autorig/live/config/task-page.json`
  (`mode` off|admin|new|all, `new_since`, `webapp`, `preview_keys`; запись — temp+rename).
  **Сейчас шаг 4 — `all` (12:09:49 UTC, после ✅ владельца на DEV `979a46632099`):**
  все задачи по умолчанию в V3 (25/25 случайных задач: страница и `v3-view` 200);
  `?v3=0` — отказ навсегда (кука `ar_task_v3=0`), `?classic=1` — классика разово;
  Telegram `mode=webapp` — классика, пока `webapp: false`. Откат — `mode` в файле
  (`new`/`admin`/`off`), без рестарта. Заголовок `X-AutoRig-Task-Page: v3|classic`.
  DEV шагов: 1 `b123c856bfc6`, 3 `6b0a7dd69471`/`7b8fa84a6c1a`, 4 `57a2d12ed9aa`.
- **i18n:** 16 ключей `taskv3_*` (en/ru — я, fa/zh/hi — Localization), JS через `I18n.t()`.
- **Для Viewer · V3** (шаблон Unity не трогаю): оболочка грузит
  `/api/mt/unity/test/index.html?api=<origin>/api/task-viewer/<id>&run=task&agent=0`
  (классика) или `?run=<mt_run>` (V3) и шлёт `autorigUnity.send('LoadRun', run)` при
  смене ревизии. Нужно: событие «модель готова»; полоса загрузки шаблона на узком
  экране шире окна; классический `animations.glb` — одна glTF-анимация `Animation`
  (так и в классике); общий профиль настроек вьювера vs. клиенты (для V3-run POST
  идёт прямо в MT).
- **Для Astra:** каталог `/dev/tools` может звать `GET /api/task/{id}/v3-view`
  (очередь/стадия/вьювер задачи) и `GET /api/task-page/live`.
- **Рестарты:** 11:25:32 UTC (`tv3-backend-20261010b`) — сброс отменил 3 running
  graph-рендера (в `render_tasks` их не было). Теперь перед рестартом:
  `curl -s -X POST 'http://127.0.0.1:8210/renderfin/api-render/reset?dry_run=1&spare_non_graph=1'`.
  Серверный текст описания+ключевых слов под вьювером (`fill_v3_description`, коммит
  `48b646d0`) уже лежит в `current` (`tv3-backend-desc-*`) и включится при ближайшем
  рестарте `autorig-storage` (любым агентом); `/tmp/tv3-backend-files/activate.sh`
  ждёт простоя renderfin и перезапустит сам, если раньше никто не перезапустит.
- **Дальше:** Telegram WebApp (`webapp: true`) после проверки Unity во встроенном
  браузере Telegram; мобильный Unity; загрузка движка 17.7 MiB; покупки/подписка и
  экспорт прямо в V3 вместо перехода на классику.

## Viewer · V3 — каналы, карусели, своё контекстное меню, новый /faq (2026-10-10 12:10 UTC, на проде)

- **Вьювер** (`/api/mt/unity/test/index.html` = `pulse-alt-r27-20261010`, тот же патч в
  кандидате `pulse-gradient-r29-20261010`; бэкапы `/srv/autorig/audits/viewer-v3ui-20261010/`;
  шаблон Unity — приватный MT `88a6538`, WIP Codex по HDR не тронут):
  - слева внизу панель каналов `1`–`0` (анимированные SVG, тултипы, подсветка текущего):
    1 свет, 2 альбедо, 3 нормали, 4 металл, 5 шероховатость, 6 свечение, 7 ID объектов,
    8 ID материалов, 9 аутлайнер, **0 части тела** (новая клавиша страницы; «8 дважды» тоже);
  - правая панель — карусели: клик = следующий пресет, последний шаг = выкл, точки-пипы
    показывают позицию по реальным событиям `effects`/`scanner`/`stand`; удержание, бейдж ⋯
    или Shift+клик открывают настройки группы;
  - правая кнопка: браузерное меню не открывается (кроме полей ввода), наше меню «FAQ» →
    `/faq` в новой вкладке (`noopener`); правый drag по-прежнему панорамирует.
- **Для Task page · V3:** над iframe ваши элементы (`#tv3-tools`, карточка) сами решают
  `contextmenu`; чтобы показать наше меню, вызовите
  `viewer.contentWindow.autorigUnity.showContextMenu({x, y})` (координаты внутри iframe),
  закрыть — `hideContextMenu()`.
- **i18n (Localization: допишите fa/zh/hi):** en/ru уже в `en.json`/`ru.json`.
  - Вьювер, 59 ключей: `viewer_ch_{bar,full,albedo,normals,metallic,roughness,emissive,object,material,outliner,part}`,
    `viewer_fx_{post,dof,volumetric,scanner,weather,stand,sun,quality,lasers,settings,hint,hint_toggle}`,
    `viewer_p_{off,on,auto,scene,cinematic,vivid,film,noir,clean,bokeh,tilt,rays,volume,blue,redarc,rain,snow,underwater,clear,studio,disco,cathedral,sponza,source,morning,noon,golden,dusk,night,overcast,low,medium,high,ultra}`,
    `viewer_ctx_{faq,faq_tip}`. Вьювер берёт язык из `autorig_lang` и читает `viewer_*` из
    `/static/i18n/<lang>.json`, так что новые языки подхватятся без правки шаблона.
  - FAQ, 71 ключ: `faq_{title,lead,nav_label,link_developers,cta_title,cta_upload,cta_support}`,
    `faq_sec_{viewer,agent,rig,animation,editing,generation,channels,export}` и пары
    `faq_q_<id>`/`faq_a_<id>` для id: viewer_open, viewer_camera, viewer_tools, agent_what,
    agent_talk, agent_sees, agent_original, rig_how, rig_inspect, rig_fix, rig_models,
    anim_which, anim_retarget, anim_frame, edit_what, edit_agent, edit_scene, gen_image,
    gen_text, gen_video, gen_media, ch_keys, ch_ids, ch_kept, export_formats,
    export_engines, export_rights, export_api. В ответах есть `<code>` (клавиши) — сохраните теги.
- **/faq** (релиз `live-20261010T120738Z`): серверный HTML с общими шапкой/подвалом,
  `data-i18n-scope="page"`, FAQPage JSON-LD (28 вопросов) + BreadcrumbList, без упоминаний V3.

## Converter · V3 — normalized_source на проде (2026-10-10 12:40 UTC, канарейка F7)

- Принятый билд конвертера: `main` `b38611581a6ebcc9848e612e95e3d8f92f9c117d`
  (eschota/autorig.online), записан в `/srv/autorig/data/var/fleet/v3_target.json`.
  Идентичность — коммит: `deploy_farm.bat` шлёт дельту от базы ноды, поэтому SHA
  артефакта у каждой ноды свой (F7, база 8d2a67c: `0059f38d…62369363`). Раскатку
  F1/F2/F13 ведёт Fleet · V3 штатным `deploy_farm.bat HEAD`; F11 выключен.
- Что умеет: `POST /api-converter-glb/v3/normalize` (bearer узла, durable identity;
  replay привязан к task_uuid/attempt/SHA/формату/запечатанному манифесту) → статус →
  две роли артефактов → `/ack` (только `durably_persisted:true` и точные хэши).
  GLB/FBX/OBJ + опциональный запечатанный бандл текстур/MTL (путь+размер+SHA-256).
  Квитанция: исходник, зависимости, GLB, билд-производитель, иерархия (узлы, меши,
  примитивы, материалы, скины, bounds). Легаси-риг не вызывается. Контракт:
  `CONTRACTS/v3-normalized-source.md` в репо конвертера.
- ASCII FBX: на ферме нет assimp, Blender его не читает → `Failed` +
  `input_error_code=FBX_ASCII_UNSUPPORTED`; конвертирует Intake на VPS.
- Канарейка F7 (прод, Blender 5.1.0): 7/7 — 4 реальные публичные модели (FBX
  11 732 верш., GLB byte-exact, 2 OBJ без MTL), запечатанные FBX/OBJ, неверный SHA
  отклонён, replay 202 / 409; байты сверены, ack принят. Найден и исправлен баг
  FBX-импортёра Blender 5.1 (`cast_shadow`: любой FBX со светом падал). Отчёт
  `/srv/autorig/audits/converter-v3-canary-20261010/f7-b386115/canary-report.json`,
  драйвер `canary_driver.py` рядом. DEV `b20cdad723b8` (Telegram 6432).
- Ревью: независимое — P0 (конвертер падал при каждом старте после первой V3-задачи),
  P1 (Blender открывал необъявленные файлы до аудита) и 7 P2 исправлены; второй
  ревьюер подтвердил, его P2 (строки с одиночным CR) тоже закрыт.
- Для Fleet: `_server_status_process_control_refresh` теперь перечисляет процессы
  один раз за обновление (8000 задач: 0,48 с вместо ~217 с) — f1/f13 больше не
  уйдут в fail-closed из-за накопленных задач.
- Для Intake: F7 обслуживает normalize (Unity не нужен), хотя AutoRig-диспетч на нём
  выключен из-за лицензии Unity.
- Не сделано: риг/ретаргет/QA/публикация, Vision, частичные превью, слои одежды/волос
  на конвертере; инструмента нет в `/dev/tools` (каталог `mt/astra/catalog.py` у Astra).
- Эта секция заменяет строки про конвертер ниже (748f36d «не merged/не deployed»,
  пункты 5–6 «Блокирующих разрывов»). Подробно: канонический converter handoff
  (`converter-source/handoff.md`, зеркала VPS/F5, SHA `bfa0d030…`).

## Skinning → converter · V3 — итог (2026-10-10 16:45 UTC; классику владелец отменил)

- Код: eschota/autorig.online `main` 8133a2d — `Vlado_Blender/topology_skin_refine.py` + хук после BindCheck в
  `tpose_autorig.step_autorig_bind`, тесты `tests/test_topology_skin_refine.py` (12/12). Выключатель `AUTORIG_TOPO_SKIN=0`.
  Квитанция `<guid>/logs/topology_skin_refine.json`, событие `topology_skin_refine` в `stage_timing.jsonl`.
- Живёт только на **f13** (drain → deploy → V3 canary 7/7, rig_canary 8/8, хвост-канарейка 8/8 → restore). f1/f2/f7 не
  раскатывались (приказ владельца: только V3 fastrig). f13 несёт и коммиты Converter speed до 3cbb1c4.
- Что делает: граф по индексам меша (швы свариваются только по совпадающим координатам), утечки = регионы кости,
  недостижимые от главного региона через соседей по скелету; одна Дейкстра по поверхности; гейт — 6 синтетических поз,
  применяется только если растянутые рёбра и «убегания» не растут. Хвост: бескостная трубка за тазом → `cc_tail_00..05.x`
  под `root.x` по осевой линии; ARP-экспорт их сохраняет (GLB канарейки: 76 джойнтов, 850 вершин на хвосте).
- Замеры: 13 прод-биндов — 6 со скелетом вне меша (пропуск, `skeleton_outside_mesh`), остальные чистые; вставленная утечка
  ладонь→бедро на реальных биндах: 44/142 вершины → 0, растянутые рёбра 156→0 / 433→59; 52k вершин — 1.7 с.
  af874411 на f13 (новый main): скелет подогнан, 384 вершины торса с весом ладони, но ладони сплавлены с торсом
  (25 общих рёбер) и ноги L/R тоже — гейт отказал (рёбра 2→244); нужен разрез контакта.
- Находка: 6/13 прод-биндов с неподогнанным дефолтным скелетом ARP, BindCheck их пропускает (af874411 на f7: всё на бёдрах).
- Контракт пружины хвоста для вьювера (не построено): кости `cc_tail_NN.x`, корень заперт на бёдрах, пресеты Astra
  `stage/topology/tail_spring.py`, шаг ≤1/240 с, треки тела не трогать.
- DEV 6541 (до/после), 6542 (скелеты). Аудит `/srv/autorig/audits/topology-skin-20261010/`.

## Converter speed · V3 — классический риг быстрее (2026-10-10 16:50 UTC; работа остановлена приказом владельца)

- Владелец отменил классический конвейер (V3-only), поэтому раскатка остановлена: **6674da2 только на f1**
  (`v3_target.json` → `nodes.f1`; общий `commit` остаётся b386115, f2/f7/f13 на нём или на 8133a2d у Skinning).
  Fleet может показывать f1 «v3 not ready» — это коммит-надмножество b386115. Коммит Limb collision 5650feb
  (на main конвертера) **никуда не выкачен**. Канарейки f1: V3 7/7
  (`/srv/autorig/audits/converter-v3-canary-20261010/f1-6674da2/`), rig_canary ok (372 с, 8 файлов). f1 restored.
- Что в конвертере (main eschota/autorig.online, мерджи b5b7bcd, 3cbb1c4, 6674da2):
  - OpenPose: свип фазами (6 видов rot=0 одним вызовом OpenPose, потом остальные 18), ранний выход на уверенном
    кандидате (поза+лицо, LR/humanoid-фильтры те же); `GLB_OPENPOSE_SWEEP_BATCH/EARLY_EXIT=0` возвращают старый. 72 → 4–20 с.
  - Blender 5.x face depth: `compositing_node_group` + `ShaderNodeMapRange` + новый File Output → markers_face 0 → 101.
  - ARP Go proxy: solidify без even offset + страж высоты. Он раздувал прокси 1.0 м до 1.59/2.24 м → риг в 1.7× меша
    (d76f84c3). Исправлено, на том же входе head.x = 0.837 м.
  - arp_bind: после PSEUDO_VOXELS поднимает c_pos на 2 м; если >15 % кожи осталось (тихий отказ bone heat, бинд 10 с
    вместо 22), тут же VHDS в том же процессе. Падавший исходник кладётся в `logs/*_pseudo_failed_source.blend`.
    Тот же исходник 4/4 детерминирован, так что причина в позе/подгонке рига, а не в мече.
  - Прогресс: метки UTC с `Z`; строки `OpenPose: frame k/n.`, `Rig: skeleton fitted (N bones).`, `Rig: weights painted.`,
    `Rig: check failed — …` перед RETRY. Статус `/api-converter-glb/status/{id}` даёт `viewer_rig_ready(_at)`.
- Бэкенд (89b94841, релиз `rig-ready-warm-20261010T1555Z`, рестарт 15:55 UTC при чистом renderfin): `/api/task/{id}/v3-view`
  прогревает `animations.glb`, как только `viewer_animations_glb_url` объявлен (processing), а не после done.
  Замер: `deploy/fleet/rig_speed_probe.py <box> <port> [url]`.
- Цифры (f1, воин 23 950 верш.): OpenPose 90→21, поза 51→52, риг 70→37, ретаргет до viewer GLB 46→40,
  **риг во вьювере 261→153 с**, задача 476→399 с. DEV 65a3c77faa35. Бюджет 60 с классика не берёт.
- Не сделано: 2ad5c0c8 (руки без своих костей) не смотрел.

## Rig judge · V3 — судья суставов и автоисправление рига (2026-10-10 13:00 UTC, на проде)

- **Сервис** `autorig-rig-judge` (127.0.0.1:8252, nginx `^~ /api/mt/rig-judge`, бэкап
  конфига `/srv/autorig/audits/autorig.online-storage.bak-rigjudge-*`). Код в MT-репо
  (`abea0f8`+): `mt/bonecode.py` (формат), `mt/joint_views.py` (изолированные рендеры),
  `mt/joint_judge.py` (джоба, CLI `python -m mt.joint_judge --run <20hex>`),
  `mt/joint_judge_service.py` (API + автотриггер), `deploy/rig-judge/`.
- **API для агента сессии / Astra** (POST: MT-ключ Bearer или локальный вызов без nginx):
  `POST /api/mt/rig-judge {"run": "<20hex>"}` или `{"task": "<uuid>"}` (+`fix`, `vision`,
  `pose`, `force`) — **идемпотентен**: та же ревизия рига (sha rig.json) → та же джоба;
  `GET /api/mt/rig-judge/{id}`, `GET /api/mt/rig-judge/run/{run}`, `GET …/spec`.
  В каждом ответе поле **`next`**: `wait` (poll 15 с) | `done` (всё ок или исправлено и
  установлено) | `rejudge` (исправил, но остались провалы — POST ещё раз, это новая
  ревизия) | `see_job` | `retry` (упала, ≤3 раз) | `give_up` (+`why`: нет данных куда
  двигать / план тела `root` / 3 раунда исчерпаны) | `judge` (риг изменился). Плюс
  `outside_scope`: что суставами не лечится (слои волос/одежды → garment/hair solver).
- **Типы моделей**: biped и quadruped (свои имена/срезы; ИИ-поза только для biped);
  `root` (проп, техника) → сразу `give_up` с причиной. Цепочки волос `Hair_*` судятся.
- **Автозапуск** без утверждения: каждые 30 с V3-сессии `needs_review` и MT-раны с
  `rig/validation.json` «needs attention»; одна ревизия рига судится один раз; ≤2 джобы
  параллельно. Риг ставится атомарно, старый — в `rig/joint_judge/<job>/before_rig/`;
  `rig.json.built_at` → вьювер перезагружает; `rig/joint-judge.json` — сводка для
  Task page / Intake. **V3-статус и dispatch-store я не трогаю** — Intake, перечитайте
  QA по `rig/joint-judge.json` (score, moved, next).
- **Session agent · V3**: цикл «POST → poll → next» безопасен для повторов; инструмент
  в `/dev/tools` (Astra `tools/bin/rig-judge`, коммит Astra `adb41fb`). `next.action`
  ещё `run_fix` (судили с `fix:false`). `give_up` старой версии судьи (`JUDGE_VERSION`)
  судится заново автоматически.
- **Вторая версия по вердикту владельца** («пятку не нашли … каждый сустав изолированно
  с глубиной … пятка, колени, локти, ладони»): к ригу добавляются неформирующие кости
  `LeftHeel`/`RightHeel` (лодыжка → пятка внутри стопы, `mt/heel_bones.py`; ретаргет их
  не трогает); Vision судит 8 главных суставов пакетами «рендер + глубина + кости, спереди /
  сбоку / вдоль конечности» (JSON inside_mesh / issue / suggested_shift_direction /
  confidence), остальные — числами. Кандидат = текущий риг + сдвиги (а не перефит с нуля).
- **16ce2f35** (сессия `252ed6e85fdaf29e8bb2`): джоба `f3caf724e3f0` (237 с) — колени
  −6.3% H и лодыжки −7.3% H по ИИ-позе (ControlNet → 3D), плечи к центру руки,
  0.78 → 0.97, рывки в клипах 6 → 0, V3 numeric stretch −22%; джоба `f2aa92eca1b7` —
  пятки, 0.96 → 1.00, `next: done`. DEV 6438–6441, 6464–6468 (сетки 8 суставов до/после).

## Live processing · V3 — каждый этап во вьювере, вживую (2026-10-10 13:30 UTC, на проде)

- **Один поток на ран**: `<run>/live/events.jsonl` (append-only, одна O_APPEND-запись на строку) +
  полезная нагрузка рядом (`live/vox_<res>.bin` и т. п.). API: `GET /api/mt/runs/{run}/live?cursor=<байт>&wait=<0..25>`
  → `{schema autorig.mt.live/1, cursor, events[], state, done}`; событие `{id (смещение), t, stage, kind, progress 0..1
  внутри стадии, key (i18n live_*), params, url (под /api/mt/files/<run>/), data}`; `state` = стадии с измеренными и
  ожидаемыми секундами, общий progress 0..1 и `eta_s` по медианам прошлых ранов (`live-stats.json`, QA × вершины).
  Код: MT `mt/live.py` (+`mt/live_voxels.py`), коммит MT `97f7969`+. Писать в поток из любого MT-кода:
  `from mt import live; live.emit(run_dir, stage, kind, progress=, key=, params=, url=, data=)` — никогда не бросает.
- **Что пишет**: `full.Phases` start/media/done/finish (все раны, и V3, и полный MT); `v3/events.jsonl` Intake
  зеркалится как `kind=v3.*`; V3: `source/mesh`, **вокселизация** (дочерний процесс рядом с анализом, без heavy-слота:
  уровни 12/24/48/96, ARV1 = заголовок 32 Б + по 4 Б на воксель x,y,z,глубина|ядро), `voxels/erosion` (глубина до
  поверхности, медиальное ядро), `analysis/classify|card`; fastrig: `rig/weld|plan|joints` (кандидаты суставов, откуда
  каждый) `|bones` (скелет + насколько симметрия/sanity сдвинули) `|weights` (+цепочки волос) `|preview|checks`;
  `judge/centring` (каждый сустав против эрозии вокселей: ok/off/outside); `retarget/clip` по клипу, `retarget/clips`;
  QA: `qa/numeric_start|clip|numeric` (3D-точки худших растяжений) `|verdict`; `judge/verdict` — снимок
  `rig/joint-judge.json` Rig judge (харвест при чтении API, по суставу ok/review/fix).
- **Вьювер** (сборка `unity/live-r31-20261010` = r27 + `LiveLayer.cs` + шейдер `AutoRig/LiveHeat`; index.html — живой
  r27 со всеми live-правками + `<script src="/api/mt/static/live.js" defer>`; r27 оставлен для отката):
  воксели строятся слоями снизу вверх, уровень за уровнем; эрозия слой за слоем до светящегося ядра; суставы
  выпрыгивают, едут на проверенные места, кости растут от родителя; после загрузки рига — тепловая карта весов на
  4,5 с; метки QA/центровки/судьи на суставах (идут за анимированными костями). Клик по суставу — имя, родитель,
  откуда сустав, вердикты; веса этой кости. Слои — иконки внизу левой панели `#ui` (воксели, ядро, кости, веса,
  проверки, повтор обработки). Команды Unity: `LiveVoxels/LiveErosion/LiveJoints/LiveBones/LiveWeights/LiveMarks/
  LiveShow/LivePick/LiveReset`, событие `live_layers`, `live_pick`.
- **Для Session agent · V3 (аватар)**: `mt/static/live.js` кормит ваш `autorigUnity.agentStatus`: прогресс рана
  (ring + %) каждые несколько секунд и строку на каждую веху (воксели 96³, эрозия, что видит Vision, рост, скелет, веса,
  центровка, клипы, проверка, вердикт судьи) с `key="live:<id>"`. Пока поток говорит (<15 с), грубые вызовы
  task-страницы (кроме done/needs_review/error) он проглатывает — двойного кольца нет. Подписка без опроса:
  `window.autorigLive.on(fn)` или `addEventListener("autorig-live", e => e.detail)` — типы `state|line|event|layers|pick`;
  `autorigLive.lines` — все строки (в т. ч. неозвученные). Хотите рисовать сами — `autorigLive.claim("session-agent")`.
- **Для Intake · V3 (пропускная способность)**, замеры на проде:
  - V3: ожидание в MT-очереди 0 с (конкуренция 4), приём→старт 3–4 с. Узкие места: **Vision фермы** 1–389 с
    (`4f85d45e`: >6 мин; причина — авто-свип Rig judge: ≤2 джобы × до 14 параллельных Vision-запросов, ~290 с
    на джобу, держат AI-ноды фермы, а единственный Vision-запрос анализа V3 ждёт за ними; нужен приоритет
    конвейеру — судья уступает, пока есть V3-ран в analysis; и ещё: пока ждём Vision, риг можно строить
    спекулятивно по пропорциям и пересобрать, если Vision не согласится) и **численный QA** 47–382 с
    (300k вершин). QA я распараллелил по клипам
    (`v3_numeric_qa.py`: воркер на клип, ≤½ ядер, по свободной памяти; отчёт байт-в-байт как последовательный —
    `report_sha256` совпал на проде; поле `parallel` вне хэша — ваше, сохранено): 41k вершин 54 → 21 с; 300k —
    один клип ~83 с, весь QA теперь ≈ самый длинный клип вместо суммы.
  - `kit.heavy_slot` (`MT_HEAVY_PROCS=2`) сериализует ВСЕ `mt.*`-дети (проекции, орбиты, трекинг) — с автозапуском
    агентских веток (танец/диорама) проекции V3 будут ждать слот. Вокселизатор и QA этот слот не берут.
  - Классика: очередь медиана 31 мин (p75 75 мин, max 100), обработка медиана 12 мин — вот «процессинг висит».
- **Рестарты MT обнуляют V3-ран**: 13:26 UTC чей-то рестарт `autorig-mt` отменил `4f85d45e` в analysis
  после >6 мин ожидания Vision — ран начался с source заново. Перед рестартом MT проверять активные V3-раны:
  `select run_id, stage from v3_runs where status in ('queued','running')` в `v3-dispatch/dispatch.sqlite3`.
- **Rig judge · V3**: ваш `rig/joint-judge.json` уже уходит в поток (метки по суставам). Если писать
  `live.emit(run, "judge", "joint", data={bone, verdict, score})` по мере судейства — метки появятся сразу.
  Находка центровки: у `63bf5d35` (сессия `0abc7c6ad86e0546967a`) весь позвоночник на x=−0.33 при симметричной
  модели около x=0 — 16 из 22 суставов вне вокселей.
- **Для Scenes · V3**: `BakedScenes/sponza/OptionalIvy/Vendored/DynIvy` (07:40 UTC) без своего `.jslib` ломал
  линковку любого WebGL-плеера (`wasm-ld: undefined symbol IvyFilesWrite…`). Добавлен запасной
  `Assets/Live/Plugins/WebGL/DynIvyFilesFallback.jslib` (localStorage) — удалите, если принесёте свой.
- **Для Viewer · V3**: сборка r31 включает несобранный ранее RadialPulse HDR WIP (r30-исходники из рабочего дерева).
  Новые иконки — в конце `#ui`; переключение `unity/test` → r31 после проверки на живой задаче.
- **Task page · V3**: `v3-view` классики отдаёт `eta_s` и `progress_basis` (`outputs`|`elapsed`): без отчёта конвертера
  карточка идёт по часам против медианы 12 мин (≤90 %). Релиз `live-proc-eta-*` в `current`, включится ближайшим
  рестартом storage (я не рестартовал).
- **Localization**: 52 ключа `live_*` в en/ru (живые) — допишите fa/zh/hi.

## Viewer · V3 (ui 3) — команды вьювера, viewer_state, лента медиа, карточка сессии (2026-10-10 14:45 UTC, на проде)

- **Где**: `unity/test` (= `pulse-alt-r27-20261010`) и `unity/live-r31-20261010`; исходник - шаблон Unity
  `AutoRig/index.html` в MT (`f0a9beb`, `52c45ac`). Правки страницы живые, без пересборки Unity.
- **Команды** (для Session agent · V3, Astra, task page): `autorigUnity.command(name, args)` -> `{ok:true, ...}` или
  `{ok:false, reason, allowed?}`. Причины: `unknown_command`, `unknown_menu`, `invalid_value` (+`allowed`),
  `loading` (меню ещё грузит, +`progress`), `not_available_in_this_build`, `not_available_here` (лазеры вне интерьера),
  `no_rig`, `no_such_clip`, `error`. Список с аргументами: `autorigUnity.commands`.
  - `channel {key 1-9,0 | mode 0-8 | name full|albedo|normals|metallic|roughness|emissive|object|material|part}`, `outliner {open?}`
  - `preset {menu post|dof|volumetric|scanner|weather|stand|sun|quality|lasers, value|next}` и короткие
    `weather {none|rain|storm|snow|blizzard|fog|underwater|auto|next}`, `time_of_day {auto|morning|noon|golden|dusk|night|overcast}`,
    `scene {none|studio|disco|cathedral|sponza}`, `quality`, `post`, `dof`, `volumetric`, `scanner {blue|redarc|off}`, `lasers`
  - `material {target?, material?, base_color?, metallic?, smoothness?, emissive?, emissive_intensity?, normal_strength?}`, `undo_material`
  - `select {plane object|material|part|node, ids, add?}`, `clear_selection`, `clip {name|auto}`, `layer {model|rig|bones|voxels}`,
    `camera {target?, yaw?, pitch?, dist?, lens?, seconds?, cut?}`, `auto_camera {on|value}`, `frame`, `pulse {x?, y?}`
  - Пресет, который докачивает данные (запечённые сцены, вода): команда возвращает `ok` + `loading:true`, в иконке меню кольцо с %,
    по готовности пресет всё равно применяется и приходит нота с отменой («загружено и применено»); пока меню грузит - `loading`.
- **viewer_state** (`autorig.viewer-state/1`): `autorigUnity.viewerState()` - снимок (channel, layer, clip, presets по каждому меню,
  `loading`, `weather {value,on,setting,rendering,kinds}`, time_of_day, scene, camera, selection, material_edits, commands).
  Пуш при каждом изменении (дебаунс 120 мс, без дублей): `window` событие `autorig-viewer-state` (`detail` = снимок),
  `autorigUnity.onViewerState(fn)`, `pageOnEvent("viewer_state", json)`, `parent.postMessage({type:"autorig-viewer-state", state})`.
  Агентский мост (`agentApply`, `viewerNow`, подтверждаемый `POST /api/mt/runs/<run>/viewer/session/<id>/state`) остаётся за Session agent · V3.
- **Погода**: страница шлёт `Effects.ClearWarm` на `ready`/`loaded` (сцена хранила пустую «прогревочную» погоду, поэтому дождь/снег
  не включались); после докачки ассетов (вода) погода применяется сама. В r31 есть `storm|blizzard|fog`, в r27 - `rain|snow|underwater`
  (`weather.kinds`; недоступное -> `not_available_in_this_build`).
- **Лента медиа**: до 5 новых миниатюр над окном агента; клик - открыть большим по центру, второй клик/Esc - закрыть;
  `autorigUnity.pushSessionMedia({url,type,thumb}, title)`. Кнопка «все материалы» (сетка) открывает карточку сессии.
- **Один read API**: `GET /api/mt/runs/<run>/session-log?after=&limit=&kind=&media=1&order=desc` и
  `GET /api/mt/runs/<run>/session-card` (модель, метаданные, все каналы текстур по материалам, Vision, судьи, графы, решения агента, ресурсы).
- **i18n**: ключи `viewer_ntf_*`, `viewer_sl_*` (en/ru в шаблоне; Localization - допишите в `static/i18n/<lang>.json`).
- Всё иконками с подсказками (`withTip`), текст только в подсказках, нотах и карточке.

## Session agent · V3 — мост к вьюверу, подтверждение из состояния, старт сессии (2026-10-10 15:25 UTC, на проде)

- **Код**: `mt/agent.py`, `agent_session_routes.py`, `task_agent.py`, `agent_work.py` (MT `5397380`); страница - живые правки
  в `unity/scenes-v3-r2-20261010` (= `test`) и `pulse-alt-r27-20261010`, бэкапы `audits/session-agent-v3-20261010/batch4|5`.
  **Viewer / Scenes: новая сборка страницы должна нести эти ханки** (искать `agentApply`, `viewerNow`, `viewerErr`, `progSnap`, `MPL`).
- **Мост**: агент ставит действие в очередь сессии, страница применяет (`agentApply`), сверяет с состоянием вьювера и шлёт
  `POST /api/mt/runs/<run>/viewer/session/<id>/state` с `ack` `{ok, confirmed, why, observed}`; результат инструмента `viewer`
  говорит «подтверждено» или причину отказа. Методы: SetChannel(1-9,0), SetWeather, SetTimeOfDay, SetStandScene, SetPost,
  SetQuality, SetEffect, SetMaterial/UndoMaterial, PlayClip/ClipPlay, ShowLayer, SetCamera, Outliner, **ShowProgress on|off**.
  Проверено вживую (все 10 каналов, дождь вкл/выкл, снег, время суток, материал/цвет): каждое подтверждено состоянием.
- **Причины отказа - от самого вьювера**: ошибки движка (событие `error`, напр. «Stand lit material is not serialized in this
  build» - стенд-сцены `disco/studio/...` в текущей сборке Unity **не работают**, это для Viewer/Scenes) попадают в `why`
  и в `viewer_state.engine_error`; погода вне `weather.kinds` отказывается сразу (`not_available_in_this_build`).
- **viewer_state в контексте агента** (`VIEWER NOW`): сцена, погода (`raining`), время суток, канал, клип, линза, выделение,
  `progress {status,pct,queue_ahead,eta_s,card}`, `multiplayer` (если есть `autorigUnity.multiplayer.state()`), `engine_error`.
- **Контракт с Multiplayer · V3** (пока модуля нет, страница ждёт): `autorigUnity.multiplayer = {joinBusiestRoom(): Promise<{room, players}>,
  state(): {room, players, online, scene}}` и события `window` `autorig-mp` `{detail:{type: ready|joined|left|offline|online|players}}`.
  Агент при старте сам входит в самую людную комнату и объясняет правила (общая комната, смена настроек сцены = офлайн) на
  языке пользователя; строки приходят с сервера в ответе `POST .../viewer/session` как `lines.mp`.
- **Старт сессии**: приветствие сервера теперь содержит «риг собирается несколько минут» + собственное место задачи
  (очередь `#N`, впереди, сколько в работе / процент и ~минут), данные - `GET /api/task/<id>/v3-view` с куками посетителя.
  Язык вне en/ru/fa/zh/hi переводится моделью один раз и кэшируется (`task_agents/lang_<код>.json`, проверено de).
- **Кнопка «скопировать весь чат»** в шапке окна агента; **«Астра …»** от владельца/админа уходит Астре («передано Астре» + её ответ, роль `astra`).
- **Инструменты** (владелец, задача 66ba97ba): `viewer ShowProgress` (карточка прогресса в вьювере), `catalogue {query}`
  (живой реестр `/dev/tools`: инструменты агента, ноды фермы, проверки fast analysis; если не нашлось - честно «такого нет»),
  `parts {status|separate}` (метки частей кластеризатора; `separate` честно отвечает, что разрезания меша ещё нет - **когда
  появится, подключить в `t_parts`**). `analysis/fast.json` агент читает через `fast_analysis.agent_digest` (Fast analysis · V3).
- Цикл инструментов агента 8 шагов (было 4) + итоговая реплика вместо падения в «Команда не выполнилась».

## Intake · V3 — численный QA кусками и реальный прогресс (2026-10-10 15:35 UTC, на проде, MT-коммит 26f44d9)

- **Что было недоделано**: предшественника оборвало на `skin_postvalidate.sample_slice/reuse_prepared` (был только в
  рабочем дереве MT, на проде нет). Теперь закоммичено и выложено (бэкапы `mt.prev/*.20261010a|b`).
- **`mt/v3_numeric_qa.py`**: отсчёты каждого клипа режутся на куски (≤48 отсчётов, ~6 кусков на воркер), куски идут в
  пул процессов, сливаются по клипу (`merge_chunks`) и по риму (`merge`). Отчёт **байт-в-байт как последовательный**:
  `report_sha256` совпал на реальном риге 300k вершин (9 клипов, 2341 отсчёт) и в тесте
  `tests/test_v3_numeric_qa_chunks.py` (куски по 1/2/3/все, с пиком и без). Если кусок не «чистый» pass/fail —
  последовательный вызов. Поле `parallel` (workers, chunks, wall_seconds, clip_seconds) вне хэша.
  BLAS в детях принудительно 1 поток (`kit.ENV` ставил 2: oversubscription давала user 44 мин вместо 20).
- **Замер (300k вершин, тот же GLB, тихий хост)**: до (воркер на клип, 9 процессов) 187 с → после 96 с (4 воркера).
  Число воркеров: 4→96 с, 6→101, 8→118, 12→129, 20→149 — **QA упёрся в пропускную способность памяти**
  (0.1 с/отсчёт в одном процессе, ~100 МБ временных массивов на отсчёт), поэтому по умолчанию 4 воркера для ≥100k
  вершин и 8 для меньших. На загруженном хосте (joint_judge ×3 при nice 5, load 10–15) тот же QA идёт 185–360 с.
  Фактически уложиться в 1 мин на 300k вершин без смены семантики нельзя: варианты — только ключевые отсчёты без
  серединных (÷2), либо публиковать вьювер/риг до вердикта QA (QA идёт после и меняет статус accepted/needs_review).
  Точный ускоритель `posed_world` без смены битов дал лишь −20 % (не внедрён).
- **Реальный прогресс** (`GET /api/mt/v3/runs/{id}`): `stage` = `qa:numeric 7/53` (куски по факту, из
  `rig/.numeric/progress.json`), `progress` идёт по доле отсчётов (0.68→0.86). Новые поля: `stage_name`, `stage_detail`,
  `percent`. Остальные этапы и раньше шли реальными вехами; тик по часам остался только пока нет первого куска.
- **Причины медленного V3 на стороне Intake/dispatch** (для расследования 66ba97ba): (1) QA 185–360 с на 300k вершин
  при нагруженном хосте — основное; (2) параллельные `joint_judge` (nice 5, ≤2 джобы) отбирают ядра у QA и Vision;
  (3) **частые рестарты `autorig-mt` посторонними агентами** (я видел 15:19, 15:20, 15:28 UTC) убивают численного
  ребёнка: ран завершается `needs_review` с `validator_error exit -15` (не возобновляется) — перед рестартом
  проверять `v3_runs` queued/running, батчить рестарты.
- Проверка end-to-end на реальном ране (MT-dispatch, источник warrior 300k): analysis 7 с, rig 2.6 с + fast_analysis
  13.5 с, retarget 0.8 с — **всё до QA ≈ 25 с**; чистый прогон до конца с новым размером кусков прерывали рестарты MT.

## Session agent · V3 — Blender-скилл и тест-мост Claude (2026-10-11 04:10 MSK, MT `4a04545`, `d9a1de2`)

- **Скилл на проде**: `mt/skills/blender_modeling.md` (в enum инструмента `skill`), канон — ASStore26 `skills/blender-reference-modeling`
  (читал с `origin/main` без правок; на сайте `/dev/skills/blender-reference-modeling`). Текстуры только из `https://pbr.autorig.online/`
  (read only, `mt/pbr_library.py`: поиск по индексу, агент называет ключ, URL строит сервис).
- **Инструмент `blender_model`** (в `/dev/tools`, группа session): `options | pbr_search | look | build | undo | versions`. Агент шлёт
  JSON-задание (части box/sphere/cylinder/cone/torus/lathe/extrude со скином на кость, материал/PBR-замена по box/bones/material),
  а не скрипт: исполняется только `mt/blender_model.py` (значения зажаты, лимиты 40 частей / 400k треугольников / 300 с).
  Результат версионируется в `agent/blender/<имя>/vN/` (GLB со всеми клипами + before/after + close-up кости), `undo` возвращает версию.
  Опции и лимиты — `op=options`. Публичные посетители: квота `blender_visitor` 12 / `blender_ip` 30 в сутки.
- **Очередь/воркер**: существующая MT-очередь Blender (`/api/mt/blender`), новый kind `model`; воркер `AutoRig MT Blender Worker F7`
  (запланированная задача на f7, Blender 5.1, CPU, `C:\ProgramData\AutoRig\mt-blender-worker\blender_worker.py`, ключ
  `%USERPROFILE%\.secrets\autorig-blender-worker.key`, хэш в `/srv/autorig/secrets/mt-blender-workers.json` имя `f7`, права 640 root:autorig).
  Воркер качает только с `autorig.online` и `pbr.autorig.online`. Замер: look 13 с, build 14-45 с на воркере, 80-95 с весь ход агента.
- **Результат тестов** (OpenAI-ключ снят посреди прогона): «добавь шлем» на аниме-девушке — работает (шлем на кости Head, едет по клипам, агент
  сам заметил, что обод закрывает глаза); «меч в руку» — у воительницы меч уже есть, агент это увидел; остальные (металл из PBR, кисть,
  task-страница) на farm-провайдере не дошли до результата (ответ «Done.» / обрыв), на Sonnet не прогнаны (см. ниже).
- **Мост Claude** (только внутренние тесты, `tools/claude_bridge/`): `claude_bridge.py` (127.0.0.1:18765, секрет, 2 параллельно, таймаут,
  лог `~/claude-bridge.log`, CLI с `--tools ""`), `provider.patch` (llm.py провайдер `claude-bridge` по флагу сессии
  `X-Autorig-Internal-Test: <секрет>`, по умолчанию выключен: нет файла `/srv/autorig/secrets/claude-bridge.secret` = выкл), `start.ps1` /
  `stop.ps1`. **Не выкачен**: (1) CLI на этом ПК разлогинен (`claude auth status` → loggedIn false; нужен `claude auth login` владельцем),
  (2) гейт `mt-deploy` падает на `lowpoly_chibi/mt_tess` (чужой, риг-путь), мой код не причём. Протокол моста проверен заглушкой CLI.

## Актуальный handoff сессии — читать прежде исторических записей

**Полный переход AutoRig на V3 НЕ выполнен. Новая task-страница НЕ выложена.**
Готовность полного рига тела/одежды/волос на пяти разных моделях: **0/5**.
Ни исходники, ни unit-тесты, ни работающий вьювер сами по себе не доказывают
завершение конвейера. Ниже указаны реальные границы готовности.

### Текущее требование владельца

- Актуальный `AGENTS.md` дополнен commit `baf45d76` во время подготовки handoff:
  **Astra** развивается из существующего `@autorigbot`, не создаётся новый бот.
  **1 task = 1 viewer**, один локальный агент сессии на задачу. Любые inputs:
  meshes/images/video/text; путь выбирает агент через инструменты, а не только
  фиксированный rig-конвейер. Единый каталог — `/dev/tools`; ещё не реализован.
  Эта концепция обязательна; текущие rig-stage helpers не заменяют её.
- Все новые website/Telegram/API/generation/retry/convert задачи должны идти
  только через V3, без скрытого fallback на старый rig.
- Заменить только страницу `/task` текущим Unity-вьювером с агентом, ригом,
  ретаргетом, эффектами, реальным прогрессом и медиа. Остальные страницы не менять.
- Сохранить старые пользовательские записи, исходники и результаты.
- Бесплатный интерактивный 3D; GLB/FBX/Unity package exports по безлимитной
  подписке $20/месяц. Это требование, не подтверждённая реализация биллинга.
- Отдельные кости одежды/волос, source-bound Qwen masks, T/A-pose IK corrections,
  перенос правок в любой ретаргет, поствалидация и реальные видео в `/dev`.
- Для пульса: Alt, градиент усиливается к краю, отдельное кольцо выключено;
  Falloff и независимые HDR множители заливки/кольца 0–10, default ×2.

### Production — перепроверено в этой сессии

| Компонент | Фактическое состояние |
| --- | --- |
| Release | `/srv/autorig/releases/adminfwd-20261009` |
| Storage | `autorig-storage.service`, PID 511626 |
| Motion Transfer | `autorig-mt.service`, PID 2364036 |
| Renderfin | `autorig-storage-renderfin.service`, PID 693215 |
| V3 dispatch | `GET http://127.0.0.1:8251/api/mt/v3` → **404** |
| Default Unity viewer | `/srv/autorig/data/motion_transfer/unity/test` → **pulse-alt-r27-20261010** |
| Fleet | Срез 08:44 UTC: F1/F7/F13/F2 обслуживали разные legacy commits; V3 endpoint 404. F11 без listener :7000. Этот срез НЕ свежая готовность к deploy. |

Не перезапускать backend вслепую: live `AUTORIG_WIPE_QUEUE_ON_START=1`.
Production main.py при старте вызывает Renderfin `/api-render/reset` с
`spare_non_graph=1`; это owner policy, её не отключать самостоятельно.
Повторить проверку render/chargen/avatar jobs непосредственно перед deploy.
Старый срез с terminal tasks не гарантирует отсутствие новых задач.

### Репозитории и сохранённый код

| Checkout | Последний проверенный commit | Что сохранено |
| --- | --- | --- |
| `R:/autorig` | `42602ea6`, branch `codex/repair-rig-transport-20261006` | V3 cutover contract, durable outbox, atomic Task bindings/lifecycle, новая task shell/resolver. **Не смонтированы.** |
| `R:/3d_video_motion_transfer` | `4ca5bd0`, branch `main` | 20hex Unity publication bridge, source/artifact checks, gradient-only pulse. **Bridge не смонтирован.** |
| `C:/3d/GLB_Convverter_Git/GLB_Convverter_WebServer` | `748f36da91c0faeef6f24ac3d6bc1b2635e8dd8c`, branch `agent/v3-normalizer` | Standalone GLB/FBX/OBJ normalizer. **Не merged в main, не deployed.** |

Канонический converter checkout восстановлен из `eschota/autorig.online`;
`C:/3d/GLB_Convverter_Git/autorig.online` — junction на тот же checkout.
Checkout sparse: до runtime/deploy проверить материализацию всех зависимостей.
`R:/autorig/.work/converter-v3-canonical-readonly` — только read-only cache,
НЕ корень разработки/deploy. Ранее отсутствующий C-checkout теперь существует.

### WIP — сохранить, не считать релизом

- Public backend: `tasks.py` — admission только с server-owned V3 binding,
  один Task+binding commit и отказ legacy poll/reset/requeue для V3.
  `task_priority.py` — исключение V3 до SQL LIMIT; новый ownership test.
- `v3_site_integration.py` и его тест — начатое wiring Task ACL/binding/status/
  publication lookup. Агент прерван; окончательное ревью/проверка не выполнены.
- `main.py` содержит большой чужой/предыдущий WIP и отличается по line endings.
  Не stage/deploy весь файл; только точечные собственные anchors, сохраняя
  production-only startup behavior. Есть прямые legacy bulk/stuck queries,
  которые ещё надо изолировать от V3 перед включением.
- Private MT: HDR/Falloff в RadialPulse math/controller/feature/shader/tests и
  WebGL template — готовые source edits, **не committed, не built, не deployed**.
- Converter: queue integration `normalized_source`, TEST_MATRIX, новый queue
  test, доработки normalizer cancellation/retention — WIP после P1 ревью.
  Callbacks/reservation уже появились в коде, но актуальная версия ещё НЕ принята.
- Другой WIP (AI services, admin_bot/agent, layer solver, graphs, worker-4090,
  GravityHouse и т.п.) не принадлежит этому slice: не уничтожать/не коммитить пачкой.

### Проверки и артефакты — точная область доказательства

- Root V3 source tranche: 28 Python + 18 subtests, 2 JS shell tests passed.
  Runtime/outbox/adapter после P1: 18 passed; tasks admission agent: 23 passed.
  Это source/fixture проверки, НЕ production end-to-end.
- Unity publication: root final 13 tests + 3 subtests passed; Linux isolated
  audit проверял предыдущую 11-test версию. Нет криптографической подписи:
  hash-bound QA требует внешнего trusted `proof_verifier`; self-hash не approval.
- Existing knight transport-only fixture:
  `R:/3d_video_motion_transfer/.work/v3-publication-real-20261010/receipt.json`,
  SHA `952c58b727b158b0800119f778c80654f72100d9dc554bfc472c91b4a10f389a`.
  Реальные rig bytes опубликованы только в локальный nonpublic fixture layout;
  status **needs_review**, full_agent/source_projection/scene/media = false.
  Никакая customer Task/API submission не создавалась.
- Normalizer root: 44 tests passed до последней cancellation/retention доработки.
  Реальный knight GLB 8,398,056 bytes: byte-exact passthrough **0.064392 s**,
  без Blender/GPU; это НЕ время полного рига. Receipt:
  `C:/3d/GLB_Convverter_Git/GLB_Convverter_WebServer/.work/root-real-normalizer-20261010/normalization.json`.
- Queue integration agent: 79 tests passed до независимого P1 review.
  Не объявлять её production-ready: актуальные исправления ещё требуют проверки.
- r29 WebGL **готов и staged как candidate**, не default: build wall 88.810 s,
  BuildReport 73.568 s; initial 17.683 MiB compressed, full 23.445 MiB,
  lazy assets 5.761 MiB, baked scenes отдельно по требованию. 17 C# tests/native
  raster и production browser Alt/typing/bones/gradient/redarc controls passed.
  Tar SHA `3f1f2e7dfa7e7ed27c0a9fd39a2cc79765000c5fbc491c6d68a2d209e69baa87`.
  Root browser receipt: `.work/pulse-alt-r26-20261010/browser-1791623010017/receipt.json`.
  Extra browser receipt: `.work/pulse-gradient-r29-20261010/browser-extra-1791623711703/receipt.json`.
  Software ANGLE не является hardware FPS benchmark; setup lens/ground не во
  всех capture совпал с requested settings; optional 404 и blocked settings POST сохранены.
- r30 HDR build **не выполнен**. Builder получил внешнюю 403 от Codex transport;
  это не ошибка AutoRig. Сейчас matching Unity/test/native процессы не обнаружены.
  Не предполагать, что build продолжается по старому agent status.

### Блокирующие технические разрывы

1. Production V3 router/rig worker/analysis authority/runtime ещё не mounted.
2. Prepared-GLB dispatch не решает raw-image/video admission и generation graph.
3. Нет принятого общего garment/hair solver; пять моделей по full quality 0/5.
4. Publication bridge умеет только проверенный rig preview; полный агент требует
   source/projections/analysis/session layout и корректной private-task авторизации.
5. Converter P1: atomic child ownership/preemption/lease release и bounded private
   staging cleanup. Не удалять unacknowledged/last-copy outputs.
6. FBX/OBJ queue сейчас self-contained only; sealed texture bundles ещё не подключены.
7. Legacy scheduler/admin/stuck queries не везде отделены от V3.
8. Subscription/export gates, FBX и настоящий Unity NPC package ещё не интегрированы.
9. Astra/session-agent orchestration и единый `/dev/tools` catalogue не подключены;
   новые инструменты не считать завершёнными до публикации в этом каталоге.

### Следующие действия

1. Завершить и независимо проверить актуальный normalizer queue P1 patch,
   source ownership/preemption/cleanup tests. Не выкладывать неподтверждённый код.
2. Завершить site integration callbacks/atomic source admission/whole graph;
   подключить MT V3 executor и авторитетный анализ; исключить legacy fallback.
3. Подключить task shell к настоящим persisted Task→run→publication bindings,
   проверить public/private ACL и результат на реальных существующих моделях.
4. Выложить точечную immutable web release и exact converter artifact штатным
   `deploy_farm.bat HEAD` только после clean pushed main/HEAD==origin/main;
   зафиксировать каждый listener-owned build. F5 development-only, owner4090 не запускать.
5. Проверить реальный новый task end-to-end, callbacks/notifications, exports/
   subscription и не менее пяти моделей с движущимися костями/скином.
6. Доделать r30 native float HDR→WebGL→production browser QA; затем default
   promotion. r29/r27 rollback artifacts сохранять; новые видео /dev не дублировать.

Агенты на момент handoff: normalizer и task-page прерваны; dispatcher/reviewer/
pulse-source завершили source slices; builder errored. Перед возобновлением
прочитать текущий Git diff и проверить реальные process/session handles.
Не писать «всё переведено», пока production и все перечисленные gates этого не доказывают.

Подробный исторический checkpoint и исследования:
`R:/3d_video_motion_transfer/HANDOFF.md`; план:
`R:/autorig/autorig-online/docs/V3_ONLY_CUTOVER.md`.

---

## Исторические checkpoints (ниже не считать текущим состоянием)

CURRENT OWNER PRIORITY 2026-10-10: FULL V3-only task conveyor+fleet, replace only
task page with currentUnity viewer/agent/effects; otherpagesunchanged. New scoped
Latestpreserved public60da1250/privatea68df07; canonical/serverSHA
872a8ae24091ede8bdcb3d6ecd6303a330ebab7ec7c325725597b42b8b1851c6.
Root28Python+18subtests/2JSshellpass. Dispatcheroutbox+authorizedtaskresolver
sourceonly/unmounted. Inversepulsealpha1sourcepreserved; r29native/Webbuilderactive.
plan autorig-online/docs/V3_ONLY_CUTOVER.md. Production /api/mt/v3=404, backend
dispatch/store/callbackmodules absent; newoutbox/task-shellsource agentsreviewing,
notmounted. Storagewipequeueflag1; no restart/Taskmutations. Five-modelquality0/5.
r28gradient/arcsource84b35f9+Webbuild82.14s readyNOTlive; ownerinversegradient+
maxopacity correctionandradius0clarificationpending. LIVE defaultstillr27.
Read canonicalprivateHANDOFF newestsection, not priorcheckpoint below.

Prior private checkpoint9871313/source740181a: canonical/server SHAa0c4e9de697579583ce73e13aafa4cab581fe238c424c19eedd0aa72e9aa81d8.
OwnerPRIORITY pulseAltfix+Webbuild nowLIVEr27 default staticindex25a4c085;
native+actualproductionkeyboard/raster/typing/bonesPASS. Webwall70.27s/warmBuild59s,
initial17.681MiB/full23.443MiB (bakedscenesextraon-demand). No backendrestart.
Nextgradientfill/optionalredarc requestworker+exclusiveUnitybuilderr28pending.
AdaptiveclothphaseI/nonlinearstrainwitnesspassedbutcontactsunchecked;361Upper
positiveBodyproposalsJ0 due259locks, sofullgarmentnotfixed/quality0of5.

Prior private checkpointeb9caa7/source67abb9f+f7e19bf: canonical/server SHA87f69d7e20b38b774db3f59e2646bb6eac09f05443489017193ed1aab6bd7f24.
Root19constraint/trust/id tests and22clipset/direct tests pass, sourcepreserved.
Actualanalytictrustsupport9/10edgeviolations exceed8controlbounds (worst26.39x);
NOTglobalnonlinearinfeasibility. Nextadaptive localtopology-awarebasis top4beta
max16influences/locked259, noinflate/rerunoldsolver. No runtimechange/quality0of5.

Prior private checkpointb44789b/sourceb4a7b33: canonical/server SHA1af551c2bdc75316fd89d30edea8576c2875b19b3ba408c3296be959f84deba8.
BodyRESTpairdiagnostic5713/5707 NOTadmittedv2coverage: wrongoldpacket andno
currentproperreclassification. Exactv2arrays/times/canonicalstatuses immutable
recomputeassigned, no newsolverrun. GlobalBody normalsunknown/open/nonmanifold.

Prior private checkpointbfba8d9/sourceb4a7b33: canonical/server SHAee9d983abc4130717dc3c1b3b822b12f7f607473bfe00f81fd4bb6f508ac63e2.
Actualcentered-v2 F5a6c run TERMINALREJECT/INCOMPLETE8e6bfc09/archivee5ee536b;
p0smallimprovement Body604->588/self254->250,1/3/4noacceptedsteps,2timeout54.12
intermediatecheckpointNOTfinal; physicalallfailed/quality0of5. No retry/deploy.
CriticalBodyterms0/hundredsunknown, trialrejecttelemetrymissing; constrained
inequality+lexicographic/sourceRESTBodypairproposal next, NOTrescaledoldrun.
Sourcepreserved directresthelper21root tests/independentreview, provenance7root
REALGLBtests includingretainedpalette/validmask; bothunmounted/runtimeunchanged.

Prior private checkpointfd8020a/source005183d: canonical/server SHA5f34ae7870db62185a14af1e1d361a673a77c2a596799efe3a6764b9a45c678f.
Revisedcenteredpacket pendingaudit, NOlaunch: BodyFD3.65e-10/strainFD3.27e-9;
hard55budgetfix+freshimmutableproposal recomputation required. Historical7830
proposal was accidentallyoverwritten, cannotreconstructfullbytes; current7c338
isDIFFERENT dependency, neverclaimoriginalhashmatch. Incident disclosedincanonical.

Prior private checkpoint4ca8fa9/source005183d: canonical/server SHAf0dc919e11bbe5ee6a7a66358f071184ec8b044100c9d48e9ba1bfe9e0b0e053.
NOcenteredoptimizerpose yet; preflight-only2failures followedpatchedFDpass,
independentreview blocksBodyplanepoint/sourcepacket/manifest/budget/checkpoints.
Directresthelper root18tests pass butunmounted dueCAS/source/publication guards.
Directlabelwriter root4tests pass butGLB/code/camera strictproof repairpending;
originalElf/BraidQwen mapsmissing, diffusedoutputsNOTdirectsemanticauthority.
Fullacceptance0/5; no restart/customer/fleet/activepointer changes.

Prior private checkpoint27abeb6/source005183d: canonical/server SHA0af1238c94b4c0a4a70f0add2a80a2f9f685d16f84c5cb12d7a3701f07ba1803.
Root24tests pass. Conservativecertguard f4b78 independentlypassesfailclosed,
admitted0; priornonzero certgradientREVOKED. Linearhomotopy1554 mechanical
proposal1165/1258 stable, NOTactualanimationCCD/anatomy/sewing. ONE isolated
centered-v2research authorized preflightFD/fullhashes+5x55s/audit, handlepending.
FiveDISTINCT matrix Girl/Elf/Braid/Red/Knight,4rigs/0passes; nofleetpromotion.

Prior private checkpointdf694a4/source7bc0381: canonical/server SHAcec4f3c26f7eb77cad2d1119d99714813a14607ab1d98905848078bfa84ee238.
Root reviewedactualFoot8f3 sideWalking/Running/Jump and comparison; NEWMEDIA
sentONCE HTTP200 UID71bca804bd9d/video4a6deb83, deliveryNOTapproval/globalFAILED.
Independentcert audit rejects c221 identity/gap/orientation/continuity admission;
guardrepairinprogress, no centeredsolver approved. Actualv1candidate strain
proof3202b9d7 closes2/3 IDs gap:all10MAIN/rank6/upper0. No unlock orrestart.
Full modelquality0/5. This section supersedes pendingMEDIA/certcoverageclaims.

Prior private checkpoint321ae0b/source7bc0381: canonical/server SHA3908f1a465ee65402127c7d9540f384020e64f2d209e14bc24beb8e706769cb6.
R5fa47 actualserializedREST/alias native+Web15 gates pass; smooth48 v1 FINALREJECT
onlypose0 strictintermediatepass, Bodystillcolliding/notproduction-safe. Centered
sourcevalidated, no optimizer yet; self-safe paircert957/1006 with49unknown,
objectiveintegration pending. Correctproducer Foot8f3 exact474weights/all8 noNR,
FootROI left8/right1->0; sideviewcaptureapproved noMEDIAyet. Modelacceptance0/5.

Prior checkpoint06b4cb1/sourcebe6f432: r25defaultviewer LIVE (indexddf0),
T/A-IK/Cancel/keyboard/retargetrace browser gates pass, oldr22 rollback andoldBuild
assets retained, NO backendrestart; actualRenderfinunit autorig-storage-renderfin.
FootMEDIA sentonce uid64f5bb91f5be/deliverynotapproval. RealglTFast16-weightnative+
isolatedWebGL raster15/15 passed259efd3f, softwareANGLEonly; NOTfullviewercloth.
Producerretargetauthority isoriginal7c79/caac, NOTviewer-derived7c5. Oldreproduction
aligned_reference+authoredanimQA+helpers exact25channels/full9same4982. All5model
body/clothing/hair acceptance0/5. R4actualREST652+73aliases+sidecar gate next;
directresteditendpoint/fleetmigration unfinished. Canonical/serverhandoff SHA
d523fd0bb48ec6304c77e11aa2942ab1ab92cfaa19c548d8cbc43298e314ac38.

Prior preserved private source e7a1b82: exact-contact cache plus section/Foot tools
3594c35 andfresh-active previewfix dd4eeae. NativefreshnessEXE/selftest/production
GETHEAD passes; r25WebGL/raceQA pending, default r22 unchanged, no backendrestart.
Clothv7 completesall5poses (new2/4 27.14/35.29s) butfinalREJECT:self/strainfail.
Foot474 existingNovelRetarget145keys/mids+145offgrid noNR, toe-tip worsens/clearance
unknown. LiveauditFootaliases prepared; matchedcloseuprecapture pending, noMEDIA.
Root23 section/Foot tests pass; finalsectionkernel6c79ac04 .7088->.2558s onGirl,
1Mgrid .3515s/peakRSS315.7MB, STAGEonly. Canonical/serverhandoff latest SHA
a5e41c47ce28d1fdd2164e025d5ca6e3567b4cdd25aa00011275a47f3a239be3.
Full5modelacceptance0/5. Smooth16-influence cloth/nativeproof anddirectrelocation
CAS/materializer re-audit active; no fullproducer/fleetpromotion.

Prior private checkpoint42d6c3f: owner foot-chat screenshot is historical reply
fromOct9 13:37:58UTC, beforetargetedbackend20:10:24UTC; no laterfallback forbb6,
currentbindingvalid/newfeetjobabsent. Reload+repeatcheck; no backendchange/restart.
Sanitizedreceipt3581d400 inprivateproject.work/screenshot-foot-history-20261010.
Canonical/serverhandoffSHA a1a7481732b3181a95347287cb9a2f874cc5f563544d56b78cc2a5750904a8fd.
Full acceptance0/5; footmotioncapture andoptionalIvy-preservingviewerQA next.

Prior source checkpoint1935cc2 in private MT repo: full-garment source/raw-order
domain guards and separate semantic/control admission,47 Python tests; chat media
layout regression passes. Canonical/server handoff SHA matched
c9a0c5ac6eb565e2a43da973c4044240744fe8d40f8d2457a3916b9fef1d9e28.
F7 whole-skirt request02 completed once50.550s/5498 tokens but malformed output
rejects semantic admission; no physical roots/weights/sewing authorized by it.
No restart/customer task. Live chat-card CSS deployed/public SHA3c70ffd7; browser
firstrow/longtitle checks pass. Whole-main rigid-pelvis field rejected:Bodycontacts
regress all5poses despite strainpass. Corrected48DOF experiment completed5poses
but finalaudit typedtimeout300s, no acceptedartifact. ActualF5 GPUcontact research
full5 Body/Face parity43.83sCPU→~1.90sGPUwarm (~23x), ONLYproper/separated corpus;
broader touching/coplanar fallback pending. Fullclothmembrane/ARAP J.033s,41tests.
FootVision11/69registeredpoints andHairQwenmaskleak bothrejected, no defaultchange.
ConservativeGPU+CPUfallback nowfullBody/self/touch parity, memorybounded7e864source;
F5v2/v3actual5pose solves terminalREJECTED. v3hardstrain0violations but contacts
worseoriginal; do not repeat Hip-onlybasis. Original5jointcloth aliasbasis30DOF
restparity2.24e-16, feasibility/selfobjective next. Red Hair53candidate0151 has0
numericregressions/0improvements, Headaliasnoflex; stablelivevideo sentMEDIAonce
uidbd1856ec7957, deliveryNOTapproval. ResearchFoot3f32 from4e5 preservesgeometry
andrebakes9clips120fps, arbitrarytime matrixdrift2.21e-4 notcontinuousproof.
ActualBodyrootbinding voxel3/20 +external9/20 =>union11/20known,9unknown;
localexterior only, no globalBody/thickness/physicalpinchange claims.
CURRENT liveactive-retarget APIhelperdb1/serviceba81 MT PID2364036, owner705
newClip43545/e927 actualbytes/rotationcalibrationverified; privateinputs outside
RUNS andGETHEAD404. NativepreviewEXE48ef productionnewclipselected+actualSHA
verified, isolatedWebGLcandidate next. Agentperformed3MTrestarts vsrootcap1,
explicitlydisclosed; storage/renderfinPIDs/queuepreserved, no furtherrestarts.
Footpivot814 rejectedbyROI; new474 lowerlegweights eliminateFoottri9→0,
transitionseverityimproves, full9/2341 zeroNR butglobal4982failed. Researchonly,
no full rig/fleetpromotion. Clothv6partial3/5 checkpoints,2timeouts no audit.
Full body/clothing/hair acceptance remains0/5; historical snapshots below are not
current acceptance. This section supersedes prior "no farm call yet" wording.

Latest verified checkpoint: default Unity MT viewer now r22 with visible T/A
feet, real pointer IK move/rotate, failed-save recovery, actual private-chat foot
inspection. Targeted-foot worker is live, MT PID1232222; other production service
PIDs/Renderfin queue untouched. Canonical HANDOFF and server mirror hash match
`0d431767ac3f593bb51496a71bfabfac9dd7bb18702e98e2d0b2cf72a4fbf28c`.
Default index SHA `0add54a02ba721d2fc49a9cebf1a9dbb527670e9b6f62a599a70f1f5f60eefc3`.
Bones inspection hides terrain obscuring feet and restores the environment on
exit. REST transition restores/disables FootGrounding before resetting bones,
preventing a stale animated snapshot from overwriting REST. Production browser
checks passed; these are viewer fixes, NOT producer anatomical/skin acceptance.
Fresh isolated-session foot-chip check returns targeted metrics:9clips/2341
samples in13.466s, no repair. Screenshot fallback is not reproduced on the current
path; exact owner-session provenance remains unknown. Preserve customer artifacts
and production queue. Canonical checkpoint7e9e866; viewer/driver83756a3.
New source-only diagnostic bind-frame rebase7dbff17 preserves animation skin
matrices during anatomical joint relocation;14tests pass, not baseline skin repair.
Knee-volume repair and sided garment-contact constraints remain in development.
Same16sample cloth fit now passes edge/area0failures but collision1/16 remains,
so it is explicitly rejected. Five-model full acceptance still0/5.
Baseline bilateral knee research candidate reduces collapsededges by68 on
9clips/2341samples without layerregressions, but badframes unchanged; independent
artifact review pending. Contact primitive has a reproduced falsecoplanar P1;
fix/tests required before further signedcontact fitting. No fullskin deployment.
Contact P1 source guard fixes now pass5tests; finalreview/refit not yet done.
Latest accumulated6helper researchrig4e5f09e7 independentnarrowaccepted: earlier
neck/shoulder/elbow tracks exact,22kneevertices changed,68fewer collapsededgeevents,
0layerregressions but4982badlayerframes stillfail.28combined tests pass.
Unsigned contact primitive now independently accepted acrossscales, but rootfound
postskinanchoroverwrite APIproof defect; physical lockedhelpertransforms required
before newfit. Sixhelper arbitraryexport and research movingvideo are active next.
Physical anchor helper source repair is preservedb5592b9,9tests; serialized16
cloth excerpt3ca667a6 independentlynarrowaccepted,edgearea/selfcollision0/16,
4424nonadjacentpairs allseparated. Bodycollision/all9/continuousnotproven.
Sixhelper lineage+NovelRetarget source96967ae,15tests;FBX30fps sampledtime proof
fourfractionaltimes maxvertexerror.275mm (notcontinuousbound). Firstcomparison
video rejectedfor mismatchedcamera/light; matchedrecapture active,noMEDIA sent.
FullWalkingcloth boundedresearchrun next; do not duplicate confirmedlive handles.
LATEST supersedespartialcloth optimism: actualWalkingPID30192completed63/63 in
352.289s,19strictfitfailures; no fullserializedartifact/collisionpass. Independent
Bodycollision auditrejects16excerpt:1508freecloth/Body propercrossings acrossALL16,
roots0/Face0. Selfcollision0doesNOTmeanclothing-safe. Agents comparinginputREST/
baselineversuscandidate andimplementingmovingBodyconstraints. Noall9/defaultswitch.
Comparison completed: originalselectedclothREST17Bodycontacts, wholegarment21;
baseline1717/16 vsnew1508/16 butlateWalkingregresses. OfficialWalking7/63fail,
prior19wasstrictedge/area (strictedgealone16). Bodyisopen/nonmanifold; stable
64/96/128localproxy13patches acceptedonlyassurrogateprior. DerivedRESTrepair21→6
stillrejected; furtherpropercrossing/boundarytouchsemanticreview active.
Real neutralfixedcamera bones+skin video sentTelegramMEDIA once uid8742d46ab0e6,
rev r22/4e5f09,deliveryNOTapproval; notpixelcomparison, cameradistancenormalized.
LATESTwholeSkirt audit supersedespartial259pilot: all3337/5616 stillstrainfail3/5,
Body/selfregressionsoutsidefield. Source19indexedcomponents quotient2; onlythin
upper259/252componenthas4anchors, main2736/5184uncontrolled. Wholecomponent
Vision/controlpolicy needed, stop259tuning. AnalyticJac21guardtests/actualonepose
~56x solve/~190xlinearization gain sourceonly (2f2b146), notallrig/GPUtiming.
Hair2755 has0dedicatedjoints;111rootcandidatesnottruth,2644unknownnotfree.
RegisteredHair/wholeSkirtRGB/depth/tri packets prepared, no farm/providercall yet.
Fullfive-modelbody/clothing/hair acceptance0/5; no productiondefault switch.
This is NOT full producer/converter migration: body/clothing/hair acceptance0/5.
F5 streaming GPU validation is terminal2341/2341, with explicitly limited CPU
parity coverage. Real moving-bones video was delivered to /dev (uid71093d934dd1),
not approved. Verify the canonical checkpoint before another experiment. Legacykit52cc
is a proven orchestration orphan, not successful; no status/requeue mutation.

The current primary checkpoint is `R:\3d_video_motion_transfer\HANDOFF.md`
(private repo), mirrored at `/srv/autorig/audits/3d-video-motion-transfer/handoff.md`.
The owner now assigns the whole V3 rig/viewer/converter migration to this session:
live mesh-aligned bones, persistent T/A-pose IK corrections consumed by retargeting,
dedicated clothing/hair bones with Qwen attachment masks, at least five real models,
and proactive agent inspection/correction tools. Read that checkpoint first.
The converter handoff below remains the supporting GPU research checkpoint.

The primary work is GPU erosion → anatomical bones → skinning. The owner has
confirmed migrating humanoids to V3, including a non-destructive calibration
T-pose and adaptation of animation retargeting. The canonical checkpoint links
the new numbered migration plan. MT/viewer/diorama presentation is complementary,
not a substitute for a working skin. F5 stays isolated; this is not yet a
production-ready full humanoid rig or a production-default switch.

Current owner-visible priority: malformed feet and hair spikes in the actual
Unity MT viewer `/api/mt/unity/test/index.html?run=bb6d2681d1bdcf4d694f`.
Main has personally reproduced Walking and the failed bones overlay there.
Independent numeric postvalidation and parallel Vision leg QA are implemented
and tested on the real girl: all nine clips failed numerical checks; Vision's
answer was not admitted. New owner priority is clothing detection from exact
hierarchy plus isolated layer images. Source body/head, skirt and shirt are
separate; shared material does not merge the garments.42 focused tests pass.
These components are not yet mandatory production gates. Another actor edits the
private MT fast-rig; preserve its WIP. See the newest canonical section first.
Ten public sources are inventoried, but ten validated rig scenes do not yet exist.

Source e20cb33 joins existing Vision taxonomy and a fresh limb-count
observation with V3 evidence; runtime parallel orchestration is still pending.
The owner explicitly requires name/description/category/body/limb analysis
alongside geometry before scenario selection. Existing elf classification is
same-source; one completed f7-only Vision call returned2 arms/2 legs/1 head in
11.068s. Do not repeat it or mistake the observation for a validated rig.

H0 bind/H3 calibration math and real H1 tests are retained. Cardinal elf views
pass13/25 numeric joint candidates (core6/15), oblique only4/25 (core1/15).
Existing MT parts labels are now privately resealed to exact source vertex
order, but only20.62% of faces were directly observed. Next: parallel semantic
branch integration plus soft label/C2 body-proxy/correspondence, then actual
bones, GPU skin and retargeting. Full V3 humanoid success remains0/6.

Read the authoritative [converter handoff](autorig-online/backups/repair-rig-dispatch-20261006/converter-source/handoff.md)
before continuing. It contains exact commits, tested timings, private artifact
locations, blockers, next steps and restoration rules. Then read current
AGENTS.md and the renderfin-pipeline skill and verify Git / live node state.
Do not rely solely on the chat summary or assume a historical snapshot is live.

Server mirrors of that full handoff:

- VPS (`ssh autorig-vps`): `/srv/autorig/audits/animal-gpu-development/handoff.md`.
- F5 (`ssh farm-f5`): `C:\3d\GLB_Convverter_Git\GLB_Convverter_WebServer\.runtime\animal_gpu_research\handoff.md`.

After each material verified milestone, blocker or next-step change, update
the canonical converter handoff and synchronize both mirrors with SHA-256
verification. Keep secrets, private model data and licensed assets out of Git.
F5 remains development-only; do not automatically re-enable production work.
Do not use the owner's worker-4090. Warm Blender is explicitly permitted.

This scoped development checkpoint does not overwrite the unrelated historical
`autorig-online/docs/handoff.md`, nor the repository's existing unrelated WIP.

## Public chat · V3 (2026-10-10, evening): auto-translate, avatars, author pages

- Service `autorig-public-chat` (127.0.0.1:8278, `deploy/public-chat/`, install via `install.sh`; build pchat-20261010.3).
- Auto-translate: readers send their browser language (`lang=` on /messages and /stream); each message is translated in
  batches of 5 by `/api/text2text`, cached per (message, language), pushed as `tr` events; live config key
  `auto_translate`. Widget: translated text + "Show original" toggle (RTL aware).
- Avatars: `GET /api/avatar/<handle|me|t-<task>>?s=64|128|256` (round WebP from the newest safe public model: MT
  proj/front_lit+mask, else the GLB via mt.render, else poster; NudeNet gate), `GET /api/people/<handle|me>`,
  host-local `GET /api/people/resolve?user_id=|email=|anon_id=` (for Multiplayer · V3: returns handle/url/avatar, never ids).
- Author gallery: `/author/<handle>` and `/<ru|zh|hi|fa>/author/<handle>`, server-rendered, i18n keys `pchat_author_*`.
  Handle = 10 hex HMAC; guests are Guest-XXXX; a user without nickname shows User-XXXX unless they already chatted.
- nginx: locations appended to `/etc/nginx/snippets/autorig-public-chat.conf`.

## Scenes · V3 (2026-10-10 15:15 UTC, на проде)

- `unity/test` теперь -> `scenes-v3-r2-20261010` = страница r32 (все live-правки Viewer) + wasm/data со сценами
  (SceneDirector, ScenePackage, LiveLayer). Откат: `ln -sfn .../pulse-alt-r27-20261010 test`. r31/r32 сцен-кода не имели.
  Multiplayer-сборка (замок 14:42Z) включит Scenes-скрипты из общего проекта сама.
- Собор: лучи работают (свой луч + тень + 75 м теней), солнце az90/el50 на центр прохода, 2 лайтмапа; бандл
  `cathedral-v3-4ea469ed1097f287` опубликован, прошлый - в `previous` каталога. Спонза: дождь только в зоне двора,
  в галереях сухо. Каталог `/api/mt/scenes`: Собор, Спонза, затем рантайм-пакеты (icon.jpg, glTFast + scene.json).
- Публикация сцены: `tools/scenes_v3_catalog.py` (R:\3d_video_motion_transfer) -> index.json + thumbs в `unity/scene-media/<id>/`.

## Live processing · V3 — классические задачи конвертера (2026-10-10 15:30 UTC, на проде)

Владелец, задача `66ba97ba` (конвертер f1): минуты вообще никакого прогресса, пока воркер ригал; нужны (a) полоса с момента,
когда воркер взял задачу, (b) вьювер показывает вокселизацию, потом эрозию / создание костей интерактивно, (c) после рига
модель проигрывает КЭШИРОВАННУЮ анимацию всей визуализации рига стадия за стадией.

- **Как устроено** (MT, коммит `mt/live_classic.py` + хвост `live.mount`): у классической задачи нет MT-рана, поэтому MT делает
  синтетический ран `runs/<sha1("classic:"+task_id)[:20]>/` (phases.json conveyor=`classic` + `live/`) и кормит его тремя источниками:
  1. **лог конвертера** `<guid>_progress.txt` (бэкенд отдаёт его как `GET /api/task/{id}/progress_log?full=1`, без авторизации), строка
     за строкой, в стадии `cl_prepare, cl_openpose, cl_pose, cl_rig, cl_retarget, cl_export, cl_preview` + мелкие факты
     (конечности, маркеры, дорожки), каждое с i18n-ключом `live_cl_*` / `live_stage_cl_*`, никакого сырого текста;
  2. **вокселизация и «первый скелет»** по `glb_cache/<task>_prepared.glb` (он есть через секунды после старта воркера):
     `mt.live_voxels` и `mt.fastrig --no-labels` детьми, пока конвертер занят OpenPose/позой (90 + 50 с мёртвой зоны);
  3. **настоящий риг конвертера** из `glb_cache/<task>_animations.glb` (когда бэкенд его закэшировал после TASK_COMPLETE):
     суставы/кости из skin GLB, каждый сустав против глубины эрозии вокселей (красные метки = вне тела), потом `run/finish`.
  Ран «done»-задачи собирается так же заново из лога и кэша (бэкфилл, ~5 с), поэтому старые задачи тоже можно проиграть.
- **API** (MT, nginx `/api/mt/` уже проксирует): `GET /api/mt/classic/{task_id}/live-run` → `{run}` (+ запускает слежение; доступ —
  тем же cookie, что `/api/task/{id}`, через бэкенд) и `GET /api/mt/classic/{task_id}/state` → `state` стрима (стадии с
  измеренными/ожидаемыми секундами, progress 0..1, eta_s). Дальше обычные `/api/mt/runs/<run>/live` и `/api/mt/files/<run>/live/...`.
- **Вьювер**: `mt/static/live.js` сам распознаёт `run=task&api=…/api/task-viewer/<id>` и берёт ран у `live-run`; если страница
  открылась уже ПОСЛЕ стадий (задача только что завершилась), то при загрузке рига кэш-стрим проигрывается заново: воксели по
  уровням, эрозия до ядра, суставы, кости, тепловая карта весов (кнопка «повтор» в `#ui` делает то же вручную).
  Новое в потоке Fast analysis: `rig/fast_category`, `rig/fast_bones` (кости вне меша — красные метки), строки в аватар.
- **Что мне нужно от конвертера / Opus (замеры стадий, 1-минутный бюджет рига)**:
  - формат лога — сейчас `YYYY-MM-DD HH:MM:SS.mmm <текст>` в **местном времени воркера (UTC+7, без зоны)**; нужен UTC или смещение
    (`…Z`), чтобы считать стадии по часам без догадок;
  - строки, которые я разбираю (не менять формулировки без записи сюда; новые строки добавлю в `_RULES` за минуту):
    `Conversion started.`, `Geometry: N vertices, M polygons.`, `Model preparation completed.`, `OpenPose analysis: detecting…`,
    `Arm|Leg|Fingers (left): yes|no; … (right): …`, `OpenPose analysis completed: markers saved (N markers).`,
    `Pose preparation…`, `Markers positioned.`, `Leg pose corrected.`, `Hands straightened.`, `Fingers detected by raycast: left a, right b.`,
    `Pose correction completed.`, `Starting rigging process…` / `RETRY n: Starting rigging process…`, `Rig created.`,
    `Starting animation retargeting…`, `Created N action tracks.`, `Animation retargeting completed.`, `Unity export started.`,
    `Unity package exported.`, `Preview video created.`, `Conversion completed. It took …`, `TASK_COMPLETE`, `FAILURE|ERROR: …`;
  - замер `66ba97ba`: prepare 3 с, OpenPose 89 с, поза 49 с, **rig 67 с (с одним RETRY: попытка 1 ≈ 23 с впустую + ещё 45 с)**,
    retarget 63 с, Unity export 94 с, preview 70 с; итого 7 мин 56 с. Рига «внутри» (voxel / skeleton fit / weights) лог не показывает;
  - чтобы стадия рига была видна изнутри, нужны строки с отметкой времени: `Rig: voxel solid ready`, `Rig: skeleton fitted (N bones)`,
    `Rig: weights painted`, `Rig: check failed — <причина>` перед `RETRY n`, и для OpenPose `OpenPose: frame k/n`
    (полоса без «мёртвых» 90 с); тогда live.js покажет их без догадок вместо моей оценки по медианам;
  - и бэкенду: запускать прогрев `animations.glb` уже по `Rig created.` / `Starting animation retargeting`, а не после TASK_COMPLETE —
    настоящие кости появятся на экране на ~4 минуты раньше (сейчас до конца задачи вьювер показывает «первый скелет по геометрии»).
- **Страница задачи**: `task-v3-shell.js` для классической задачи берёт реальные стадии/процент из `/api/mt/classic/<id>/state`
  (вместо часов против медианы 12 мин), а запрос же стартует слежение ещё в очереди — полоса и вокселизация начинаются сразу, когда воркер взял задачу.

- **Live processing · V3, дополнение 15:45 UTC**: (1) в живой `unity/scenes-v3-r2-20261010/index.html` вставлен `<script src="/api/mt/static/live.js" defer></script>`
  после строки scene-plate (без него LiveLayer не получает поток; шаблон `Assets/WebGLTemplates/AutoRig/index.html` его содержит — пересборки и ручные
  копии индекса должны его сохранять, Viewer/Scenes). Сборка с LiveLayer уже в r2: слои вокселей/ядра/костей/весов/проверок работают, проверено на настоящей GPU
  (классическая задача `66ba97ba`, повтор по кэшу, DEV 6526). (2) При загрузке рига кэш-стрим проигрывается заново, если ран свежий (<30 мин) —
  воксели по уровням, эрозия, суставы, кости, веса. (3) Находка: у задачи `d76f84c3` (повторная загрузка prepared.glb) риг конвертера выше меша в 1,5 раза —
  судья центровки честно красит 66 суставов из 67 красным; стоит смотреть масштаб риг/меш у конвертера на уже подготовленных входах.
  (4) Случайно на 1 минуту (14:37) выставил `/dev/null` на VPS в 644 autorig (cp -p); сразу исправлено на 666 root, сервисы живы.

## Rig path · V3 — риг первым, кластеризатор деталей (2026-10-10 15:50 UTC, на проде, MT `1c87b65`)

- **Классика (не-V3 задачи)**: `mt/classic_mirror.py` (цикл в autorig-mt, раз в 3 с читает `tasks` из БД сайта read-only)
  даёт каждой новой задаче зеркальный ран сразу при загрузке (`task_agents/<task>.json`, та же привязка, что у
  `task_agent.py`) и запускает `python -m mt.rig_first` по GLB загрузки (или `glb_cache/<task>_prepared.glb`):
  фронт по геометрии, пропы отдельно, fastrig, 8 клипов. Бэкенд (`task_page_v3_routes.fast_rig`, релиз
  `rigpath-fast-20261010`) отдаёт этот риг вьюверу `/api/task-viewer/<id>/files/task/rig/*`, пока классический
  `animations.glb` не в кэше (`viewer.fast_rig: true`, `revision: fast-<mtime>` → потом `rig-<mtime>`, оболочка
  перезагружает сама). Фазы зеркала идут за классикой: `rig_fast`, `rig` (waiting/running с ready/total/done/failed).
  `GET /api/mt/classic-mirror` — состояние; `python -m mt.classic_mirror --repair [--apply]` — зеркала, отставшие от
  задачи (5 починено, включая `b5c8da5e…` задачи 66ba97ba; менялся только статус фаз). Выключить: `MT_CLASSIC_MIRROR=off`.
  Замер: `9a34e8c0` (наш тест) — риг+8 клипов во вьювере через 2.7 с после аплоада; классика закончила через 10.5 мин.
- **V3-конвейер** (`mt/v3_conveyor.py`): source → rig (`rig_first.build`) → fast → retarget → **риг с клипами во вьювере**
  (событие `first_ready` / v3 `artifact publish:rig_in_viewer`, `timings.first_rig_in_viewer`) → analysis (Vision) →
  если Vision не согласен (фронт, план тела, оружие в словах) — риг заново за секунды → QA → publish. Prerig-ребёнок
  убран. Замер `8a1b1cc5` (ран `a20f3f57…`): 1.5 с от старта рана, ~4 с от аплоада. Причина 300–400 с была в ожидании
  Vision фермы (2 запроса подряд) ДО рига и численном QA до публикации. Валидатор QA, убитый сигналом (рестарт MT),
  больше не вердикт: ждём 8 с (останавливающийся сервис отменит ран → store его возобновит), иначе повтор один раз.
- **Кластеризатор** `mt/parts_cluster.py` (op `parts_cluster` в реестре fast analysis, `DEFAULTS_REV=3`): шеллы + контакты
  (cell hash) + PCA + кость у контакта + слова Vision/названия; зеркальные близнецы (руки манекена, наплечники) не пропы.
  0.1–0.3 с на 24–120k вершин, 1.5 с на 300k (CPU VPS, GPU не нужен). Выход: `analysis/parts.json` (id, kind, verts,
  bbox, attached_bone), `parts/<id>.glb` (каждый проп отдельным GLB), `objects/objects.npz`+`objects.json` (parents) →
  fastrig делает проп жёсткой костью `Prop` под этой рукой. Тесты: меч воина 66ba97ba (LeftHand), меч рыцаря 9fb7d4d9,
  синтетика 9de055bf; без ложных: аниме-девушки, Ариэль, манекены.
- **fastrig.py — мои 2 ханка (Rig tools · V3, учтите)**: `run_info` читает ещё `proj/forward.json` (догадка фронта);
  цикл prop-костей берёт родителя из `objects/objects.json` `parents`. Больше fastrig не трогал.
- **Агент сессии**: инструменты `list_parts` (публичный), `separate_part` / `attach_part` (владелец; перестройка рига
  ~1 с, клипы сохраняются); дайджест fast analysis содержит строку PARTS. В `/dev/tools` новая группа `fast`
  (ops реестра, rig_first, parts_cluster, classic-mirror); `catalog.py` синхронизирован и в `/srv/autorig/astra/src`.
- **Не сделано / дальше**: fastrig ставит руки согнутой позы (воин с мечом) как висящие — кисть уходит на 0.3 H от
  настоящей (это и ломает бинд воина; для Rig tools · V3). Классический конвертер по-прежнему биндит меч (BindCheck 445):
  `parts.json` можно отдать ему. Астрин QA-гейт `rest_relative_escape` применяется чисто и тесты проходят, но QA
  дольше в ~5 раз (воин 7.5 → 36 с; 300k ≈ 8 мин, риск таймаута 600 с) — НЕ установлен.
- **Рука из вокселей (на будущее, не строилось)**: заменить кисть/руку на меш из воксельной сетки = (1) маска кисти
  из `parts.json`/весов Hand*; (2) вокселизация с динамической детализацией нод (уже в API опций) в объёме кисти +
  захват рукояти пропа; (3) marching cubes / dual contouring → изоповерхность, ретопология под деформацию пальцев;
  (4) перенос UV/текстуры проекцией с исходной кисти; (5) сшивка по запястью (общий контур, сварка вершин);
  (6) веса Hand/пальцев геодезически, проп — жёстко к кисти; (7) QA: зазор кисть–рукоять, растяжения в Idle/Walking.

## Rig tools · V3 — инструменты скина и суставов с опциями (2026-10-10 16:00 UTC, на проде)

- **Зачем**: владелец в V3-сессии задачи 91a9513a: «плечи цепляют лишнее, как и локти … понизить дальность трешодов»,
  агент ответил, что инструмента нет. Правило AGENTS.md «Tools Have Options».
- **Код** (MT-репо): `mt/skin_tools.py` (SPEC опций, движки, метрики, версии, CLI `python -m mt.skin_tools
  get|reskin|recentre|use|undo|list|save --run <run>`), `tools/patch_skin_options.py` (24 якорные правки
  `mt/fastrig.py`: `skin(..., opts)`, `build(..., skin_options=None)`; дефолты = прежние константы, проверено:
  пересборка с пустыми опциями даёт те же веса), `tools/patch_agent_rig_tools.py` (строка 7 в SYSTEM + блок
  инструментов в конце `mt/agent.py`). Скрипты идемпотентны: **Rig path / Fast analysis, если перезаливаете
  fastrig.py или agent.py целиком — прогоните их снова**, иначе инструменты пропадут.
- **Опции** (`SPEC`): `influence` {shoulder elbow wrist hip knee ankle neck spine} 0.1–2 (1 = как сейчас),
  `influence_bone`, `capture_radius`, `smooth_steps`, `smooth_joint_steps`, `smooth_keep`, `max_influences` 1–4,
  `distance_mode` euclidean|geodesic, `recentre`/`recentre_strength`, `prop_mode` rigid|hand|skin, `hair_weld_band`,
  `limb_root_cut`, `clavicle_distance_scale`. Ошибки — локализованные (en/ru/fa/zh/hi) с допустимым диапазоном.
- **Движки**: `refine` (любой риг: классический 71 кость ARP, fastrig, judge) — свой вес конвертера сохраняется, у
  каждого сустава (плечо = корень ключицы и корень плеча, локоть, запястье, бедро, колено, лодыжка, шея) кость
  конечности держит вершины остального тела только в пределах influence × сегодняшнего радиуса (от пивота;
  geodesic — по треугольникам), и наоборот; после сглаживания ограничения применяются снова. `rebuild` (только
  fastrig) — fastrig.build с закреплёнными суставами и опциями, веса переносятся в текущий GLB по сварке (клипы
  остаются).
- **Метрики** (одни позы до/после, каждая крутит один тип сустава): leak (неподвижная группа сдвинута > 1 % H дальше
  1.5 радиуса сустава), lag (движущаяся конечность отстаёт), stretch_2x/4x. A/B картинка `rig/skin/v<N>/ab.png`.
- **Версии**: `<run>/rig/skin/index.json`, `v0` = риг как был (не меняется). `use` пишет версию туда, откуда читает
  вьювер: `rig/rigged.glb` + `rig.json.built_at` (MT/V3), или для классической задачи — `glb_cache/<task>_animations*.glb`
  (атомарно; оригинал конвертера в v0; скачивания FBX/ZIP остаются от конвертера). Вьювер перезагружает сам (~10 с).
- **Агент**: `rig_options_get` (публичный, чтение), `reskin`, `recentre_joints`, `rig_version list|use|undo`,
  `rig_options_save scope=run|category` — меняющие риг только для owner/admin-сессий; category — только owner
  (`--admin`), хранится в `fast_registry.json` → `skin_options` (вне `categories`, версии + history), fastrig.build
  применяет его к новым ригам этой категории. В `/dev/tools` все пять видны.
- **Проверено на проде** (задача 91a9513a, ран 475f08c2ae7aae69c11d): reskin shoulder 0.4 elbow 0.4, 4 с — плечи leak
  72→0, lag 89→36, локти leak 188→0, stretch локтей 143→130, плеч 255→257; применено как v1, вьювер задачи загрузил
  v1 сам. Шея (2676 leak) не трогалась. DEV 6532–6533. Откат: `rig_version undo` или
  `python -m mt.skin_tools use --run 475f08c2ae7aae69c11d --version v0`.
- **Не сделано**: per-task-owner доступ к reskin (сейчас только admin), отдельная кнопка A/B во вьювере (A/B =
  переключение версий), recentre для классики двигает мало (суставы ARP уже у центра своего сечения).
- **Раунд 2 (16:16 UTC, вердикт владельца DEV 6534 «проблемы между предплечьем и локтем»)**: у конвертера жёсткий
  шов весов у локтя (arm_stretch | forearm_twist_2), в клипе рвётся. Добавлено: опция `blend_width` {joint: 0.2–4
  радиуса} (refine: плавный переход по оси конечности, центр в суставе, каждая сторона со своими костями, twist
  тоже); метрика `clip_2x/clip_4x` — рваные рёбра на собственном клипе GLB (то, что играет вьювер), по суставам;
  внутри «трубки» конечности дальность меряется вдоль оси (не по геометрической группе — рукав у локтя не рвётся);
  сфера сустава 2 радиуса; `METRICS_REV=2` (старые кэши метрик пересчитываются). `patch_agent_rig_tools.py` теперь
  заменяет свой блок и строку промпта, если они старые.
- **91a9513a сейчас v3** (`blend_width` локоть+плечо 1.0, `influence` 0.4/0.4): разрывы >4x в клипе локоть 126→5,
  плечо 315→228, всего 692→363; худший кадр 46/48 у левого локтя 140→4, у правого 43→2; плечо lag 74→0, но leak
  тестовой позы плеча 19→167 (широкий бленд тянет больше груди). v1 (только influence 0.4) шов не лечил. DEV 6535.

## Limb collision · V3 — рука в теле: причина, детектор, инструмент (2026-10-10 16:50 UTC, на проде, MT `72a610a`..`ea3f503`)

- **Причина 98c1247c (правое предплечье в торсе, кисть из живота):** ретаргет конвертера добавляет аддитивный
  NLA-слой `arms_spread` (`tpose_remap_animation.py`, по ширине таза, тут 17.9°) — поворот `c_arm_fk.l/.r` вокруг их
  ЛОКАЛЬНОЙ X. Ролл руки ARP берёт из изгиба локтя почти прямой T-позы (prepare оставил правый локоть в 6 мм от линии),
  поэтому X куда угодно: у этой модели правая X смотрит вперёд (+X опускает руку в тело), левая — вверх (+X уводит
  назад). Цифры: без слоя правое плечо совпадает с клипом библиотеки до 1.9° (было 16.8°), ошибка зеркала кистей в idle
  13.5→2.5 см. Не веса и не T-поза. Фикс конвертера: знак слоя на руку, которую +X опускает (`5650feb`, не выкачен).
- **Детектор** `mt/limb_collision.py` (numpy, 5–12 с на модель): предплечье+кисть внутри остального тела (обобщённое
  число обмоток по кластеризованному телу без этой руки + псевдонормали), растяжение рёбер у плеча (смаз рукава),
  асимметрия (углы рук в покое, рассогласование ролла, зеркало локтей/кистей в idle). Пишет
  `analysis/limb_collision.json`, `analysis/limb_collision.png`, `results.limb_collision` в `analysis/fast.json`
  (оп `limb_collision` в реестре быстрого анализа), live `rig/fast_limbs` → красные метки на костях (live.js).
  Запуск: V3 — после каждого `_retarget` в фоне; классика — `live_classic.final_rig` в фоне, `--source classic`.
- **Инструменты агента** (в `/dev/tools`): `limb_check`, `fix_limb_collision(side, options)` (abduction_deg, swing_deg,
  undo_spread_deg, restraighten_rest, auto+max_auto_deg+clearance_pct_H, reweight_band+band_pct_H, clips), `limb_fix_undo`,
  `limb_options_get`. Офсет в покое вмножается в каждый ключ плеча (точно при любом ролле). V3 — меняет `rig/rigged.glb`
  (прежний в `analysis/limb_fix/`), классика — копия `analysis/limb_fix/classic_v<N>.glb`, файл конвертера не трогается.
  98c1247c: `--fix right {"undo_spread_deg": 35.83}` (= фикс конвертера) → правая 89.6%→0%.
- **Судья центровки классики** сравнивал суставы T-позы с вокселями A-позы: теперь `live_voxels --rest-glb` строит поле
  по bind-позе самого рига (`live/voxfield_rest.npz`): 98c1247c «снаружи» 22→0.
- **14 последних классических задач:** high 4 (98c1247c; 66ba97ba левая в бедре; e0cdbf0b рука под плащом — плащ
  считается телом; 8b16a847 кисть в большой голове), medium 2 (9d466924, 91a9513a), чисто 7, у 2ad5c0c8 руки не
  привязаны к костям (0 вершин предплечья/кисти, всё на `root.x`). V3 fastrig 8369add — high с обеих сторон.
  DEV 6536, 6537.

## Viewer integration · V3 — одна сборка вьювера со всем (2026-10-10 16:42 UTC, на проде)

- **`unity/test` → `v3-all-r1-20261010`** (было `scenes-v3-r2-20261010`; откат и история — `unity/.test-history.log`).
  Одна сборка: Scenes (лучи, бейки, плашка, рантайм-пакеты) + Multiplayer (комнаты, WASD-контроллер) + LiveLayer + HDR-пульс
  из рабочего дерева (WIP Codex не коммитился) + исправленные стенды. Собрано `ScenesV3Build.BuildPlayer -sv3PlayerOut Builds/WebGL-v3-all`.
- **Шаблон в Git = живая страница** (MT `7f1fe2b`, `459d54f`): все живые ханки Session agent (`agentApply`, `viewerNow`, `viewerErr`,
  `progSnap`, `MPL`, строки агента на 5 языках, `lines.mp`), выключенный звук аватара, `live.js`, `scene-plate.js`, `mp-rooms.js`.
  Больше не править index.html только на проде: правка → шаблон → коммит.
- **Адаптер комнат** (хвост шаблона): `autorigUnity.multiplayer` (контракт Session agent) поверх `autorigUnity.rooms` (Multiplayer),
  события `autorig-rooms` → `autorig-mp`. Вход в самую людную комнату ждёт модель, риг и плашку сцен; пока комната недоступна
  (`offline`), агент не говорит «ты в общей комнате».
- **Стенды** (studio/disco/…): `Main.unity` старше StandLit, поэтому `StandScene` без шаблона падал. Теперь берёт URP Lit из
  `Assets/Resources/AutoRigStand/StandLit(.Emissive).mat` (в Git). Сам фолбэк — в `StandScene.cs`, а вся папка `Assets/Scripts/Runtime`
  в MT **не отслеживается Git** (кроме Viewer.cs) — правка в рабочем дереве, в коммит не вошла.
- **Агент сессии**: `room_stats`, `join_busiest_room` подключены (`mt/agent.py` MT `63bb9e4`, на проде, рестарт autorig-mt 16:12 UTC),
  `Mp*` методы разрешены, `/dev/tools` их показывает (`catalog.py` = прод, обе копии).
- **Для Multiplayer · V3**: у классических задач один клип `Animation` → `rigged:false` в `MpInfo`, комнаты «norig»; в play-режиме
  в Спонзе камера/спавн у колонны — модель почти не видна (кадр DEV).
- **Регрессионный гейт**: `gate all --tier base` после выкладки — GATE PASS (65 PASS, 16 XFAIL, 1 PENDING), отчёт `autotests/reports/20261010T164445Z-all-base.json`. Самого вьювера (WebGL) в корпусе нет.

## Downloads · V3 — скачивание только новых файлов, экспорт по запросу, только с Unlimited (2026-10-10 17:15 UTC, на проде)

- **Старый конвейер выключен для новых задач** (см. строку в «Координации»): флаг `classic_new_tasks` в живом
  `v3-routes.json` (`v3_intake.classic_new_tasks_enabled`), при `off` все маршруты (website/api/telegram/generation/retry)
  идут в V3, `create_conversion_task` не создаёт новых rig/convert. Проверено: анонимная загрузка `e8960693` (тест,
  синтетика 8.8 КБ) → `pipeline_kind=v3`; с 17:00:32Z новых классических задач 0, классических в работе 0.
- **API** (`backend/task_downloads_v3.py`, роутер в main.py после task_page_v3): `GET /api/task/{id}/downloads-v3`
  (манифест: access, plan, rig sha/version/clips, formats со state ready|instant|missing|queued|running|failed),
  `POST|GET /api/task/{id}/downloads-v3/{fmt}` (запуск/статус: реальный процент + стадия), `GET …/{fmt}/file?v=<sha16>`
  (X-Accel из `glb_cache/<task>_v3exports/<sha16>/`). Форматы: `glb` (rigged.glb рана, все клипы), `fbx` (все клипы
  takes), `zip` (GLB+FBX+клипы GLB+README), `clip-<i>.glb` (режется на VPS за ~0.1 с), `clip-<i>.fbx`. Внутренний клип
  `rig_check` не отдаётся. Ключ кэша = sha256 текущего `rig/rigged.glb` (новая версия Rig tools/limb fix = новый sha16,
  `version` = id из `rig/skin/index.json`). Только задачи `pipeline_kind=v3`; классические получают ссылку на свои файлы.
- **Гейт** (fail closed, до любого экспорта): админ — всегда; владелец задачи с активной подпиской
  (`users.autorig_subscription_status in active|canceling` и `period_end > now`) — да; аноним → 401 `download_signin`,
  не владелец → 403, без подписки → 402 `download_subscription`; detail = `user_error_detail` + `plan` (цена из config,
  checkout `/buy-credits/checkout/autorig-unlimited-monthly?source=task_download&task_id=…`, login
  `/auth/login?next=/task?id=…&dl=1`). «Смотреть как бесплатный» для админа: кука `ar_view_as=free`
  (`POST /api/admin/view-as {mode}`, глаз в панели) или `?as=free` (для `site_as_owner` Астры) — только для админ-сессий.
- **Экспорт FBX**: Blender на VPS нет, MT-очередь Blender без воркеров (все её задачи «no Blender worker took it»).
  Свой pull-воркер `deploy/v3-export-worker/export_worker.py` на **f7** (Blender 5.1, CPU, задача планировщика
  `AutoRig V3 Export Worker F7`, AtLogOn, pythonw, лог `C:\Users\user\v3-export-worker.log`, ключ
  `%USERPROFILE%\.secrets\autorig-v3-export-worker.key`, хэш в `/srv/autorig/secrets/v3-export-workers.json`).
  Скрипт Blender — `backend/v3_export_blender.py` (воркер берёт его с сервера на каждую задачу), реимпорт FBX —
  проверка, провал = failed. Очередь `/srv/autorig/data/var/v3-exports/queue`, lease 120 с, 3 попытки, без воркера
  10 мин → failed. Замер 4f85d45e (300k верш., 18 МБ): FBX 50 с (из них ~25 с выгрузка с f7), клип FBX 36 с, ZIP 108 МБ +15 с.
- **UI**: `js/task-v3-downloads.js` + `css/task-v3-downloads.css`, подключены в `task-v3.html`; кнопка загрузки
  (`#tv3-classic`) открывает панель: GLB/FBX/ZIP, клипы, прогресс-бар со стадией, пейволл (замок, 3 строки-иконки,
  $20/мес, «Войти через Google» / «Подписаться» в новой вкладке; вкладка ждёт активации подписки и продолжает скачивание
  сама). 42 ключа `dlv3_*` / `error_download_*` / `error_classic_pipeline_off` во всех 5 языках.
- **Автотесты**: кейс `v3_downloads` (base: `tests.test_task_downloads_v3`, 14 тестов; extended: `export_fbx` —
  корпусный `8369addb.mt-rig.glb` через настоящую очередь и воркер f7 → валидный FBX, 8 takes, ≤240 с; PASS за 32 с).
- **Проверено / не проверено**: аноним в Chrome (ru) и на телефоне (fa) → панель → FBX → пейволл со входом, DEV 6550/6551;
  админ и `?as=free` — на уровне роутера (harness с реальной БД, очередью и f7): 402 без экспорта; админ получает GLB,
  FBX, ZIP, клип FBX. **Не проверено в браузере**: путь вошедшего пользователя (Chrome не залогинен на autorig.online,
  логиниться нельзя), сама страница Gumroad, отдача файла через nginx X-Accel под сессией.
- **Не сделано**: Unity package (нужен конвертер с Unity, по запросу не подключён); старые эндпоинты скачивания
  классики остаются на прежних правилах (владелец + кредиты) — решение владельца, переводить ли их на подписку;
  ~~второй экспорт-воркер~~ — сделано 2026-10-11, см. ниже.
- **Экспорт-воркеры на 4 боксах (2026-10-11, приказ владельца «да поставь»)**: тот же воркер и задача планировщика
  `AutoRig V3 Export Worker F1|F2|F7|F13` (AtLogOn user, pythonw: f1/f13 Python313, f2 Python310; Blender 5.1;
  ключи по боксу в `/srv/autorig/secrets/v3-export-workers.json`, на боксе `%USERPROFILE%\.secrets\autorig-v3-export-worker.key`).
  Маршрутизация: pull — задачу берёт свободный воркер (занятый не опрашивает), зависший lease (120 с) переходит к
  другому. Для канареек `enqueue(..., pin=<box>)` (у закреплённой копии свой job id; клиентские задачи не закрепляются),
  autotests `v3_export_fbx` принимает `params.worker`. Манифест `exporter.workers` — кто опрашивал за 60 с.
  Проверено настоящим экспортом корпусного `8369addb.mt-rig.glb` на каждом боксе (закреплено): f1 32 с, f2 28 с,
  f13 32 с, f7 34 с — валидный FBX, реимпорт ok, 8 takes; незакреплённый extended-гейт PASS 26 с. Релиз
  `dlv3-pin2-20261011`, коммит `c24b7382`. Заметки во флоте: `operator_notes.json` для f1/f2/f7/f13.
- **Уведомления V3 в Telegram (регрессия V3-only, починено 2026-10-10 20:24 UTC, релиз `v3notify-20261011`)**:
  с 17:00 «New task started» шёл только из `tasks.start_task_on_worker` (классический диспатч), а «Task completed»
  только для `done` (у V3 почти всё `needs_review`) и без контент-рейтинга. Теперь `backend/v3_notify.py`:
  `schedule_new` из всех путей создания V3 (`v3_intake.admit_glb` — сайт/API/Telegram/retry, нормализация FBX/OBJ,
  генерация, `bind_existing_task`), один раз на задачу (`telegram_new_notified_at`); `schedule_terminal` из
  `v3_runtime_mount.after_commit` для `done` и `needs_review` (убитый и перезапущенный ран сюда не доходит):
  NudeNet-рейтинг по preflight-рендеру или `proj/front_lit.png` рана, затем классический «Task completed» +
  строка needs review с причинами QA, ссылка «V3 viewer», тайминги фаз; без ожидания видео (`video_wait_seconds=0`).
  `telegram_bot.reserve_and_broadcast_task_done/broadcast_task_done` получили `extra_html` и `video_wait_seconds`.
  Ошибки V3 — прежний `reserve_and_broadcast_task_error`. Проверено задачей `21da1b8e` (оба сообщения в канале,
  рейтинг safe). Бэкфил: один дайджест о 20 задачах 17:00–20:19 без уведомлений, им проставлены флаги.
  Перезапущены autorig-storage и autorig-storage-telegram. Три теста (`test_v3_intake_runtime`,
  `test_v3_task_creation`, `test_telegram_generate_button`) читали живой `v3-routes.json` прода и падали после
  V3-only — изолированы (`isolate_live_routes`).

## Limb stabilization · V3 — стабилизация конечностей перед ригом (2026-10-10 17:40 UTC, на проде, MT `17a38c2`+)

- **Зачем**: владелец, задача d1522b45 («Fantasy warrior with a raised weapon»): меш в анимированном кадре (обе руки над
  головой с оружием, правое колено поднято), классика на f7 за 1060 с положила T-шаблон поверх позы (руки не привязаны:
  0 вершин предплечья/кисти, limb_collision «arm not skinned»), fastrig ставил висящие руки. Источник — Tripo FBX со
  своим скелетом (67 суставов, скин, bind-поза = поза меша, клипов нет); V3-intake отдаёт его как assimp-GLB со скином.
- **Код** (MT-репо, `mt/limb_stabilize.py`, монтаж `tools/patch_limb_stabilize.py` — якорные идемпотентные ханки в
  `fastrig.py` (`model_glb(run)`, `build` читает `stab/model_canonical.glb` когда этап сработал, пинит суставы из
  `stab/joints.json`, пишет `fit_check`+`stabilization` в каждый rig.json), `parts_cluster.py` (тот же меш),
  `rig_first.py` (этап до fastrig, fit check после, клип «Source pose»), `v3_conveyor.py` (клип «Source pose» в
  `_retarget`, центровка по `voxfield_rest.npz` когда меш повёрнут, **фикс живого бага** ниже), `agent.py` (блок
  инструментов в конце). **Если перезаливаете эти файлы целиком — прогоните `patch_limb_stabilize.py mt` снова.**
- **Этап** (`rig_first.build` → `limb_stabilize.stage`, 0.3–0.8 с на 30k вершин): состояние позы из исходного скелета
  (цепи по `skin_tools.bone_kind` + иерархия; углы: подъём/вынос руки, локоть, наклон бедра, колено) → T | A | hanging |
  posed; в анимациях источника ищется T/A-кадр (`frame_world`), берётся лучший. Повороты цепей сегментно-жёсткие
  вокруг суставов (плоскость сгиба локтя/колена — к фронту/назад), меш идёт за ними LBS по весам источника,
  **ужесточённым**: вершина следует за конечностью только в её «трубке» (≤1.5 радиуса сегмента; у кистей/пальцев уже),
  не глубже «трубки» головы/шеи, лицом от кости; отдельный шелл (оружие) — целиком; 2 голосования по соседям; у корня
  (плечо/бедро, 2.2 радиуса, плавный спад ×1.5) остаётся авторский бленд. Сварные контакты между группами (кисть–кисть на
  рукояти, предплечье–волосы) режутся с дублированием вершин, петли закрываются веером (где удаётся). Выход
  `stab/model_canonical.glb` (+573 вершин на d1522b45), `stab/stabilization.json`, `stab/before_after.png`,
  `stab/joints.json` (суставы источника в канонической позе → пин для fastrig: без него хвост утягивал колено).
- **Fit check** «скелет не подогнан» в каждом rig.json (`fit_check`): масса весов на 2 костях > 0.6, кость конечности
  без вершин (< 0.2 %), кисть дальше 15 % H от своих вершин, > 1 сустав конечности вне меша. Вердикт пишется, не
  блокирует (QA/триаж читают `rig.json.fit_check.ok`).
- **Опции** (`SPEC`, «Tools Have Options»): mode auto|on|off, target_pose auto|T|A (auto = ближняя к рукам),
  arm_angle_deg, limbs, straighten_elbows/knees, leg_spread_deg, foot_pitch_deg, posed_* пороги, stabilize_hanging,
  use_animations, use_source_joints auto|on|off, blend_radius_pct_H, limb_capture, fit_*; сохранение на ран —
  `stab/options.json`. CLI `python -m mt.limb_stabilize --dir <run> [--check|--stage|--apply --opts JSON|--undo|--describe]`.
- **Агент сессии / /dev/tools**: `pose_check` (публичный), `stabilize_pose(options, note)` (owner/admin; пересборка
  рига+клипов, версии `stab/versions/v<N>`, до/после), `stabilize_undo`, `stabilize_options_get`. В /dev/tools видны.
- **Автотесты**: кейс `posed_fox_warrior` (входы `d1522b45.upload.glb` = assimp-GLB, `.upload.fbx`, `.converter.glb`),
  kind `pose_stabilization` в `checks.py`, контроль `mt_stab_control` на `warrior_sword` (T-поза не трогается).
  Скрипт `autorig-online/deploy/autotests/patch_limb_stabilization.py` (якорный, оба формата corpus.json).
  Гейт `mt-deploy` PASS (83 кейса) дважды; на проде d1522b45: posed → T за 0.73 с, риг 3.3 с, 9 клипов, fit ok
  (top2 0.27, min limb 1.1 %), limb_collision severity 0 / 722 вершин рук (конвертер: 0), skeleton_fit 0 вне bbox.
- **Живой баг, найденный по пути (V3 triage, важно)**: с ~13:00 UTC 10.10 каждый V3-ран получал `clip_errors` на все 8
  клипов («truth value of an array…»: `times` клипа — numpy, `or []` в live-эмите `_retarget`), и QA-гейт `clip_errors`
  делал **каждый** ран `needs_review` («часть анимаций не перенеслась»). Раны cc22a7e0, 7b45d6a0, 9980a38a, 7c9aaae7.
  Починено ханком в `v3_conveyor.py` (round 2, 17:35 UTC). Также: прод `mt/retarget.py` отличается от Git HEAD
  (sha 39225aaa vs db8af918) — кто-то правил на проде без коммита.
- **Проверено вживую**: агент сессии демо-задачи сам вызвал `stabilize_pose {"target_pose": "A"}` на ране 7c9aaae7
  (версия v1 за 1.6 с, `stab/versions.json`), т.е. инструмент работает через агента; входящая задача 7c1b6748 без
  скелета: этап честно отказал (`why: no skeleton`, 2.3 с), fit_check записал «LeftHand, LeftFoot вне меша».
- **Не сделано / дальше**: (1) путь без исходного скелета (голый меш в позе — Hunyuan/Tripo без рига): детектор позы по
  геометрии и цепи из медиального скелета не построены, `stage` честно пишет `why: no skeleton`, fit_check всё равно
  срабатывает; (2) волосы, сваренные с кистями над головой, частично уезжают с правым предплечьем (видно на
  `stab/before_after.png`); (3) клип «Source pose» приблизительный (ошибка ~7 % H в среднем: пивоты fastrig ≠ источник);
  (4) fast_analysis `bone_outside_check` на открытых мешах (Tripo: 13.9k граничных рёбер) двигает суставы в хвост —
  на ране 7c9aaae7 испортил ноги после хорошего рига (owner: V3 triage); с пином суставов источника (round 3) риг
  устойчивее. Демо: задача 04a85183 (V3, ран 7c9aaae7ca1c5bf9618c, код round 2) и ран `d1522b45a0b1c2d3e4f5` (round 3, прямой
  `mt.rig_first` на проде: posed → T 0.65 с, 14 пинов, fit ok, limb collision none, 2.2 с всего; вьювер
  `/api/mt/unity/test/index.html?run=d1522b45a0b1c2d3e4f5`), аудит `/srv/autorig/audits/limb-stab-20261010/`.
  **Рестарт autorig-mt после round 3 отложен**: шёл V3-ран 5ee6219f (задача 7c1b6748, numeric QA ~1 чанк/мин); фоновый
  цикл рестартует в первое окно простоя (до 2 ч). До рестарта конвейер в памяти — round 2 (без пинов суставов).

## V3 triage · rig quality — список входящих задач, Астра, корни дефектов фастрига (2026-10-10 18:00 UTC, на проде)

- **Триаж-лист** (MT `8f325d0`): `mt/triage.py`, цикл в autorig-mt раз в 60 с по задачам сайта за 48 ч (V3 и зеркала
  классики) — только читает то, что уже записали проверки (category, rig.json, fast.json, limb_collision.json,
  parts.json, rig-qa.json, joint-judge.json, веса из самого GLB раз на файл → `analysis/triage_weights.json`).
  `GET /api/mt/triage?since=6h|<epoch>&format=text&defects=1&task=<id,…>` — только админ (cookie), ключ MT
  owner/codex/astra или локальный вызов на хосте без прокси-заголовков; снаружи 403. CLI `python -m mt.triage --since
  24h --text [--refresh]`. Файлы: `MT_ROOT/triage/items/<task>.json`, `classes.json` (первое появление каждого класса),
  `feed.jsonl` (строка на новую/изменившуюся задачу). Классы — стабильные ключи: `limb_in_body.left|right`,
  `arms_unbound.*`, `sleeve_smear.*`, `bent_arms_as_hanging.*`, `bone_outside`, `prop_skinned`, `skeleton_unfitted`
  (топ-2 кости ≥ 85 % вершин или ≥ 2 весовых сустава вне коробки меша — то, о чём просил Skinning → converter),
  `scale_mismatch`, `unweighted`, `rig_failed`, `rig_slow`, `run_error`, `qa.<gate>`, `judge_fix`. Новый класс →
  `VERSION` в triage.py не трогать без нужды (кэш пересчитывается).
- **Астра** (MT `8f325d0`): `watchloop.triage_events/triage_new_classes/triage_picture` + в `bridge.wake` источник
  `triage` (первое пробуждение ставит базу, бэклог не повторяется); новый класс дефекта мост сам шлёт в DEV от «Astra»
  с картинкой худшего кадра (`triage_escalate`), строки задач идут в watch-ход в UNTRUSTED-заборе. Тесты
  `tests/test_astra_safety.py` 38/38 (как user astra). Перезапущен только `autorig-admin-bot`.
- **arm_clearance** (MT `968f92b`, новый шаг конвейера): после ретаргета V3 вместо голой проверки `_limb_check` гоняет
  `python -m mt.arm_clearance --dir <run> --check` (сначала тот же отчёт limb_collision; если сторона ≥ medium —
  покадровый доворот плеча из тела по радиальной нормали, ключи с «шатром» по времени, 2 прохода); новая версия
  рига через `skin_tools.Store` (v0 остаётся), ставится только если детектор говорит «лучше» и риг не менялся
  за время расчёта. QA ждёт его (≤ 420 с). Выключатель `MT_ARM_CLEARANCE=off`. Корпус: рыцарь 8369addb high/high →
  none/none; мальчик 98c1247c R high → none; af874411 medium/medium → none; 7831327b high/medium → low/none;
  хуже — не ставится (d76f84c3, манекены). DEV 6552. Чек автотестов `mt_arm_clearance` (boy base, fastrig_arms ext).
- **retarget: концевые кости** (прод `mt/retarget.py` 21e5270f, патч `tools/patch_retarget_end_align.py`; владелец
  DEV 6553 «рука … должна в продолжение кисти идти»): кисть/носок/голова без дочернего направления в библиотеке не
  выравнивались → постоянный излом запястья = угол рук модели в покое (Idle 22–96° между кистью и предплечьем на всех
  фастригах). Теперь берут выравнивание родителя цепи: Idle 17° у всех (это собственное движение клипа), носки 0°.
  Прод был 37b3dbf (коммит 6a278dd с `transport_world_deltas` / политикой `retarget_reference_policy` на прод не
  выкатывался); теперь прод = Git HEAD + мой ханк (MT `a21f3da`, sha 89cc157d; политика по умолчанию
  `aligned_reference` = прежнее поведение, `tests/test_retarget_bind_invariants.py` 4/4, запястья те же). Порог `maniac_neck/mt_numeric bad_frame_share` 0.80 → 0.85 (слой в Idle
  101 → 504, в Walking 63 → 0; горячие точки растяжений те же) — только в прод-манифесте, в Git его ещё нет.
- **fastrig: оболочки = тело** (MT `46bd1f1`, патч `tools/patch_fastrig_dense_attach.py`): коробки-манекены
  (63bf5d35, 8a1b1cc5, dlv3-probe e8960693) — ложных Prop 3 → 0, позвоночник −0.33 H → центр, суставы вне тела
  16 → 2, обе руки привязаны; меч в руке остаётся пропом. Цели манекенов в манифесте переведены в PASS. DEV 6554.
- **fastrig: плотные меши на прокси** (MT `132487e`, `tools/patch_fastrig_proxy.py`, `FASTRIG_PROXY_VERTS`=300000):
  Meshy-енот 7c1b6748 (943k сваренных): fastrig 43 → 28 с, rig_first 55 → 42 с (в гейте 27.9 с), растяжения 85k → 26k.
  Кейс `meshopt_dense` (extended) в корпусе. Меши < 300k не меняются.
- **Живой ремонт** (новые версии v1, v0 цела): 5f6f91fc (ран 6052580a) L/R high → medium/none; 8635aa06 (cc22a7e0)
  medium/high → none/none. Остальные V3 за сутки — без флага.
- **fast_analysis: открытые меши** (MT `8cc9600`, `tools/patch_fast_open_mesh_guard.py`; просьба Limb stabilization):
  фикс суставов не применяется, если до него > 25 % проб «снаружи» (меш не замкнутое тело: фокс 7c9aaae7 222/528 —
  ноги и шея уезжали в хвост на 2–5 % H). Только отчёт.
- **Категорийные дефолты скина (91a9513a) — из данных**: сетка `influence` плечо/локоть 0.7/0.5/0.4 (engine rebuild)
  на 9 гуманоидах корпуса против сегодняшних 1.0: leak −2826…−4218 и lag −294…−638, но stretch_2x +4686…+6030,
  clip_4x +143…+1211, stretch_4x +895…+2068 (рыцарь, эльф — разрывы). Для фастрига значения 91a9513a (подобраны на
  классическом риге конвертера, engine refine) хуже → `fast_registry.json → skin_options` НЕ меняю, дефолт humanoid = 1.0.
  Данные: `/srv/autorig/data/v3triage/grid/g1|g2/results.json`.
- **Дополнение V3 triage (18:40 UTC)**: рестарт autorig-mt в 18:20:16 — мой: сервис висел (главный поток 100 % CPU,
  310 непрочитанных соединений, своп 16/16 после енота), он убил проекции 66d83b2e → я перезапустил задачу attempt 2
  (`v3_retry` из бэкенда, ран 659d941d). Дальше только `sudo autorig-mt-restart --wait`. Триаж: класс
  `run_error.killed` (пустой stderr / сигнал) — **Intake**: такие раны надо ретраить автоматически и не слать как ERROR.
  `mt/reanimate.py` (MT `db52891`): клипы рана заново текущим ретаргетом как новая версия рига; применён к клиентам
  47dc80e9 (ран 9980a38a: v3 reanimate, затем arm_clearance v4 — L medium/R high → none/none) и 7c1b6748
  (5ee6219f: v1 reanimate, arm_clearance идёт). Данные всех прогонов — `/srv/autorig/data/v3triage/` (bench, grid, exp).
- **Инцидент 18:10 UTC (память)**: плотный енот 1M вершин — numeric QA (2 партии воркеров по ~0.9 ГБ, первая
  осиротела после повтора QA), 2 joint_judge (3.7 ГБ), limb_collision — своп 16/16 ГБ, autorig-mt в D-state, API MT
  не отвечал. Убил только осиротевших воркеров `v3_numeric_qa` (ppid 1) — API ожил. **Intake**: воркеры QA должны
  умирать с родителем и учитывать своп; **Rig judge**: не брать 1M-вершинные раны параллельно с QA.
- **Не сделано / кому**: (1) arm_clearance на 1M вершин: таймаут 420 с сработал на 5ee6219f (теперь одна детекция
  вместо четырёх, но детекция limb_collision на 1M ~80 с) — для плотных брать прокси-меш; (2) v3-triage добавлен
  Астре (`tools/bin/v3-triage`, registry, /dev/tools — виден).

## Hunyuan 3D API · V3 (2026-10-10, агент «Hunyuan 3D API»)

- Платный Tencent HY 3D Global подключён: `mt/hunyuan3d_cloud.py` (MT `dc9f35b`; hunyuan.intl.tencentcloudapi.com,
  2023-09-01, ap-singapore, TC3-подпись без SDK; ключи только из `/srv/autorig/secrets/tencent-hunyuan3d.env` внутри
  процесса). Леджер `/srv/autorig/data/var/hunyuan3d/ledger.jsonl`, конфиг `config.json` там же (`daily_credit_cap`
  100, `session_allowance` owner=null / account=0 / anonymous=0 / run_chat=0, `rapid_text`/`rapid_image` false).
- Серия 10.10: 15 моделей + 1 тест инструмента = 465 кредитов по прайсу (5 FAIL Rapid не списываются);
  результаты `/srv/autorig/data/hunyuan3d/<job>/model.glb`, индекс `series_20261010.json`, лист DEV 6561/6562.
  В пакеты сцен castle-yard / scifi-hangar / sakura-shrine НЕ ставились (ждут вердиктов владельца).
- Инструменты сессии `generate_3d` / `generate_3d_options_get` (agent.py, видны в /dev/tools), скилл
  `mt/skills/hunyuan3d_generation.md`. autorig-mt рестарт ждёт простоя через `autorig-mt-restart --wait`.
- Выводы: FaceCount (+10) не нужен (бесплатный gltf-transform simplify + resize даёт то же); Rapid на intl
  ненадёжен (текст 0/5, фото 1/2) → draft идёт как Pro 3.1 (25); Smart Topology (50) отдаёт геометрию без текстуры.

## Intake · killed runs (2026-10-11, агент «Intake · killed runs»)

- **Правило**: V3-ран, у которого дочерний процесс стадии убит снаружи (рестарт сервиса, OOM, kill; триаж-класс
  `run_error.killed`), повторяется сам и не показывается клиенту/Telegram как «Task failed».
- **MT** (`R:\3d_video_motion_transfer` ff498c3, на проде через `mt-deploy`): `kit.sub` кидает `ChildKilled`
  (сигнал/пустой вывод с кодом 137/143) и убивает ребёнка при отмене; `V3Conveyor.execute` повторяет ран до
  `AUTO_RETRIES`=2 с паузой 8/20 с (статус `running`, стадия `resuming:killed_retry n/2`, счётчик в `run.json`
  `killed_retries`/`last_killed`); после исчерпания ошибка начинается с `[killed; auto-retries exhausted 2/2]`.
  Рестарт сервиса во время паузы отменяет её, ран подхватывает `store.resumable()` как раньше.
- **Numeric QA**: пул-воркеры умирают с родителем (`prctl PDEATHSIG` + сторож ppid, `_stop_pool` в `finally`,
  SIGTERM→`sys.exit`), сам валидатор запускается в своей группе процессов (`start_new_session`, `killpg` при таймауте/отмене).
  Проверено на проде: `kill -9` родителя -> все 4 воркера мертвы за <1 с.
- **Backend** (`v3_runtime_mount.py`, ef561ff6, релиз `killedretry-20261010T184935`, `current` уже на нём):
  `project_task` для failed-попытки с «убитой» ошибкой (без `exhausted`, `auto_retries` < 2) ставит задачу в
  `processing`/`v3.state=retrying`, без `error_message`; `after_commit` -> `retry_killed_task` -> `v3_retry`
  (новая попытка), Telegram ERROR не уходит; `sweep_retrying` на старте подбирает зависшие `retrying`.
  **Код загрузится при следующем рестарте autorig-storage** (на момент деплоя renderfin был занят графами, queued/running ≠ 0 —
  рестарт не делал). Проверено вручную скриптом на тестовой задаче b8d271f7 (попытка 2 прошла до needs_review).
- **Триаж**: класс `run_killed_retried` (severity 1) из `run.json.killed_retries` и из прошлых failed-попыток
  с убитой ошибкой; в `triage.py` заодно починен regex (в нём был символ backspace вместо `\b`).
- Тест на проде: задача 9d2a5ae8 (маленький корпус-манекен): `mt.project` убит SIGTERM дважды подряд, ран дошёл до
  needs_review без ошибок; тесты `tests/test_v3_killed_runs.py` (MT), `tests/test_v3_killed_retry.py` (backend).

## Astra TODO · horse — вьювер, четвероногие ноги, триаж (2026-10-11, агент «Astra TODO · horse»)

- **Задача** a742491a / run 3ef6defe (gltfpack-лошадь, 18 483 вершины, один сварной shell, хвост до скакательных
  суставов). Аудит и исходники: `/srv/autorig/audits/astra-todo-horse-20261011/` (source_model.glb, run_v0/,
  unity_loader_log.txt, horse_before_after.png, repair.py / reqa.py).
- **Вьювер**: glTFast 6.20 в Unity-вьювере не знает `EXT_texture_webp` (required) -> `ExtensionUnsupported;EXT_texture_webp`,
  крутилка навсегда. `KHR_mesh_quantization` он умеет. Фикс в `fastrig.Writer._viewer_safe`: WebP -> JPEG (PNG при альфе),
  квантованные атрибуты -> float, оба расширения уходят из Used/Required; исходник `proj/model.glb` не трогаем (sha-контракт
  конвейера). `rig.json.viewer_safe` пишет, что поменялось.
- **Риг боком**: Vision выбрал боковой тайл как «перед» (+x), риг пересобрался поперёк тела, «левые» ноги = задние.
  `fastrig.quadruped_forward`: у четвероногого перед вдоль длинной горизонтали (иначе гео-догадка / выше конец),
  `rig.json.forward_check`; `v3_conveyor._rig_disagrees` не пересобирает по такому ответу Vision и сам пересобирает боковой риг.
- **Ноги по топологии** (`leg_regions`/`isolate_legs`/`hanging_tail`): копыта сидят семенами, Dijkstra по поверхности;
  ниже колена вершина только своей ноги, перед+зад не смешиваются нигде, висящее ниже колен (хвост, кисти) ногам не
  достаётся, боковые суставы ноги ставятся на свою ногу. `rig.json.checks.leg_isolation` / `leg_topology`.
- **Триаж** (VERSION 5): проваленный гейт QA = review, никогда не зелёный (в т.ч. rig-qa.json без `failing`, как у
  123ac7be / ada7f10a); новый класс `legs_mixed` (sev 3).
- **Ремонт клиента**: `rig/skin` v0 = старый риг (не тронут), v1 активен. v0 -> v1: rig_check рёбер >2x 399 -> 25,
  max 42x -> 8.4x, вершин перед+зад 26 -> 0, межножных рёбер 896 -> 10; numeric max stretched edges 2493 -> 723 (bad_frames
  145/145 остаются: гейт 1.25 проваливают все риги). Статус QA needs_review, триаж 🟠.
- **Автотесты**: кейсы `horse_opens_in_viewer`, `horse_quadruped_legs` (kinds `mt_quadruped_rig`, `viewer_opens`,
  `quadruped_legs`; `deploy/autotests/patch_quadruped_horse.py`). MT `ff968dc`, gate PASS 73 + 10 XFAIL.
- **Не сделано**: пряди хвоста, приваренные к крупу (пары Hips/Tail3 в rig_check), — это слой волос, не ноги.

## Skinning · V3 — «скиннинг сломался» (bfbd3248): причина, фикс, гейт (2026-10-10 19:30 UTC, на проде, MT `1ec3fd5`)

- **Что увидел владелец**: задача bfbd3248 (воин, Blender-экспорт, 51 шелл, без скелета; тот же меш, что 66ba97ba /
  d76f84c3), ран 3dbc776ff6bffe8c8f3b, вид весов: юбка и пластины живота лоскутами между руками и ногами. Это НЕ d1522b45.
- **Бисект** (fastrig.py по всем бэкапам 10.10 в `mt.prev/`, 98c1247c и 8a1b1cc5 — одинаковые числа на всех 7 версиях;
  заметки Skinning-агента `.work/skin_regression_notes.md`: воин рвался одинаково с 1c87b65, 09.10 — хуже): **кодового
  регресса нет**. «Регресс» = переход на V3-only: утром владелец видел ARP-скин классики, теперь fastrig.
- **Причина**: fastrig отдавал вершину ближайшей кости «по воздуху» — согнутое предплечье перед животом забирало
  пластины и юбку (ребра Hips/LeftForeArm 65, rig_check 1255/644 рёбер >2x/4x за 3 кадра); затем fast_analysis
  `--apply` (сдвиг кистей на 9.6 % H по «outside») пересобрал риг ещё хуже — 1010/468 за кадр — и ничто не гейтило
  растяжение (судья и arm_clearance веса не трогали).
- **Фикс** (`tools/patch_fastrig_reach.py`, якорные ханки; гейт PASS 81/0): `fastrig.limb_reach` — кость конечности
  владеет только тем, до чего дотягивается по поверхности (компонента с «ядром» в трубке и путём к корню плечо/бедро;
  отдельный шелл — только если конечность у него в большинстве и он в трубке ≤1.6 радиуса); `contact_split` — сварные
  контакты конечность|туловище / проп|волосы вне корня режутся как волосы (writer дублирует шов); опции
  `limb_reach`, `limb_capture`, `limb_split` в `skin_tools.SPEC`; `fast_analysis.apply_fixes` не ставит пересборку, если
  rig_check stretch вырос. Числа (sum >2x/>4x): bfbd3248 1255/644 → **231/25**, 66ba97ba.upload 1473 → 240,
  d1522b45 539 → 81, 7c9aaae7 471 → 91; хорошие модели без изменений (16ce2f35 25, 98c1247c 5, 9a34e8c0 0, 8a1b1cc5 6→0).
  Классика (skin_tools.measure stretch_4x на сваренном меше): воин 100, fastrig было 1736 → 403 (сваренный меш не видит
  разрез шва; rig_check со skip-маской видит).
- **Почему гейт пропустил**: ни один чек не мерил растяжение скина fastrig (время/кости/клипы/коллизии/центровка).
  Теперь kind `rig_stretch` (`deploy/autotests/patch_rig_stretch.py`): rig_check stretch из rig.json + веса по костям +
  skin_tools.measure, пороги = сегодняшняя база ×1.2 на warrior_sword, boy_tshirt, knight_rigpath, anime_heel,
  posed_fox_warrior и новом `warrior_kitbash` (66ba97ba.prepared.glb = источник bfbd3248). Первый прогон гейта с ним
  поймал 2 вещи (max_stretch от сварки проп|волосы → добавлен в split; warrior_sword worst_share 0.50→0.80 — доля
  настоящей руки в теле выросла, порог 0.85, дефект — Limb collision).
- **Перескин клиентских ранов** (`tools/rerig_version.py`, новая версия через skin_tools.Store, v0 цел, вьювер
  перезагружает): 3dbc776f v3 3291/1675→231/25; 7c9aaae7 v3 471→91; cc22a7e0 32; 410da78e 318→0; 659d941d 2680→1278;
  1cb4a5ea/ca12fe99 (эльф 4f85d45e, волосы) 4404→3280 — другой класс дефекта; рыцари 42–45 без изменений.
  DEV 6575 (до/после/классика), 6576 (вьювер). Рестарт: `sudo autorig-mt-restart --wait 600` 19:30:44 UTC.

## Renderfin resume · V3 — рестарт без потерь (2026-10-10 19:35 UTC, на проде, агент «Renderfin resume · V3»)

- **Правило владельца 2026-10-11**: «перезапускай сервер не дожидаясь завершения графов, они должны подхватываться
  автоматически при рестарте». Правило 2026-09-27 (рестарт = вайп очереди) снято; ждать простоя renderfin перед
  рестартом `autorig-storage`/renderfin больше не нужно (AGENTS.md, «These still apply»).
- **Renderfin** (`renderfin/config.py`, `models.py`, `queue.py`, `api.py`): `RENDERFIN_RESTART_POLICY=resume` (по умолчанию;
  `wipe` = старое поведение). На старте: queued остаются, running на известном боксе — `followed@<box>` (опрос того же
  prompt), иначе свой prompt снимается с бокса (`/queue delete` или `/interrupt`+`/free`, чужие не трогаются) и задача
  снова Pending под ТЕМ ЖЕ id (тот же output URL, новая lease-идентичность). Кап `RENDERFIN_RESTART_RESUME_MAX`=3 ->
  `restart_resume_exhausted: …`. Задачи, отменённые рестартом (ошибка `cancelled: server restarted…`) за последний час,
  возвращаются сами на старте; `POST /renderfin/api-render/resume[?dry_run=1][&task_id=…]` (только localhost) — любые id;
  `GET /renderfin/api-render/last-start-resume` — что вернулось. Статус задачи: `restart_resumes_int`, `notice_string`.
- **Backend**: `main.py` на старте больше не зовёт `/api-render/reset`, а зовёт `/api-render/resume` (`AUTORIG_RESTART_POLICY`,
  по умолчанию resume; переменная `AUTORIG_WIPE_QUEUE_ON_START=1` в backend.env теперь действует только при `wipe`).
  `task_owner.py`: редактору больше не отвечают 409 `cancelled_by_restart`. `ai_avatar_build.py`: незаконченная сборка
  возобновляется (кап 3). `ai_video_tools.py`: запрос задания лежит в `…/ai-video-tools/pending/<id>.json` до конца,
  после рестарта статус/повторный POST перезапускает его под тем же `vt_` id. `/api/ai/render-status/{id}`:
  `notice_string` + локализованный `notice_message_string` (i18n `render_resumed_after_restart`,
  `render_restart_resume_exhausted`, en/ru/fa/zh/hi).
- **Восстановлено**: c6dbd111 (была на f15) и 149ada75 (Raptor) — Done на Raptor с тем же id/URL (владелец ip:12ca…,
  запросы `qwen_image`, кэш запросов указывает на те же id). f8ad7030 не терялась: Done на worker-4090 в 19:10:55, до вайпа.
- **Проверено на проде**: свой тест 4d8dc53f шёл на f15 в момент рестарта renderfin -> followed -> Done; 76b25dc1 шёл
  во время рестарта autorig-storage -> Done, 0 отмен. Релиз `restart-resume-20261010T1930Z` (гейт backend PASS 6/6, новый
  кейс `restart_resume`), затем `i18n-restart-resume-20261010T1935Z` (live_static, i18n).
- **Не сделано**: строка notice в редакторе /nodes (`ai-nodes.js` `taskStateReporter`) — правка JS меняет build-хэш
  редактора и отбивает открытые вкладки `editor_outdated`, оставил владельцу редактора; API уже отдаёт текст.
  `ai_civitai_post.JOBS` (in-memory) не трогал.

## Hunyuan queue · V3 — одна очередь 3D-генерации: бесплатная ферма первой, платный Tencent по нужде (2026-10-10 ~20:00 UTC, на проде)

- **Очередь**: `mt/gen3d_queue.py` (MT `1dee635`), сервис `autorig-gen3d` (127.0.0.1:8283, nginx `^~ /api/gen3d`,
  unit/nginx/policy в `autorig-online/deploy/gen3d/`). Отдельно от autorig-mt: рестарт MT не роняет очередь; сам
  autorig-gen3d рестартовать, когда `GET /api/gen3d` → `queue.cloud_running` 0 (фермерские задачи подхватываются,
  облачная в полёте — нет). Деплой кода — только `autotests.py mt-deploy --mt-file mt/gen3d_queue.py=…`, потом
  `systemctl restart autorig-gen3d`.
- **API**: `GET /api/gen3d` (ферма, очередь, кредиты, политика), `POST /api/gen3d/jobs` (Bearer MT-ключ; prompt |
  image_url, purpose, quality, urgency interactive|urgent|background|autonomous, deadline_seconds, backend
  auto|farm|cloud, estimate_only), `GET /api/gen3d/jobs/<id>` (+ `/model.glb`, `/render.png`, `/input.png`),
  `GET /api/gen3d/decisions`. Каждое решение — `/srv/autorig/data/var/hunyuan3d/routing.jsonl`.
- **Политика живая**: `/srv/autorig/live/config/gen3d-routing.json` (без рестарта). Платят только ключи owner / codex /
  astra / admin-media-v3 и сессии в своём credit allowance, только interactive/urgent, в рамках `daily_credit_cap`
  (config.json Hunyuan 3D API). Background / autonomous / сайт — только ферма. Interactive дедлайн 900 с.
  **f12 исключён** (`farm.exclude_boxes`): чинят Vertex-PBR (отдельная сессия) + обучение Lina LoRA держит GPU
  2–3 ч — вернуть, убрав f12 из списка.
- **Сайт**: `/api/3dmodel` (релиз `killedretry-20261010T184935-gen3d-190921`, web `6eba4c4b`) при `site_via_queue: true`
  ставит задачу в очередь (только ферма, вместо 503), принимает `prompt`; статус `/api/3dmodel/status/gen3d.<uuid>`.
- **Инструмент сессии** `generate_3d` (agent.py): через очередь; `backend`, `urgent`, `job` (забрать долгую);
  автономный ход — только `backend=farm`.
- **Бенчмарк фермы** (те же объекты, что платная серия; лист DEV 6582, `/srv/autorig/data/hunyuan3d/bench_20261011`):
  f12 3080 Ti ~465 с, f7 1080 Ti ~850 с, f13 1080 Ti ~1320 с; картинка из текста Krea 26–41 с. Результат: 25–40k граней,
  PBR 2048+1024+1024, LOD 10k/1k. Облако 96–159 с, 25–35 кр. Дефекты фермы: ящик падает «Vertex-PBR manifest is
  missing» и на f12, и на f7 (не только f12 — входозависимо), у фонаря мусорные осколки, у чучела потеряна перекладина.
  1080 Ti с резидентной LLM отказывает в первом задании по VRAM-гейту (7000 MiB), повтор через 15 с проходит.
- **Новые бесплатные модели (без скачиваний)**: открытых весов новее Hunyuan3D 2.1 нет (2.5/3.0/3.1 — только облако).
  Быстрее: Hunyuan3D-2 turbo / mini-turbo + FlashVDM (форма за секунды, 6 ГБ, но текстура 2.0 — 16 ГБ). Сильнее:
  TRELLIS.2 (MIT, 4B, PBR; Linux, 24 ГБ офиц., low-VRAM ~6.5–12 ГБ на 512³; Triton → не Pascal), Pixal3D (май 2026, на
  TRELLIS.2). Решение и скачивание — только владелец.

## V3 backfill — каждый V3-ран проверяется, чинится и сортируется непрерывно (2026-10-11, на проде, MT `25c788e`..`3fe9b79`)

- **Владелец 2026-10-11**: «никаких возвратов, чиним на проде … всё что обработано уже V3 — должно автотестироваться
  пока не будет всё отлично заригано, отсортировано». Без отката на классику.
- **Сервис** `autorig-v3-backfill` (`deploy/backfill/autorig-v3-backfill.service`, `python -m mt.backfill --loop`,
  user autorig, Nice 19, IO idle, CPUQuota 400 %, **MemoryMax 6G без свопа** — сервер больше не уходит в своп из-за
  него). Один дочерний процесс за раз; ждёт, пока живой V3-ран клиента до стадии QA (source/analysis/rig/retarget)
  не закончится; свой numeric QA не запускает, пока идёт QA клиента (ран вернётся позже); MemAvailable ≥ 5 ГБ.
- **На каждый V3-ран** (последняя попытка каждой V3-задачи из dispatch, в т. ч. пробы, которых нет в БД сайта):
  1. проверки на АКТИВНОЙ версии рига, только если устарели: limb_collision, fast_analysis (отчёт, без --apply),
     numeric QA (`analysis/backfill_qa.json`, 2 воркера) если QA на записи от другого GLB; триаж добавляет
     rig_stretch (rig_check >4x ≥150 review, ≥500 defect), skeleton_unfitted, legs_mixed и остальное;
  2. дефект ≥ 2 → ремонт новыми версиями (Store, v0 цела): полный пере-риг в scratch-копии
     (`/srv/autorig/data/v3triage/backfill_scratch`: rig_first.build со стабилизацией позы, limb_reach/limb_split,
     лапы четвероногих, fast fix со стражем растяжения, ретаргет, arm_clearance) → ставится ТОЛЬКО если мерится
     лучше (`backfill.score`: вес дефектов, рёбра >4x, доля руки в теле, рёбра >2x) и живой риг не менялся; иначе
     arm_clearance отдельно (сам ставит только «лучше»);
  3. перепроверка, ведро: `ok` (дефектов не было) | `repaired` (наша или чужая версия починила) | `open` (худший
     оставшийся класс + агент-владелец, `backfill.OWNERS`). Попытка ремонта одна на (sha рига, ревизия
     инструментов рига); правка правил триажа только пересортировывает.
- **Где смотреть**: `<run>/analysis/backfill.json`, сводка `MT_ROOT/triage/backfill/summary.json`, лог `log.jsonl`;
  `GET /api/mt/triage` → `buckets` {counts, open_classes}, у V3-задач `bucket` / `open_class` / `open_owner`; текст —
  вторая строка шапки. Новые открытые классы → `triage/backfill/corpus_candidates.json` (кандидаты в корпус) и
  через ленту триажа — в DEV через Астру.
- **Триаж**: QA засчитывается только для того GLB, который проверяли (иначе `qa.stale`, нота);
  `bone_outside` review только при 5–25 % проб снаружи, выше 25 % — `bone_outside.unreliable` (меш не тело);
  `arms_unbound` относительно числа вершин (бокс на 224 вершины), `limb_unchecked` (морф-таргеты/плотный меш).
- **Важное для всех**: `qa.numeric_deformation` падает почти у всех ранов — порог numeric QA 1.25 по ребру и любой
  плохой кадр валит клип (рыцарь 0/9, 505665ad 0/9 при чистом риге). Это открытый класс владельца «Rig tools · V3»:
  либо гейт меряет то, что видно глазу (как rig_check / clip_4x), либо он навсегда «open».

## Gallery · V3 (2026-10-11): V3-style posters for every gallery card

- `autorig-gallery-poster.service` (`deploy/gallery-poster/`, install.sh): one 9:16 capture per public task in
  `/srv/autorig/data/static/posters-v3/<task>.jpg` (viewer GLB, mt.render 3/4 camera, one of the viewer environments,
  contact shadow; blank/low-contrast captures retried on another environment). State per task in `posters.sqlite3`
  (ok | failed | no_source). Newest and V3 tasks first, loops forever; no_source tasks keep the old poster.
- Backend `main.py`: `/thumb/<task>` serves the capture first (`v3_poster_path`), gallery JSON carries
  `thumbnail_url=/thumb/<id>?v=<mtime>` (`thumb_url_for`), homepage `/` has the 12 newest cards in the first HTML
  (`_home_gallery_cards`, 60 s cache), `task-card.js` and the SSR cards fall back to `static/images/poster-missing.svg`.
- Triage (`mt/triage.py`): `poster_failed` / `poster_no_source` defects (sev 1) and `triage/manual_flags.json`
  operator flags; task 2ad5c0c8 (stretched arm, 708k tris, unmeasured by the rig checks) is flagged `rig_stretch.owner_report`.
- Open: the poster is the rest pose, not an animation frame; `/api/gallery` still returns `author_email` (public) and
  TaskCard shows its local part: replace it with the public handle (`/api/people`).

## Pre-rig tessellation · V3 — тесселятор перед ригом для низкополигональных моделей (2026-10-10 ~21:30 UTC, на проде, MT `51daf27`+)

- **Зачем**: владелец, задача dd498832 «Пиксельный Чибик» (580 треугольников, рёбра до 31 % H, медиана 8.5 %): огромные
  треугольники на плечах/бёдрах берут по одному весу на угол и мнутся/рвутся в клипах. Его слова: «нужен автоматический
  тесселятор как шаг перед полным ригом в таких случаях, в зависимости от средней тексельности треугольников».
- **Код**: `mt/tessellate.py` + `tools/patch_tessellate.py` (якорные ханки: `fastrig.model_glb` предпочитает
  `tess/model_tess.glb`; `rig_first.build` после первого прохода fastrig (суставы известны) решает и режет, риг
  пересобирается на тесселированном меше; `rig_first.animate` пишет `rig/rigged_original.glb` — веса на исходной
  топологии (исходные вершины сохраняют индексы); опции в `skin_tools.SPEC`: `tessellate auto|on|off`,
  `tess_edge_ratio` 0.6 радиуса конечности, `tess_joint_reach` 2 радиуса, `tess_max_edge_pct_H` 8, `tess_max_factor` 4,
  `tess_texel_ratio` 0.25 (UV-площадь/мировая против медианы модели), `tess_deliver tessellated|original`).
  **Если перезаливаете fastrig/rig_first/skin_tools целиком — прогоните `patch_tessellate.py mt`** (и
  `patch_fastrig_reach.py`, `patch_limb_stabilize.py`).
- **Решение**: только грубые модели (медиана ребра > 3.5 % H, или < 3000 граней при медиане > 2 %) — плотные выходят
  за 20 мс («dense mesh»), лошадь a742491a (10k граней, медиана 2.45 %) не трогается. Грань режется, если её длинное
  ребро > max(0.6 × локальный радиус конечности, 2.5 % H) и центроид в 2 радиусах от сустава, или ребро > 8 % H где
  угодно, или тексельность < 0.25 медианы у сустава. Срединный сплит рёбер (1→2/3/4 грани), все атрибуты интерполируются,
  швы UV/жёсткие рёбра сохраняются (пары сырых вершин), квантованные источники (gltfpack) пишутся float32. Силуэт тот же.
- **Числа** (чибик, одни кадры rig_check): 580 → 1844 граней (×3.2, 0.3 с), рёбра медиана 8.5 → 5.0 % H, max 31 → 18;
  схлопнувшиеся грани 5 → 1, рёбра >4x 20 → 2 (v0 → тесселированный). Манекены 63bf5d35/8a1b1cc5 (416 граней) 416 → 1220,
  stretch 0/0. Корпус без регрессий (гейт PASS 86/0, round 7). Поставка: вьювер и скачивания — тесселированный риг;
  `rig/rigged_original.glb` — исходная топология с весами (опция `tess_deliver=original` для агента).
- **Автотесты**: кейс `lowpoly_chibi` (вход `dd498832.upload.glb` = proj/model.glb рана 2fd6423ac6aa115b0e97), kind
  `tessellation` (запись этапа + схлопнувшиеся грани/рёбра по 8 кадрам rig_check), контроли «не тесселировать» на
  boy_tshirt и knight_rigpath (`deploy/autotests/patch_tessellation_case.py`).
- **Ран клиента** 2fd6423ac6aa115b0e97 перескинен `tools/rerig_version.py` → v8 (580 → 1900 граней; v0 цел). На кадрах
  Walking/Running паутина и разрывы торса/юбки ушли; **остаток**: короткие руки-варежки — Hand владеет 33 вершинами,
  Arm 55, ForeArm 426, и кисть «звездит» в клипах (collapsed-граней по 8 кадрам Walking 5 → 23 из-за этого).
  Дальше: на грубых моделях сливать кости конечности с < N вершинами в соседнюю (Hand → ForeArm) или ставить
  суставы по длине стаба. DEV 6603 (до/после), 6604 (вьювер). Round 9: установлен только tessellate.py (ханки fastrig/rig_first
  переживают чужие перезаливки — проверено по маркерам после деплоя Hands-агента).

## Hands rig · V3 — риг рук как отдельная категория (2026-10-11, на проде, агент «Hands rig · V3»)

- Владелец: «отправь отдельно … на риг рук, это отдельная категория и ветка автоматического рига». Задача 99420015
  (tactical combat gloves, две перчатки без тела) шла через humanoid-конвейер: план root, 1 кость, 0 клипов.
- Сделано (MT `mt/handrig.py` + якорные ханки в rig_first / fastrig / v3_conveyor / fast_analysis; подробности в
  R:\3d_video_motion_transfer\HANDOFF.md «Hands rig · V3»): детектор «только руки» по геометрии (ветви-пальцы на одном
  конце каждой оболочки) + слова Vision; скелет на руку: ForeArm (если оболочка уходит за запястье), Hand, Thumb/
  Index/Middle/Ring/Pinky 1–4 (имена Mixamo), сторона L/R по большому пальцу относительно ладони, пара: зеркало →
  L+R, одна сетка повёрнутая → одна сторона дважды; веса по пальцу вдоль поверхности (без утечек через воздух);
  7 клипов рук (Fist, Open, Point, Grip, Wave, Finger Curl, Finger Spread) видны во вьювере; категория `hands`
  в `fast_registry.json` (v5) с правилами для агента сессии; op `hand_rules` (фаланги вне меша, утечки, стретч).
- Цифры по перчаткам (ран 712321a1, версия v1, v0 цел): детект 0.03 с, риг 0.8 с, 45 костей, обе перчатки левые
  (одна и та же сетка, повёрнута на 180°), 0 фаланг вне меша, 0 утечек, Fist 82/16 рёбер >2x/>4x.
- Автотесты (autorig `deploy/autotests`): kinds `hand_rig`, `hand_detect`; кейсы hands_gloves,
  hands_synthetic_{left,fps_arm,mitten,pair} (синтетика `make_hand_corpus.py`, без скачиваний),
  hands_detector_controls (мальчик, лошадь, воин с мечом — не руки). Гейт 20261010T211933Z: 33 кейса PASS.
- DEV: до/после отправлены от «Hands rig · V3». Не сделано: две руки в одной оболочке, рука без большого пальца
  (4 пальца + пометка), Mixamo-библиотека анимаций рук конвертера (нет в Git), тесселяция на ветке рук.

## NSFW split · V3 — отдельный защищённый домен 18+ (2026-10-11, агент «NSFW split»; код на проде, домен НЕ куплен)

- Владелец: весь NSFW-контент — на отдельный домен (autorig.red или подобный), фильтр-разделитель между доменами,
  /nodes целиком туда, вход только с Google OAuth + согласие и подтверждение 18+, правила стран.
- На проде (релиз `…-nsfwsplit-badge`, коммиты `63cd8936`, `974b23a5`): `backend/site_mode.py` — режим по Host;
  `backend/geo_country.py` — чтение .mmdb (страна + штат США) без пакетов; i18n en/ru/zh/hi/fa (`adult_*`,
  `error_nsfw_domain_only`, `error_adult_*`); 18 тестов `tests/test_site_mode.py`.
  - Уже действует: `/api/gallery` (и авторские страницы) и `owner_tasks` на autorig.online не показывают `adult`.
  - Готово, но выключено до домена (`/srv/autorig/live/config/site-modes.json`, перечитывается без рестарта, файла
    пока нет = дефолты): `nsfw_hosts`, `main_split_section` (/nodes, /workflows, /queue, /lora, /system_prompts и их
    API → нейтральная страница / 403 `nsfw_domain_only`), `main_split_items` (adult-задача → нейтральная страница,
    thumb/video 404). Внутренние вызовы 127.0.0.1:8200 (autorig-mt, surabot, режиссёр) не трогаются никогда.
  - На домене 18+ каждый запрос проходит гейт: сессия Google → запись согласия в `adult_consents` (user_id, версия,
    страна по IP, время) → гео. Без гео-базы страна неизвестна → закрыто (проверено вживую: 451). Админ — без гео.
    Админ-ключ API проходит без согласия. nginx-шаблон ставит `auth_request /api/age-gate/check` на всё, кроме гейта.
  - Стейджинг: админ открывает `https://autorig.online/api/site-mode/stage?mode=nsfw` (или `main-split`), выход —
    `?mode=off` (бейдж в углу). Статус: `GET /api/site-mode`, `GET /api/age-gate/status`.
  - `/nsfw` на autorig.online — Telegram-валидатор (8221), не тронут. На домене 18+ `/nsfw` → `/nodes`.
- Домен: `deploy/nsfw-domain/namecheap_domain.py check` (только чтение). 2026-10-11: autorig.red свободен,
  $9.68 первый год / $32.18 продление; autorigonline.red то же; autorigred.com $11.18/$18.68; .xxx/.adult $70/$155.
  Покупка — только по подтверждению владельца: `namecheap_domain.py register <d> --confirm <d>`; затем
  `sudo bash deploy/nsfw-domain/activate.sh <d>` (DNS, certbot --nginx, серверный блок без `listen IP:443`,
  site-modes.json) и redirect URI `https://<d>/auth/callback` в Google OAuth-клиенте (вручную, консоль Google).
- Нужна гео-база (DB-IP Lite City или GeoLite2-City) в `/srv/autorig/data/geoip/country.mmdb` — скачивание не
  делалось, ждёт разрешения владельца. Без неё домен 18+ закрыт для всех, кроме админов.

## Viewer integration · V3 — модель не грузилась (2026-10-11 22:15 UTC, на проде)

- **`unity/test` → `v3-all-r2-20261011`** (откат — `unity/.test-history.log`, прошлый `v3-all-r1-20261010`).
- **Лёгкий GLB для вьювера**: `mt/view_glb.py` + `?view=1` в `/api/mt/files/<run>/…glb` (MT `29a7999`, через `autotests.py mt-deploy`,
  GATE PASS). Картинки ≤2048 px, JPEG (PNG только с настоящей альфой), геометрия/скин/анимации байт-в-байт; кэш
  `rig/.view/<stem>-<ключ>.glb` на ревизию рига, первая выдача +3–5 с. a438fdd8 28,3 → 3,3 МБ, 68c845eb 24,6 → 7,1 МБ.
  Unity (`Viewer.cs`: `RigUrl()` и `proj/model.glb`) просит `view=1`; полный `rigged.glb` без параметра не меняется.
- **Замер на 1 МБ/с** (CDP-троттлинг, холодный кэш): вьювер готов за 33 с, сам GLB 2–4 с (было 25+ с GLB поверх Build).
- **Адаптер комнат**: на старте агент больше не переносит в общую сцену (в Спонзе модель стояла в колонне — пустой вьювер);
  только сообщает о комнате, если посетитель уже в ней. `join_busiest_room` переносит по просьбе.
- 503 на `.data.unityweb` был не от `limit_req` (у него статус 429, в error.log «limiting requests» = 0).
- Шаблон в рабочем дереве несёт чужой WIP (horizon / scene_template, HDR Codex) — в r2 страница взята с прода r1, их правки не выкатывал.
