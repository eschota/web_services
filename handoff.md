# AutoRig development — start here after context compression

Updated: 2026-10-10 17:15 Asia/Novosibirsk (10:15 UTC). Owner-required persistent checkpoint.

## Координация агентов V3 (2026-10-10)

- **Localization · V3** владеет языком пользователя и локализацией (fa/RTL и др.).
  Сырой алерт artifact-cache (`cache=… cap=… reserve=…; last-copy deliverables
  preserved`) чинит Localization: пользователю уходит локализованное сообщение
  `storage_paused`, фронт не показывает сырой 5xx `detail`. **Astra это не делает.**
- Язык — поле API (деплой в процессе): `support_chat_sessions.language`,
  `tasks.owner_language`, `GET /auth/me` → `language`, `GET/POST /api/me/language`.
  Бэкенд-хелпер: `user_language.py` (`language_instruction(code)`,
  `support_session_language(db, session_id)`). **Astra / support_ai.py**: брать язык
  из сессии/API, отвечать на нём; если посетитель пишет на другом языке — на языке
  его сообщения. Строки нового `/task` (Task page · V3) — только через `I18n.t()`.
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
- **Рестарт MT** (ключ OpenAI координатора + мой conveyor): безопасен, когда нет running
  веток. Агенты сессий сами запускают GPU-ветки (танец/диорама) на каждую V3-задачу
  (`MT_AGENT_AUTOSTART`) — учесть ёмкость фермы при открытии V3 всем.
- **Не смонтировано**: `v3_site_integration.py`/`task_v3_shell.py` (заменены read API выше),
  `v3_cutover.py` (контроллер фермы), MT→backend push-callbacks (`task_callbacks.py`):
  идемпотентность даёт durable outbox (poll, lease CAS, повтор проекции).

## Task page · V3 — живая страница задачи (2026-10-10 11:50 UTC, проверено на проде)

- **Свежий JS на каждом заходе.** nginx `/static/`: оверлей `/srv/autorig/live/static`,
  затем релиз. JS/CSS без точного 10-hex `?v=` отдаются `no-cache` + ETag (304),
  со штампом — immutable. `/task` собирается на каждый запрос (`task_page_live.py`):
  шаблон и partials из live-корней, каждый `/static` JS/CSS получает `?v=<sha1[:10]>`
  текущего содержимого. Живая правка статики:
  `sudo python3 /srv/autorig/tools/live_static.py put <rel> <file>` (атомарная запись,
  затем promotion в релиз под локом, откат — `previous`). Состояние:
  `GET /api/task-page/live`. Доказано: тот же URL без hard refresh взял новый билд
  (`tv3-20261010.2` → `.4` → `.5`), DEV `672ce48ed028`.
- **/task = V3-оболочка** (`task-v3.html`, `js/task-v3-shell.js`, `css/task-v3.css`):
  тот же SEO head/canonical/OG/JSON-LD, шапка, подвал и h1, что у классики; один
  Unity-вьювер; реальная позиция в очереди (порядок диспетчера) и прогресс до модели;
  инструменты: скачать/классика (`?classic=1`), ссылка, полный экран.
- **API** `GET /api/task/{id}/v3-view` (`autorig.task-page-v3/1`), только сервер решает:
  - V3-задачи (`pipeline_kind=v3`) открывают MT-run из проекции Intake
    (`viewer_settings.v3.session.mt_run_id`, контракт `v3_runtime_mount._shell`),
    с агентом сессии;
  - классические открывают свои GLB из glb_cache через `/api/task-viewer/{id}/…`
    (`files/task/rig/rig.json`, `rig/rigged.glb`, `proj/model.glb` через X-Accel;
    texlib/scenes/settings — 307 на `/api/mt/…`; POST настроек от не-админа
    игнорируется), `agent=0`. Кэш прогревается фоном через классические эндпоинты;
  - ACL owner / anon_id / admin, чужая приватная задача → 404.
  Доказано: `492210fd` (классика, 70 костей, клип играет, DEV `b123c856bfc6`),
  `cfa2e6a1` (V3 run `31682814bd06170f1063`, агент на связи).
- **Раскатка без рестарта:** `/srv/autorig/live/config/task-page.json`
  (`mode` off|admin|new|all, `new_since`, `webapp`, `preview_keys`; запись — temp+rename).
  Сейчас `admin`: V3 у админа с `?v3=1` (кука `ar_task_v3`) или с превью-кукой
  `ar_task_v3_preview`; **V3-задачи всегда в V3-оболочке**; `?v3=0` — отказ,
  `?classic=1` — разово классика; Telegram `mode=webapp` — классика, пока `webapp: false`.
- **i18n:** 15 ключей `taskv3_*` в prod `en.json`/`ru.json` (+Git); fa/zh/hi — Localization.
- **Для Viewer · V3** (шаблон Unity не трогаю): оболочка грузит
  `/api/mt/unity/test/index.html?api=<origin>/api/task-viewer/<id>&run=task&agent=0`
  (классика) или `?run=<mt_run>` (V3) и шлёт `autorigUnity.send('LoadRun', run)` при
  смене ревизии. Нужно: событие «модель готова» для оболочки; классический
  `animations.glb` приходит одной glTF-анимацией `Animation` — разбить на клипы по
  каталогу задачи; общий профиль настроек вьювера vs. клиенты.
- **Рестарт** 11:25:32 UTC (`tv3-backend-20261010b`): стартовый сброс отменил 3 running
  graph-рендера (Raptor, f15, worker-4090), в `render_tasks` их не было — проверять
  живую очередь renderfin. Коммит `da882c56`.
- **Дальше:** шаг 3 (`mode: new`) — после вердикта владельца; шаг 4 (`all`) — когда во
  вьювере будут клипы классических задач и загрузки/покупки внутри V3; мобильный Unity;
  загрузка движка 17.7 MiB.

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
