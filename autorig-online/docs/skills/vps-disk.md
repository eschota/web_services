---
name: vps-disk
description: Диск VPS autorig.online (way-fr, 878 ГБ): что где лежит, кто пишет, какие таймеры чистят и чего они не покрывают, как проверить свободное место, где решается пауза новых задач и в каком порядке безопасно освобождать место. Читай перед любой чисткой диска и когда сайт отвечает 503 storage_paused.
---

# Диск VPS autorig.online

Составлено 2026-10-15 по замерам на самом сервере (`ssh autorig-vps`). Заказ владельца: «куда опять все место делось разберись … скилл по диску на сервере нашем».
Числа в таблицах — снимок 2026-10-15 около 10:30 UTC; перемеряй командами из раздела 3 перед решениями.

## 0. Главное за минуту

* Один раздел `/dev/md3`, 878 ГБ, смонтирован в `/`. `/srv`, `/home`, `/var`, `/tmp` лежат на нём, отдельных дисков нет. Занято 814 ГБ (94 %), свободно около 55 ГБ.
* Диск делят **AutoRig (около 440 ГБ, 54 %)** и **чужие проекты на том же VPS (около 335 ГБ, 41 %)**: Postgres 124 ГБ (m_plus_trends и quickmeta_analytics), `/home/debian/m_plus_trends` 112 ГБ, `qsd` 28 ГБ, docker 24 ГБ и т. д. Чужое не трогать и не чистить.
* Пауза новых задач: когда свободно меньше `NEW_TASK_MIN_FREE_GB` (сейчас 10 ГБ в `/srv/autorig/secrets/storage-host.env`) или меньше резерва кэша артефактов `ARTIFACT_CACHE_RESERVE_GB` (сейчас 10), каждая загрузка получает 503 `storage_paused`.
* 2026-10-14 16:56 свободно было 96.6 ГБ, к 23:30 стало 49 ГБ: **минус 47 ГБ за 7 часов**. Дальше диск держится около 50 ГБ только потому, что `autorig-disk-keeper` каждые 10 минут стирает медиа ботов (раз в 1–10 часов по 5–6 ГБ).

## 1. Что где лежит

Колонки: путь, что это, кто пишет, правило хранения, можно ли удалять.
«Клиентское» = исходники и выходы клиентов, **не удаляется никогда** (AGENTS.md: customer sources and outputs are never deleted).

### 1.1 AutoRig (`/srv/autorig`, `/tmp`)

| Путь | Размер | Что это | Кто пишет | Правило хранения | Удалять? |
|---|---|---|---|---|---|
| `/srv/autorig/data/motion_transfer/runs/<run>` | 99 ГБ, 600 запусков, среднее 165 МБ, максимум 1.9 ГБ | запуски V3-рига (MT): вход, `rig/`, `analysis/`, `texture/`, ветки | `autorig-mt` | клиентское; дедуп одинаковых файлов хардлинками (`vps_pressure_cleanup.py`) | Запуск целиком нет. Подкаталоги итераций см. ниже |
| `…/runs/*/rig/skin/v0..vN` | 33.6 ГБ | промежуточные версии скиннинга (итерации) | `autorig-mt`, агенты скиннинга | срока нет, ничего не чистит | Кандидат (регенерируемое), только по решению лида/владельца; оставить активный `rig/rigged.glb` и `rig/skin/index.json` |
| `…/runs/*/rig/joint_judge` | 24.2 ГБ | отчёты и рендеры проверки суставов | `mt.joint_gate`, `rig-judge` | срока нет, хардлинк-дедуп есть | Кандидат, как выше |
| `…/runs/*/rig/.view`, `analysis`, `texture` | 5.3 / 3.7 / 3.8 ГБ | превью, анализ, текстуры | `autorig-mt` | клиентское или производное | Нет |
| `…/motion_transfer/blender/jobs/<id>` | 20 ГБ, 1107 каталогов с 2026-10-09 | задания Blender-воркеров MT (вход, рендеры, status.json) | `autorig-mt` | **ничего не чистит** | Кандидат (регенерируемое): 18 ГБ старше 24 ч, 6.1 ГБ старше 72 ч |
| `…/motion_transfer/v3-dispatch/{runs,sources}` | 17 + 5.8 ГБ, 479 запусков | рабочие каталоги диспетчера V3 на ферму и скачанные исходники | `autorig-mt` (v3 dispatch) | **ничего не чистит** | Кандидат: 14 + 4.8 ГБ старше 24 ч; сначала убедиться, что результат принят в `runs/<run>` |
| `…/motion_transfer/{texlib,cas,triage,models}` | 4.9 / 2.4 / 0.65 / 0.29 ГБ | библиотека текстур, content-addressed store, триаж, модели | `autorig-mt` | постоянные | Нет |
| `/srv/autorig/data/static/glb_cache` | 32 ГБ | кэш GLB для быстрого просмотра и скачивания | `autorig-storage` | лимит `GLB_CACHE_MAX_GB=6`, фактически 31 ГБ: чистка пропускает файлы, которых больше нет на воркере («last copy») | Нет (в логе pressure-cleanup: 40 cached tasks are the only surviving copy) |
| `…/static/{previews-v3,posters-v3,tasks}` | 3.0 / 1.2 / 0.63 ГБ | превью и постеры галереи, кэш загрузок | `autorig-storage`, `autorig-gallery-small-poster` | постоянные | Нет |
| `/srv/autorig/data/var/ai-models/loras` | 32 ГБ | LoRA и модели фермы | ручная установка | постоянные | Нет |
| `…/var/ai-model-transit` | 12 ГБ (flux-2-klein-4b 7.3, qwen_3_4b_fp4_flux2 3.6, flux2-vae 0.3), файлы от 2026-09-21 | транзит моделей на ферму | ручная раздача | нет | Кандидат: Flux не в утверждённом стеке (см. docs/flux1-removal-20260923.md); спросить владельца |
| `…/var/uploads` | 8.3 ГБ | исходные загрузки клиентов | `autorig-storage` | клиентское; старые исходники терминальных задач чистит только давление | Нет |
| `…/var/renderfin` | 7.1 ГБ | выходы Renderfin | `autorig-storage-renderfin` | **24 ч** (`media_retention.py`, явный приказ владельца 2026-10-12), кроме файлов, на которые ссылается задача (235 шт.) | Автомат; руками не нужно |
| `…/var/ai-video-references`, `anthro_work`, `ai-avatar-*` | 3.3 / 1.5 / 0.8 + 0.4 ГБ | референсы, рабочие каталоги категорий, аватары | агенты, аватар-сборка | нет | Нет без решения владельца |
| `/srv/autorig/data/autotests/{work,corpus,mt-tree,reports}` | 7.5 / 1.4 / 0.1 / 0.07 ГБ | регрессионные гейты; work = рабочие копии прогона | `autotests/gate`, `autotests.py` | work стирается через 3 ч после прогона (`vps_pressure_cleanup.py`); corpus read-only | work автомат; corpus **никогда** |
| `/srv/autorig/data/farmjobs` | 8.4 ГБ (почти всё за последние 24 ч, 2516 файлов за 10-14) | результаты фермы до приёма переписью | `autorig-mt`, census | результаты старше 2 суток стираются | Автомат |
| `/srv/autorig/data/v3-intake` | 5.3 ГБ | приём V3 (нормализованные источники) | `autorig-mt` intake | нет | Нет (исходники клиентов) |
| `/srv/autorig/data/{red_film,devbot,red_autopost,misu,lina,kira,nsfwbot,stream_film,jet_film}` | 5.0 / 3.8 / 1.3 / 2.7 / 0.5 / 0.5 / 0.4 / 1.4 / 0.5 ГБ | медиа и LoRA-проекты ботов | боты (Режиссёр, devbot, автопост) | медиа **24 ч** (`media_retention.py`); LoRA, датасеты, модели защищены | Автомат; руками только по решению владельца |
| `/srv/autorig/data/{census,tmp_rigclasses,vehicle-segmentation,tmp-joints}` | 2.9 / 2.7 / 2.1 / 0.35 ГБ | перепись, временные классы, сегментация | агенты | нет | Кандидаты, спросить автора |
| `/srv/autorig/webgl-assets`, `audits` | 21 / 2.6 ГБ | WebGL-ассеты сцен, аудиты | ручные | `audits/*/stage*` старше 24 ч чистит `release_sweep.py audits` | Нет |
| `/srv/autorig/releases` | 2.1 ГБ, 17 каталогов | релизы (hardlink-копии) | деплой | активный + 1 предыдущий (`release_sweep.py`), плюс используемые процессами и моложе 2 ч | Автомат. 17 каталогов не нарушение: хардлинки, часть занята процессами |
| `/srv/autorig/data/db` | 0.5 ГБ | SQLite AutoRig (`autorig.db`) | `autorig-storage` | постоянная | **Никогда** |
| `/tmp` (владелец `debian`, 9272 записи) | 24 ГБ (autorig 2 ГБ); старше 24 ч 6.1 ГБ | scratch агентов: `abruns` 15 ГБ, `wings` 2.8, `eqm30` 1.2, `orientv3`, `mt_test` … | сессии агентов по ssh как `debian` | см. 2.2: фактически не чистится | Своё да. Чужие каталоги агентов только спросив лида |
| `/var/log/journal` | 4.1 ГБ | journald | systemd | лимит `SystemMaxUse` не задан (`journald.conf` пустой) | Можно `journalctl --vacuum-size=1G` (безопасно) |

### 1.2 Чужие проекты на том же VPS (AutoRig их не чистит)

| Путь | Размер | Что это | Заметки |
|---|---|---|---|
| `/var/lib/postgresql/15/main` | 124 ГБ | БД `m_plus_trends` 85 ГБ, `quickmeta_analytics` 36 ГБ, `qwertystock_support_bot` около 1 ГБ, `nishelov` 0.25 ГБ | Проекты Trends / QuickMeta. WAL 1.1 ГБ. Ни `DROP`, ни `VACUUM FULL` без владельца проекта |
| `/home/debian/m_plus_trends` | 112 ГБ | `data/visual_thumbnails` 105 ГБ (1.54 млн файлов; прирост за 7 суток 3.7 ГБ) | Растёт медленно, не причина скачка |
| `/home/debian/qsd/wwwroot` | 28 ГБ | сайт QuickMeta (`qsd`) | |
| `/home/debian/.local/share/rf2node/wwwroot` | 17 ГБ | веб-корень узла rf2 | |
| `/home/debian/analytics_microstock_plus` | 11 ГБ | аналитика (Mongo и Neo4j в docker) | |
| `/var/lib/docker` | 24 ГБ | образ `docker-detectron2` 18 ГБ + 2 висящих `<none>` (общие слои); `docker system df`: 19.5 ГБ reclaimable | Контейнеры analytics-neo4j, analytics-mongo, detectron2 работают 6 суток; amnezia (VPN) 3 месяца |
| `/home/debian/.cache` | 5.1 ГБ | pip 3.2, playwright 0.9, huggingface 0.6 | |
| `/swapfile-mplus-monitor` и `-extra` | 2 × 8 ГБ | swap (занят полностью: 16 из 16 ГБ) | Не трогать: меньше swap = OOM |
| `/root` | 9.9 ГБ | | |
| `/var/log/3proxy` | 0.7 ГБ | логи qwerty-3proxy | |

## 2. Что чистит диск сейчас и чего не покрывает

### 2.1 Таймеры и службы (замер 2026-10-15)

| Таймер / служба | Период | Что делает | Скрипт |
|---|---|---|---|
| `autorig-disk-keeper` | 10 мин, root, nice 19 | (1) стирает видео, уже опубликованные на civitai; (2) если свободно < `KEEP_GB`=50, стирает медиа ботов от старых к новым (видео, аудио, картинки), старше 2 ч, пока не станет `TARGET_GB`=55; лог `/var/log/autorig-disk-keeper.log` | `/srv/autorig/fleet/disk_keeper.py` |
| `autorig-vps-cleanup` | 30 мин, root, nice 19 | дедуп хардлинками внутри запуска MT; `autotests/work` старше 3 ч; `/tmp` scratch старше 6 ч **только по именам** (`bench*`, `work*`, `scratch*`, `autorig-*`, `*.tar*`, `*.zip`, `pc`); результаты фермы старше 2 суток. Затем `release_sweep.py all` (релизы, unity-сборки, `mt.prev`, `audits`) и `media_retention.py` (медиа 24 ч) | `/srv/autorig/fleet/{vps_pressure_cleanup,release_sweep,media_retention}.py` |
| `autorig-storage-disk-pressure-cleanup` | 15 мин, user autorig | только при давлении (< `MIN_FREE_SPACE_GB`=60): ZIP-бандлы, кэш задач, GLB-кэш, кэш артефактов, исходники терминальных задач. Задачи в БД не удаляет. Пропускает «последние копии» | `backend/scripts/run_disk_pressure_cleanup.py` |
| `autorig-surabot-clean` | ежечасно в :17 | стирает то, что бот Суры сделал 24 ч назад | `/srv/autorig/surabot/surabot_clean.py` |
| `autorig-task-flow-alert`, `autorig-storage-healthcheck` | 5 мин / 6 ч | алерты «нет новых задач», проверка здоровья | |
| `logrotate.timer`, `systemd-tmpfiles-clean.timer` | раз в сутки | логи; `/tmp` по правилу `D /tmp` только при загрузке | |
| **`autorig-cleanup.timer`** | не работает | релизы и `/tmp` старше 24 ч (debian и autorig) | **выключен** (`disabled`, `inactive`) |

### 2.2 Дыры (из-за них диск заполнился)

1. **`/tmp` не чистится.** `autorig-cleanup.timer` выключен; `vps_pressure_cleanup.py` берёт по имени, а `abruns` (15 ГБ), `wings`, `eqm30` под шаблон не попадают. Чистка при загрузке бесполезна: сервер не перезагружался 2 года 22 недели (ежедневный ребут из старых заметок больше не действует). Сейчас там 24 ГБ от `debian`, из них 6.1 ГБ старше суток.
2. **`motion_transfer/blender/jobs` (20 ГБ) и `v3-dispatch/{runs,sources}` (23 ГБ): никто не чистит.** Ни один скрипт чистки эти пути не упоминает. Растут на 100–350 каталогов в сутки.
3. **Итерации скиннинга и joint_judge внутри `runs/*/rig/` (58 ГБ из 99 ГБ запусков):** срока хранения нет, дедуп хардлинками снимает только байт-в-байт одинаковое.
4. **Прогоны гейтов:** `autotests/work` записал 7.5 ГБ только за последние 3 часа (10 293 файла); при параллельных гейтах пик десятки ГБ. Чистка через 3 ч, а не сразу.
5. **`autorig-disk-keeper` лечит симптом удалением медиа ботов.** Порог `KEEP_GB`=50 выше нового барьера паузы (10), поэтому он стирает медиа раньше, чем нужно: 2026-10-15 10:19 удалено 11 470 файлов (6.09 ГБ), 09:29 1 494 файла (6.11 ГБ), 10-14 23:39 1 215 файлов (5.44 ГБ). Защиты `media_retention.PROTECT` (refs, lora, models, characters, datasets …) действуют, но кадры и видео в рабочих каталогах могут быть нужны владельцу.
6. **Давление GLB-кэша не снимается:** кэш 31 ГБ при лимите 6 ГБ, чистка отказывается удалять «последние копии» (воркер больше их не отдаёт). Это клиентские файлы (40 задач); освободить можно, только перенеся копии в другое место.
7. **Нет лимита journald** (4.1 ГБ) и ротации логов `3proxy` (0.7 ГБ).
8. **Чужие проекты** (Postgres 124 ГБ, Trends 112 ГБ, QSD 28 ГБ) растут независимо от AutoRig и не входят ни в один таймер; общий порог их не учитывает.
9. **Резервы расходятся.** `autorig-fleet` читает `ARTIFACT_CACHE_RESERVE_GB` при старте: в `/api/fleet` сейчас `artifact_cache_reserve_gb_float = 50` (старое значение), хотя в `storage-host.env` уже 10. Алерты «до паузы осталось N ГБ» считаются от 50, пока `autorig-fleet` не перезапущен (перезапуск безопасен, но это решение лида). Кроме того `MIN_FREE_SPACE_GB=60` (порог периодической чистки) выше обоих барьеров, а значения по умолчанию в `config.py` 65 и 60.

## 3. Как проверить диск (команды)

```bash
ssh autorig-vps
df -h /                                    # общая картина (один раздел)
df -i /                                    # inode (сейчас 8 %)
sudo nice -n 19 du -xsh /* 2>/dev/null | sort -rh | head      # верхний уровень
sudo nice -n 19 du -xsh /srv/autorig/data/* | sort -rh | head -25
sudo nice -n 19 du -xsh /srv/autorig/data/motion_transfer/* | sort -rh | head
sudo nice -n 19 du -xsh /home/debian/* /var/lib/* /tmp/* 2>/dev/null | sort -rh | head -25
sudo docker system df; journalctl --disk-usage
tail -30 /var/log/autorig-disk-keeper.log      # динамика free и удаления
tail -30 /var/log/autorig-vps-cleanup.log /var/log/autorig-release-sweep.log /var/log/autorig-media-retention.log
systemctl list-timers --all --no-pager | grep -iE 'autorig|clean'
ls /srv/autorig/releases; readlink -f /srv/autorig/current
```

* Тяжёлые `du` по `/srv/autorig/data` занимают минуты: всегда `nice -n 19`, `-x`, не запускай несколько одновременно (правило Customer Tasks First: нагрузка на VPS не должна мешать клиентским задачам; 2026-10-12 сервер лёг от параллельных тяжёлых процессов).
* Что писалось за последние 3 часа (кто растёт сейчас):
  ```bash
  for p in /srv/autorig/data/motion_transfer/runs /srv/autorig/data/motion_transfer/blender /srv/autorig/data/autotests /srv/autorig/data/farmjobs /tmp; do
    printf "%-55s" $p; sudo nice -n 19 find $p -xdev -type f -mmin -180 -printf '%s\n' | awk '{s+=$1;n++} END{printf "%.2f GB, %d files\n", s/2^30, n}'; done
  ```
* Размер запусков MT по дате: `find runs -maxdepth 1 -mindepth 1 -newermt "YYYY-MM-DD 00:00" ! -newermt "YYYY-MM-DD 23:59:59" -print0 | du -xsc --files0-from=-`.
* Состояние флота одним запросом: `curl -s https://autorig.online/api/fleet` → `vps_object`: `disk_object` (`free_gb`, `used_percent`), `new_tasks_paused_bool`, `artifact_cache_reserve_gb_float`, `below_reserve_bool`. Текстом: `?format=text`.
* Маркер паузы кэша артефактов: файл `.pause-new-tasks` в `/srv/autorig/data/artifact-cache/` (читает `autorig-fleet`; в бэкенде имя в `artifact_cache.py`, `PAUSE_MARKER`). Состояние паузы дискового гарда (по нему шлются алерты PAUSED и accepted again): `/srv/autorig/data/var/funnel/new-work-pause.json`.
* Postgres по базам, только чтение: `sudo -u postgres psql -Atc "select datname, pg_size_pretty(pg_database_size(datname)) from pg_database order by pg_database_size(datname) desc"`.

## 4. Барьер и пауза новых задач

* Переменные в `/srv/autorig/secrets/storage-host.env` (файл читают `autorig-storage` и `autorig-storage-disk-pressure-cleanup`; **содержимое секретов не печатать**, имена переменных безопасны):
  * `NEW_TASK_MIN_FREE_GB`: свободное место на `/`, ниже которого новые загрузки получают 503. Сейчас 10 (по умолчанию в `backend/config.py` 60).
  * `ARTIFACT_CACHE_RESERVE_GB`: резерв кэша артефактов. Сейчас 10 (по умолчанию 65). Ниже резерва `artifact_cache.artifact_creation_block_reason()` блокирует создание артефактов.
  * `MIN_FREE_SPACE_GB=60` порог фоновой чистки; `TASK_CACHE_MAX_GB=14`, `PERIODIC_TASK_CACHE_MAX_GB=14`, `GLB_CACHE_MAX_GB=6`, `ARTIFACT_CACHE_SOFT_CAP_GB=30`.
  * Смена любой из них требует рестарта `autorig-storage` (и `autorig-fleet` для алертов).
* Где решается пауза: `backend/main.py`, `ensure_request_disk_headroom(db, context=…)` вызывается на запросах создания работы. Порядок: (1) `artifact_creation_block_reason()`; при блоке пробует `run_artifact_cache_retention`; (2) `enforce_task_cache_max_size` и `cleanup_disk_space(min_free_gb=NEW_TASK_MIN_FREE_GB, delete_task_rows=False)`; (3) если свободно всё ещё меньше `NEW_TASK_MIN_FREE_GB`: HTTP 503 `storage_paused`, `Retry-After: 600`. `_new_work_pause_event()` ведёт файл состояния и шлёт алерт только при смене состояния. Строки задач из БД этот путь **не** удаляет.
  * Отдельный путь `ensure_disk_headroom_for_new_task` умеет удалять строки старых задач (`AUTOMATIC_TASK_DB_DELETION`); не включать.
* Алерты в DEV (`https://autorig.online/dev`, UTF-8 с VPS):
  * «🛑 AutoRig: new tasks are PAUSED by the disk guard …» и «✅ … accepted again» из `main.py`;
  * «VPS: НОВЫЕ ЗАДАЧИ НА ПАУЗЕ …», «VPS: диск N ГБ свободно, до паузы … ГБ», «VPS: пауза снята» из `autorig-fleet` (`fleet_api.py`: предупреждает при резерве + 20 ГБ, повтор раз в 3 часа);
  * `disk_keeper.py`: одно сообщение в 3 часа, если после чистки всё ещё < 50 ГБ;
  * бот: `broadcast_disk_space_low` (Telegram), когда после чистки места мало.
* AGENTS.md требует: алерт при свободном < резерв + 20 ГБ, при паузе, и CRITICAL, если 3 часа нет новых задач или `/api/task/create` падает 5 раз за 10 минут (`autorig-task-flow-alert`).
* Не менять барьеры без приказа лида/владельца: 2026-10-12 пауза остановила приём на 10 часов; 2026-10-15 барьер опущен до 10 ГБ как мера, чтобы продолжать принимать задачи.

## 5. Как освобождать место безопасно (по порядку)

Правило: чистка только под давлением, ничего клиентского не удалять, всё тяжёлое с `nice -n 19`, перед `rm` посчитать размер и записать, что и почему.

1. **Свой scratch в `/tmp` и рабочие каталоги своей сессии.** Каждый агент удаляет то, что создал (`/tmp/abruns`, `/tmp/wings`, `/tmp/eqm*`, `/tmp/zeb_*` …) сразу, как закончил. Чужие каталоги `debian` в `/tmp` только с согласия автора (лид). Всего сейчас до 24 ГБ, из них 6.1 ГБ старше суток.
2. **Журналы:** `sudo journalctl --vacuum-size=1G` (около 3 ГБ), старые `3proxy.log.*` (0.7 ГБ). Риск нулевой.
3. **Docker:** `sudo docker image prune` для висящих образов, но образы принадлежат Trends и аналитике; сначала спросить, висящие `<none>` могут быть откатом (reclaimable 19.5 ГБ).
4. **Регенерируемый кэш MT:** `blender/jobs` старше 72 ч (6.1 ГБ), `v3-dispatch/runs` и `sources` старше 72 ч (6.0 + 2.2 ГБ) после проверки у лида, что `runs/<run>` принял результаты. Правильное долгосрочное решение: научить `vps_pressure_cleanup.py` этим путям (раздел 6).
5. **Итерации `runs/*/rig/skin/v*` и `rig/joint_judge`** (до 58 ГБ): только по решению владельца и MT-лида, оставляя последнюю версию и активный `rig/rigged.glb`. Запуск целиком не удалять.
6. **Автомат `disk_keeper` и `media_retention`:** руками не нужны, они сами удаляют медиа ботов старше 24 ч (под давлением старше 2 ч). Что именно он стёр бы: `sudo /usr/bin/python3 /srv/autorig/fleet/disk_keeper.py --dry-run`.
7. **`ai-model-transit` (12 ГБ, Flux), `tmp_rigclasses`, `census`:** спросить владельца или автора.
8. **Чужие проекты** (Postgres, Trends thumbnails, QSD, rf2node, docker): только через владельца проекта. Самый большой объём (около 335 ГБ) там, но это не наша зона.

**Никогда:** `/srv/autorig/data/db`, `var/uploads`, `v3-intake`, `motion_transfer/runs/<run>` целиком, `autotests/corpus`, `/srv/autorig/secrets`, активный и предыдущий релиз, `glb_cache` («последняя копия»), `ai-models/loras`; `rm -rf` без `--one-file-system`; `git reset --hard` ради места; перезапуск сервисов ради места (работы фермы не терять; `autorig-mt` только через `sudo autorig-mt-restart`).

## 6. Что сделать, чтобы это не повторялось (предложения для лида)

1. Включить `autorig-cleanup.timer` либо расширить `vps_pressure_cleanup.py`: каталоги `debian` в `/tmp` старше 24 ч по mtime, без привязки к имени (с исключением активных сессий).
2. Добавить в `vps_pressure_cleanup.py` или в MT: `blender/jobs` и `v3-dispatch/{runs,sources}` старше 72 ч (после приёма результата).
3. Ввести политику для `runs/*/rig/skin/v*` и `rig/joint_judge`: хранить последние N версий и метрики, остальное в архив на ферме или удалять.
4. Пересогласовать пороги: `disk_keeper` KEEP 50 → 20 ГБ (барьер паузы 10), иначе он уничтожает медиа без нужды; `MIN_FREE_SPACE_GB`, `ARTIFACT_CACHE_RESERVE_GB`, `NEW_TASK_MIN_FREE_GB` и резерв в `autorig-fleet` держать одним набором.
5. `SystemMaxUse=1G` в `journald.conf`, ротация `3proxy`.
6. Чистить `autotests/work` сразу после прогона, а не через 3 часа.
7. Тяжёлую фоновую работу выносить на ферму (приказ Customer Tasks First); на VPS не плодить scratch-каталоги на 15 ГБ.
8. Договориться с владельцами Trends и QuickMeta о лимитах (Postgres 124 ГБ, thumbnails 105 ГБ) или вынести их на отдельный диск.

## 7. Что заполнило диск 2026-10-15 (находки)

Динамика по логу `disk_keeper`: свободно 96.6 ГБ (10-14 16:56), 89.5 (18:36), 82.1 (19:36), 65.2 (20:36), 61.1 (22:39), меньше 50 (23:39: keeper стёр 5.44 ГБ медиа и поднял до 55.0); до утра 52–58 ГБ; 09:29 снова < 50 (стёрто 6.11 ГБ); 10:19 снова 49.3 (стёрто 6.09 ГБ, 11 470 файлов). Скорость падения утром около 1 ГБ за 10 минут (6–7 ГБ/ч).

Кто писал (по mtime):

| Источник | Объём | Комментарий |
|---|---|---|
| `motion_transfer/runs` | запуски, тронутые 10-14: 35.2 ГБ (190 запусков); 10-13: 15.7; 10-12: 26.9; 10-11: 12.6; 10-10: 5.8; 10-15 до 10:30: 8.2 ГБ | всего 99 ГБ; 58 ГБ из них итерации `rig/skin/v*` и `rig/joint_judge`. За последние 3 часа записано 2.9 ГБ |
| `/tmp` от `debian` | 24 ГБ; `abruns` 15 ГБ с 10-14 20:02 до 23:00 (61 каталог) | совпадает со скачком 82 → 61 ГБ вечером 10-14; 6.1 ГБ уже старше суток |
| `autotests/work` | 7.5 ГБ записано за последние 3 часа (10 293 файла) | пики при параллельных гейтах |
| `motion_transfer/blender/jobs` | 20 ГБ (1107 каталогов с 10-09; +341 каталог за 10-14) | не чистится |
| `motion_transfer/v3-dispatch` | 22.8 ГБ (479 запусков; +178 за 10-14) | не чистится |
| `farmjobs` | 8.4 ГБ (2516 файлов 10-14 и 552 10-15) | результаты старше 2 суток чистятся |
| Renderfin и боты | 1.4 ГБ за 3 часа | чистятся каждые 30 мин, не растут |
| Postgres | 124 ГБ; файлы переписываются (39 ГБ изменено за 3 часа), размер баз стабилен | не причина скачка; чужой проект |
| Trends thumbnails | +3.7 ГБ за 7 суток (+0.76 ГБ с 10-14) | не причина |

Вывод: скачок вечером 10-14 (около 47 ГБ за 7 часов) собран из трёх нечищеных источников AutoRig: **итерации рига в `runs` (+35 ГБ за день)**, **scratch агентов в `/tmp` (15 ГБ `abruns` и др.)**, **Blender и V3-dispatch (около 20 ГБ)**, плюс пик гейтов (7.5 ГБ). При этом 41 % диска занимают чужие проекты (Postgres 124 ГБ, home 186 ГБ), которые не растут быстро, но и не чистятся, поэтому запас AutoRig мал. Сейчас диск держится только на `disk_keeper`, который стирает медиа ботов.

## 8. Памятка агенту

* Перед любой чисткой: `df -h /`, `du` нужного каталога, лог что и сколько. После: `df -h /` и запись в handoff.
* Свой scratch называй с префиксом `autorig-`, `work-` или `scratch-` (тогда `vps_pressure_cleanup` уберёт его через 6 ч) и удаляй сам по завершении.
* Тяжёлое на ферму, не на VPS; на VPS `nice -n 19` и один процесс за раз.
* Не печатай содержимое `/srv/autorig/secrets/*`; называй только файл и имя переменной.
* Если сайт отдаёт 503 `storage_paused`: `curl -s https://autorig.online/api/fleet`, смотри `vps_object`, затем разделы 3 и 5. Барьеры не менять без лида.
