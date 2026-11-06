# Renderfin: три уровня приоритета (M+ > AutoRig > генерация) — заметка к выкладке

Ветка `renderfin-tier-priority` (локально, не запушена). Код НЕ выложен, сервисы НЕ перезапускались.
Владелец, 2026-11-06: «чёткий балансер, который все задачи раскидает с приоритетом для М+, потом AutoRig,
и только потом Renderfin-генерация картинок/видео; держать прогретыми нужные ноды; оптимизировать под нагрузку M+».

## Что меняется

| Тир | Ранг | Класс брокера | Кто |
|---|---|---|---|
| `mplus` | 0 | `ai_vision` | запрос с `X-Autorig-Priority: mplus` и верным `X-Priority-Key` |
| `autorig` | 1 | `autorig_interactive` | как с 2026-10-15: `priority_class=autorig` изнутри хоста или user_name из `RENDERFIN_AUTORIG_USER_NAMES` |
| `generation` | 2 | `comfy` | всё остальное |
| `background` | 3 | `collection_background` | `queue_class=collection_background` |

- Очередь Renderfin (`renderfin/queue.py`): порядок по тиру; mplus и autorig — старые первыми, generation — прежний LIFO/starvation,
  background — после generation (зависший дольше `RENDERFIN_STARVE_MIN` идёт вместе со «starving» generation).
- Боксы под тир (`RENDERFIN_TIER_BOXES`): пока ждёт задача тира, бокс не берёт работу ниже (`reserved-mplus` / `reserved-autorig`).
  Для autorig по умолчанию — прежние `RENDERFIN_AUTORIG_PRIORITY_BOXES` (worker-4090), правила 4090 не изменились.
- Вытеснение AutoRig — прежнее `_preempt_for_autorig` (worker-4090, 45 с, без новых лимитов: «правила 4090 не меняются»),
  с тирами оно только не трогает M+-рендеры.
- Вытеснение M+ `_preempt_for_tier`: только задача строго ниже тиром, только на боксах `RENDERFIN_TIER_BOXES` mplus; ждать 15 с;
  не чаще 1 раза на бокс за 60 с; не трогаем 3D (`image_to_3d.json`), задачи с lease брокера, задачи дальше 70 % среднего
  времени бокса, задачи у предела рестартов (`restart_resumes >= RESTART_RESUME_MAX-1`) или сбоев отправки (`submit_failures >= 2`);
  H3-видео — последними; после 2 вытеснений задача неприкосновенна и идёт первой в своём тире. Лог:
  `[Renderfin][Queue] preempt tier=mplus f15: render … (tier=generation, …)`.
- Vision/Text (`ai_vision_api.py`): заголовки читаются в `/api/vision` и `/api/text2text`; `_pick_worker(tier=…)` ведёт M+ на
  закреплённые ноды (`RENDERFIN_TIER_VISION_BOXES`), если они не заняты конвертацией/3D и очередь там не длиннее 30 с;
  остальные тиры уходят с закреплённой ноды, пока на ней идёт M+-запрос. Прогрев: раз в `RENDERFIN_VISION_WARM_MINUTES`
  закреплённая нода, которая простаивает без загруженной модели, получает текстовый запрос в одну строку.
- Наблюдаемость: `tier_string` и `preemptions_int` в задаче Renderfin и в `/api/ai/render-status/{id}`;
  `queue_object.by_tier_object` и `tiers_object` (без ключей) в `/api/ai/fleet`; `tiers` в `/renderfin/health`.
- Брокер: `RenderTask.workload_class` = класс тира (влияет на выбор бокса по reserve role), но у брокера Renderfin просит только
  `comfy`/`hunyuan`/`collection_background` (иначе брокер ответит 403 — у принципала renderfin нет прав на ai_vision/autorig_interactive).
  Новое: `GET /renderfin/api-render/broker-preflight` (только localhost) — проверка identity всех managed-боксов без включения брокера;
  `RENDERFIN_WORKLOAD_BROKER_UNVERIFIED=unmanaged` — непроверенный managed-бокс работает без lease, а не уходит offline.
- `/renderfin/api-render` через nginx: тело с `priority_class=mplus` без ключа обнуляется.

Откат одной переменной: `RENDERFIN_TIER_PRIORITY=0` в обоих сервисах + рестарт — код идёт по путям 2026-10-15
(тесты прогнаны с этим флагом: те же результаты, что до правки).

## Перед выкладкой

1. Прод мог уйти вперёд от снимка, на котором сделана ветка (коммит `c96e6bd9`, релиз `live-20261022T151610Z-tripofb-isofill`).
   Сверить 9 файлов: если md5 прода отличается от `git show c96e6bd9:<файл>`, сначала перенести правки ветки на прод-версию.
   ```bash
   ssh autorig-vps 'cd /srv/autorig/current/autorig-online/backend && md5sum ai_fleet.py ai_graph_context.py ai_vision_api.py main.py renderfin/api.py renderfin/config.py renderfin/models.py renderfin/queue.py renderfin/workload_lease.py'
   # локально, в worktree:
   for f in ai_fleet.py ai_graph_context.py ai_vision_api.py main.py renderfin/api.py renderfin/config.py renderfin/models.py renderfin/queue.py renderfin/workload_lease.py; do git show c96e6bd9:autorig-online/backend/$f | md5sum; done
   ```
2. M+ (чат «Renderfin keywording», клиент `src/libs/mpcore/renderfinvisionapi.ts`) уже шлёт `X-Autorig-Priority: mplus` и
   `X-Priority-Key` из env `MPLUS_RENDERFIN_PRIORITY_KEY`. Значение ключа на стороне M+ и у нас должно совпасть.

## Выкладка (AGENTS.md: релизы, не `git pull` — релизы на way-fr не git-чекауты)

ЗАМЕЧАНИЕ: задание лида говорило «git pull on way-fr», но на проде `current` → неизменяемый релиз без `.git`, а AGENTS.md
прямо запрещает pull в релиз. Ниже — штатная процедура релиза.

```bash
# локально: собрать файлы ветки
cd R:/autorig/autorig_renderfin_tier_priority/autorig-online/backend
tar czf /tmp/tier.tgz ai_fleet.py ai_graph_context.py ai_vision_api.py main.py \
  renderfin/api.py renderfin/config.py renderfin/models.py renderfin/queue.py renderfin/workload_lease.py \
  renderfin/priority.py tests/test_renderfin_tier_priority.py
scp /tmp/tier.tgz autorig-vps:/tmp/tier.tgz

ssh autorig-vps
CUR=$(readlink -f /srv/autorig/current)
sudo cp -a "$CUR" "$CUR-tier-priority"
sudo tar xzf /tmp/tier.tgz -C "$CUR-tier-priority/autorig-online/backend"
sudo chown -R autorig:autorig "$CUR-tier-priority/autorig-online/backend"
cd "$CUR-tier-priority/autorig-online/backend" && sudo -u autorig /srv/autorig/venv/bin/python -m pytest -q \
  tests/test_renderfin_tier_priority.py tests/test_renderfin_autorig_priority.py tests/test_renderfin_queue_position.py \
  tests/test_renderfin_exclude_boxes.py tests/test_ai_vision_api.py tests/test_ai_fleet_status.py tests/test_task_owner_graph_link.py
sudo /srv/autorig/autotests/gate backend --release "$CUR-tier-priority"     # GATE PASS или стоп
```

### Секреты и env (значения не печатать)

Ключ M+ — в отдельный файл секретов, его подключают оба сервиса:

```bash
umask 077
sudo install -m 0640 -o root -g autorig /dev/null /srv/autorig/secrets/renderfin-priority.env
# сгенерировать ключ прямо в файл, не выводя его на экран:
echo "RENDERFIN_PRIORITY_KEYS=$(openssl rand -hex 24)" | sudo tee -a /srv/autorig/secrets/renderfin-priority.env >/dev/null
# передать то же значение M+ (MPLUS_RENDERFIN_PRIORITY_KEY) по защищённому каналу; при ротации — два ключа через запятую
sudo tee -a /srv/autorig/secrets/renderfin-priority.env >/dev/null <<'EOF'
RENDERFIN_TIER_PRIORITY=1
RENDERFIN_TIER_VISION_BOXES=mplus=f13-ai,f7-ai
RENDERFIN_VISION_WARM_MINUTES=5
EOF
for u in autorig-storage autorig-storage-renderfin; do
  sudo mkdir -p /etc/systemd/system/$u.service.d
  printf '[Service]\nEnvironmentFile=-/srv/autorig/secrets/renderfin-priority.env\n' | \
    sudo tee /etc/systemd/system/$u.service.d/zz-tier-priority.conf >/dev/null
done
sudo systemctl daemon-reload
```

`RENDERFIN_TIER_PRIORITY` по умолчанию в коде = 1, так что сам по себе релиз уже включает тиры; без ключа M+ тир mplus просто
не выдаётся. Comfy-боксы под M+ (`RENDERFIN_TIER_BOXES=mplus=…`) НЕ задаём, пока M+ не шлёт рендеры в Renderfin
(сейчас M+ ходит только в Vision). Остальные ручки (по умолчанию): `RENDERFIN_MPLUS_PREEMPT_SECONDS=15`,
`RENDERFIN_AUTORIG_PREEMPT_SECONDS=45`, `RENDERFIN_TIER_PREEMPT_BOX_INTERVAL_SECONDS=60`, `RENDERFIN_TIER_PREEMPT_MAX_PER_TASK=2`,
`RENDERFIN_TIER_PREEMPT_MAX_PROGRESS=0.7`, `RENDERFIN_MPLUS_PIN_MAX_WAIT_SECONDS=30`, `RENDERFIN_MPLUS_INFLIGHT_SECONDS=300`.

### Переключение и рестарт

Правило владельца (по заданию лида): рестарт сбрасывает очередь — делать в тихий момент. (В handoff от 2026-10-11 записано,
что `RENDERFIN_RESTART_POLICY=resume` возвращает задачи под тем же id; всё равно выбирать момент, когда `pending`/`rendering` ≈ 0.)

```bash
curl -s http://127.0.0.1:8210/renderfin/health | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["pending"],d["rendering"])'
sudo ln -sfn "$CUR-tier-priority" /srv/autorig/current.new
sudo mv -Tf /srv/autorig/current.new /srv/autorig/current
sudo systemctl restart autorig-storage-renderfin.service autorig-storage.service
systemctl status --no-pager autorig-storage.service autorig-storage-renderfin.service | head -20
```

## Проверка

```bash
# тиры и конфиг без секретов
curl -s http://127.0.0.1:8210/renderfin/health | python3 -m json.tool | sed -n '/"tiers"/,/}/p'
curl -s https://autorig.online/api/ai/fleet | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["tiers_object"]);print(d["queue_object"].get("by_tier_object"))'
# ключ принят (с хоста, через nginx — как M+); ключ идёт из файла заголовка (не в argv, не на экран)
(umask 077; sudo sed -n 's/^RENDERFIN_PRIORITY_KEYS=\([^,]*\).*/X-Priority-Key: \1/p' \
  /srv/autorig/secrets/renderfin-priority.env > /dev/shm/tier-key.hdr)
curl -s https://autorig.online/api/text2text -H 'X-Autorig-Priority: mplus' -H @/dev/shm/tier-key.hdr \
  -H 'Content-Type: application/json' -d '{"prompt":"Say ok (tier check '"$(date +%s)"')","wait_seconds":60}' | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d.get("node_string"),d.get("status_string"))'
rm -f /dev/shm/tier-key.hdr
# ожидание: node_string = f13-ai или f7-ai (если они не заняты конвертацией)
# ключ неверный -> запрос проходит как generation, в журнале одна строка "X-Autorig-Priority: mplus refused"
curl -s https://autorig.online/api/text2text -H 'X-Autorig-Priority: mplus' -H 'X-Priority-Key: wrongwrongwrongwrong' \
  -H 'Content-Type: application/json' -d '{"prompt":"Say ok (bad key '"$(date +%s)"')","wait_seconds":60}' >/dev/null
sudo journalctl -u autorig-storage --since '-5 min' | grep -c 'mplus refused'
# прогрев (если RENDERFIN_VISION_WARM_MINUTES>0)
sudo journalctl -u autorig-storage --since '-30 min' | grep -E 'Vision warm-up'
# вытеснения
sudo journalctl -u autorig-storage-renderfin --since '-1 h' | grep -E 'preempt tier=|re-queued for AutoRig'
```

Уникальный текст в prompt нужен, чтобы ответ не пришёл из `ai_request_cache` (кэш не выбирает ноду).

## Откат

```bash
echo RENDERFIN_TIER_PRIORITY=0 | sudo tee -a /srv/autorig/secrets/renderfin-priority.env >/dev/null   # последний перекрывает
sudo systemctl restart autorig-storage-renderfin.service autorig-storage.service
# или полный откат кода: current -> предыдущий релиз ($CUR) + рестарт обоих
```

## Брокер (`RENDERFIN_WORKLOAD_BROKER_ENABLED`) — НЕ включать в этой выкладке

Код по умолчанию не меняет (`RENDERFIN_WORKLOAD_BROKER_ENABLED=0` в `/srv/autorig/secrets/renderfin.env`). Порядок, когда решим:

1. После выкладки: `curl -s http://127.0.0.1:8210/renderfin/api-render/broker-preflight | python3 -m json.tool` —
   у каждого managed-бокса `identity_ok_bool: true`. Сейчас на проде `RENDERFIN_MANAGED_COMFY_NODES=f1,f2,f5,f7`
   (из Comfy-боксов только f5); для f12/f15/Raptor нужна запись с `machine_*`-id в `RENDERFIN_GPU_CONTROL_NODES_JSON`.
2. Строки для `/srv/autorig/secrets/renderfin.env` (заменить существующую `RENDERFIN_WORKLOAD_BROKER_ENABLED`):
   ```
   RENDERFIN_WORKLOAD_BROKER_ENABLED=1
   RENDERFIN_WORKLOAD_BROKER_UNVERIFIED=unmanaged
   RENDERFIN_MANAGED_COMFY_NODES=f5,f12,f15,raptor
   ```
   (последнюю — только для боксов, прошедших preflight).
3. РИСК, который этот код НЕ закрывает: с брокером включается и Hunyuan-путь `character_gen._ensure_hunyuan_workload`
   (требует проверенную identity Hunyuan-воркера, иначе 3D-стадии ждут `NoWorkerAvailable`) и меняется блокировка
   `_gpu_admission_guard` (файловый lock → брокер). Это надо проверить отдельно до включения.
4. Чтобы M+-рендеры Renderfin вытесняли comfy-lease через брокер, принципалу renderfin в `workload_broker._principal_allows`
   нужно разрешить `ai_vision`/`autorig_interactive` (сейчас только comfy/hunyuan/background) — отдельное решение владельца.

## Ноды ai-node (конфиг на самих нодах — описание, не применено)

`deploy/ai-node/config.json` на f13 и f7 (ноды под M+):

- `keepalive_seconds`: 3600 (сейчас f13 600, f7 300) — модель не выгружается между пачками M+.
- `gpu_lease_hold_file`: только если на карте вторая нода/ComfyUI делит VRAM; файл держит карту за llama-server, пока модель
  загружается/держится (README ai-node, «Two nodes on one card»).
- Нода всё равно уступает GPU ComfyUI/конвертеру (`accepting_ai_vision:false`) — прогрев это учитывает и такую ноду не трогает.
- Опция на потом: режим `owner_priority`/`owner_warm_seconds` по образцу director-ноды 4090 и отправка `"priority":"owner"` для
  mplus — даст приём M+ даже во время рендера ComfyUI, но `owner_preempt_comfy` снимает рендеры Renderfin; решать владельцу.

Порядок на ноде: правка config.json → перезапуск задачи ai-node (как в README ai-node), проверка `/server-status`
(`keepalive_seconds` в ответе).
