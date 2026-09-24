# AutoRig Regen v2 — план реализации и живого тестирования

> **Кому:** исполнителю на ПК (человек или AI-агент) с локальной копией `R:\autorig`
> (репозиторий `eschota/web_services`), `ssh autorig-vps`, доступом к боксам фермы,
> Unity 2022.3+ и своей GPU ≥ 24 ГБ.
> **Ветка:** `claude/intelligent-einstein-47l2ti` (всё из v1 уже там).
> **Файлы:** этот план — `autorig-online/docs/regen-v2-plan.md`; отчёт исполнителя —
> `autorig-online/docs/regen-v2-report.md` (заводит исполнитель).

---

## Быстрый старт (первый день)

1. Прочитать этот файл целиком, затем `AGENTS.md`, `RENDERFIN.md` (раздел Regen) и
   `.claude/skills/renderfin-pipeline/references/gotchas.md`.
2. Подготовить ПК по разделу 3, прогнать базовую линию тестов — числа должны совпасть.
3. `ssh autorig-vps "systemctl cat autorig-renderfin autorig; df -h /; nvidia-smi || true"` —
   зафиксировать реальную раскладку прода в отчёте.
4. Найти бокс фермы с 24 ГБ VRAM, поставить файлы Qwen-Image-Edit-2511 (этап A1).
5. Задеплоить v1 (A2) и прогнать ♻️ Regen на двух задачах из обсуждения (A3).
6. Завести `autorig-online/docs/regen-v2-report.md`, записать результаты, коммит + push.
7. **Срочно и независимо от Regen:** закрыть выпуск кредитов через `viewer-settings`
   (этап E0.1) — дыра есть на проде уже сейчас.

---

## 0. Зачем и что должно получиться

Пользователи присылают на AutoRig сгенерированных персонажей с руками, сросшимися с
торсом, и волосами одним комом — риг наследует все дефекты. Идея команды: перегенерировать
персонажа image-edit моделью в чистую T-позу, отделить волосы и одежду и анимировать их
как ткань.

К концу v2:
1. **Конвейер ткани.** Кнопка ♻️ Regen (Telegram-оператор, API, затем сайт) выдаёт не
   только новый ригнутый персонаж, но и `<stem>_cloth.fbx` / `<stem>_cloth.glb` с цепочками
   костей на волосах, юбках, плащах + манифест `<stem>.autorig-cloth.json`.
2. **Кнопка Regen на сайте** для владельца задачи, за кредиты, с лимитами.
3. **Превью ткани в веб-вьюере** на странице задачи: волосы и юбка качаются под анимацией.
4. **AutoRig Cloth в Unity** проверен вживую и упакован для Asset Store.

## 1. Что уже сделано (v1) и в каком состоянии

Всё ниже протестировано только в облачном контейнере (юнит-тесты, синтетика). **На ферме,
на проде и в Unity ничего не запускалось.**

| Компонент | Где | Коммит | Проверено |
|---|---|---|---|
| Контракт манифеста ткани + канонические пресеты | `autorig-cloth/spec/` | 2b69abf, efca423 | схема + тесты с обеих сторон |
| Кадр модели задачи (`--still`, `--views`) | `autorig-online/tools/renderfin/glb_turntable.mjs`, `backend/renderfin/turntable.py` | ae65287 | реальный рендер в headless Chrome |
| Regen v1: `regen_source` → Qwen-Image-Edit-2511 ×2 → выбор оператора → Hunyuan → облёт → конверт | `backend/renderfin/{character_gen,regen_source,regen_prompts,queue,routing,image_quality}.py`, `assets/workflows/qwen_edit.json`, `backend/telegram_bot.py` (кнопка `rgx:`) | 75c2083 | 567 тестов renderfin/regen/бот |
| Health-check знает `regen_source` | `deploy/healthcheck/renderfin_healthcheck.py` | 52af9e2 | компиляция |
| Декомпозиция одетой модели по базовому телу | `backend/regen/` (`python -m regen.cli decompose`) | 2a8d6f5 | 68 тестов на синтетике, 341k+276k вершин за 6.5 с |
| Blender-шаг после рига (цепочки, веса, экспорт) | `tools/regen/` (`cloth_rig_runner.py`, `blender_cloth_rig.py`) | 2a8d6f5, efca423 | 42 теста через `bpy` 4.3 |
| Unity-пакет AutoRig Cloth v0.1 | `autorig-cloth/package/` | b5ab441, efca423 | 91 тест ядра, компиляция Runtime/Editor по reference-сборкам |

**Решения, которые не пересматривать без причины:**
- Базовое тело просим как **«манекен в облегающем матовом комбинезоне, лысый»**, никогда
  «без одежды»: безопасно для публичной галереи (там есть чиби/детские пропорции) и лучше
  как тело-коллайдер.
- **Две генерации, а не три.** Hunyuan нормализует каждую модель отдельно (масштаб/центр
  теряются) и делает ткань/волосы толстыми замкнутыми объёмами. Поэтому генерим одетого (A)
  и базовое тело (B), а волосы/ткань вырезаем из A по расстоянию до B (`backend/regen`).
- **Qwen-Image-Edit-2511** (Apache-2.0). Qwen-Image-2.1 — некоммерческая лицензия, нельзя.
- **Свой Unity-рантайм**, не Magica Cloth 2: MC2 нельзя класть в пакеты клиентам
  (лицензия на место), нет WebGL, FBX не несёт его настройку.
- Лицензия Hunyuan3D 2.1 **не действует в ЕС, Великобритании и Южной Корее** — это вопрос
  владельца, касается и текущего пайплайна.

## 2. Правила работы (обязательно)

- Источник правил — `AGENTS.md`. Перед отладкой renderfin прочитать
  `.claude/skills/renderfin-pipeline/references/gotchas.md`.
- **Прод-дерево `/root/autorig-online` не принимает `git pull`.** Деплой: сверить sha256
  прод-файла с тем, от чего работали → `scp` только изменённых файлов → тесты на проде →
  рестарт (`autorig-renderfin` первым, потом `autorig-telegram`; `autorig.service` — только
  если трогали `backend/main.py` и соседей). Процедура — `RENDERFIN.md`.
- **Локальные эксперименты на ПК разрешены владельцем для этого плана:** ComfyUI/Qwen/Hunyuan
  на своей GPU, CLI `regen`, Blender, Unity. UI сайта по-прежнему проверяется на проде
  (`https://autorig.online`), без локальных dev-серверов (`AGENTS.md`).
- Не удалять кэши задач, GLB-кэш, видео, постеры (`AGENTS.md`, «Runtime Storage»).
  Не коммитить рендеры, логи, venv, секреты, модели.
- Новые возможности — **за флагами, выключенными по умолчанию**; включать после зелёных
  тестов и смоук-прогона.
- После каждого этапа: коммит + push в ветку, запись результатов в отчёт (последний раздел).
- Ошибки Blender/Hunyuan/ComfyUI **не класть сырым текстом** в `job.error`/`last_error`:
  фразы вроде «timed out after» совпадают с маркерами фермы (`_FLEET_ERROR_MARKERS`,
  `_FARM_BREAKAGE_MARKERS` в `character_gen.py`), и задача будет вечно парковаться и
  воскресать. Только тип ошибки и короткий код (как в `regen_source`).

## 3. Подготовка ПК

1. Код: `git fetch origin && git checkout claude/intelligent-einstein-47l2ti`.
2. Python 3.11 venv: `pip install -r autorig-online/backend/requirements.txt pytest`
   (`scipy==1.17.1`, `trimesh==5.1.0` уже в файле).
3. Node 18+ и Chrome (для `glb_turntable.mjs --still`).
4. Blender **4.3.2** portable (Windows zip) → путь в `REGEN_BLENDER_BIN`.
   Опционально `bpy==4.3.0` + `numpy==1.26.4` в отдельном CPython 3.11 venv — для
   Blender-тестов (процесс падает с кодом 139 после итоговой строки pytest — это известный
   краш модуля `bpy`, смотреть на строку итога).
5. .NET 8 SDK + Git Bash/WSL → `bash autorig-cloth/tests/run-checks.sh`.
6. Unity **2022.3 LTS** (URP) + для проверки совместимости 2021.3.
7. Локальный ComfyUI ≥ 0.4.0 с теми же файлами Qwen-Image-Edit-2511 и узлом RMBG, что на
   ферме (список в `RENDERFIN.md`, раздел Regen) — для быстрой итерации промптов на своей GPU.
   Опционально Hunyuan3D 2.1 локально.
8. Базовая линия тестов (должна совпасть до любых правок):
   - `autorig-online/backend`: `pytest tests/test_renderfin_*.py tests/test_regen_*.py
     tests/test_telegram_generate_button.py tests/test_render_prompting.py -q` →
     **567 passed, 14 skipped** (skip = Blender-тесты без `bpy` и MP4 без ffmpeg x264);
   - `bash autorig-cloth/tests/run-checks.sh` → **91 passed**, обе компиляции 0/0.

**Грабли Windows-чекаута:**
- Перед работой: `git config core.autocrlf false` (или `input`) и перечекаутить файлы.
  `.gitattributes` фиксирует LF только для `*.sh`; `.py/.mjs/.json` с CRLF дадут
  несовпадение sha256 и мусор на проде. Хэши считать в Git Bash, не в PowerShell.
- `tests/test_regen_large.py` импортирует Unix-only `resource`; три теста раннера в
  `tests/test_regen_cloth_rig.py` запускают фейковый Blender через shebang. На Windows они
  падают → гонять тесты в **WSL** (или на VPS). Сделать их кроссплатформенными — задача D0.
- Glob `tests/test_regen_*.py` раскрывается только в bash.

---

## Порядок этапов

```
E0 (дыры: кредиты, API renderfin) — сразу, параллельно со всем
A (v1 вживую) ──► B (декомпозиция/риг руками) ──► D (сайдкар ткани в renderfin) ──► E (кнопка на сайте)
                        │                                                     └──► F (превью во вьюере)
                        └──► C (Unity вживую) ─────────────────────────────────────► G (Asset Store)
```
Ориентир (рабочие дни): E0 — 1 · A — 1–2 · B — 2–3 · C — 1–2 (параллельно с B) ·
D — 0.5 спайк + 3 · E — 3–5 · F — 3–5 · G — 2–3.
Каждый этап заканчивается коммитом, пушем и записью в отчёт. Следующий этап начинать только
после «выхода» предыдущего: B без живого v1 бессмыслен, D без B тюнит вслепую.

---

## Этап A — v1 вживую (ворота для всего остального)

**A1. Бокс под Qwen-Image-Edit-2511.**
- VRAM боксов по seed-файлам `autorig-online/deploy/renderfin-servers/*.json`:
  `f5`, `f12`, `Raptor` — RTX 3080 Ti (12 ГБ), `f15` — RTX 3070 Ti (8 ГБ),
  `worker-4090` — RTX 4090 (24 ГБ). Для fp8 (≈20.5 ГБ + текст-энкодер с выгрузкой) годится
  **только `worker-4090`** (+ ≥ 32 ГБ ОЗУ, лучше 64) — он же сейчас держит `image_to_3d`,
  проверить, что очередь это выдержит. Запасной путь для 12 ГБ-боксов — 4-битные сборки
  (Nunchaku / GGUF Q4) с обязательным сравнением качества на 10 задачах из A3. Конвертер-боксы
  f7/f13 не трогать — там Hunyuan, которому самому нужно 21–29 ГБ.
  Сверить с реальностью (`nvidia-smi` на боксах), seed-файлы могут устареть.
- Поставить 4 файла (имена, папки, ссылки — `RENDERFIN.md`, раздел Regen), ComfyUI ≥ 0.4.0,
  RMBG как для t_pose. Проверка: `GET /object_info/UNETLoader` видит файл,
  `GET /object_info/TextEncodeQwenImageEditPlus` отвечает. Один раз прогнать
  `backend/renderfin/assets/workflows/qwen_edit.json` руками в ComfyUI.
- Объявить токен: **остановить renderfin** (работающий сервис перезаписывает
  `servers/*.json` при опросе), добавить `"qwen_edit.json"` в `available_workflows` в
  `/var/autorig/renderfin/servers/<box>.json`, запустить. Если объявлять через
  `POST /renderfin/api-render` с `render_operation: "info"` — обязательно передать `"status"`,
  иначе бокс уйдёт в offline до следующего опроса.
- Сначала сверить реальную раскладку прода: `ssh autorig-vps "systemctl cat
  autorig-renderfin autorig"` — в репо есть и схема `/root/autorig-online` (порты
  8000/8010), и `deploy/storage-host/` (`/srv/autorig`, 8200/8210). Пути брать с живой машины.

**A2. Деплой v1 на прод.**
- База для сверки хэшей — **`2b69abf`** (родитель первого Regen-коммита), а не `HEAD~1`,
  как написано в доке: `git show 2b69abf:<path> | sha256sum` против
  `ssh autorig-vps "sha256sum /root/autorig-online/<path>"`. Не совпало — на проде ручная
  правка: сначала перенести её в репо.
- Файлы: renderfin `models.py, routing.py, queue.py, image_quality.py, character_gen.py,
  config.py, api.py, telegram_delivery.py` + новые `regen_source.py, regen_prompts.py,
  assets/workflows/qwen_edit.json`; `turntable.py` + `tools/renderfin/glb_turntable.mjs`
  (без них Regen всегда стартует с постера); бот `telegram_bot.py, render_prompting.py`;
  `deploy/healthcheck/renderfin_healthcheck.py`.
- Тесты на проде (скопировать и новые тест-файлы):
  `cd /root/autorig-online/backend && PYTHONPATH=/root/autorig-online/backend
  /root/autorig-online/venv/bin/pytest tests/test_renderfin_*.py -q`.
- Рестарт: `autorig-renderfin`, затем `autorig-telegram`. Health-check из `runbook.md`.
- Мелкая правка доки в этом же коммите: `RENDERFIN.md` (стр. 342) ещё говорит, что
  health-check не знает `regen_source` — это исправлено в `52af9e2`; в разделе Regen явно
  указать базу для сверки хэшей `2b69abf` (общий пример на стр. 28 с `HEAD~1` верен только
  для деплоя одного свежего коммита).

**A3. Живой прогон.** ♻️ Regen (или `POST /renderfin/api-character-gen/regen`) для двух
задач из обсуждения — `9e3341e6-b6aa-4e97-9412-b9ea6e396433` (руки склеены, плохие волосы)
и `accd8f44-3075-48c7-9722-6a77f0daec88` — и ещё 8–10 разных: длинные волосы, юбка,
плащ/пальто, чиби/большая голова, доспех, low-poly, реализм, животное (ожидаем отказ или
плохой результат — записать). На каждую строку в таблице отчёта:
кадр модели верный (фронт, не спина)? · вариант a/b: T-поза, узнаваемость, зазоры руки/торс ·
alpha-проверка прошла? · Hunyuan: руки отделены? · конверт и риг ОК? · время по стадиям.
Логи: `journalctl -u autorig-renderfin` (`regen source for task …`), бот
`journalctl -u autorig-telegram` (`regen job … started`); стадии — из sqlite (`runbook.md`).

**A4. Тюнинг** на своей GPU в локальном ComfyUI тем же `qwen_edit.json`: промпты
`backend/renderfin/regen_prompts.py`, порог alpha-проверки (ширина/высота ≥ 0.7,
`validate_qwen_edit_bundle` в `image_quality.py`), ракурс (`front` vs запасной). Итоговые
значения — коммитом с тестами.

**Выход из A:** ≥ 7 из 10 задач дают ригнутую модель с отделёнными руками и узнаваемым
персонажем; записано p50/p90 времени по стадиям; health-check чистый, зависших джоб нет.

---

## Этап B — декомпозиция и cloth-риг на реальных данных (руками, до кода v2)

Цель — доказать, что `backend/regen` и `tools/regen` работают на настоящих выходах Hunyuan и
конвертера, и подобрать пороги **до** того, как встраивать их в конвейер.

**B1. Базовое тело (B).** Для 5 задач из A с юбкой/плащом/длинными волосами:
- Вход — **полная** выбранная картинка `job.image_url`, не вырезка: `LoadImage` в
  `qwen_edit.json` выкидывает альфу.
- Промпт — `BASE_BODY_PROMPT` из `regen_prompts.py`, **но сначала исправить «серое на
  сером»**: он просит mid-grey комбинезон на mid-grey фоне, RMBG и alpha-проверка на этом
  сломаются. Подобрать контрастный цвет комбинезона (светлый тёплый бежевый/слоновая кость
  на сером фоне) на своей GPU, закоммитить промпт.
- Проверка: лысый, облегающий, **та же поза и кадр** (наложить A и B с 50 % прозрачности —
  руки/ноги совпадают в пределах нескольких пикселей), без анатомии.
- Запуск: локальный ComfyUI или на ферме через `POST /renderfin/api-render`
  (`type: "qwen_edit"`, `user_name: "regen-test"`), опрос
  `GET /renderfin/api-render-get-task-by-url?url=<output_url>`.

**B2. Hunyuan для B.** `POST /renderfin/api-character-gen/from-image` с публичным URL
вырезки и `user_name`, **не равным** `autorig-bot` (иначе бот отправит её в полный конверт),
либо локальный Hunyuan3D 2.1. Скачать `A.glb` (`job.glb_url`) и `B.glb`.

**B3. Декомпозиция.**
`python -m regen.cli decompose --dressed A.glb --body B.glb --out out/<task>`
(0 = ок, 2 = `DecompositionError`). Открыть `out/<task>/parts.glb` в Blender:
`outer` / `hair` / `cloth_k` / `body_inner` / `debug_chains`. Критерии: волосы и свободная
ткань отделены; кисти и обувь не стали тканью; цепочки висят из правильных мест;
`body_inner` не торчит сквозь одежду. Тюнинг — `--config overrides.json` (ключи и
значения по умолчанию: `loose_threshold` 0.035, `hair_threshold` 0.02, `min_hang_cloth` 0.06,
`min_hang_hair` 0.08, `loop_sectors` 8, `joints_per_chain` 4, `align_max_rms` 0.02;
полный список — `backend/regen/config.py`). Если меняются значения по умолчанию — правка
`regen/config.py` + тесты.

**B4. Cloth-риг.** Дождаться конверта задачи A. Вход — **`{guid}_100k/{guid}_all_animations_unity.fbx`**
(единственный ригнутый файл, который открывается в Blender 4.3.2: 84 кости Auto-Rig Pro;
запасные — `_10k`, `_1k`). Взять из artifact cache или публичным
`/api/task/{id}/animations.fbx`. Затем:
`python autorig-online/tools/regen/cloth_rig_runner.py --model <fbx> --decomposition out/<task>
--out rig/<task> --blender "<путь к Blender 4.3 blender.exe>"`.
Проверить в Blender: цепочки прицеплены к `head.x` / корню таза / груди; weight paint на юбке
и волосах; `cloth_rig_report.json` — маппинг костей и остаток совмещения; `<stem>_cloth.fbx`
и `.glb` переимпортируются. **Отдельно проверить текстуры:** FBX конвертера несёт 1
картинку против 41 в `_all_animations.glb` — если `_cloth.glb` без текстур, решить: GLB-выход
делать из `_all_animations.glb`, FBX-выход — из FBX (Blender-шаг принимает оба входа).

**Выход из B:** для ≥ 3 персонажей корректные `_cloth.fbx/.glb` + манифест; зафиксированы
пороги и цвет комбинезона; известные провалы описаны.

---

## Этап C — AutoRig Cloth в Unity вживую (параллельно с B)

- **C1.** Проект Unity 2022.3 LTS (URP) и контрольный 2021.3. Пакет: Package Manager →
  Add from disk → `R:\autorig\autorig-cloth\package\package.json` (или git URL
  `https://github.com/eschota/web_services.git?path=/autorig-cloth/package#claude/intelligent-einstein-47l2ti`).
  Ноль ошибок компиляции, `.meta` импортируются без конфликтов GUID.
- **C2. Демо.** Samples → Procedural Demo → пустая сцена → GameObject с компонентом Cloth Demo
  → Play. Проверить: симуляция устойчива; Profiler — 0 B GC Alloc в кадре после прогрева;
  пауза (`Time.timeScale = 0`); телепорт персонажа не взрывает цепочки.
- **C3. Реальный персонаж** из B4: `_cloth.fbx` (Rig: Humanoid, **Optimize Game Objects —
  выкл**), анимации `_all_animations_unity.fbx` через Humanoid-ретаргет, Animator в режиме
  Normal (не Animate Physics — известное ограничение). Манифест
  `<stem>_cloth.autorig-cloth.json` рядом с FBX → Tools → AutoRig Cloth → Apply Manifest to
  Selected Character. Idle / walk / run / jump: волосы и юбка качаются, не проходят сквозь
  ноги, нет дрожи и взрывов. Записать видео.
- **C4. Производительность:** 1 / 10 / 50 персонажей, мс на кадр у `AutoRigClothManager`
  (Profiler), сборка IL2CPP Windows; по возможности Android.
- **C5.** Баги чинить в `autorig-cloth/`, после правок — `bash autorig-cloth/tests/run-checks.sh`
  (зелёный), коммит.

**Выход из C:** чек-лист C2–C4 пройден, видео и цифры в отчёте.

---

## Этап D — код v2: конвейер ткани в renderfin

Дизайн ниже уже проверен против кода; в первоначальном варианте нашлись три блокирующих бага
(помечены ⚠). Не упрощать эти места.

### D0. Спайк на полдня — go/no-go
- Поставить Blender 4.3.2 на VPS (см. D6), руками прогнать `python -m regen.cli decompose` и
  `tools/regen/cloth_rig_runner.py` на 2 реальных Regen-родителях из B (правка базового тела +
  `_all_animations_unity.fbx` конвертера) **прямо на VPS**, под тем же окружением, что будет у
  сервиса.
- Проверить, что GLB базового тела проходит гейт качества Hunyuan (`validate_glb_bytes`,
  `renderfin/glb_quality.py`).
- Кроссплатформенность тестов: `tests/test_regen_large.py` (`resource` → условный импорт) и
  три теста раннера в `tests/test_regen_cloth_rig.py` (фейковый Blender через `sys.executable`).
- Цвет комбинезона в `BASE_BODY_PROMPT` — по итогам B1.

### D1. Архитектура: дочерняя джоба ткани («сайдкар»)
Главный Regen-поток v1 не меняется. Ветка ткани — дочерняя `CharacterGenJob`, которая
переиспользует надёжные стадии `flux_render` (одна правка Qwen) и `hunyuan`; её падение не
трогает основной результат.

```
родитель kind=regen:       regen_source → flux_render(2×qwen) → [выбор] → hunyuan(A) → turntable → ready → submitted
                                        cloth_requested=1 ─┐ (реконсилер, не хук)
дочь kind=regen_cloth:     flux_render(1×qwen, BASE_BODY_PROMPT) → hunyuan(B) → cloth_decompose
                           → cloth_wait_convert → cloth_rig → cloth_ready        (иначе failed с коротким кодом)
```

- **Флаг** `RENDERFIN_REGEN_CLOTH` (по умолчанию 0; при 0 v1 не меняется) + поле
  `cloth_requested` у родителя, выставляется в `create_regen(..., cloth=None)`.
- **Создание — реконсилером, а не хуком.** `_reconcile_cloth_children()` вызывается на старте
  и на каждом тике ретраев: для родителя `kind=regen` с `cloth_requested`, `chosen_variant`,
  `image_url`, стадией из {hunyuan, turntable, ready, submitted} и без живой дочери создаёт
  ровно одну (дедуп по `parent_job_id`). Переживает падение сервиса между двумя сохранениями;
  хуки в `approve_image`/`_stage_flux` не нужны.
- **Поля дочери:** `kind="regen_cloth"`, `render_type="qwen_edit"`, старт `flux_render`,
  `prompt=BASE_BODY_PROMPT`, `prompt_b=""` (один вариант → выбор пропускается существующим кодом,
  `character_gen.py` ≈1809–1825), `seq=parent.seq` (не тратить номер), `queue_class=
  "collection_background"` (не обгоняет интерактивные джобы в Hunyuan и вытесняется ими),
  **`telegram_chat_id=0`** + `notify_chat_id=parent.telegram_chat_id` (иначе дочь попадёт в
  дайджест очереди, статистику, «❌» карточки и в чистку чата на рестарте), `parent_job_id`,
  `user_name=parent.user_name`.
- ⚠ **Собственная копия картинки.** При создании скопировать выбранную картинку родителя в
  `RENDER_DIR/<user>/<child.id>_regen_source.png` и указать её в `source_image_url`. Иначе
  гард qwen_edit (≈1755) отправит дочь назад в `regen_source` рендерить исходную модель, а
  `_cleanup_artifacts` при удалении дочери сотрёт картинку родителя.
- **Развилка после Hunyuan** (≈1853): для `regen_cloth` → `cloth_decompose`, иначе как было.
- ⚠ **`resume()` зависит от вида** (сейчас любая упавшая джоба с `glb_url` идёт в
  `turntable` → `ready`, а бот мог бы отправить тело в конверт): есть выходы FBX → `cloth_ready`;
  есть декомпозиция → `cloth_wait_convert`; есть GLB → `cloth_decompose`; иначе рендер/Hunyuan.
  `regenerate_image` и `mark_submitted` дочь отклоняют; бот (`_auto_submit_ready_jobs`) явно
  пропускает `kind=="regen_cloth"`.
- **Каскад от родителя:** `discard(parent)` → discard дочери (и убить Blender);
  `regenerate_image(parent)` → discard дочери, очистить `cloth_child_job_id`;
  `stats()` дочерей не считает.

### D2. Стадии дочери
Шаблон добавления стадии: константа в `models.py` → `_ACTIVE_STAGES` (иначе рестарт бросит
дочь) → ветка в `_run` → бюджет на сохранённых часах (`_persisted_stage_budget`) → ветка в
`resume()` → метки/наборы в `telegram_delivery.py` (`cloth_ready` — в `SWEEP_EXEMPT_STAGES`) →
`public_dict` → health-check. **Ожидания — циклами опроса внутри стадии на сохранённых
часах**, а не парковкой через `retry_at`: health-check пропускает запаркованные джобы и не
увидит зависание.

| Стадия | Что делает | Ошибки |
|---|---|---|
| `cloth_decompose` | Ждёт `parent.submitted_task_id` и GLB родителя (потолок 48 ч). **Копирует GLB родителя себе и пишет sha256** (родительский GLB может быть перезаписан, если проверка облёта вернёт его в Hunyuan). Декомпозиция — **подпроцессом** `python -m regen.cli decompose` с таймаутом, `nice`, семафор «один CPU-тяжёлый шаг за раз» (в процессе сервиса +0.5–1 ГБ памяти и зависший event loop, а он держит heartbeat'ы лизов и доставку в Telegram). | Выход 2 (`DecompositionError`) → сразу `failed: cloth:decompose_rejected`, без повторов. |
| `cloth_wait_convert` | Опрос раз в 60 с: главная БД read-only, если есть `AUTORIG_QUEUE_DB_PATH` (прецедент — `hunyuan_client.py` ≈161–195), иначе `GET /api/task/{id}`. `error` → fail; `created/processing` → ждать; главное приложение недоступно → ждать без траты попыток; `input_url` конверта не совпал или sha256 GLB родителя сменился → назад в `cloth_decompose`. Потолок 12 ч. | `cloth:convert_failed`, `cloth:convert_deadline`. |
| `cloth_rig` | FBX по очереди: манифест artifact cache → GLB-кэш (`{task}_{file}`) → `GET /api/task/{id}/animations.fbx` с разбором `X-Accel-Redirect` (`regen_source.accel_local_path`). Blender — **подпроцессом** (`runner.build_command(...)`, `start_new_session=True`), проверка через `runner.verify_artifacts`; при таймауте и **отмене** убить группу процессов (шаблон — `renderfin/turntable.py`); отдельная рабочая папка на попытку; pgid сохранить в джобу и на старте стадии добивать зависший Blender; если готовые выходы уже проверены — Blender не запускать. **Не `asyncio.to_thread`**: отмена не убивает Blender, а рестарт упирается в SIGKILL через 45 с. | `cloth:rig_failed`, `cloth:rig_deadline`. |
| `cloth_ready` | Терминальная; публикация и доставка (D3). | — |

**Ошибки только двух санированных типов** — `ClothTerminal(code)` и `ClothWaiting(code)` —
обрабатываются в начале `_handle_stage_error`. Тексты `ClothRigError`/`DecompositionError`
содержат «Blender timed out after …» и хвосты вывода, которые совпадают с маркерами фермы
(`_FLEET_ERROR_MARKERS`/`_FARM_BREAKAGE_MARKERS`): джоба вечно парковалась бы и воскресала.
HTTP-ошибки — как «status N» (так уже делает `regen_source.py`).

Новые поля (со значениями по умолчанию — старые строки грузятся): у родителя
`cloth_requested=False`, `cloth_child_job_id=""`; у дочери `parent_job_id=""`,
`notify_chat_id=0`, `cloth_convert_task_id=""`, `cloth_dressed_sha256=""`,
`cloth_fbx_url/cloth_glb_url/cloth_manifest_url=""`, `cloth_summary={}`, `cloth_rig_pgid=0`.

### D3. Файлы, публикация, доставка
- **Работа — в `DATA_DIR/cloth/<job>/`** (не публично). Раннер пишет туда же
  `cloth_rig_blender.log` и отчёт — **они не должны попасть в публичный `RENDER_DIR`**.
- **Публикация** — только `_cloth.fbx`, `_cloth.glb` и манифест (+ его копия-алиас):
  для операторских джоб — в `RENDER_DIR/<user>/<job>_cloth/` (публичная ссылка с UUID, как у
  GLB в v1); для **сайтовых** (`user_name="site"`) — в непубличную папку, отдаёт главное
  приложение с `_require_task_download_access` (этап E).
- **Диск:** после публикации удалить скачанный FBX и папки попыток; при discard — всю папку
  ткани; **никогда** не удалять опубликованное по возрасту (может быть последней копией).
- **Telegram:** новые виды доставки `cloth` и `cloth_failed` с первой веткой в
  `pending_delivery`, использующей `notify_chat_id`; сообщение со **ссылками** (`sendDocument`
  в коде нет, лимит Bot API 50 МБ); при провале — короткий код.
- **API:** в запросе regen — поле `cloth`; `GET /renderfin/api-character-gen/cloth-for-task/{task_id}`
  (с учётом вытесненных task id). В боте — пропуск `regen_cloth` в автосабмите и `cloth` в
  `render_prompting.start_character_regen`.

### D4. Чек-лист реализации (≈3 дня после спайка)
1. `models.py`: `CHARGEN_KIND_REGEN_CLOTH`, 4 константы стадий, поля из D2, `public_dict`.
2. `config.py`: `REGEN_CLOTH_ENABLED` (`RENDERFIN_REGEN_CLOTH`), `REGEN_CLOTH_QUEUE_CLASS`,
   `..._PARENT_WAIT_SECONDS=172800`, `..._CONVERT_WAIT_SECONDS=43200`, `..._POLL_SECONDS=60`,
   `..._DECOMPOSE_TIMEOUT=900`, `..._RIG_TIMEOUT=1800`, `..._CONCURRENCY=1`, пути
   `REGEN_TOOLS_DIR`, `CLOTH_WORK_DIR`, `BLENDER_HOME`.
3. Новый `renderfin/cloth.py`: `ClothTerminal`, `ClothWaiting`, `materialize_source`,
   `run_decompose`, `conversion_status`, `fetch_rigged_fbx`, `run_rig`, `publish`, `kill_stale`.
   Раннер грузить по пути файла из `tools/regen` (как `config.py` делает для turntable).
4. `character_gen.py`: стадии в `_ACTIVE_STAGES` и `_run`; развилка ≈1853; гард ≈1755 —
   пересоздать копию картинки дочери, а не отправлять в `regen_source`; `resume`,
   `regenerate_image`, `discard` (каскад + kill Blender), `_cleanup_artifacts`, `stats()`,
   `mark_submitted`; `create_regen(cloth=None)`; `_reconcile_cloth_children`; обработка
   `ClothTerminal/ClothWaiting` в `_handle_stage_error`; три метода стадий.
5. `telegram_delivery.py`, `api.py`, бот и `render_prompting.py` — по D3.
6. `deploy/healthcheck/renderfin_healthcheck.py`: новые стадии, свой порог для
   `cloth_wait_convert` (≈14 ч).
7. Доки: раздел в `RENDERFIN.md`, запись в `.claude/skills/renderfin-pipeline/references/gotchas.md`.

### D5. Тесты D — новый `tests/test_renderfin_regen_cloth.py`
Хелперы: `_RegenEnv`, `_queue_and_manager` (отдельный `queue.db`), `_regen_job`/`_idle_job`,
`_wait_stage`, `_FakeManager`.
1. Реконсилер создаёт ровно одну дочь с верными полями — и после двойного одобрения, и после
   рестарта между сохранениями; при выключенном флаге и для `kind=generate` — ни одной.
2. Дочь: рендер → Hunyuan (и ComfyUI-фолбэк) → `cloth_decompose`, никогда не `turntable`;
   родитель Regen по-прежнему идёт в `turntable`.
3. Пропавшая копия картинки пересоздаётся, а не уходит в `regen_source`.
4. `resume(child)` из каждой точки никогда не попадает в `turntable`/`ready`.
5. Каскад discard; картинка родителя переживает удаление дочери; `regenerate_image(parent)`
   убивает дочь.
6. `ClothRigError` с текстом «timed out after … HTTP 403» → дочь `failed` с чистым кодом,
   `_failed_on_empty_fleet` = False, ретрай-луп её не воскрешает; выход 2 декомпозиции → fail
   после одной попытки.
7. Ожидание конверта: нет task id, processing, error, done, done→created, несовпадение
   `input_url`, сменился sha256 (назад в декомпозицию).
8. Готовые выходы — без Blender; зависший pgid убивается; отмена убивает фейковый Blender
   (шаблон шима — `tests/test_regen_cloth_rig.py`).
9. Дочь не видна в дайджесте, статистике и чистке; карточку ткани получает на `cloth_ready`.
10. Бот пропускает дочерей; маршрут `cloth-for-task` работает.

### D6. Деплой D
- Прод-venv: `pip show numpy` → `pip install scipy==1.17.1 trimesh==5.1.0` (scipy 1.17.1
  требует numpy ≥ 1.26.4, < 2.7; иначе подобрать версию и поправить `requirements.txt`).
- Blender 4.3.2 в `/opt/blender-4.3.2-linux-x64` + системные библиотеки (`tools/regen/README.md`,
  раздел Linux VPS; проверка `ldd`). В `/etc/autorig-renderfin.env`:
  `REGEN_BLENDER_BIN=/opt/blender-4.3.2-linux-x64/blender`. Юнит renderfin —
  `ProtectSystem=strict`, запись только в `/var/autorig/renderfin`, `PrivateTmp=true`, root без
  HOME-переопределения: раннеру ставить `HOME`, `XDG_*`, `BLENDER_USER_RESOURCES/CONFIG/
  SCRIPTS/DATAFILES` и `TMPDIR` внутрь `DATA_DIR`.
- Скопировать `backend/regen/`, `tools/regen/` (путь к `weights.py` по умолчанию —
  `tools/regen/../../backend/regen/weights.py`) и изменённые файлы renderfin/бота; рестарт
  renderfin, затем бот; `RENDERFIN_REGEN_CLOTH=1` — только после смоук-прогона.

---

## Этап E — кнопка Regen на сайте

### E0. Сначала закрыть две существующие дыры (блокируют кнопку; первая есть на проде уже сейчас)
1. **Выпуск кредитов через `viewer-settings`.** `POST /api/task/{id}/viewer-settings`
   (`backend/main.py` ≈16149–16184) даёт владельцу (включая анонимную сессию-владельца)
   заменить **весь** `viewer_settings`, сохраняя с сервера только два ключа. А насос генерации
   (`pump_generation_tasks` в `backend/generation_tasks.py`) читает `charged`, `job_id`,
   `refunded` из `viewer_settings["generation"]`, и при `failed` у renderfin-джобы
   `refund_generation_credits` зачисляет `meta.charged`. Итог: владелец идущей генерации может
   записать `charged: 1000000` + id любой упавшей джобы и получить «возврат». Исправление:
   сервер **всегда** сохраняет свой ключ `generation` (как два других) и никогда не берёт его из
   клиента — лучше вынести состояние в отдельную колонку/таблицу. Тест: подмена
   `generation` через `viewer-settings` ничего не меняет. Это же лечит подвох 1 ниже.
2. **Открытый API renderfin.** `location /renderfin/` проксируется наружу без авторизации, в
   `renderfin/api.py` проверок нет: кто угодно может создать/одобрить/удалить/«кикнуть»
   любую джобу, включая оплаченные сайтовые, и обойти оплату, вызвав regen напрямую.
   Закрыть `/renderfin/api-*` для внешних (токен и/или allow/deny в nginx), публичным оставить
   `/renderfin/render/`. Сначала посмотреть боевой конфиг nginx.

Браузер никогда не ходит в renderfin напрямую — только главный бэкенд, изнутри (`127.0.0.1:8010`).

**Решения владельца:** Regen стоит **5 кредитов** (с активной подпиской — бесплатно, как
генерация) и создаёт **новую задачу** в кабинете со ссылкой «из задачи X»; исходная не меняется.

**Образец — существующий сайтовый поток «картинка → модель → риг»**
(`backend/generation_api.py` + `backend/generation_tasks.py`): строка `Task` владельца с
`pipeline_kind="generate"` создаётся сразу; фоновый цикл (`pump_generation_tasks`,
`_start_generation` → `_advance_generation`) запускает renderfin-джобу с `user_name="site"`
(бот такие не автосабмитит — метка `SITE_OWNED_JOB_GUARD` в `telegram_bot.py`), опрашивает
её и превращает ту же строку в `convert`. Диспетчер и приоритизатор пропускают строки
`generate` (`backend/tasks.py`, `backend/task_priority.py`, `backend/main.py`) — **Regen
использует тот же `pipeline_kind="generate"` с `kind: "regen"` в мете**, чтобы не трогать все
эти исключения.

**Три подвоха, найденные в коде (обязательно учесть):**
1. **Состояние генерации лежит в `viewer_settings["generation"]`, а открытый вьюер владельца
   каждые 3 с перезаписывает `viewer_settings`** (сервер сохраняет только два ключа, `main.py`
   ≈16179). Пользователь откроет новую задачу *до* готовности модели (чтобы выбрать вариант),
   состояние сотрётся, и насос перезапустит джобу. Решается тем же исправлением, что E0.1.
2. **Списание не атомарно** (`generation_api.py`: читает баланс, потом пишет) — два
   параллельных запроса пере-тратят. Для Regen — атомарный
   `UPDATE users SET balance_credits = balance_credits - :c WHERE id = :id AND balance_credits >= :c`
   + ключ идемпотентности на (пользователь, исходная задача, минута).
3. **При двух вариантах renderfin ждёт одобрения, а с `telegram_chat_id=0` ничего не
   доставляет** — сайт обязан сам показать `image_url` / `image_url_b` из статуса джобы и
   вызвать `approve-image`, иначе насос ждёт вечно.

- **E1. Бэкенд.** `POST /api/task/{id}/regen` (регистрация — как у `generation_api.py`):
  только вошедший владелец (`require_login_user`, `_is_task_owner_or_admin`; аноним → 401),
  задача `done`, гуманоид — **тип берём из исходной задачи** (животных не берём; отдельная
  vision-проверка не нужна), лимит **1 активный Regen на пользователя** проверкой в БД
  (slowapi здесь ключуется только по IP) + суточный потолок; атомарное списание
  `REGEN_CREDITS = 5` (константа рядом с `GENERATION_CREDITS`; подписка — бесплатно) →
  `create_conversion_task(..., owner_type="user", owner_id=user.email, pipeline_kind="generate")`
  — эта же строка потом станет конвертом, владелец — пользователь. Мета
  `{kind: "regen", source_task_id, job_id, charged}` — **в серверном ключе** (E0.1).
  Насос получает стадию `regen_start`: `start_character_regen(src, user_name="site",
  telegram_chat_id=0, cloth=True)` (`render_prompting.py`). `_advance_generation` переводит
  `awaiting_image_approval` renderfin в сайтовую стадию `choose` с двумя картинками.
  `POST /api/task/{id}/regen/choose?variant=a|b` → `approve_character_gen_image`; ограниченные
  перегенерации — `regenerate_character_gen_image`; отмена — `discard_character_gen` + возврат;
  нет выбора 24 ч → `a` автоматически. **При переходе строки в `convert` вызвать
  `mark_character_gen_submitted(job_id, task.id)`** — иначе дочерняя джоба ткани не найдёт
  конверт (сейчас сайтовый поток этого не делает). Возврат (`refund_generation_credits`) —
  при провале renderfin, конверта, отмене и таймауте выбора; ткань входит в цену «по
  возможности», без отдельного списания. Скачивание ткани для сайтовых задач — только через
  эндпоинт главного приложения с `_require_task_download_access`, не сырыми ссылками
  `/renderfin/render/`. `GET /api/task/{id}/regen` — стадия, картинки, статус ткани.
- **E2. Модерация.** Варианты Qwen проверяются до показа пользователю (NudeNet-проверка
  постеров в `content_moderation.py` — прогнать и на вариантах); промпт базового тела —
  только «манекен в комбинезоне».
- **E3. UI.** Кнопка Regen для владельца рядом с кнопкой видимости (`updateVisibilityUI` в
  `static/task.html`), панель — отдельным самодостаточным скриптом по образцу
  `static/js/model-sale-offers.js` (монтируется в `#task-sidebar-bundle`, строки на 4 языках):
  прогресс по стадиям, две картинки на выбор, ссылка на новую задачу, блок «Ткань» со
  ссылками на `_cloth.fbx/.glb`/манифест. Опрос — как у страницы (`GET /api/task/{id}` раз в
  4 с). Кнопка не crawl-critical — рендер JS'ом допустим (`AGENTS.md`, SEO). Тексты —
  `static/i18n/{en,ru,zh,hi}.json`. **Поднять `?v=` у тегов скриптов:** `/static` кэшируется
  как immutable на 7 дней. Клиент: 401 → `/auth/login`, 402 → `/buy-credits` (как в `app.js`).
- **E4. Тесты:** владелец / чужой / аноним; нет кредитов (402); подписка без списания; два
  параллельных запроса — одно списание; возврат при провале renderfin, конверта, отмене,
  таймауте выбора; лимит активных; автовыбор варианта; вьюер открыт во время Regen —
  состояние не теряется; подмена `generation` через `viewer-settings` не даёт кредитов (E0.1);
  `mark_character_gen_submitted` вызывается и дочь ткани находит конверт; HTML-контракт кнопки
  (как `tests/test_task_viewer_contract.py`). UI — на проде.
- **Выход из E:** владелец задачи на проде нажимает Regen → видит два варианта → выбирает →
  получает новую ригнутую задачу в своём кабинете (+ ткань, если есть); кредиты списаны
  один раз и возвращены при провале; чужая задача и аноним без аккаунта — кнопки нет /
  403; лимиты срабатывают; мобильная вёрстка ок; все языки сайта.

---

## Этап F — превью ткани в веб-вьюере

**Принцип — паритет с Unity.** Во вьюере работает **порт того же солвера**, что в
`autorig-cloth/package/Runtime/Core` (не three-vrm и не свой «похожий» код): иначе веб-превью
будет обещать поведение, которого не будет в Unity.

- **F1. JS-ядро** `autorig-online/static/js/autorig-cloth-core.js` — чистая математика без
  импорта three (частицы, связи, `stiffness` на 1/60 с, лимит угла, follow-the-leader,
  коллайдеры сфера/капсула/плоскость с тегами, инерция, телепорт, ветер, фиксированный шаг
  90 Гц, встроенные пресеты). Порядок операций — строго как в `ClothSolver.Step.cs`.
- **F2. Золотые сценарии паритета.** Добавить в `autorig-cloth/tests/AutoRig.Cloth.Core.Tests`
  генератор фикстур (5–6 сценариев: висящая цепь, раскачка корня, кольцо юбки, капсулы ног,
  телепорт, ветер) → JSON с позициями по кадрам в `autorig-cloth/spec/golden/`. Node-тест
  `autorig-online/static/js/tests/autorig-cloth-core.test.mjs` (стиль как у соседних:
  `node:test`, импорт модуля через data-URL) сверяет JS с C# с допуском ~1e-4 м.
  Запуск: `cd autorig-online && node --test static/js/tests/autorig-cloth-core.test.mjs`.
- **F3. Контроллер ткани во вьюере** `autorig-online/static/js/task-cloth-preview.js`.
  Вьюер — один inline-модуль в `static/task.html` (three r160 с jsDelivr через importmap).
  Встраивание:
  - **Какой GLB грузить.** `_cloth.glb` уже содержит риг, клипы и цепочки, поэтому он
    **заменяет** `animations.glb`, а не грузится поверх. Доступность ткани — новое поле в
    ответе `GET /api/task/{id}` (≈ `backend/main.py` 6013–6078) + эндпоинты
    `/api/task/{id}/cloth.glb` и `/cloth.json` по образцу `/rig.json` (≈ 13942).
  - **Имена костей.** GLTFLoader пропускает имена через `sanitizeNodeName`: убирает
    `[ ] . : /`, пробелы → `_`, дубликатам добавляет `_1` (`head.x` → `headx`,
    `mixamorig:Hips` → `mixamorigHips`). Имена из манифеста прогонять через ту же функцию
    (`THREE.PropertyBinding.sanitizeNodeName`) или сопоставлять через `parser.associations`.
    Без этого ни одна цепочка не найдётся.
  - **Кадр.** Цикл ≈ 15734–15814: `mixer.update(dt)` → привязка к полу →
    `transformManager.update()` → `playModeController.update(dt)` → рендер. Ткань — сразу
    после `mixer.update`: вернуть цепочки в позу покоя (оптимизатор клипов выкидывает треки,
    совпадающие с покоем), шаг солвера, записать вращения. `AnimationMixer` пересоздаётся при
    каждой загрузке/смене клипа — брать через геттер. Шаблон конструирования — как у
    `PlayModeController` (геттеры + `window.__playModeController`, ≈ 16284–16305).
  - **Ожидания.** Оптимизатор пинит корневое движение по X/Z, поэтому в обычном просмотре
    персонаж стоит на месте: ткань качается от движения конечностей и корпуса, а инерцию
    перемещения видно только в Play-режиме. Это норма, не баг.
  - UI: переключатель «Ткань» в панели вьюера; i18n-ключи в `static/i18n/{en,ru,zh,hi}.json`;
    поднять `?v=` у изменённых скриптов.
- **F4. Бюджет:** ≤ 1 мс/кадр на 300 частиц на среднем ноутбуке; без аллокаций в цикле;
  пауза при скрытой вкладке; мобильные — проверить на реальном телефоне.
- **F5. Проверка.** Без браузера: добавить в харнесс `tools/renderfin/glb_turntable.mjs`
  (сырой CDP, vendored three r160, программный WebGL; Playwright в репо нет) режим, который
  грузит `_cloth.glb` + манифест, делает N кадров анимации и проверяет, что кости цепочек
  двигаются, нет NaN и разлёта. Вживую — **на проде** (`AGENTS.md` запрещает локальную
  браузерную QA сайта): задача с тканью — волосы/юбка качаются под idle/walk и в Play-режиме;
  задача без ткани — без изменений; консоль без ошибок; отладка через
  `window.__mainViewerDebug`.
- **Выход из F:** золотые тесты паритета зелёные; превью на проде для ≥ 3 задач; видео.

---

## Этап G — AutoRig Cloth для Unity Asset Store

- **Формат публикации:** проверить актуальные требования Asset Store (классический
  `.unitypackage` через Asset Store Tools vs UPM-пакет). Для `.unitypackage`: содержимое под
  `Assets/AutoRig/Cloth/`, семплы — обычной папкой (не `Samples~`, это только UPM),
  документация — `Documentation/` (PDF или ссылка). UPM-вариант из `autorig-cloth/package`
  оставить для своих клиентов и git-URL.
- **Совместимость:** 2021.3 LTS (минимум из `package.json`), 2022.3 LTS, Unity 6; Built-in,
  URP, HDRP (сам пакет от рендера не зависит, демо-материалы — на каждый пайплайн);
  Windows/macOS, IL2CPP, Android/iOS. WebGL — проверить отдельно: это наше преимущество перед
  Magica Cloth 2 (там WebGL нет).
- **Демо-сцена с настоящим персонажем.** Не использовать персонажа, сделанного через
  Hunyuan (лицензия не действует в ЕС/UK/Корее, а Asset Store продаёт по всему миру) и не
  Mixamo-персонажей (их нельзя перераспространять). Взять CC0-персонажа (например,
  Quaternius) или заказного, прогнать через AutoRig + Blender-шаг, положить с манифестом.
- **Материалы страницы:** 5+ скриншотов 1920×1080, key image, видео (волосы/юбка/плащ под
  бегом и прыжком, 50 персонажей в кадре), описание, сравнение «из коробки vs ручная настройка»,
  цифры производительности из C4, контакт поддержки. Цена — решение владельца.
- **Качество:** Asset Store Tools → Validator без ошибок; ноль предупреждений компиляции;
  без зависимостей от платных пакетов; namespace `AutoRig.Cloth` и asmdef уже есть.
- **Выход из G:** пакет прошёл валидатор, демо работает «из коробки» в 3 версиях Unity,
  материалы страницы готовы, отправлено на ревью (отправляет владелец).

---

## Итоговая приёмка v2 (живой прогон E2E)

Прогнать 10 задач из A3 + 5 новых на проде с включёнными флагами:
1. ♻️ Regen в Telegram **и** кнопка на сайте → новая ригнутая задача, у сайтовой — владелец
   тот же пользователь, кредиты списаны один раз (при провале — возвращены).
2. Для персонажей с волосами/юбкой/плащом — `_cloth.fbx`, `_cloth.glb`, манифест доступны
   по ссылкам; сайдкар без зависаний (health-check чистый).
3. На странице задачи превью ткани включается и качается под анимацией; без него страница
   работает как раньше.
4. Тот же `_cloth.fbx` + манифест в Unity даёт то же поведение, что в веб-превью.
5. Метрики: доля успешных Regen, доля успешной ткани, p50/p90 времени от кнопки до ткани,
   мс/кадр ткани во вьюере и в Unity.

## Риски и ручки настройки

| Риск | Где видно | Ручка |
|---|---|---|
| Qwen не держит T-позу или узнаваемость | A3, отказы alpha-проверки | промпты `regen_prompts.py`, второй вариант, другой ракурс |
| Базовое тело не совпадает по позе с A (руки съехали > 5°) | наложение в B1, `align_rms` | промпт B, вход = полная картинка A, повтор seed |
| Волосы сливаются с капюшоном/плащом | `parts.glb` | маска волос (`--hair-mask`), `hair_column_head_scale` |
| Сапоги/кисти становятся тканью | `parts.glb` | `min_hang_*`, `loose_threshold` |
| Текстуры теряются в `_cloth.glb` | B4 | GLB-выход из `_all_animations.glb` |
| Блокировка sqlite в тестах | флейки `database is locked` в `test_renderfin_api` и `EmptyFleetTests` (есть и на чистом `main`) | найти, где отмена джобы рвёт транзакцию в `_persist` (`character_gen.py`), и сделать запись устойчивой к отмене |
| API renderfin открыт без авторизации | `deploy/nginx.conf` `location /renderfin/`, в `renderfin/api.py` нет проверок | закрыть пишущие эндпоинты (токен и/или allow/deny в nginx), публичными оставить только `/renderfin/render/`; **обязательно до этапа E** |
| Выпуск кредитов через `viewer-settings` (есть на проде сейчас) | `main.py` ≈16149–16184 + `generation_tasks.py` | E0.1: серверный ключ `generation` / отдельная колонка; закрыть до кнопки на сайте, лучше сразу |
| Лицензии | Hunyuan (ЕС/UK/Корея), Qwen 2.1 | решение владельца; в коде только 2511 |

## Формат отчёта исполнителя

Файл `autorig-online/docs/regen-v2-report.md` (обновлять после каждого этапа, коммитить):
- таблица задач: id, тип персонажа, результат по каждому шагу, время по стадиям, ссылки;
- картинки/видео — не в git: ссылки на `/renderfin/render/...` или внешнее хранилище;
- принятые значения порогов и промптов (с коммитами);
- найденные баги: где, как воспроизвести, чем исправлено (коммит) или почему нет;
- что не сделано и почему.

