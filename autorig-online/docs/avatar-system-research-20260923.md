# Аватар из одного изображения или видео: ракурсы, консистентность, формат v2

Дата: **2026-09-23**. Исследование, ничего не скачивалось и не запускалось. Опирается на
[character-consistency-research-20260922.md](character-consistency-research-20260922.md) (CHARACTER_PROFILE v1,
метрики, лицензии), [video-avatar-research-20260922.md](video-avatar-research-20260922.md),
[video-avatar-progress-20260922.md](video-avatar-progress-20260922.md) (профили Maya/Leo, канарейки klein multi-ref),
[ai-models-market-scan-20260923.md](ai-models-market-scan-20260923.md) (выбор моделей, Ingredients 2.5, Lightning) и
[handoff.md](handoff.md) (решения владельца 23.09: Krea 2 — основной движок картинок, LTX-2.5 — база видео,
H3 — премиум, Wan выброшен, лицензии на личной ферме не ограничение). Повторять их здесь не буду.

Метка **[DL]** означает, что нужна новая загрузка (не одобрено, сейчас не качать). Метка **[LoRA-mgr]** — опциональная
LoRA, которую можно поставить через менеджер `/lora`, когда владелец решит.

---

## 0. Выводы и решения

1. **Граф `Avatar Builder`: один вход (IMAGE или VIDEO), выход `AVATAR` (формат v2, §6) плюс отдельные IMAGE-выходы
   на каждый слот.** Шаги такие: `Source Select` (для видео — выбор кадров, §5) → `Vision Describe` (уже есть
   `ai_avatar_from_image.py`, `VISION_INSTRUCTION`) → `Master Front` → `Face Close-up` → `3/4 L/R` → `Profile L/R` →
   `Back` → `Expressions` → `QA Gate` → `Sheet Compose` (без генерации, просто склейка в PIL) → `Save Avatar revision`.
2. **Одна генерация на один ракурс, а не сетка с последующей нарезкой.** Так у каждого ракурса своё полное
   разрешение, его можно перегенерировать и проверить отдельно. Лист 2×4 для людей и для LTX Ingredients собираем
   сами из одобренных слотов, раскладка детерминированная. Сетку от LoRA (Krea 2 QuadView / CharacterSheet) можно
   держать как быстрый предпросмотр **[LoRA-mgr]**, но слоты из неё не заполнять.
3. **Модель на каждый ракурс:**
   - основная — **FLUX.2 klein 4B** (distilled, 4 шага, CFG 1, у нас измерено 18.5 с и пик 13.1 GB на 4090)
     для front, face, 3/4 и expressions;
   - запасная и основная для **profile и back** — **Qwen-Image-Edit-2511**. У неё встроенная генерация новых ракурсов
     и лучший Elo среди Apache-редакторов. Опционально к ней можно добавить **Multiple-Angles LoRA** от fal
     **[LoRA-mgr]**: 96 положений камеры, промпт `<sks> …`. Для ускорения нужна Lightning 8-step **[LoRA-mgr]**;
     без неё это 40 шагов и true_cfg 4, то есть медленно.
   - **Krea 2 без сторонних нод референс лица не принимает.** Нативно у неё есть только *style reference*
     (`krea2_style_reference.safetensors` **[DL]**). Её роль — генерация по identity-LoRA аватара (§3), а не
     построение листа.
4. **Цепочка без «испорченного телефона».** В каждый вызов передаются **якоря**: исходник (или лучший кадр) плюс
   одобренный `front`. К ним добавляется **не больше одного** соседнего сгенерированного ракурса для геометрии:
   для профиля это 3/4 той же стороны, для спины — оба профиля. Всего не больше 3–4 референсов. Если подавать
   только что сгенерированный кадр как единственный референс, дрейф накапливается.
5. **QA-гейт работает автоматически, ручного шага нет.** Проверки: детекция лица, ArcFace/AuraFace cosine к
   центроиду якорей, направление взгляда по знаку yaw из 5 точек лица (ловит зеркальные и перепутанные L/R),
   для спины обязательно *отсутствие* фронтального лица, VLM-чеклист на Qwen3.5 (f13), проверка фона по
   рамке. Даётся 3 попытки на исходной модели, затем модель-фолбэк, затем слот получает статус `failed`.
   Подделывать слот нельзя.
6. **Стартовые пороги ArcFace (buffalo_l, cosine), потом калибровать:** front, face_closeup и expressions ≥ 0.50;
   3/4 ≥ 0.42; profile ≥ 0.30 (или хотя бы 0.8 × «профильный» baseline); ниже 0.25 — жёсткий отказ.
   Порог адаптивный: `max(порог, 0.8 × baseline)`, где baseline — средняя попарная similarity между хорошими
   кадрами самого исходника. Для одного изображения baseline не считается, работают фиксированные пороги.
7. **LoRA нужна не всегда.** Лист и разовые кадры делаются только по референсам. Identity-LoRA обучается, когда
   аватар «продвинут» (серия больше ~20 генераций, видео-сериал, публикация на Civitai), либо когда без LoRA
   сцены не проходят QA, либо когда аватар нужен в моделях без референс-входа (Krea 2, Z-Image T2I).
   Датасет — 15–30 изображений: слоты листа плюс сцены, сделанные klein. Приоритет базы: **Krea 2 RAW** (обучение
   на RAW, инференс на Turbo), затем Klein 4B Base, затем Z-Image Base. Все базы для обучения — **[DL]**, в
   `C:\LoraTraining\models` сейчас лежит только `h3`.
8. **Видео: аватар используется тремя способами.**
   - LTX-2.5 I2V: первый кадр — сцена, собранная klein из `front` или `three_quarter_*`.
   - **LTX-2.5 Ingredients IC-LoRA**: лист, склеенный из слотов (крупное лицо плюс full-body front/profile/back на
     чёрном фоне), подаётся как статичное видео ≥121 кадра, 768×448. Проверить, что адаптер уже стоит после миграции
     на 2.5.
   - **H3 Ref2VA** (премиум): до 9 изображений, туда идут face_closeup, front, 3/4, profile, back и full_body. В
     `subject_definitions` задаётся `<Subject 1>`, внешность повторяется в каждом шоте.
9. **Порядок работ:** (a) klein-цепочка для front/face/3-4 и QA на уже существующих Maya/Leo; (b) Qwen-2511 для
   profile/back; (c) выбор кадров из видео; (d) формат v2 с миграцией с v1; (e) склейка листа для Ingredients и
   H3; (f) LoRA на Krea 2 по кнопке «Train».

---

## 1. Промпт-шаблоны по слотам (EN)

Общие правила:
- в edit-моделях лучше ссылаться на картинку, чем пересказывать внешность (так рекомендует Qwen); текстом
  называем только одежду, чтобы не было outfit drift на спине;
- фон `plain light grey seamless studio background (#D9D9D9)`, свет ровный софтбокс, без теней на фоне;
- поза A-pose нейтральная. Для AutoRig это заодно удобно под риггинг и Hunyuan3D.

`{WARDROBE}` — строка `wardrobe` из Vision. `{ID}` — короткий `identity_prompt`, **только** для Krea/H3/LTX, где
референса нет или его мало.

Общий префикс для klein и Qwen:
`Same person as in image 1: identical face, hairstyle, hair color, skin tone, body proportions, wearing exactly the same outfit ({WARDROBE}), same shoes and accessories. Plain light grey seamless studio background, soft even lighting, photorealistic, sharp focus, single person.`

| Слот | Шаблон (добавляется к префиксу) | Модель, референсы | Размер |
|---|---|---|---|
| `front` (master) | `Full body shot, standing in a relaxed neutral A-pose, arms slightly away from the body, facing the camera directly, neutral expression, eye-level camera, whole body visible from head to shoes.` | klein: [src, src_face_crop] | 832×1216 |
| `face_closeup` | `Head and shoulders close-up portrait, facing the camera directly, neutral expression, eyes open, looking into the lens.` | klein: [front, src_face_crop] | 1024×1024 |
| `three_quarter_left` | `Full body shot, body and head turned 45 degrees so the person faces the left edge of the frame, three-quarter view, both eyes visible, neutral A-pose.` | klein: [src, front, face_closeup]; фолбэк Qwen | 832×1216 |
| `three_quarter_right` | то же, `faces the right edge of the frame` | так же | 832×1216 |
| `profile_left` | `Full body shot, strict side profile, the person faces the left edge of the frame, only one eye visible, nose silhouette clearly visible, neutral A-pose.` | Qwen-2511: [front, three_quarter_left]; фолбэк klein | 832×1216 |
| `profile_right` | то же, `faces the right edge of the frame` | Qwen-2511: [front, three_quarter_right] | 832×1216 |
| `back` | `Full body shot seen from directly behind, the back of the head and hair visible, face not visible, back side of the same outfit ({WARDROBE}), neutral A-pose.` | Qwen-2511: [front, profile_left, profile_right] | 832×1216 |
| `full_body` | обычно это копия `front`; как отдельный слот — `Full body shot, natural relaxed standing pose, slight three-quarter angle.` | klein: [front, face_closeup] | 832×1216 |
| `expr_<name>` | `Head and shoulders close-up, facing the camera, {expression} expression (e.g. warm smile / laughing / surprised / angry / sad), same lighting.` | klein: [face_closeup, front] | 1024×1024 |

Вариант с Multiple-Angles LoRA **[LoRA-mgr]** (strength 0.8–1.0, только для 2511). К префиксу добавляется:
`<sks> {front-left quarter view | left side view | back view | …} eye-level shot medium shot`. Названия азимутов
заданы от камеры (`front-right quarter` = 45°, `right side` = 90°, `back` = 180°, `left side` = 270°). Соответствие
нашим слотам «лицо смотрит к левому краю» нужно **один раз откалибровать** на тестовом изображении и записать в
`workflow`. Сам QA по yaw всё равно поймает неверную сторону.

Соглашение об именах: `*_left` / `*_right` = **куда направлен нос в кадре** (к левому или правому краю
изображения). Так проверку можно выполнить операционно по landmarks, без споров о том, чья это «левая» сторона.
Численный yaw тоже сохраняется в слот.

---

## 2. Модели и типичные сбои (только одобренный стек)

| | FLUX.2 klein 4B | Qwen-Image-Edit-2511 | Krea 2 |
|---|---|---|---|
| Референсы | цепочка `ReferenceLatent` (нативно), 1–4 на практике | `TextEncodeQwenImageEditPlus`, официально до 3 | нативно только style reference **[DL]**; identity — сторонние edit-ноды (ostris / Krea2Edit) **[DL]** |
| Сильная сторона | идентичность лица и одежды, скорость | новый ракурс (заявлено в 2511, «geometric reasoning»), меньше drift, чем у 2509 | фотостиль, живая экосистема LoRA |
| Настройки | 4 шага, CFG 1, около 1 МП | 40 шагов, true_cfg 4 или Lightning 8 шагов, CFG 1 **[LoRA-mgr]**; около 1 МП; у нас Q3_K_S, на 12 GB — Q4_K_M | Turbo 8–10 шагов, CFG 1 |
| Роль в аватаре | front, face, 3/4, expressions, сцены | profile, back, фолбэк 3/4 | генерация по identity-LoRA аватара |

Типичные сбои и чем они ловятся:
- **Спина** — лицо на затылке, одежда «придумана» или спереди пересажена назад (надписи, пуговицы). Ловится
  детекцией фронтального лица (должна быть 0) и VLM-вопросом «same outfit colors/items?». Не передавать
  `face_closeup` в вызов для спины.
- **Профиль** — дрейф лица (нос и подбородок «усредняются»), профиль выходит 3/4, сторона зеркальная. Ловится
  |yaw| ≥ 60° и знаком yaw, similarity на профиле всегда ниже, поэтому порог отдельный.
- **Outfit drift** — пропадают аксессуары, асимметричные детали переворачиваются. Помогает перечислить одежду
  текстом (`{WARDROBE}`), VLM-чеклист и DINOv2/CLIP similarity торса к `front`.
- **Qwen-Edit** — лёгкий сдвиг и масштаб относительно входа, цветовой сдвиг. Для слотов это не критично,
  потому что композиция задана промптом.
- **Сетки и листы** — склеенные панели, разный масштаб фигуры, при нарезке режутся ступни. Это ещё одна
  причина генерировать по одному ракурсу за вызов.
- **Нечеловеческие и стилизованные персонажи** — ArcFace не применим: лицо не детектируется. QA остаётся на VLM
  и DINOv2, а в manifest пишется `subject.kind`.

Готовые решения, на которые можно посмотреть: нода `muse-character-sheet-klein` (5 поз на klein с кнопками
confirm/re-roll, лист 4096×2304; ставить не нужно, это образец UX) и `comfyui-ReferenceLatentPlus` (сила и окно
по timestep для каждого референса, пригодится, если klein слишком копирует позу исходника) **[DL]**.

---

## 3. Без обучения или per-avatar LoRA

| | Только референсы | Identity-LoRA |
|---|---|---|
| Время до результата | сразу (лист ≈ 9 вызовов × 20–40 с на 4090) | +1–3 ч на 4090 |
| Где работает | только в моделях с референс-входом (klein, Qwen-Edit, H3 Ref2VA, LTX Ingredients) | в любом T2I своей базы (Krea 2, Z-Image), с ControlNet и в сценах с несколькими людьми |
| Риск | слабее на непривычных ракурсах, в сценах, при сильном стиле | переобучение на одну одежду или фон, привязка к одной базе |

LoRA на `C:\LoraTraining\musubi-tuner` (коммит `4e7c714`, 16.09.2026). В нём есть тренеры `krea2`, `flux_2`
(`--model_version klein-base-4b`), `zimage`, `qwen_image` и `minimax_h3`. **LTX нет**, у LTX свой `ltx-trainer`.
- **Krea 2**: обучение на RAW, инференс на Turbo. Нужны `--fp8_base --fp8_scaled` (только вместе) или
  `--convrot_int8`, плюс `--timestep_sampling krea2_shift`. По документации на 3090 при 1024² это 7.1 с/шаг
  (fp8) и 5.3 с/шаг (int8). На 4090 по оценке 3–5 с/шаг, 1500–2500 шагов займут **≈ 1.5–3 ч**. Нужны веса
  RAW **[DL]**.
- **Klein 4B Base**: по гайду BFL влезает в 24 GB, около 1 ч на 4090, 15–40 изображений. Веса Base **[DL]**.
- **Z-Image Base**: около 1.5 ч на 4070 Ti S 16 GB (по сравнению mesmer.tools). **[DL]**
- **Qwen-Image / Edit**: только bf16 20B (fp8 тренер не принимает). Для персонажей избыточно, не рекомендую.
- **H3**: `--h3_teacher_matching --h3_teacher_conditions subject_ref`. Это identity-LoRA, после которой на
  инференсе достаточно текста, а референсами служат 1–9 фото субъекта. Технически лучший путь для видео, но
  тяжёлый (INT8 ≈ 34 GB DiT плюс block swap). Имеет смысл на этапе «сериал».

Датасет LoRA собирается автоматически из аватара: 8–9 слотов листа плюс 10–20 сцен (klein по `front`: разные
фоны, свет, крупности, 2–3 варианта одежды, если wardrobe не заблокирован). Подписи описывают сцену, одежду и
позу, **но не лицо**. Лицо «отдаётся» trigger-токену: по сравнению mesmer.tools это дало самый большой прирост.
Все кадры датасета перед обучением проходят тот же QA-гейт. Приёмка LoRA: 12 контрольных промптов, медиана
ArcFace ≥ порога для front, плюс VLM-проверка.

---

## 4. Видео по аватару

| Путь | Что подаётся из аватара | Параметры | Статус |
|---|---|---|---|
| LTX-2.5 I2V | ключевой кадр сцены, собранный klein из `front` или `three_quarter_*` | как у текущего LTX-2.5 | есть после миграции |
| **LTX-2.5 Ingredients IC-LoRA** | склеенный лист: крупные лица плюс full-body front/profile/back на чёрном фоне, без текста. Чем больше панель, тем лучше она переносится | 768×448, 121 кадр, 24 fps, strength 1.0; промпт `Reference sheet: … Generated video: …`; лист подаётся как статичное видео ≥121 кадра; воркфлоу `LTX-2.5_ICLoRA_Ingredients_Single_Stage_Distilled.json` | проверить, что адаптер установлен |
| LTX + Union-Control | ключевой кадр плюс поза или глубина из видео-драйвера | см. video-avatar-research | есть |
| **H3 Ref2VA** (премиум) | до 9 изображений: face_closeup, front, 3/4 L/R, profile L/R, back, full_body | 1344×768, кадры по формуле 17k+5 (243 кадра ≈ 10 с), промпт из 6 секций, `<Subject N>` задаётся в `subject_definitions`; внешность повторяется в каждом шоте, иначе модель «придумывает человека». На 5090 2 референса и 243 кадра шли 437 с | премиум, 4090 |
| H3 FL2VA | первый и последний кадр из аватара и сцены | | премиум |
| LoRA | Krea LoRA → ключевые кадры; H3 subject_ref LoRA → сериал | | фаза 2–3 |

Правило: видео получает `avatar_id@revision` и записывает его в provenance. Нода `Avatar → Sheet` отдаёт две
раскладки: `ingredients` (под 768×448) и `h3_refs` (список до 9 изображений в фиксированном порядке).

---

## 5. Выбор лучших кадров из видео

Конвейер без новых моделей там, где это возможно:
1. Декодирование через ffmpeg с частотой 2–4 fps (или `select='gt(scene,0.3)'` плюс равномерная выборка), не
   больше ~300 кадров.
2. Резкость — дисперсия Лапласиана (`cv2.Laplacian(gray, CV_64F).var()`) **на кропе лица**, приведённом к
   256 px. Абсолютный порог не ставим: отбрасываем кадры < 0.5 × медианы по ролику и берём верхние 20 %.
   Смаз от движения дополнительно отсекается анизотропией градиентов (соотношение Sobel x/y) и разностью с
   соседним кадром.
3. Лицо:
   - **Haar-каскады идут внутри `opencv-python`** (`cv2.data.haarcascades`: `frontalface_default`,
     `profileface`), это грубый фолбэк;
   - **YuNet (`FaceDetectorYN`) в пакете нет**, onnx лежит в `opencv_zoo` (~230 KB) **[DL]**;
   - лучше взять детектор `det_10g` из **InsightFace buffalo_l**, если он уже есть на ферме: он даёт 5
     landmarks, и из них считается yaw.
   - Фронтальность: |yaw| < 15° и pitch < 15°, глаза открыты, сторона лица ≥ 160 px, экспозиция без клиппинга.
4. Идентичность: эмбеддинги всех лиц кластеризуются, берётся самый большой кластер (или человек в центре кадра).
   Так отсекаются чужие люди в ролике.
5. Выход: `best_front_face`, `best_full_body` (все ключевые точки DWPose видны с головы до ступней),
   опционально `best_left`/`best_right`. Все они — `sources[]` с `frame_time_s` и оценками. Попарная similarity
   между ними и есть **baseline** для порогов QA.

Что проверить на ферме (только посмотреть файлы, ничего не качать): `models/insightface/models/{buffalo_l,antelopev2,auraface}/*.onnx`,
`models/insightface/inswapper_128.onnx` (ReActor), `models/facedetection/` и `facexlib` (`detection_Resnet50_Final.pth`,
`parsing_parsenet.pth`: CodeFormer/ReActor/PuLID), `models/ultralytics/bbox/face_yolov8*.pt` (Impact FaceDetailer — у нас
есть face_fix), `models/pulid`, `models/instantid`, `models/ipadapter/*faceid*`, `models/clip_vision/*` (SigCLIP, CLIP-ViT-H),
`custom_nodes/comfyui_controlnet_aux/ckpts/**/dw-ll_ucoco_384*.onnx` и `yolox_l.onnx` (DWPose — стоял для Wan-Animate v1 на
worker-4090), `dlib` и `ComfyUI_FaceAnalysis`. Эти файлы обычно приносят IPAdapter FaceID, PuLID, InstantID, ReActor,
Impact-Pack и FaceAnalysis.

Метрики идентичности:
- **ArcFace (buffalo_l / antelopev2)** — основная. Для реальных фото «тот же человек» обычно получается 0.5–0.8,
  у разных людей < 0.3. Граница верификации на open-source моделях — 0.3–0.4, и она зависит от FAR. Для
  генераций пороги из §0.6 с калибровкой. Веса InsightFace некоммерческие, для личной фермы это допустимо (решение
  23.09). Замена под Apache — **AuraFace** **[DL]**.
- AdaFace — устойчивее на низком качестве, **[DL]**, не нужна.
- **DINOv2 / CLIP / SigLIP** на кропе тела подходят для одежды и силуэта, но не для лица. Пороги калибровать по
  парам «тот же аватар / другой аватар». DINOv2 — **[DL]**, если на ферме нет.
- **VLM-судья** (Qwen3.5 на f13) — строгий JSON-чеклист: `view_correct`, `one_person`, `same_outfit`, `same_hair`,
  `face_visible_where_expected`, `extra_limbs`, `background_plain`. Он надёжно ловит грубые ошибки, но не
  подменяет ArcFace на лице.
- Как QA делают сообщество и студии: генерируют 10–20 кандидатов на ракурс и выбирают лучший, сравнивают с
  baseline из собственных фото субъекта (так рекомендует автор FaceAnalysis), смотрят лист целиком. Мы
  автоматизируем всё то же самое: best-of-N по score и ручной `manual_pass` как исключение.

---

## 6. Формат аватара v2

Существующие форматы и что из них берём:
- **Character Card V2/V3** (SillyTavern): текстовая персона, `spec`/`spec_version`, пространство `extensions{}` для
  чужих полей. Берём версионирование спецификации и `extensions`.
- **VRM** (3D-аватар): блок `meta` с авторами, лицензией и разрешениями, humanoid-кости. Берём блок
  `rights`/permissions. Позже можно сослаться на наш риг (`rig.glb`), это прямой мост к AutoRig.
- **Метаданные LoRA в Civitai/kohya** (`ss_base_model_version`, `ss_tag_frequency`, trainedWords) — модель для
  `adapters[].training_receipt`.
- У ComfyUI стандарта нет: листы — это просто картинки-сетки. Именованные слоты с provenance и QA — наше
  преимущество.

Миграция с v1 (`AvatarDraft` в `ai_avatars.py`): `display_name`, `identity_prompt`, `appearance`, `wardrobe` и
`negative_identity_prompt` переходят в `identity`/`wardrobe`; `references[]` с `role` — в `sources[]`; `adapter` —
в `adapters[0]`; `provenance` — в `provenance`. Первая face/body-картинка становится кандидатом в `views.front`.

```json
{
  "schema": "autorig.avatar/v2",
  "id": "av_1c0510ac1ba21445555d2ab1", "revision": 2, "parent_revision": 1,
  "status": "draft|approved|retired", "created_at": "2026-09-23T00:00:00Z",
  "display_name": "Maya",
  "subject": {"kind": "human|creature|robot|stylized", "style": "photoreal|3d_render",
              "real_person": false, "consent_asset": null},
  "identity": {"identity_prompt": "...", "appearance": "...", "body": "...",
               "negative_identity_prompt": "...", "trigger_token": "mayaav2"},
  "wardrobe": {"default": "main", "locked": true,
               "outfits": {"main": {"description": "..."}}},
  "sources": [{"asset": {"url": "...", "sha256": "...", "width": 1920, "height": 1080},
               "kind": "image|video_frame", "video_sha256": "...", "frame_time_s": 3.25,
               "scores": {"sharpness": 812.4, "yaw_deg": 4.1, "face_px": 240}}],
  "views": {
    "front": {
      "asset": {"url": "...", "sha256": "...", "width": 832, "height": 1216},
      "outfit": "main", "expression": "neutral", "background": "#D9D9D9",
      "camera": {"yaw_deg": 0, "pitch_deg": 0, "framing": "full_body|head_shoulders|close_up"},
      "provenance": {"model": "flux2-klein-4b", "workflow": "avatar_view_klein_v1",
                     "refs": ["source:0", "source:1"], "prompt": "...", "seed": 123,
                     "steps": 4, "cfg": 1.0, "task_id": "..."},
      "qa": {"status": "pass|fail|manual_pass|failed_no_candidate", "attempts": 2,
             "face_sim": 0.61, "yaw_est_deg": 3.0, "body_sim": 0.88, "vlm": {"view_correct": true}}
    },
    "face_closeup": {}, "three_quarter_left": {}, "three_quarter_right": {},
    "profile_left": {}, "profile_right": {}, "back": {}, "full_body": {}
  },
  "expressions": {"smile": {}, "laugh": {}, "surprised": {}, "angry": {}},
  "extra_views": {"hands_closeup": {}, "outfit_winter/front": {}},
  "sheets": {"ingredients": {"asset": {}, "layout": "ingredients_768x448_v1", "built_from": ["face_closeup", "front", "profile_left", "back"]},
             "h3_refs": {"order": ["face_closeup", "front", "three_quarter_left", "profile_left", "back"]}},
  "embeddings": {"face": {"model": "arcface_buffalo_l", "centroid_asset": "sha256:...",
                          "of": ["source:0", "front", "face_closeup"], "baseline_intra_sim": 0.71},
                 "body": {"model": "dinov2_vitl14", "centroid_asset": "sha256:..."}},
  "adapters": [{"family": "krea2|flux2-klein-4b-base|zimage-base|minimax-h3|ltx-2.5",
                "mode": "multi_reference|lora|ic_lora_ingredients|ref2va",
                "status": "candidate|training|ready|failed|retired",
                "weights": {"url": "...", "sha256": "..."}, "trigger": "mayaav2", "strength": 0.9,
                "base_model_sha256": "...", "training_receipt": {"tool": "musubi-tuner@4e7c714",
                "images": 24, "steps": 2000, "rank": 32, "ss_metadata": {}}}],
  "voice": null,
  "rig": null,
  "qa_summary": {"suite": "avatar-sheet-qa/v1", "thresholds": {"front": 0.5, "three_quarter": 0.42, "profile": 0.30},
                 "required_slots": ["front", "face_closeup"], "passed": ["front", "face_closeup"],
                 "human_approved": false},
  "rights": {"commercial_use": null, "note": "personal research farm"},
  "provenance": {"builder": "avatar_builder_v1", "source_kind": "uploaded|generated|video"},
  "extensions": {}
}
```

Правила формата:
- Revision неизменяем, любая правка создаёт новую revision.
- Имена слотов соответствуют `^[a-z0-9_]+(/[a-z0-9_]+)?$`. Префикс `outfit/…` задаёт вариант одежды.
- Потребители игнорируют неизвестные ключи и слоты. Новые слоты добавляются в `extra_views` и `expressions` без
  смены версии схемы; меняются только `required_slots`.
- Пустой или `failed` слот — это честное «нет», фолбэк на другой слот решает потребитель.
- Эмбеддинги хранятся как asset, в логи и API-ответы они не попадают (см. биометрию в документе 09-22).

---

## Источники

- FLUX.2 klein: [ComfyUI guide](https://docs.comfy.org/tutorials/flux/flux-2-klein), [BFL klein LoRA < 60 min](https://huggingface.co/blog/black-forest-labs/flux-2-klein-lora), [BFL training docs](https://docs.bfl.ml/flux_2/flux2_klein_training), [muse-character-sheet-klein](https://github.com/muse-collective-26/muse-character-sheet-klein), [ReferenceLatentPlus](https://github.com/shootthesound/comfyui-ReferenceLatentPlus)
- Qwen-Image-Edit-2511: [model card](https://huggingface.co/Qwen/Qwen-Image-Edit-2511), [fal Multiple-Angles LoRA](https://huggingface.co/fal/Qwen-Image-Edit-2511-Multiple-Angles-LoRA), [guide](https://qwen-image-2512.com/blog/qwen-image-edit-multiple-angles-lora-guide-en)
- Krea 2: [ComfyUI Krea 2 (style reference)](https://docs.comfy.org/tutorials/image/krea/krea-2), [ostris Krea2 edit nodes](https://github.com/ostris/ComfyUI-Krea2-Ostris-Edit), [comfyui-krea2edit](https://github.com/lbouaraba/comfyui-krea2edit), [Krea character sheets (hosted)](https://www.krea.ai/blog/character-design-with-krea-2), [CharacterSheet LoRAs (Klein 9B / Krea 2)](https://huggingface.co/Alissonerdx/CharacterSheet/blob/main/README.md), [QuadView Krea 2](https://www.patreon.com/TheLocalLab/posts/free-krea-2-lora-167433127)
- Видео: [LTX-2.5 Ingredients](https://huggingface.co/Lightricks/LTX-2.5-22b-IC-LoRA-Ingredients), [LTX Ingredients overview](https://ltx.io/model/capabilities/ingredients), [ComfyUI MiniMax H3](https://docs.comfy.org/tutorials/video/minimax/minimax-h3), [H3 Ref2VA two characters benchmark](https://ai-muninn.com/en/blog/minimax-h3-ref2va-two-characters), локально: `C:\LoraTraining\musubi-tuner\docs\minimax_h3*.md`, `krea2.md`, `flux_2.md`, `zimage.md`, `qwen_image.md`
- LoRA-базы: [mesmer.tools — six base models](https://mesmer.tools/blog/best-base-model-for-lora-training-2026)
- Лица и кадры: [OpenCV DNN face (YuNet/SFace)](https://docs.opencv.org/4.13.0/d0/dd4/tutorial_dnn_face.html), [opencv_zoo YuNet](https://github.com/opencv/opencv_zoo/tree/main/models/face_detection_yunet), [InsightFace thresholds issue](https://github.com/deepinsight/insightface/issues/2239), [ComfyUI_FaceAnalysis](https://github.com/cubiq/ComfyUI_FaceAnalysis)
- Форматы: [Character Card V2](https://github.com/malfoyslastname/character-card-spec-v2), [Character Card V3](https://github.com/kwaroran/character-card-spec-v3), [VRM spec](https://github.com/vrm-c/vrm-specification)
