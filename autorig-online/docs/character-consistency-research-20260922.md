# Исследование: постоянный персонаж «Social Influencer» между нодовыми графами

Дата проверки источников: **2026-09-22**. Основной пример — вымышленный совершеннолетний персонаж. Для реальных людей применяются обычные требования к правам на загруженные изображения и допустимому использованию; это исследование не вводит отдельный безусловный запрет на benign-rendering реальных людей.

## Вывод

Персонаж должен быть отдельным версионируемым ресурсом графа — `CHARACTER_PROFILE`, а не seed, prompt или один файл LoRA. Профиль хранит канонические референсы, раздельные признаки лица/тела/одежды/стиля, разрешённые модельные адаптеры и точные хеши зависимостей. Каждый генератор обязан явно сообщать, какой совместимый способ identity-conditioning он применил. Если подходящего адаптера нет, граф должен завершаться понятной ошибкой совместимости, а не тихо игнорировать персонажа.

Для текущего стека лучший **кандидат для первого production-пути** — **FLUX.2 [klein] 4B + 3–6 канонических multi-reference изображений**, но только после локальных acceptance-тестов. Модель официально поддерживает single/multi-reference editing и выпущена под Apache-2.0. Официальная model card указывает **~13 GB VRAM**, поэтому RTX 4090 24 GB подходит по заявленному ориентиру, а 12 GB worker нельзя считать поддержанным без измерения. В примере Diffusers используется `enable_model_cpu_offload()`, что может снизить VRAM ценой RAM/latency; сторонние quantization-варианты также существуют, но их качество, лицензия и память требуют отдельной проверки ([официальная model card](https://huggingface.co/black-forest-labs/FLUX.2-klein-4B)). Для максимальной повторяемости после накопления качественного набора изображений следует обучить отдельную **identity LoRA именно для FLUX.2 [klein] 4B Base**, но сам профиль должен сохранять референсы и уметь работать без LoRA. Официальный FLUX.2 repo рекомендует distilled 4B (4 шага, guidance 1.0) для production, а Base (около 50 шагов, guidance 4.0) — для fine-tuning ([официальный repo](https://github.com/black-forest-labs/flux2), [точные defaults в исходном коде](https://github.com/black-forest-labs/flux2/blob/main/src/flux2/util.py)).

`Qwen-Image-Edit` полезен как экспериментальный второй backend для semantic editing и создания дополнительных ракурсов: официальный проект показывает сохранение персонажа и прямо рекомендует ссылаться на входное изображение, потому что словесное повторение внешности ухудшает likeness ([Qwen-Image-Edit](https://github.com/QwenLM/Qwen-Image/blob/main/Qwen-Image-Edit.md), [инструкция Qwen-Image-2.1](https://github.com/QwenLM/Qwen-Image-2.1/blob/main/prompt_rewrite/prompts/system_prompt_edit.txt)). Официальный README датирует релиз Qwen-Image-2.1 **2026-09-20** и заявляет до 10 reference images и preservation identity; это заявленные возможности, ещё не локальная валидация ([официальный README](https://github.com/QwenLM/Qwen-Image-2.1)). Его Qwen Research License разрешает материалы только для non-commercial целей, а для commercial use требует отдельную лицензию; поэтому его нельзя ставить production-default для платного сервиса без такого разрешения ([официальная лицензия](https://github.com/QwenLM/Qwen-Image-2.1/blob/main/LICENSE)).

Для видео LTX-2.3 image conditioning нельзя считать гарантией идентичности. Его штатные first-frame/reference conditions задают визуальный контекст, а не identity contract. В официальном issue для LTX-2.3 описана смена лица, волос и одежды при I2V, то есть тот же риск уже наблюдался пользователями ([issue #255](https://github.com/Lightricks/LTX-2/issues/255)). Для видео профиль сначала создаёт утверждённый keyframe через image backend, затем LTX получает этот кадр; устойчивость по длинному ролику требует отдельной character/video LoRA или reference IC-LoRA и покадровой проверки. Официальный trainer поддерживает LoRA, first-frame и `reference` IC-LoRA conditioning ([training modes](https://github.com/Lightricks/LTX-2/blob/main/packages/ltx-trainer/docs/training-modes.md), [configuration reference](https://github.com/Lightricks/LTX-2/blob/main/packages/ltx-trainer/docs/configuration-reference.md)).

## Сравнение подходов

| Подход | Без обучения | Что хорошо сохраняет | Главные ограничения | VRAM/пригодность | Роль в AutoRig |
|---|---:|---|---|---|---|
| FLUX.2 [klein] 4B multi-reference | Да | лицо, одежду и общий визуальный субъект по нескольким refs | likeness вероятностный; нельзя обещать пиксельно идентичного человека | официальный ориентир ~13 GB; 24 GB — кандидат, 12 GB требует offload/quantization и измерения | **Первый кандидат** для изображений после acceptance |
| FLUX.2 [klein] 4B Base + identity LoRA | Нет | устойчивый token/identity внутри одной base-family | обучение, переобучение на фон/одежду; LoRA несовместима с другой архитектурой | обучение планировать на 4090; inference на 12/24 GB после проверки | Фаза 2 для утверждённого персонажа |
| DreamBooth full checkpoint | Нет | сильная привязка субъекта к уникальному token | гигабайтный checkpoint, дороже хранить/обновлять, риск overfit | хуже для фермы и версионирования | Не default; только исключительные случаи |
| InstantID / IP-Adapter FaceID | Да | лицо с одного изображения, SDXL/SD1.5 ecosystem | зависит от face-recognition embedding; не хранит тело/гардероб; типовые веса InsightFace non-commercial | лёгкий runtime, но юридический блок для production | Только лабораторная ветка до лицензирования encoder |
| PuLID-FLUX | Да | лицо при редактируемости prompt | официальный PuLID-FLUX рассчитан на FLUX.1-dev; beta limitations; лицензия следует FLUX.1-dev | заявлена работа на 12/16 GB | Исследовательский fallback, не FLUX.2 default |
| PhotoMaker v2 | Да | несколько фото человека в SDXL | отдельная SDXL-ветка; weaker ecosystem freshness; identity прежде всего лицо | приемлемо для 24 GB, тестировать 12 GB | Compatibility backend для SDXL |
| Qwen Image Edit / 2.1 | Да | semantic edit, multi-image reference, смена сцены | новая модель; 2.1 research-only без commercial grant | измерить на локальном worker | Эксперимент, не production-default |
| LTX-2.3 first frame/reference | Да | начальный кадр, композицию и движение | идентичность может дрейфовать по времени | тяжёлый 22B video backend; не следует считать image method | Генерация видео из уже утверждённого keyframe |
| LTX-2.3 character/reference LoRA | Нет | потенциально лицо, тело, одежду по времени | нужен специально подготовленный video dataset и тесты | обучение только на подходящем worker/режиме | Фаза 3 после image identity |

Основания для таблицы:

- DreamBooth учит уникальный идентификатор по нескольким изображениям и переносит субъект в новые сцены/позы; исходная работа использовала примерно 3–5 изображений ([CVPR paper](https://openaccess.thecvf.com/content/CVPR2023/papers/Ruiz_DreamBooth_Fine_Tuning_Text-to-Image_Diffusion_Models_for_Subject-Driven_Generation_CVPR_2023_paper.pdf)). LoRA обучает малую добавку весов, обычно занимает сотни MB и комбинируется с DreamBooth ([Diffusers LoRA guide](https://huggingface.co/docs/diffusers/training/lora)); full DreamBooth обычно даёт checkpoint размером в несколько GB ([Diffusers adapters guide](https://huggingface.co/docs/diffusers/v0.24.0/using-diffusers/loading_adapters)).
- IP-Adapter-FaceID объединяет ArcFace ID embedding и LoRA; Plus V2 добавляет CLIP features для редактируемости. Это объясняет, почему такой embedding полезен для лица, но не является полным «персонажем» ([официальная IP-Adapter wiki](https://github.com/tencent-ailab/IP-Adapter/wiki/IP%E2%80%90Adapter%E2%80%90Face)). InstantID также является single-image tuning-free методом ([model card](https://huggingface.co/InstantX/InstantID)).
- PuLID-FLUX v0.9.1 относится к FLUX.1-dev, улучшает facial similarity относительно v0.9.0, но сам проект отмечает beta limitation для части входов; лицензия следует FLUX.1-dev ([официальный документ](https://github.com/ToTheBeginning/PuLID/blob/main/docs/pulid_for_flux.md)). Это не доказательство совместимости с FLUX.2.
- PhotoMaker использует stacked ID embedding и несколько ID-изображений; официальный код основан на SDXL и имеет Apache-2.0 code license ([официальный repo](https://github.com/TencentARC/PhotoMaker)). Лицензии каждой загруженной base model и face encoder всё равно проверяются отдельно.

## Почему seed не является идентичностью

Seed воспроизводит начальный шум только при совпадении всей вычислительной среды: checkpoint, VAE, text encoder, sampler/scheduler, steps, guidance, resolution, prompt, adapters, precision и часто версии runtime. Смена prompt, pose, размера или модели меняет изображение и может изменить лицо. Поэтому seed следует хранить для воспроизводимости конкретного render recipe, но он не входит в обязательные признаки персонажа.

Также нельзя обещать «всегда абсолютно одинакового» персонажа. Реалистичный контракт: похожесть выше утверждённого порога на наборе разных сцен и отсутствие критических мутаций. Лицо, телосложение, гардероб и художественный стиль — независимые условия: пользователь может сохранить лицо и тело, сменить одежду, либо оставить стиль серии фиксированным. Один LoRA/embedding, обученный на одинаковой одежде и фоне, часто смешивает их с identity.

## Предлагаемый тип `CHARACTER_PROFILE`

Профиль — immutable manifest с новой revision при любом изменении. Файлы хранятся content-addressed; граф передаёт `character_profile_id + revision`, worker разрешает его в manifest и проверяет SHA-256. Нельзя передавать произвольные пути от клиента.

Ниже **иллюстративный, ещё не подтверждённый реальными прогонами** пример manifest. Значения `candidate`/`untested` намеренно не утверждают, что adapter уже прошёл проверку.

```json
{
  "schema": "autorig.character-profile/v1",
  "id": "chr_01J_SOCIAL_MAYA",
  "revision": 3,
  "display_name": "Maya — fictional adult influencer",
  "subject_policy": {
    "kind": "fictional_adult",
    "adult_confirmed": true,
    "real_person": false,
    "consent_asset": null,
    "commercial_use_allowed": true
  },
  "canonical": {
    "identity_refs": [
      {"asset_id": "sha256:...front", "view": "front", "crop": "head_shoulders"},
      {"asset_id": "sha256:...three_quarter", "view": "three_quarter_left", "crop": "head_shoulders"},
      {"asset_id": "sha256:...profile", "view": "profile_right", "crop": "head_shoulders"},
      {"asset_id": "sha256:...full", "view": "front", "crop": "full_body"}
    ],
    "face": {"locked": true, "description": "internal concise traits"},
    "body": {"locked": true, "description": "height/build/proportions; no pose"},
    "wardrobe": {"locked": false, "default_outfit_asset": "sha256:...outfit"},
    "style": {"locked": false, "default": "photoreal editorial"}
  },
  "adapters": [
    {
      "backend": "flux2-klein-4b",
      "mode": "multi_reference",
      "model": "black-forest-labs/FLUX.2-klein-4B",
      "model_revision": "<immutable commit>",
      "model_sha256": "...",
      "reference_asset_ids": ["sha256:...front", "sha256:...three_quarter", "sha256:...profile", "sha256:...full"],
      "recommended": {"steps": 4, "guidance": 1.0, "width": 960, "height": 540},
      "status": "candidate"
    },
    {
      "backend": "flux2-klein-4b-base",
      "mode": "identity_lora",
      "base_model_revision": "<immutable commit>",
      "weights_asset_id": "sha256:...lora",
      "trigger": "chr_maya_v3",
      "weight": 0.8,
      "training_receipt_asset_id": "sha256:...receipt",
      "status": "candidate"
    },
    {
      "backend": "ltx-2.3",
      "mode": "approved_first_frame",
      "checkpoint_sha256": "...",
      "status": "untested"
    }
  ],
  "evaluation": {
    "suite": "character-consistency/v1",
    "approved_reference_centroid_asset": "sha256:...embedding-set",
    "last_report_asset_id": "sha256:...report",
    "human_approved": false
  },
  "provenance": {
    "created_at": "2026-09-22T00:00:00Z",
    "source_asset_ids": ["sha256:..."],
    "licenses": [{"component": "flux2-klein-4b", "spdx": "Apache-2.0"}]
  }
}
```

Нодовый контракт:

1. `Create/Update Character` принимает 3–12 изображений, проверяет качество/разнообразие, отделяет лицо, full-body и wardrobe refs, создаёт immutable revision.
2. Выходной порт имеет тип `CHARACTER_PROFILE`, а не `IMAGE` или `FACE_EMBEDDING`.
3. `Generate Image` принимает `CHARACTER_PROFILE` и выбирает только `validated` adapter, совместимый с выбранным checkpoint. В UI показывает `multi-reference`, `identity LoRA` или `unsupported`.
4. `Change Outfit` и `Apply Style` являются отдельными условиями; они не переписывают canonical identity.
5. `Generate Video` сначала требует утверждённый image keyframe и затем применяет LTX profile. Выход сохраняет source profile revision и validation report.
6. Результат содержит provenance receipt: manifest revision, все SHA-256, effective prompt, seed, scheduler, dimensions и версии custom nodes.

## Практический план внедрения

### Фаза 1 — без обучения

- Создать `CHARACTER_PROFILE` и хранилище immutable refs/manifest.
- Добавить FLUX.2 [klein] 4B multi-reference как **production candidate**, закрытый feature flag до прохождения acceptance. Это Apache-2.0 backend с официальной multi-reference поддержкой; model card заявляет около 13 GB VRAM ([model card](https://huggingface.co/black-forest-labs/FLUX.2-klein-4B)).
- На 4090 протестировать 960×540, 540×960 и квадратный контрольный размер. На 12 GB worker сначала проверить запуск с официальным CPU offload и, отдельно, выбранной quantization; зафиксировать peak VRAM, системную RAM, latency и качество. До этих измерений 12 GB worker остаётся `unsupported/experimental`. Размер 960×540 не надо считать универсальным training resolution: это default output recipe, а канонические refs сохраняются в исходном качестве.
- Для каждого результата формировать contact sheet и validation report. Низкий score отправляет результат на retry/fail, но не «подкручивает» профиль молча.

### Фаза 2 — identity LoRA

- После утверждения 12–30 разнообразных канонических изображений обучать identity LoRA на **FLUX.2 [klein] 4B Base**, поскольку официальный BFL repo предназначает Base для fine-tuning. Не обучать production LoRA на distilled 4-step варианте без подтверждённой рецептуры.
- Dataset должен менять фон, освещение, ракурс, выражение и одежду. Caption явно называет одежду/аксессуары, чтобы они не сливались с identity token.
- Хранить LoRA вместе с base-model revision/hash и training receipt; запретить автоматическую загрузку на несовместимый checkpoint.
- Сравнить LoRA, multi-reference и hybrid на одном blind test set. Продвигать adapter в `validated` только после автоматических и человеческих проверок.

### Фаза 3 — видео

- Создать утверждённый первый кадр через image pipeline и использовать его как LTX first-frame condition.
- Тестировать identity drift на каждом 8–16-м кадре и отдельно на последнем кадре. Короткие shots предпочтительнее длинной однопроходной генерации.
- Если drift не проходит порог, собрать consented synthetic video dataset и обучить LTX-2.3 LoRA/IC-LoRA. Официальная конфигурация поддерживает `first_frame` и `reference`; checkpoint family определяется из metadata, поэтому manifest должен ссылаться на точный checkpoint и encoder ([LTX trainer documentation](https://github.com/Lightricks/LTX-2/blob/main/packages/ltx-trainer/docs/configuration-reference.md)).

## Приёмочные испытания

Нельзя валидировать персонажа одним удачным портретом или фиксированным seed. Для каждой revision нужен неизменяемый набор минимум из 24 изображений:

- 6 сцен × 2 ракурса × 2 seed: портрет, full-body, улица, интерьер, сложный свет, две персоны;
- минимум три одежды, включая одну новую; лицо/тело должны сохраняться, одежда должна управляемо меняться;
- neutral/style transfer пары, чтобы измерять, не «съедает» ли identity выбранный стиль;
- pose/depth/canny conditions, чтобы подтвердить совместимость с ControlNet без потери identity;
- отрицательные проверки: отсутствующий adapter, неверный base hash, несовместимая model family — явная ошибка до постановки в очередь.

Метрики следует использовать как сигналы, а не как единственную истину:

- face cosine similarity между каждым выходом и centroid канонических face refs;
- DINO/CLIP similarity по сегментированному full-body crop для тела/силуэта;
- landmark stability и доля кадров без обнаруживаемого лица;
- diversity по background/pose, чтобы высокий likeness не получался копированием референса;
- LPIPS/SSIM только для edit-задач, где часть пикселей должна остаться неизменной;
- слепая человеческая проверка: «тот же персонаж?», «одежда изменилась как просили?», «нет ли переноса фона/позы из референса?», «нет ли смешения личностей в multi-person сцене?».

Порог нельзя заимствовать из чужой статьи: его калибруют на локальном наборе positive/negative пар и фиксируют в версии evaluation suite. Acceptance должен требовать одновременно: автоматический порог, отсутствие критических дефектов и человеческое подтверждение. Публичный pretrained InsightFace encoder использовать для production-метрики нельзя без отдельной лицензии.

Для видео: score лица по кадрам, процент успешных detections, worst-decile score, резкие скачки embedding между соседними кадрами, стабильность одежды/волос и ручной просмотр полного непрерывного ролика. Первый/последний кадр или среднее значение не доказывают временную устойчивость.

## Лицензии и коммерческий риск

- FLUX.2 [klein] 4B и 4B Base — Apache-2.0; 9B и FLUX.2 [dev] — non-commercial по официальной таблице. Для платного AutoRig default должен оставаться на 4B, пока не получена отдельная лицензия ([официальный repo](https://github.com/black-forest-labs/flux2), [4B model card](https://huggingface.co/black-forest-labs/FLUX.2-klein-4B)).
- Код InsightFace имеет MIT, но опубликованные pretrained models, включая автоматически скачанные, разрешены только для non-commercial research. Для commercial face recognition weights нужно отдельное разрешение; авторы указывают отдельный контакт для лицензирования ([официальный README](https://github.com/deepinsight/insightface/blob/master/README.md), [model zoo license](https://github.com/deepinsight/insightface/blob/master/python-package/docs/model_zoo.md)). Apache-лицензия InstantID не снимает это ограничение с зависимого `antelopev2`/ArcFace encoder.
- Qwen-Image-2.1 с 2026-09-20 распространяется по Qwen Research License, коммерческое использование требует отдельной лицензии ([текст лицензии](https://github.com/QwenLM/Qwen-Image-2.1/blob/main/LICENSE)).
- Лицензия кода, base model, adapter weights, face encoder и training images проверяется раздельно. Manifest обязан хранить license receipt по каждому компоненту.
- Профиль лица и embeddings — биометрически чувствительные данные. Нужны согласие, назначение, срок хранения, удаление всех revisions/assets по запросу, запрет публичных URL и запрет логирования embeddings. Для вымышленного персонажа всё равно сохраняется provenance исходных/generated assets.

## Решение для AutoRig

Начальная архитектура: `CHARACTER_PROFILE v1` + FLUX.2 [klein] 4B multi-reference + строгая compatibility matrix. До реальных прогонов этот backend имеет статус `candidate`, а 12 GB worker — `experimental`. После прохождения полного тестового набора можно продвинуть его в `validated` и добавить FLUX.2 Base identity LoRA. LTX-2.3 подключать через утверждённый keyframe и держать `untested/candidate`, пока image-to-video и затем собственная video identity LoRA не пройдут непрерывную проверку. InstantID/IP-Adapter FaceID/PuLID оставить research-only за feature flag до разрешения лицензии face encoder и отдельной валидации с используемой model family.

Такой дизайн позволяет заменить модель новой версией без потери персонажа: неизменными остаются canonical refs и семантические части профиля, а для новой family добавляется новый adapter с собственными hash, defaults и validation report. Старый adapter можно вывести из `validated`, не переписывая историю графов и не делая старые результаты невоспроизводимыми.
