# Бесплатные LLM/VLM API для AutoRig (состояние на 2026-10-11)

Исследование без правок кода и без регистрации аккаунтов. Приказ владельца 2026-10-11: платных API нет, подключать все бесплатное. OpenRouter `:free` и OpenCode Zen free уже подключаются отдельно, здесь они только для полноты.

## 0. Как читать и чему верить

* Первичные страницы Groq (`console.groq.com/docs/...`) и часть страниц NVIDIA отдали 403/404. Цифры Groq и NVIDIA взяты из ежедневно проверяемых каталогов (gravity.fast, 10.10.2026) и из статей октября 2026. Они совпадают между собой, но перед боевым включением главная сессия должна сделать живой прогон по каждому ключу: `GET /models` и один `chat/completions` с tools. Лимит на странице аккаунта важнее любой таблицы.
* Бесплатные квоты в 2026 меняются еженедельно: Google перестал публиковать цифры, Mistral убрал сумму кредита (7.10.2026), Cerebras и GitHub Models закрыли бесплатный доступ. Роутер обязан переживать 429, исчезновение модели и смену лимитов.
* Строгое юридическое чтение: ни один из бесплатных тарифов прямо не разрешает публичный трафик чужих пользователей как продукт. Явно запрещают прод: NVIDIA (пробный API, только прототипы), Cohere trial (не коммерческое), Cerebras (dev/eval). Остальные формулируют «для прототипирования/оценки». Решение о риске за владельцем (см. раздел 4).

Источники (основные):
* [Gravity.fast, Free LLM API Tiers, проверено 4 и 10.10.2026](https://gravity.fast/data/free-llm-api-tiers/)
* [DEV: Free LLM API Tiers in October 2026](https://dev.to/tariqnasser/free-llm-api-tiers-in-october-2026-whats-left-and-how-i-chain-them-227l)
* [mnfst/awesome-free-llm-apis](https://github.com/mnfst/awesome-free-llm-apis)
* [BenchLM: Groq free tier](https://benchlm.ai/md/free-tier/groq.md)
* [Cerebras: pricing](https://www.cerebras.ai/pricing), [rate limits](https://inference-docs.cerebras.ai/support/rate-limits), [models](https://inference-docs.cerebras.ai/models/overview)
* [Gemini API terms](https://ai.google.dev/gemini-api/terms), [pricing/data use](https://ai.google.dev/gemini-api/docs/pricing), [rate limits](https://ai.google.dev/gemini-api/docs/rate-limits), [OpenAI compat](https://ai.google.dev/gemini-api/docs/openai)
* [Cloudflare Workers AI pricing](https://developers.cloudflare.com/workers-ai/platform/pricing/), [OpenAI compat](https://developers.cloudflare.com/workers-ai/configuration/open-ai-compatibility/), [models](https://developers.cloudflare.com/workers-ai/models/)
* [SambaNova rate limits](https://docs.sambanova.ai/docs/en/models/rate-limits)
* [Hugging Face pricing](https://huggingface.co/docs/inference-providers/pricing)
* [Cohere rate limits](https://docs.cohere.com/docs/rate-limits)
* [OpenRouter limits](https://openrouter.ai/docs/api-reference/limits), [OpenCode Zen](https://opencode.ai/docs/zen)
* [GitHub Models retired](https://github.blog/changelog/2026-07-30-github-models-is-now-retired/)
* [Mistral: pricing diff Aug 2026](https://pricingsaas.com/companies/mistral/diffs/2026W34), [opt-out](https://help.mistral.ai/en/articles/455207-can-i-opt-out-of-my-input-or-output-data-being-used-for-training)
* [NVIDIA forum: free tier = 40 RPM, not for production](https://forums.developer.nvidia.com/t/is-nim-free/369368), [freellm.net NVIDIA NIM](https://freellm.net/providers/nvidia-nim)
* [OVHcloud AI Endpoints limits](https://docs.ovhcloud.com/pt/guides/public-cloud/ai-machine-learning/ai-endpoints-capabilities), [OVH free AI API](https://www.ovhcloud.com/en/public-cloud/free-ai-api/)
* [Ollama cloud docs](https://docs.ollama.com/cloud)
* [Kluster EOL](https://kluster.ai/blog/end-of-life-announcement)

## 1. Ранжированная таблица «что подключать первым»

Колонка «Сессии» = пригодность для агентов сессий (tools + длинный контекст + vision). «Фон» = заголовки, классификация, перевод, рейтинг контента.

| # | Провайдер | Лимиты free | Ключевые модели | Vision | Карта | Приватность | Сессии | Фон | Вердикт |
|---|-----------|-------------|-----------------|--------|-------|-------------|--------|-----|---------|
| 1 | **Google Gemini (AI Studio)** | не публикуются, ориентир 10-15 RPM, 250-1500 RPD на проект (смотреть aistudio.google.com/rate-limit) | gemini-3.8-flash, 3.6/3.5-flash, 3.5-flash-lite, gemma-4 (контекст до 1M) | да | нет | **free-вход идет в обучение**, люди могут читать; в EEA/UK/CH действуют платные условия | лучший: 1M, tools, vision | да | Подключать первым для vision и длинного контекста. Только не-чувствительный контент |
| 2 | **Groq** | на модель и на организацию: 30 RPM, 1000 RPD, **8000 TPM**, 200 000 TPD (compound: 250 RPD) | openai/gpt-oss-120b, gpt-oss-20b, qwen/qwen3.8-27b (131k, tools, **vision**) | да (qwen3.8-27b) | нет | по умолчанию данные не хранятся, ZDR включается в консоли | ограниченно: 8K TPM не тянет длинный контекст и картинки | отлично | Подключать вторым. Самый быстрый (450+ ток/с), но TPM 8K |
| 3 | **NVIDIA NIM (build.nvidia.com)** | 40 RPM общий на все модели; дневного лимита нет (по одним данным 10 000 RPD) | 100+ моделей: nemotron-3-super-120b-a12b, nemotron-3-ultra-550b, gpt-oss, Llama, Gemma, Mistral; контексты 8K-1M | есть VLM | нет, но **телефон** | пробный сервис, логи сессий «для улучшения без привязки к личности» | да по tools/контексту | отлично | Подключать третьим как большой резерв. **ToS: только прототипы, не прод** |
| 4 | **Cloudflare Workers AI** | 10 000 Neurons/день на аккаунт (общие на все модели), до 300 req/min | gpt-oss-120b/20b, llama-4-scout (vision, 131k), mistral-small-3.1-24b (vision+tools, 128k), qwen3.8-27b (vision), gemma-4-26b | да | нет (на free) | клиентские данные у Cloudflare в обучение не идут (публичная политика, в этом прогоне не перепроверялась) | ограниченно: квота мала | да | Четвертым. OpenAI-совместимый, vision-модели есть |
| 5 | **Mistral (Free/Experiment)** | ~1 req/s, 500K TPM на модель; по новому тарифу $10/мес кредитами (сумма с 7.10 не публикуется) | mistral-medium-3.5, small-4, large-3, ministral, codestral | да (medium/small) | нет, нужен телефон | **Experiment: вход идет в обучение**, opt-out в Admin > Privacy > «Anonymous improvement data» | средне | отлично для перевода/классификации | Пятым. Включить opt-out сразу после регистрации |
| 6 | **OpenRouter `:free`** (идет отдельно) | 20 RPM, 50 RPD (1000 RPD после покупки кредитов на $10) | nemotron-3-super-120b-a12b:free, gemma-4-31b-it:free, ~16 моделей | у части | нет | «free-провайдеры могут логировать промпты» | резерв | резерв | Уже в работе. Пополнение на $10 = платежи, запрещено приказом |
| 7 | **OpenCode Zen free** (идет отдельно) | не публикуются; 13 free-моделей, ротация | Big Pickle, Nemotron, MiMo, LongCat, Step 5 Preview Free, Jev 1.13 Free | у части | нет | free-модели: данные могут идти в обучение | резерв | резерв | Уже в работе |
| 8 | **Ollama Cloud** | сессионный (5 ч) и недельный лимит по GPU-времени, цифры не публикуются | gpt-oss:120b, deepseek, qwen3-coder:480b, kimi-k2, glm, gemma4:31b (контекст 128-262K) | у части | нет | «мы не используем данные для обучения» (docs.ollama.com/cloud) | хорошо для tools и длинного контекста | да | Шестым. Лимит непредсказуем |
| 9 | **SambaNova Cloud** | на модель: 20 RPM, **20 RPD**, 200K TPD | DeepSeek-V3.1/V3.2, Llama-3.3-70B, gpt-oss-120b, gemma-4-31b-it | gemma-4 (препросмотр) | нет | нет SLA по приватности | нет | только редкие задачи | Седьмым: 20 запросов в сутки на модель, 5 моделей = ~100 в сутки |
| 10 | **Z.AI (Zhipu)** | 1 параллельный запрос | glm-4.7-flash (~200K, tools), glm-4.6v-flash (vision) | да | нет | китайская юрисдикция, условия не перепроверены | средне | можно | Опционально. GLM-4.5-Flash выводится из работы |

### Не подключать (причина)

| Провайдер | Почему |
|-----------|--------|
| **Cerebras** | С лета 2026 нет постоянного free: $5 кредитов на 30 дней **только после привязки карты**. Нарушает «без платных». |
| **GitHub Models** | Закрыт полностью 30.07.2026. |
| **Hugging Face Inference Providers** | Бесплатным пользователям кредитов больше нет (PRO получает $2). Только за деньги. |
| **Cohere** | Trial-ключ: 1000 вызовов в месяц, только оценка, коммерческое использование запрещено. |
| **OVHcloud AI Endpoints** | Анонимно 2 RPM на IP на модель (бесполезно). Ключ с 400 RPM требует Public Cloud проект с картой (trial $200 на месяц). |
| **Together, Fireworks, xAI, DeepSeek, Chutes** | По каталогу gravity.fast (4.10.2026) бесплатного тарифа нет. Часть агрегаторов пишет, что у Chutes/Together есть бесплатные модели. Не подтверждено, пропускаем. |
| **Kluster.ai** | Инференс закрыт в июле 2025. |
| **Alibaba Model Studio** | Единовременная квота ~1M токенов на 90 дней, после нее **автоматическое списание**, пока не включить «Free Quota Only». Риск платежа. |
| **ModelScope, SiliconFlow** | Требуют проверку личности (китайское удостоверение/real-name). |
| **LLM7.io, Pollinations, Kilo Code (анонимные)** | Без регистрации, без SLA, неясное происхождение провайдеров и условия. Подходят лишь как аварийный последний резерв для заведомо публичного текста. По умолчанию выключены. |

## 2. Что важно по условиям (публичный трафик, обучение на промптах, коммерция)

* **Google Gemini.** Условия: сервис для разработчиков в профессиональных целях, не для потребителей; нельзя использовать в сервисе, направленном на лиц младше 18 или вероятно доступном им; вам должно быть 18+. На бесплатном уровне контент используется для улучшения продуктов Google, люди могут читать его (отвязанным от ключа). Не отправлять чувствительное. Для EEA/UK/CH применяются платные условия (без обучения). Публичный сайт AutoRig «вероятно доступен несовершеннолетним»: прямой риск нарушения условий. Для фоновых задач на не-пользовательских данных риск мал.
* **Groq.** Free-тариф положен для прототипов и оценки, для прода Groq предлагает Developer. По умолчанию данные не хранятся, ZDR включается в Data Controls. Лимиты считаются на организацию, не на ключ: несколько ключей квоту не увеличивают. Самый сильный лимит для нас: 8000 токенов в минуту.
* **NVIDIA.** API Trial Terms: бесплатный доступ для разработки, не для прода (подтверждено модераторами форума NVIDIA). Телефон при регистрации. Использовать только как резерв.
* **Cloudflare.** Лимиты сбрасываются в 00:00 UTC, при превышении ошибка, а не списание. Нейроны общие на все модели аккаунта: эмбеддинги могут съесть квоту чата.
* **Mistral.** Experiment/Free: вход и выход могут использоваться для обучения, право на opt-out есть (отдельный переключатель для API). Экспериментальные Labs-модели обучаются по умолчанию, отдельно.
* **OpenRouter.** Free-варианты могут быть отозваны без предупреждения, free-провайдеры могут логировать промпты.
* **Ollama Cloud.** В документации: данные для обучения не используются. Коммерческие условия не найдены.

Практическое правило для роутера: каждому запросу ставить метку `sensitivity`:
* `public` (заголовки по публичным данным, переводы UI): любой провайдер;
* `user_content` (рендеры и модели пользователей, чаты): только Groq, Cloudflare, Ollama, NVIDIA (по необходимости); **не** Gemini free, Mistral Experiment, OpenRouter free, Zen free, Z.AI;
* `pii` (почта, платежи): никакого free-провайдера.

## 3. Конфиг для универсального OpenAI-совместимого роутера

Контексты в токенах. `null` = не подтверждено, надо взять из `GET {base_url}/models` при первом прогоне. Лимиты описаны полями `rpm`, `rpd`, `tpm`, `tpd` на указанную область (`scope`). Порядок в массиве = рекомендуемый приоритет.

```json
{
  "version": "2026-10-11",
  "policy": {
    "max_retries_per_provider": 0,
    "on_429": "next_provider",
    "cooldown_on_daily_exhaustion": "until 00:00 UTC (Gemini: 00:00 PT)",
    "sensitivity_routing": {
      "public": "any",
      "user_content": ["groq", "cloudflare", "ollama_cloud", "nvidia"],
      "pii": []
    }
  },
  "providers": [
    {
      "name": "gemini",
      "base_url": "https://generativelanguage.googleapis.com/v1beta/openai/",
      "env_var_for_key": "GEMINI_API_KEY",
      "terms_flags": ["free_tier_trains_on_prompts", "no_apps_for_under_18", "eea_uk_ch_treated_as_paid"],
      "models": [
        {"id": "gemini-3.8-flash", "ctx": 1048576, "tools": true, "vision": true},
        {"id": "gemini-3.5-flash", "ctx": 1048576, "tools": true, "vision": true},
        {"id": "gemini-3.5-flash-lite", "ctx": 1048576, "tools": true, "vision": true},
        {"id": "gemma-4-31b-it", "ctx": null, "tools": null, "vision": true}
      ],
      "limits": {"scope": "project", "rpm": 10, "rpd": 250, "tpm": 250000, "note": "ориентир, реальные цифры в aistudio.google.com/rate-limit; тарифицируется по каждой модели отдельно"}
    },
    {
      "name": "groq",
      "base_url": "https://api.groq.com/openai/v1",
      "env_var_for_key": "GROQ_API_KEY",
      "terms_flags": ["free_for_prototyping", "zdr_available"],
      "models": [
        {"id": "openai/gpt-oss-120b", "ctx": 131072, "tools": true, "vision": false},
        {"id": "qwen/qwen3.8-27b", "ctx": 131042, "tools": true, "vision": true, "max_output": 16384},
        {"id": "openai/gpt-oss-20b", "ctx": 131072, "tools": true, "vision": false}
      ],
      "limits": {"scope": "organization_per_model", "rpm": 30, "rpd": 1000, "tpm": 8000, "tpd": 200000, "note": "TPM 8000: запрос с большой историей или картинкой не пройдет; сжимать контекст до ~6K"}
    },
    {
      "name": "nvidia_nim",
      "base_url": "https://integrate.api.nvidia.com/v1",
      "env_var_for_key": "NVIDIA_API_KEY",
      "terms_flags": ["trial_terms_prototyping_only_not_production", "phone_verification"],
      "models": [
        {"id": "nvidia/nemotron-3-super-120b-a12b", "ctx": null, "tools": true, "vision": false},
        {"id": "openai/gpt-oss-120b", "ctx": null, "tools": true, "vision": false},
        {"id": "openai/gpt-oss-20b", "ctx": null, "tools": true, "vision": false}
      ],
      "limits": {"scope": "account_all_models", "rpm": 40, "rpd": null, "note": "40 RPM общий на все модели; список VLM брать из /v1/models"}
    },
    {
      "name": "cloudflare",
      "base_url": "https://api.cloudflare.com/client/v4/accounts/${CLOUDFLARE_ACCOUNT_ID}/ai/v1",
      "env_var_for_key": "CLOUDFLARE_API_TOKEN",
      "extra_env": ["CLOUDFLARE_ACCOUNT_ID"],
      "terms_flags": ["neurons_shared_across_models"],
      "models": [
        {"id": "@cf/openai/gpt-oss-120b", "ctx": null, "tools": true, "vision": false},
        {"id": "@cf/meta/llama-4-scout-17b-16e-instruct", "ctx": 131000, "tools": true, "vision": true},
        {"id": "@cf/mistralai/mistral-small-3.1-24b-instruct", "ctx": 128000, "tools": true, "vision": true}
      ],
      "limits": {"scope": "account", "neurons_per_day": 10000, "rpm": 300, "note": "сброс в 00:00 UTC; на превышении ошибка"}
    },
    {
      "name": "mistral",
      "base_url": "https://api.mistral.ai/v1",
      "env_var_for_key": "MISTRAL_API_KEY",
      "terms_flags": ["experiment_plan_trains_on_prompts_unless_opt_out", "phone_verification"],
      "models": [
        {"id": "mistral-medium-latest", "ctx": null, "tools": true, "vision": true},
        {"id": "mistral-small-latest", "ctx": null, "tools": true, "vision": true},
        {"id": "mistral-large-latest", "ctx": null, "tools": true, "vision": true}
      ],
      "limits": {"scope": "per_model", "rps": 1, "tpm": 500000, "note": "по новому тарифу бесплатно $10/мес кредитами, сумма не публикуется; перед включением сделать opt-out"}
    },
    {
      "name": "ollama_cloud",
      "base_url": "https://ollama.com/v1",
      "env_var_for_key": "OLLAMA_API_KEY",
      "terms_flags": ["no_training_per_docs", "limits_unpublished"],
      "models": [
        {"id": "gpt-oss:120b", "ctx": 131072, "tools": true, "vision": false},
        {"id": "gemma4:31b", "ctx": null, "tools": null, "vision": true}
      ],
      "limits": {"scope": "account", "session_window_hours": 5, "weekly_window_days": 7, "note": "метрика по GPU-времени, цифр нет; реагировать на 429; base_url подтвердить по документации Ollama"}
    },
    {
      "name": "sambanova",
      "base_url": "https://api.sambanova.ai/v1",
      "env_var_for_key": "SAMBANOVA_API_KEY",
      "terms_flags": ["no_privacy_sla"],
      "models": [
        {"id": "gpt-oss-120b", "ctx": null, "tools": true, "vision": false},
        {"id": "DeepSeek-V3.1", "ctx": null, "tools": true, "vision": false},
        {"id": "Meta-Llama-3.3-70B-Instruct", "ctx": null, "tools": true, "vision": false}
      ],
      "limits": {"scope": "per_model", "rpm": 20, "rpd": 20, "tpd": 200000, "note": "20 запросов в сутки на модель; использовать только для редких тяжелых задач; base_url подтвердить"}
    },
    {
      "name": "openrouter",
      "base_url": "https://openrouter.ai/api/v1",
      "env_var_for_key": "OPENROUTER_API_KEY",
      "terms_flags": ["free_providers_may_log_prompts"],
      "models": [
        {"id": "nvidia/nemotron-3-super-120b-a12b:free", "ctx": null, "tools": true, "vision": false},
        {"id": "google/gemma-4-31b-it:free", "ctx": null, "tools": null, "vision": true}
      ],
      "limits": {"scope": "account", "rpm": 20, "rpd": 50, "note": "уже подключается отдельно; без покупки кредитов 50 запросов в сутки"}
    },
    {
      "name": "opencode_zen",
      "base_url": "https://opencode.ai/zen/v1",
      "env_var_for_key": "OPENCODE_API_KEY",
      "terms_flags": ["free_models_may_train"],
      "models": [],
      "limits": {"note": "уже подключается отдельно; список free-моделей ротируется, брать из GET /zen/v1/models"}
    },
    {
      "name": "zai",
      "base_url": "https://api.z.ai/api/paas/v4",
      "env_var_for_key": "ZAI_API_KEY",
      "enabled_by_default": false,
      "terms_flags": ["china_jurisdiction", "terms_not_verified"],
      "models": [
        {"id": "glm-4.7-flash", "ctx": 200000, "tools": true, "vision": false},
        {"id": "glm-4.6v-flash", "ctx": null, "tools": null, "vision": true}
      ],
      "limits": {"scope": "account", "concurrency": 1}
    }
  ],
  "disabled_do_not_use": ["cerebras", "github_models", "huggingface", "cohere_trial", "ovhcloud", "alibaba_model_studio", "modelscope", "siliconflow", "llm7", "pollinations", "kilo_anonymous"]
}
```

Рекомендованные цепочки по задачам:
* **Фон: заголовки, классификация, перевод, рейтинг:** groq/gpt-oss-20b, nvidia/gpt-oss-120b, mistral-small, cloudflare/gpt-oss-120b, gemini-3.5-flash-lite, ollama. Запросы короткие, 8K TPM на Groq хватает.
* **Агент сессии (tools, длинный контекст):** gemini-3.8-flash (если контент допускает), nvidia/nemotron-3-super, ollama gpt-oss:120b, groq/gpt-oss-120b (только при сжатом контексте).
* **Vision (рендеры):** gemini-3.8-flash, groq/qwen3.8-27b (1-2 малых кадра), cloudflare/llama-4-scout и mistral-small-3.1, mistral-medium.

## 4. Шаги для владельца (ключи кладутся в `/srv/autorig/secrets/free-llm.env` на VPS)

Пароли в чат не присылать. Нужны только API-ключи. Ключи владелец вставляет в чат, главная сессия сама запишет их в файл.

Создать аккаунты (платежная карта не нужна нигде):

1. **Google AI Studio.** Войти с Google-аккаунтом на aistudio.google.com, в меню «Get API key» создать ключ. Переменная `GEMINI_API_KEY`. Условие: тебе 18+. Помни про обучение на free-запросах.
2. **Groq.** Регистрация по email или соцсети на console.groq.com, раздел API Keys. Переменная `GROQ_API_KEY`. В Settings > Data Controls можно включить Zero Data Retention.
3. **NVIDIA.** Регистрация в NVIDIA Developer Program на build.nvidia.com (понадобится подтверждение телефона), в профиле Settings > API Keys. Переменная `NVIDIA_API_KEY`. Условия «для прототипов».
4. **Cloudflare.** Бесплатный аккаунт на dash.cloudflare.com. Нужны два значения: Account ID (на главной странице Workers & Pages, справа) и API Token с правом «Workers AI: Read» (My Profile > API Tokens > Create Token, шаблон Workers AI). Переменные `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`.
5. **Mistral.** console.mistral.ai, регистрация, подтверждение телефона, тариф Experiment (карта не нужна), раздел API Keys. Переменная `MISTRAL_API_KEY`. Сразу после регистрации в Admin > Privacy выключить «Anonymous improvement data» (opt-out от обучения).
6. **Ollama.** Аккаунт на ollama.com, затем ollama.com/settings/keys. Переменная `OLLAMA_API_KEY`.
7. **SambaNova (по желанию).** cloud.sambanova.ai, регистрация без карты, раздел API Keys. Переменная `SAMBANOVA_API_KEY`. Только резерв, 20 запросов в сутки на модель.
8. **Z.AI (по желанию, низкий приоритет).** z.ai, раздел API Keys. Переменная `ZAI_API_KEY`.

НЕ создавать: Cerebras (требует карту), Hugging Face (кредитов нет), Cohere (trial не для коммерции), OVH (нужна карта), Alibaba (автосписание), Together/Fireworks/xAI/DeepSeek (нет бесплатного уровня).

Формат файла (пример структуры, без значений):

```
GEMINI_API_KEY=
GROQ_API_KEY=
NVIDIA_API_KEY=
CLOUDFLARE_API_TOKEN=
CLOUDFLARE_ACCOUNT_ID=
MISTRAL_API_KEY=
OLLAMA_API_KEY=
SAMBANOVA_API_KEY=
ZAI_API_KEY=
```

## 5. Риски

1. **Условия.** Free-тарифы не покупают право отдавать их публичному сайту; NVIDIA прямо запрещает прод, у Gemini ограничение на сервисы, доступные несовершеннолетним. Мера: пользовательские данные направлять только на Groq/Cloudflare/Ollama, остальные для публичных и служебных текстов.
2. **Обучение на промптах.** Gemini free, Mistral Experiment (до opt-out), OpenRouter free, Zen free. Рендеры и модели пользователей туда не отправлять.
3. **Малые квоты.** Groq 8K TPM, SambaNova 20 RPD, OpenRouter 50 RPD, Cloudflare 10K нейронов в день. Нужны счетчики по каждому провайдеру, суточный cooldown и fallback-цепочка. В цепочке `max_retries=0`.
4. **Нестабильность.** За последние три месяца: GitHub Models закрыт, Cerebras перевел free в trial с картой, Groq урезал gpt-oss-safeguard-20b с 30 до 3 RPM, Mistral скрыл сумму кредита, Pro-модели Gemini ушли из free. Имена моделей (3.8, 4, Qwen3.8) менялись быстро: брать список из `/models`, не зашивать.
5. **Одна квота на организацию.** Groq: несколько ключей не помогают, считается на организацию. Создание нескольких аккаунтов для обхода лимитов нарушает условия и не рекомендуется.
6. **Нужна проверка.** Не подтверждены по первичным источникам: контексты NVIDIA/Mistral/SambaNova/Cloudflare-gpt-oss, точный id моделей Cloudflare для Qwen3.8, base_url Ollama и SambaNova, условия Z.AI. Это первое, что надо снять живым прогоном при подключении.
