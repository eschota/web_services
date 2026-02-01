Ты должен написать АПИ эндпоинт вебсервер на питоне, который принимает input_url
на /api-submit-cgtrader
этот домен и сервер на котором мы находися - https://autorig.onilne/
подними вебсервер отдельный на фласк для работы, посмотри настройки нгинкс, чтобы правильно получить доменное имя и подбери нужный порт 3701
Твоя задача сделать очередь процессов сабмита, которая формируется принятием на АПИ Пост запроса вида 
{
input_url : string - обязательная проверка на .zip формат, иначе отказ
}
в ответ сервер 
выдает
{
task_id; GUID
}
сделай необходимые дополнительные /api-submit эндпоинты для проверки статусов текущих заданий выполняемых и т п.
у нас неконкурентный 1 поточный конвеер для загрузки и атрибутирования файлов на 3д сток CGtrader.com

Твоя основная задача написать скрипт, который
1. Авторизуется на сайте cgtrader.com
Используй itisai3d@gmail.com Zaebaliuzhe55

NoDeadLine Niak_ris, [04.01.2026 16:21]
3. сопровождай все операции сообщениями в телеграм бот
TELEGRAM_BOT_TOKEN=8222979873:AAEpUHrwYm32GDVb_GHQ-58m-vlf9jffX-g
в 
-1003555866288  supergroup  AutoRig Online  ✅ Да

в том числе операция старта сервиса, приема новой задачи, сделай чтобы была кнопка у бота, нажатие которой выдает статус текущей задачи.

NoDeadLine Niak_ris, [04.01.2026 16:23]
Сначала разберись с авторизацией, потом проанализируй работу их сайта на странице https://www.cgtrader.com/profile/upload/batch
используй инструкцию.
Batch uploads have never been easier!
Hi there first time user! This is how our batch upload process works. Do not forget to pay special attention to your file structure.

Drag and drop your batch folder
Drag and drop your batch folder
Preview the upload structure
Preview the upload structure
Add info and edit each model
Add info and edit each model
Publish and start selling
Publish and start selling
Drag & Drop your batch folder or BrowseФайл не выбран
Uploading guidelines
Models must be placed into their respective folders. Please follow this structure:

guidelines diagram
A single model and all of its related files (format files, textures, preview images) should be in a dedicated folder.

Requirements and recommendations:
all models should have preview images and 3D model format files
all textures must be archived – otherwise they will show up as preview images
a single file size cannot exceed 5.5 GB
archive large files for a smoother upload experience
use the Latin alphabet for file names
uploaded models show up as drafts – you can come back and edit or publish any model whenever you want
Please note that the Batch uploader only works with Webkit-based browsers. 

5. твоя задача загрузить на эту страницу распакованный zip, из которого сформирован URL данной задачи, то есть ты должен загрузить содержимое на сайт и заполнить атрибуты этого файла, после чего отправить её на продажу,
6. метаданные, используй токен Bearer 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
для OpenAI, Распознавай изображение
057530c6-94bf-4b7d-b9c0-21b061398a30.zip\057530c6-94bf-4b7d-b9c0-21b061398a30_100k\057530c6-94bf-4b7d-b9c0-21b061398a30_100k\057530c6-94bf-4b7d-b9c0-21b061398a30_Unity_HDRP_Render_1_view.jpg
которое находится в каждом архиве в подпапке _100k и содержит в себе Unity_HDRP_Render_1_view.jpg, отправляй его и распознавай все метаданные которые необходимы для заполнения этой страницы ```
Model Upload
Batch Upload
New Tutorial
Visit Helpcenter
Upload files
Drag & Drop files here orФайл не выбран
Uploading guidelines 
You can upload your model as an archive (.zip or .rar preferred) or as separate files (.obj, .fbx, .stl, etc.). Add previews in .png or .jpeg file formats.
Simply drag & drop your model folder on this page to upload.

guidelines schema
Requirements and recommendations
add wireframe previews
all textures must be archived – otherwise they will show up as preview images
archive large files for a smoother upload experience
archive your model in a flat file structure
single file size limit: 5.5 GB
use the Latin alphabet for file names
Файл не выбран
Files
Archived files
uploads_files_6662657_all_formats.zip
Please choose file format
glTF (.gltf, .glb)
Version
Please choose file format
PNG (.png)
Version
Please choose file format
EXR (.exr)
Version
Please choose file format
Alembic (.abc)
Version

Native
Please choose file format
Blender (.blend)
Version
Renderer
Version

Native
Please choose file format
Cinema 4D (.c4d)
Version
Renderer
Version
Please choose file format
Autodesk FBX (.fbx)
Version

Native
Please choose file format
Autodesk Maya (.ma, .mb)
Version
Renderer
Version

Native
Please choose file format
Autodesk 3ds Max (.max)
Version
Renderer
Version
Please choose file format
OBJ (.obj, .mtl)
Version
Please choose file format
Stereolithography (.stl)
Version
Please choose file format
Marmoset Toolbag (.tbscene, .tbmat)
Version
Please choose file format
JPG (.jpg)
Version

Native
Please choose file format
Unity 3D (.unitypackage, .prefab)
Version
Renderer
Version
Please choose file format
Python Script (.py, .pyc)
Version
Please choose file format
Maya Mel Script (.mel)
Version
Please choose file format
3ds Max macroScript (.ms)
Version
Please choose file format
Autodesk FBX (.fbx)
Version
Please choose file format
Stereolithography (.stl)
Version
Please choose file format
OBJ (.obj, .mtl)
Version
Please choose file format
Alembic (.abc)
Version

Native
Please choose file format
Blender (.blend)
Version
Renderer
Version

Native
Please choose file format
Cinema 4D (.c4d)
Version
Renderer
Version

Native
Please choose file format
Autodesk Maya (.ma, .mb)
Version
Renderer
Version

Native
Please choose file format
Autodesk 3ds Max (.max)
Version
Renderer
Version

Native
Please choose file format
Unity 3D (.unitypackage, .prefab)
Version
Renderer
Version
Please choose file format
Textures
Version
Please choose file format
Marmoset Toolbag (.tbscene, .tbmat)
Version
+ Add file info
Preview images
Please upload at least one preview image.
Embeds
Youtube logo+ Add Youtube preview
Vimeo logo+ Add Vimeo preview
Marmoset logo+ Add Marmoset preview
Details
Title *
low-poly 3D model
Description *

Tags *
characterman
Recommended tags:clothingwomanfantasygamefashionarthumancartoon
Technical details
AI generated content
Unwrapped UVs
Geometry
Non-overlapping
Polygon mesh
Polygons
100000
Vertices
100000
Category and subcategory
Category *
Character
Subcategory *
Man
Pricing and license
Price *
$
37
Share for free
License *
Royalty free
Publishing score: 6.3
Please note: Uploading the content you do not own is a serious breach of our Terms and Conditions and might result in a listing removal and account suspension.

Keep the same settings for further model uploads
```



Во время составления любого плана по этому файлу дописывай и изменяй его, сделай нормальную верстку и дописывай новые данные которые мо получаем во время разработки и планирования.

---

## Архитектурные решения (обновлено 04.01.2026)

### Ограничения памяти (2GB RAM)
- Headless Chrome с оптимизированными флагами: ~300-500MB
- Flask + Worker: ~50-100MB  
- Остаётся для autorig-online: ~1.2-1.5GB

### Chrome Memory Optimization
```python
chrome_options.add_argument('--headless=new')
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')
chrome_options.add_argument('--disable-extensions')
chrome_options.add_argument('--js-flags=--max-old-space-size=256')
chrome_options.add_argument('--single-process')
```

### Персистентность задач (SQLite)
```sql
CREATE TABLE tasks (
    id TEXT PRIMARY KEY,              -- UUID
    input_url TEXT NOT NULL,
    status TEXT DEFAULT 'created',    -- created/downloading/extracting/analyzing/uploading/filling_form/publishing/done/error
    step TEXT DEFAULT NULL,           -- Текущий checkpoint
    error_message TEXT,
    download_path TEXT,               -- Путь к скачанному ZIP
    extract_path TEXT,                -- Путь к распакованной папке
    metadata_json TEXT,               -- Сгенерированные метаданные
    cgtrader_draft_id TEXT,           -- ID черновика на CGTrader
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    attempts INTEGER DEFAULT 0,
    max_attempts INTEGER DEFAULT 3
);
```

### Recovery при перезапуске
1. При старте: найти все tasks с status NOT IN ('done', 'error')
2. Продолжить с последнего checkpoint
3. Если файлы удалены — перескачать с input_url

### Структура файлов для CGTrader Batch Upload

**Входящий ZIP:**
```
input.zip/
├── model_uuid/
│   ├── model_uuid_100k/
│   │   ├── model.fbx
│   │   ├── model.obj
│   │   ├── textures/
│   │   │   └── *.png
│   │   ├── model_Unity_HDRP_Render_1_view.jpg  ← превью
│   │   └── model_Unity_HDRP_Render_2_view.jpg  ← превью
│   └── model_uuid_50k/
│       ├── model.fbx
│       └── model_view.jpg  ← превью
```

**После обработки (готово для batch upload):**
```
prepared_folder/
├── model_Unity_HDRP_Render_1_view.jpg    ← превью в корень
├── model_Unity_HDRP_Render_2_view.jpg    ← превью в корень
├── model_view.jpg                         ← превью в корень
├── model_uuid_100k.zip                    ← архив подпапки
└── model_uuid_50k.zip                     ← архив подпапки
```

**Алгоритм обработки:**
1. Распаковать входящий ZIP
2. Рекурсивно найти все файлы с `_view` в имени (превью изображения)
3. Скопировать превью в корень подготовленной папки
4. Каждую подпапку (с моделями и текстурами) заархивировать в отдельный .zip
5. Загрузить подготовленную папку через CGTrader batch upload

### Логирование ошибок в Telegram
Формат критических ошибок:
```
🔴 CGTRADER ERROR
Task: {task_id}
Step: {current_step}
Attempt: {attempt}/{max_attempts}
URL: {input_url}
Error: {error_type}
{stack_trace[:1000]}
```

### Telegram команды
- `/status` — статус очереди
- `/task {id}` — детали задачи
- Inline button "🔄 Retry" для failed задач

### API Endpoints
- `POST /api-submit-cgtrader` — создать задачу
- `GET /api-submit-cgtrader/status/{task_id}` — статус задачи
- `GET /api-submit-cgtrader/queue` — очередь
- `POST /api-submit-cgtrader/retry/{task_id}` — повторить failed задачу

### Структура проекта
```
/root/CGTrader_SUBMIT_SERVER/
├── app.py                    # Flask API + worker thread
├── worker.py                 # Однопоточный обработчик очереди
├── file_preparer.py          # Подготовка файлов для batch upload
├── cgtrader_automation.py    # Браузерная автоматизация
├── telegram_notifier.py      # Уведомления + логирование ошибок
├── metadata_extractor.py     # OpenAI Vision API
├── database.py               # SQLite + управление задачами
├── config.py                 # Конфигурация
├── requirements.txt
├── deploy/
│   ├── cgtrader_submit.service
│   └── nginx_location.conf
├── db/
│   └── cgtrader.db
└── MAIN_TASK.md
```