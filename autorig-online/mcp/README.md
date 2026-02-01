# RenderFin MCP Server

MCP-сервер для генерации изображений через [RenderFin](https://renderfin.com) API.

## Установка

```bash
uv sync
```

Для запуска тестов:
```bash
uv sync --all-extras
```

## Использование

### Запуск сервера

```bash
uv run renderfin-mcp
```

### Конфигурация для Cursor/Claude Desktop

Добавьте в конфигурацию MCP клиента:

```json
{
  "mcpServers": {
    "renderfin": {
      "command": "uv",
      "args": ["--directory", "/path/to/renderfin-mcp", "run", "renderfin-mcp"]
    }
  }
}
```

## Инструменты

### `generate_image` (синхронный)

Генерирует изображение и ожидает завершения (блокирующий вызов).

**Параметры:**

| Параметр | Тип | Обязательный | Описание |
|----------|-----|--------------|----------|
| `prompt` | string | да | Описание изображения |
| `output_file` | string | да | Путь для сохранения PNG |
| `aspect_ratio` | float | нет | Соотношение сторон (по умолчанию 1.0) |

**Возвращает:**
- `output_url` — URL изображения на renderfin.com
- `local_path` — абсолютный путь к сохранённому файлу

---

### `schedule_render` (fire-and-forget)

Ставит задачу в очередь и **мгновенно возвращает ответ**. Генерация происходит в фоне, файл появится на диске когда будет готов.

**Параметры:**

| Параметр | Тип | Обязательный | Описание |
|----------|-----|--------------|----------|
| `prompt` | string | да | Описание изображения |
| `absolute_output_path` | string | да | **Абсолютный** путь для сохранения PNG (например `C:/images/out.png` или `/home/user/img.png`) |
| `aspect_ratio` | float | нет | Соотношение сторон (по умолчанию 1.0) |

**Важно:** Путь должен быть абсолютным (начинаться с буквы диска `C:/` на Windows или `/` на Linux/Mac).

**Возвращает:**
- `status` — `"scheduled"` или `"error"`
- `task_id` — идентификатор задачи
- `message` — `"Task accepted. Image will be saved to: ..."`
- `output_path` — путь куда будет сохранён файл

**Пример:**
```json
{
  "prompt": "A beautiful sunset over mountains",
  "absolute_output_path": "R:/images/sunset.png",
  "aspect_ratio": 1.777
}
```

**Ответ:**
```json
{
  "status": "scheduled",
  "task_id": "abc12345",
  "message": "Task accepted. Image will be saved to: R:/images/sunset.png",
  "output_path": "R:/images/sunset.png"
}
```

---

## Примеры aspect_ratio

- `1.0` — квадрат (1:1)
- `1.777` — горизонтальный (16:9)
- `0.5625` — вертикальный (9:16)

## Архитектура

```
MCP Client --> schedule_render --> JSON Queue --> Async Worker --> RenderFin API
                    |                                  |
                    v                                  v
              "Task accepted"                    Save PNG to disk
```

- **JSON Queue** (`./vars/tasks.json`) — файловая очередь задач с file locking
- **Async Worker** — работает в фоне пока MCP сервер активен, опрашивает очередь каждые 5 секунд

## Логирование

Логи записываются в `./vars/logs/renderfin.log`:
- `REQUEST` — входящие запросы
- `QUEUED` — задача добавлена в очередь
- `SUCCESS` — успешная генерация
- `ERROR` — ошибка
- `TASK_START` — worker начал обработку задачи

## Тестирование

```bash
uv run pytest tests/ -v
```

## Структура проекта

```
src/renderfin_mcp/
├── __init__.py
├── server.py          # MCP server + generate_image + schedule_render
├── task_queue.py      # JSON file queue с filelock
├── async_worker.py    # Background worker (asyncio.create_task)
└── logger.py          # Logging

tests/
├── test_task_queue.py   # TDD тесты для очереди
├── test_worker.py       # TDD тесты для worker
└── test_integration.py  # E2E тесты
```
