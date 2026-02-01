# Инструкция по экспорту cookies из браузера

## Способ 1: Chrome/Edge (через DevTools)

1. Откройте CGTrader.com в браузере и войдите вручную
2. Нажмите F12 (или ПКМ -> Inspect) чтобы открыть DevTools
3. Перейдите на вкладку **Application** (или **Storage** в Firefox)
4. В левой панели найдите **Cookies** -> `https://www.cgtrader.com`
5. Вы увидите список всех cookies
6. Скопируйте значения важных cookies:
   - `_session_id` (или `session`)
   - `remember_token` (если есть)
   - Другие cookies связанные с авторизацией

### Экспорт в JSON формате (Chrome Extension)

Установите расширение "Cookie-Editor" или "Get cookies.txt LOCALLY":
1. После установки, откройте CGTrader.com
2. Нажмите на иконку расширения
3. Выберите "Export" -> "JSON"
4. Сохраните файл как `db/cgtrader_cookies_manual.json`

## Способ 2: Ручное создание JSON файла

Создайте файл `db/cgtrader_cookies_manual.json`:

```json
{
  "_session_id": "ваш_session_id_здесь",
  "remember_token": "ваш_token_здесь"
}
```

Или в расширенном формате:

```json
[
  {
    "name": "_session_id",
    "value": "ваш_session_id_здесь",
    "domain": ".cgtrader.com",
    "path": "/",
    "secure": true,
    "httpOnly": true
  },
  {
    "name": "remember_token",
    "value": "ваш_token_здесь",
    "domain": ".cgtrader.com",
    "path": "/",
    "secure": true,
    "httpOnly": true
  }
]
```

## Способ 3: Через переменные окружения

Установите переменную окружения:

```bash
export CGTRADER_SESSION_COOKIE="ваш_session_id_здесь"
```

Или для JSON формата:

```bash
export CGTRADER_SESSION_COOKIE='{"_session_id": "value", "remember_token": "value"}'
```

## Использование

После экспорта cookies:

1. Убедитесь, что файл `db/cgtrader_cookies_manual.json` создан (или переменная окружения задана)
2. Запустите приложение - оно автоматически загрузит cookies
3. Проверка авторизации будет выполнена автоматически
4. Если cookies валидны, автоматический login будет пропущен

## Проверка cookies

Для проверки, что cookies работают, можно использовать тестовый скрипт:

```python
from cgtrader_http import CGTraderHTTPClient

client = CGTraderHTTPClient()
if client.is_logged_in():
    print("✅ Cookies valid!")
else:
    print("❌ Cookies invalid or expired")
```
