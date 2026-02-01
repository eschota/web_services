# Заметки для развертывания на новом сервере

## Сохраненные данные аутентификации

Cookies сохранены в: `db/cgtrader_cookies_manual.json`
CSRF токен можно установить через env: `CGTRADER_CSRF_TOKEN`

## Установка на новом сервере

1. Распаковать архив:
```bash
unzip CGTrader_SUBMIT_SERVER.zip
cd CGTrader_SUBMIT_SERVER
```

2. Создать виртуальное окружение:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

3. Настроить конфигурацию (если нужно):
```bash
# Опционально: создать .env файл
cp .env.example .env  # если есть
# или установить переменные окружения
```

4. Проверить cookies:
```bash
# Cookies уже сохранены в db/cgtrader_cookies_manual.json
# Проверить можно тестом:
python3 test_auth_with_cookies.py
```

5. Запустить:
```bash
python3 app.py
# или через systemd service (см. deploy/cgtrader_submit.service)
```

## Важные файлы

- `config.py` - конфигурация
- `db/cgtrader_cookies_manual.json` - сохраненные cookies
- `proxy_manager.py` - автоматический менеджер прокси
- `cgtrader_http.py` - HTTP клиент для CGTrader
- `worker.py` - обработчик задач
- `app.py` - Flask API сервер

## Тестирование

Перед запуском в продакшн:
```bash
# Тест авторизации с cookies
python3 test_auth_with_cookies.py

# Тест прокси-менеджера (опционально)
python3 test_proxy_manager.py
```

## Примечания

- Автоматический прокси-менеджер включен по умолчанию (ENABLE_AUTO_PROXY=true)
- Cookies уже сохранены и будут использоваться автоматически
- CSRF токен можно задать через env переменную если нужно
