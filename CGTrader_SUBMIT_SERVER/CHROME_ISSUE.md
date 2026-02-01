# Проблема с Chrome для тестирования CGTrader

## Статус
Chrome (snap версия) не запускается через Selenium WebDriver.

## Решение
Для тестирования авторизации CGTrader нужно установить Google Chrome (не snap версию):

```bash
# Скачать и установить Google Chrome
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
dpkg -i google-chrome-stable_current_amd64.deb
apt-get install -f  # Fix dependencies if needed
```

Или использовать Firefox + geckodriver как альтернативу.

## Что уже работает
- ✅ Загрузка файлов
- ✅ Распаковка ZIP
- ✅ Подготовка файлов (extract views, archive subfolders)  
- ✅ Flask API
- ✅ Worker система
- ✅ База данных

После установки правильного Chrome можно будет протестировать авторизацию.
