# 🚀 AutoRig Online

<div align="center">

![AutoRig Logo](https://img.shields.io/badge/AutoRig-Online-blue?style=for-the-badge&logo=blender&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=flat&logo=fastapi&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-003B57?style=flat&logo=sqlite&logoColor=white)

**Автоматический сервис риггинга 3D моделей**

*Загружайте GLB, FBX или OBJ модели и получайте их с риггингом и 50+ анимациями*

[![GitHub Repo](https://img.shields.io/badge/GitHub-Repo-black?style=flat&logo=github)](https://github.com/eschota/web_services)
[![Live Demo](https://img.shields.io/badge/Live-Demo-green?style=flat&logo=web)](https://autorig.online)

</div>

---

## ✨ О проекте

**AutoRig Online** - это инновационный веб-сервис для автоматического риггинга 3D моделей. Просто загрузите вашу модель или укажите ссылку, и система автоматически:

- 🔄 Обработает модель с помощью ИИ
- 🎭 Добавит профессиональный риггинг
- 🎬 Сгенерирует 50+ разнообразных анимаций
- 📦 Предоставит готовый результат для скачивания

## 🌟 Возможности

<table>
  <tr>
    <td align="center">
      <h3>📤 Загрузка</h3>
      <p>Загружайте файлы или вставляйте ссылки<br>Поддержка GLB, FBX, OBJ форматов</p>
    </td>
    <td align="center">
      <h3>🤖 ИИ Обработка</h3>
      <p>Автоматический выбор наименее загруженного воркера<br>Интеллектуальная балансировка нагрузки</p>
    </td>
    <td align="center">
      <h3>📊 Мониторинг</h3>
      <p>Отслеживание прогресса в реальном времени<br>Подробные статусы обработки</p>
    </td>
  </tr>
  <tr>
    <td align="center">
      <h3>🔐 Авторизация</h3>
      <p>Google OAuth2 аутентификация<br>Безопасный вход через Google</p>
    </td>
    <td align="center">
      <h3>🎁 Бесплатный тариф</h3>
      <p>3 анонимные конверсии<br>+7 после входа (всего 10)</p>
    </td>
    <td align="center">
      <h3>⚙️ Админ-панель</h3>
      <p>Управление балансами пользователей<br>Мониторинг системы</p>
    </td>
  </tr>
  <tr>
    <td align="center">
      <h3>🌓 Темы</h3>
      <p>Темная и светлая темы интерфейса<br>Адаптивный дизайн</p>
    </td>
    <td align="center">
      <h3>🌍 Локализация</h3>
      <p>Русский и английский языки<br>Полная поддержка i18n</p>
    </td>
    <td align="center">
      <h3>📱 Адаптивность</h3>
      <p>Работает на всех устройствах<br>Мобильная версия</p>
    </td>
  </tr>
</table>

## 🛠️ Технологии

<div align="center">

### Backend
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-000000?style=for-the-badge&logo=sqlalchemy&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-003B57?style=for-the-badge&logo=sqlite&logoColor=white)

### Frontend
![HTML5](https://img.shields.io/badge/HTML5-E34F26?style=for-the-badge&logo=html5&logoColor=white)
![CSS3](https://img.shields.io/badge/CSS3-1572B6?style=for-the-badge&logo=css3&logoColor=white)
![JavaScript](https://img.shields.io/badge/JavaScript-F7DF1E?style=for-the-badge&logo=javascript&logoColor=white)

### DevOps & Security
![Nginx](https://img.shields.io/badge/Nginx-009639?style=for-the-badge&logo=nginx&logoColor=white)
![Let's Encrypt](https://img.shields.io/badge/Let's_Encrypt-003A70?style=for-the-badge&logo=letsencrypt&logoColor=white)
![Google OAuth](https://img.shields.io/badge/Google_OAuth-4285F4?style=for-the-badge&logo=google&logoColor=white)

</div>

### Архитектура
- **Backend**: Python 3.11+ с FastAPI для высокопроизводительного API
- **Database**: SQLite с SQLAlchemy ORM для надежного хранения данных
- **Frontend**: Vanilla HTML/CSS/JS без тяжелых фреймворков
- **Authentication**: Google OAuth2 для безопасной авторизации
- **Server**: Nginx с SSL сертификатами Let's Encrypt

## 📁 Структура проекта

```
autorig-online/
├── 📂 backend/              # Python FastAPI сервер
│   ├── main.py             # 🚀 Главное приложение FastAPI
│   ├── config.py           # ⚙️ Конфигурация приложения
│   ├── database.py         # 💾 Модели SQLAlchemy
│   ├── models.py           # 📋 Pydantic схемы
│   ├── auth.py             # 🔐 Google OAuth2 аутентификация
│   ├── workers.py          # ⚡ Интеграция с воркерами
│   ├── tasks.py            # 📝 Логика задач
│   └── requirements.txt    # 📦 Python зависимости
├── 📂 static/               # Статические файлы фронтенда
│   ├── css/styles.css      # 🎨 Стили с темами
│   ├── js/
│   │   ├── app.js          # 🎯 Основная логика приложения
│   │   ├── i18n.js         # 🌍 Локализация
│   │   └── admin.js        # 👑 Админ панель
│   ├── i18n/               # Переводы
│   ├── index.html          # 🏠 Главная страница
│   ├── task.html           # 📊 Страница прогресса задач
│   ├── admin.html          # 👑 Админ панель
│   └── robots.txt          # 🤖 SEO для поисковиков
├── 📂 deploy/               # Конфигурация развертывания
│   ├── autorig.service     # 🔧 Systemd unit
│   └── nginx.conf          # 🌐 Nginx конфигурация
├── 📂 db/                   # База данных
├── 📂 uploads/              # Временные загрузки
└── 📄 README.md            # 📖 Документация
```

## 🚀 Быстрый старт

### Для разработчиков

```bash
# 1. Клонируйте репозиторий
git clone https://github.com/eschota/web_services.git
cd web_services/autorig-online

# 2. Создайте виртуальное окружение
python3.11 -m venv venv
source venv/bin/activate  # или venv\Scripts\activate на Windows

# 3. Установите зависимости
pip install -r backend/requirements.txt

# 4. Настройте переменные окружения
cp backend/.env.example backend/.env
# Отредактируйте .env файл с вашими ключами

# 5. Запустите сервер разработки
cd backend
python main.py
```

Откройте [http://localhost:8000](http://localhost:8000) в браузере.

## 🚀 Развертывание

### 📋 Предварительные требования

```bash
# Обновление системы
sudo apt update && sudo apt upgrade -y

# Установка Python и зависимостей
sudo apt install python3.11 python3.11-venv python3-pip nginx certbot python3-certbot-nginx -y
```

### ⚙️ Настройка приложения

```bash
# Создание директорий
sudo mkdir -p /opt/autorig-online
sudo mkdir -p /var/autorig/uploads

# Копирование файлов
sudo cp -r /root/autorig-online/* /opt/autorig-online/

# Обновление уже установленного продакшена после git pull (backend + /developers + i18n)
# sudo /root/autorig-online/deploy/sync-prod-from-repo.sh

# Создание виртуального окружения
cd /opt/autorig-online
sudo python3.11 -m venv venv
sudo ./venv/bin/pip install -r backend/requirements.txt

# Настройка прав доступа
sudo chown -R www-data:www-data /opt/autorig-online
sudo chown -R www-data:www-data /var/autorig
```

### 🔧 Конфигурация окружения

Создайте `/opt/autorig-online/backend/.env`:

```env
# Настройки приложения
APP_URL=https://autorig.online
DEBUG=false
SECRET_KEY=your-very-secret-random-key-here

# База данных
DATABASE_URL=sqlite+aiosqlite:///./db/autorig.db

# Google OAuth2
GOOGLE_CLIENT_ID=your-google-client-id-here
GOOGLE_CLIENT_SECRET=your-google-client-secret-here
GOOGLE_REDIRECT_URI=https://autorig.online/auth/callback

# Администратор
ADMIN_EMAIL=eschota@gmail.com

# Загрузки
UPLOAD_DIR=/var/autorig/uploads
UPLOAD_TTL_HOURS=24
MAX_UPLOAD_SIZE_MB=100
```

### 🔄 Настройка systemd сервиса

```bash
# Копирование конфигурации сервиса
sudo cp /opt/autorig-online/deploy/autorig.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable autorig
sudo systemctl start autorig
```

### 🌐 Настройка Nginx

```bash
# Копирование конфигурации
sudo cp /opt/autorig-online/deploy/nginx.conf /etc/nginx/sites-available/autorig.online
sudo ln -s /etc/nginx/sites-available/autorig.online /etc/nginx/sites-enabled/

# Получение SSL сертификата
sudo certbot --nginx -d autorig.online -d www.autorig.online

# Проверка и перезагрузка
sudo nginx -t
sudo systemctl reload nginx
```

### 🧹 Настройка очистки загрузок

```bash
# Редактирование crontab
sudo crontab -e

# Добавление задачи очистки файлов старше 24 часов
0 */6 * * * find /var/autorig/uploads -type f -mmin +1440 -delete
0 */6 * * * find /var/autorig/uploads -type d -empty -delete
```

## 🔧 Переменные окружения

| Переменная | Описание | По умолчанию |
|------------|----------|--------------|
| `APP_URL` | Публичный URL сайта | `https://autorig.online` |
| `DEBUG` | Включить режим отладки | `false` |
| `SECRET_KEY` | Секретный ключ для сессий | **Обязательно** |
| `DATABASE_URL` | Путь к SQLite базе данных | `sqlite+aiosqlite:///./db/autorig.db` |
| `GOOGLE_CLIENT_ID` | Google OAuth Client ID | **Обязательно** |
| `GOOGLE_CLIENT_SECRET` | Google OAuth Client Secret | **Обязательно** |
| `GOOGLE_REDIRECT_URI` | URL обратного вызова OAuth | `{APP_URL}/auth/callback` |
| `ADMIN_EMAIL` | Email администратора | `eschota@gmail.com` |
| `UPLOAD_DIR` | Директория для загрузок | `/var/autorig/uploads` |
| `UPLOAD_TTL_HOURS` | Время жизни загрузок (часы) | `24` |
| `MAX_UPLOAD_SIZE_MB` | Максимальный размер файла (MB) | `100` |

## 🔌 API Endpoints

### 🌐 Публичные эндпоинты

| Метод | Эндпоинт | Описание |
|-------|----------|----------|
| `GET` | `/` | 🏠 Главная страница |
| `GET` | `/task?id=X` | 📊 Страница прогресса задачи |
| `GET` | `/auth/login` | 🔐 Вход через Google OAuth |
| `GET` | `/auth/callback` | 🔄 Обратный вызов OAuth |
| `GET` | `/auth/logout` | 🚪 Выход из системы |
| `GET` | `/auth/me` | 👤 Информация о текущем пользователе |
| `POST` | `/api/task/create` | ➕ Создание задачи конвертации |
| `GET` | `/api/task/{id}` | 📋 Получение статуса задачи |
| `GET` | `/api/history` | 📚 История задач пользователя |

### 👑 Админ эндпоинты *(требуется `eschota@gmail.com`)*

| Метод | Эндпоинт | Описание |
|-------|----------|----------|
| `GET` | `/admin` | 👑 Админ панель |
| `GET` | `/api/admin/users` | 👥 Список пользователей |
| `POST` | `/api/admin/user/{id}/balance` | 💰 Обновление баланса пользователя |

## ⚡ Воркеры обработки

<div align="center">

### 🖥️ Кластер воркеров

| Статус | URL | Порт |
|--------|-----|------|
| 🟢 Активен | `http://5.129.157.224` | `:5132` |
| 🟢 Активен | `http://5.129.157.224` | `:5279` |
| 🟢 Активен | `http://5.129.157.224` | `:5131` |
| 🟢 Активен | `http://5.129.157.224` | `:5533` |
| 🟢 Активен | `http://5.129.157.224` | `:5267` |

**Все воркеры обслуживают эндпоинт:** `/api-converter-glb`

</div>

## 🧪 Тестирование

### Тест конвертации через curl:

```bash
# Создание задачи с ссылкой
curl -X POST https://autorig.online/api/task/create \
  -F "source=link" \
  -F "input_url=http://5.129.157.224:5267/converter/glb/56938dbb-7d33-4966-bb09-64e4d1fd9fbf/56938dbb-7d33-4966-bb09-64e4d1fd9fbf.glb" \
  -F "type=t_pose"

# Проверка статуса задачи
curl https://autorig.online/api/task/{task_id}
```

### 📤 Тест загрузки файла:

```bash
# Создание задачи с файлом
curl -X POST https://autorig.online/api/task/create \
  -F "source=file" \
  -F "file=@model.glb" \
  -F "type=a_pose"
```

## 📊 Мониторинг

### 🔍 Проверка статуса сервисов

```bash
# Статус основного сервиса
sudo systemctl status autorig

# Логи приложения
sudo journalctl -u autorig -f

# Логи Nginx
sudo tail -f /var/log/nginx/access.log
sudo tail -f /var/log/nginx/error.log
```

### 📈 Метрики системы

```bash
# Загрузка CPU и памяти
top -p $(pgrep -f "python main.py")

# Использование диска
df -h /var/autorig/uploads

# Количество активных задач
curl https://autorig.online/api/admin/stats
```

## 🤝 Вклад в проект

Мы приветствуем вклад в развитие AutoRig Online! Вот как вы можете помочь:

### 📝 Как внести вклад:

1. **Fork** репозиторий
2. Создайте **feature branch** (`git checkout -b feature/amazing-feature`)
3. **Commit** изменения (`git commit -m 'Add amazing feature'`)
4. **Push** в ветку (`git push origin feature/amazing-feature`)
5. Откройте **Pull Request**

### 🐛 Сообщить о баге

Используйте [GitHub Issues](https://github.com/eschota/web_services/issues) для сообщений о багах:

- Опишите проблему детально
- Укажите шаги для воспроизведения
- Добавьте скриншоты если возможно
- Укажите версию браузера и ОС

### 💡 Предложить фичу

Хотите новую функцию? Откройте [GitHub Issue](https://github.com/eschota/web_services/issues) с меткой `enhancement`:

- Опишите желаемую функциональность
- Объясните зачем она нужна
- Приведите примеры использования

## 📜 Лицензия

**Proprietary Software** - Все права защищены © 2024

Этот проект является проприетарным ПО. Использование только с разрешения владельца.

---

<div align="center">

**Сделано с ❤️ для 3D артистов и разработчиков**

[![GitHub](https://img.shields.io/badge/GitHub-@eschota-black?style=flat&logo=github)](https://github.com/eschota)
[![Website](https://img.shields.io/badge/Website-autorig.online-blue?style=flat&logo=web)](https://autorig.online)

*Если проект оказался полезным, поставьте ⭐ репозиторию!*

</div>


## Telegram Web App Integration

### Setup Bot (one-time)
1. Open @BotFather in Telegram
2. `/newbot` → create bot or use existing
3. `/mybots` → select your bot → **Bot Settings** → **Menu Button** → **Configure menu button**
4. Set URL: `https://autorig.online`
5. Copy bot token and add to `.env`:
   ```
   TELEGRAM_BOT_TOKEN=your_token_here
   TELEGRAM_BOT_USERNAME=YourBotName
   ```
6. Restart service: `systemctl restart autorig`

### How it works
- Users open bot → click Menu button → opens autorig.online as Web App
- Web App automatically expands and applies Telegram theme
- Share button appears when task is complete
- Back button navigates history or closes Web App
- Haptic feedback on task completion/error

### Deep links
- Open specific task: `https://t.me/YourBotName/app?startapp=task_TASKID`
- Main page: `https://t.me/YourBotName/app`
