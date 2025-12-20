# 🔧 Настройка для разработчиков

## 🚀 Быстрый старт

### 1. Настройка Git
```bash
git config --global user.name "Ваше Имя"
git config --global user.email "ваш-email@example.com"
```

### 2. Доступ к репозиторию

#### Вариант A: SSH (рекомендуется)
```bash
# Создать SSH ключ
ssh-keygen -t rsa -b 4096 -C "ваш-email@example.com"

# Скопировать публичный ключ
cat ~/.ssh/id_rsa.pub

# Добавить ключ в GitHub: Settings → SSH and GPG keys → New SSH key

# Клонировать репозиторий
git clone git@github.com:eschota/web_services.git
```

#### Вариант B: HTTPS с токеном
```bash
# Создать Personal Access Token на GitHub
# Settings → Developer settings → Personal access tokens → Generate new token
# Выбрать scopes: repo, workflow

# Клонировать репозиторий
git clone https://github.com/eschota/web_services.git

# При push использовать токен вместо пароля
```

### 3. Настройка проекта

```bash
cd web_services/autorig-online
python3.11 -m venv venv
source venv/bin/activate
pip install -r backend/requirements.txt
cp backend/.env.example backend/.env
# Отредактировать .env файл
```

### 4. Запуск разработки
```bash
cd backend
python main.py
# Открыть http://localhost:8000
```

## 📝 Правила разработки

- Всегда создавайте feature branches: `git checkout -b feature/nazvanie-fichi`
- Пишем осмысленные commit messages
- Перед push делаем `git pull --rebase`
- Не коммитим секреты и большие файлы

## 🔐 Доступ

Для получения доступа к репозиторию:
1. Дайте знать администратору проекта
2. Он добавит вас как collaborator в GitHub
3. Или поделится актуальным SSH ключом

## 📞 Контакты

- **Администратор**: Константин Ермолаев
- **Email**: eschota@gmail.com

---

*Последнее обновление: $(date)*
