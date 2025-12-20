#!/bin/bash

# Простой скрипт для коммитов с сервера
# Использование: ./commit.sh "commit message"

if [ $# -eq 0 ]; then
    echo "❌ Ошибка: укажите сообщение коммита"
    echo "📝 Пример: ./commit.sh 'feat: add new feature'"
    exit 1
fi

COMMIT_MESSAGE="$1"

echo "🔄 Добавляю изменения..."
git add .

echo "💾 Создаю коммит: '$COMMIT_MESSAGE'"
git commit -m "$COMMIT_MESSAGE"

echo "📤 Отправляю на GitHub..."
git push origin main

echo "✅ Готово! Коммит отправлен на GitHub"
echo "👤 Автор: $(git config user.name) <$(git config user.email)>"
echo "📅 Время: $(date)"
