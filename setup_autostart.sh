#!/bin/bash

# Скрипт настройки автозапуска AutoRig Online
# Запуск: ./setup_autostart.sh

set -e

echo "🚀 Настройка автозапуска AutoRig Online..."

# Проверяем наличие systemd
if ! command -v systemctl &> /dev/null; then
    echo "❌ SystemD не найден. Этот скрипт предназначен для систем с systemd."
    exit 1
fi

# Проверяем наличие проекта в /opt
if [ ! -d "/opt/autorig-online" ]; then
    echo "❌ Проект не найден в /opt/autorig-online"
    echo "💡 Запустите копирование проекта в /opt командой:"
    echo "sudo cp -r /root/autorig-online/* /opt/autorig-online/"
    exit 1
fi

# Проверяем виртуальное окружение
if [ ! -f "/opt/autorig-online/venv/bin/activate" ]; then
    echo "⚠️  Виртуальное окружение не найдено. Создаю..."
    cd /opt/autorig-online
    sudo python3.11 -m venv venv
    sudo ./venv/bin/pip install -r backend/requirements.txt
fi

# Проверяем наличие main.py
if [ ! -f "/opt/autorig-online/backend/main.py" ]; then
    echo "❌ main.py не найден в /opt/autorig-online/backend/"
    exit 1
fi

# Создаем systemd сервис если его нет
if [ ! -f "/etc/systemd/system/autorig.service" ]; then
    echo "📝 Создаю systemd сервис..."
    sudo tee /etc/systemd/system/autorig.service > /dev/null <<EOF
[Unit]
Description=AutoRig Online Backend
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/opt/autorig-online/backend
ExecStart=/opt/autorig-online/venv/bin/uvicorn main:app --host 127.0.0.1 --port 8000
Restart=always
RestartSec=5

Environment="PYTHONPATH=/opt/autorig-online/backend"
Environment="HOME=/root"
Environment="PATH=/opt/autorig-online/venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

StandardOutput=journal
StandardError=journal
SyslogIdentifier=autorig

[Install]
WantedBy=multi-user.target
EOF
fi

# Перезагружаем systemd
echo "🔄 Перезагружаю systemd..."
sudo systemctl daemon-reload

# Включаем и запускаем сервис
echo "▶️  Включаю автозапуск..."
sudo systemctl enable autorig

echo "▶️  Запускаю сервис..."
sudo systemctl start autorig

# Ждем немного и проверяем статус
sleep 3
STATUS=$(systemctl is-active autorig)

if [ "$STATUS" = "active" ]; then
    echo "✅ Сервис успешно запущен!"
    echo ""
    echo "📊 Статус:"
    systemctl status autorig --no-pager -l
    echo ""
    echo "🔍 Логи:"
    sudo journalctl -u autorig -n 10 --no-pager
    echo ""
    echo "🌐 Приложение доступно на: http://127.0.0.1:8000"
    echo ""
    echo "🛑 Для остановки: sudo systemctl stop autorig"
    echo "🔄 Для перезапуска: sudo systemctl restart autorig"
    echo "📋 Для просмотра логов: sudo journalctl -u autorig -f"
else
    echo "❌ Ошибка запуска сервиса!"
    echo ""
    echo "📋 Детали ошибки:"
    systemctl status autorig --no-pager -l
    echo ""
    echo "🔍 Логи ошибок:"
    sudo journalctl -u autorig -n 20 --no-pager
    exit 1
fi

echo ""
echo "🎉 Настройка завершена! Сервис будет автоматически запускаться при перезагрузке сервера."
