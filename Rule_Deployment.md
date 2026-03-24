# AutoRig Online — Deployment Rules

## Single source of truth

Исходники — **только git** (например `/root/autorig-online/`). Отдельной «второй копии» кода в репозитории нет. На сервере дерево под `/opt/autorig-online/` — это **деплой**, не второй проект.

## Server layout (актуально под `deploy/autorig.service`)

`WorkingDirectory=/opt/autorig-online/backend` → `STATIC_DIR` = **`/opt/autorig-online/static`**.

```
/opt/autorig-online/
├── venv/
├── backend/
│   └── main.py
└── static/
    ├── developers.html
    ├── index.html
    ├── i18n/
    └── ...
```

## Deploy после `git pull` на сервере

```bash
sudo /root/autorig-online/deploy/sync-prod-from-repo.sh
```

Или вручную:

```bash
sudo cp /root/autorig-online/backend/*.py /opt/autorig-online/backend/
sudo cp /root/autorig-online/static/developers.html /opt/autorig-online/static/
sudo cp /root/autorig-online/static/i18n/*.json /opt/autorig-online/static/i18n/
sudo systemctl restart autorig
```

Страница **`/developers`**: файл **`static/developers.html`**, маршрут в **`main.py`**. Если не скопировать HTML на прод, страница не появится.

## Nginx

Шаблон: `autorig-online/deploy/nginx.conf`. После правок:

```bash
sudo cp /root/autorig-online/deploy/nginx.conf /etc/nginx/sites-available/autorig.online
sudo nginx -t && sudo systemctl reload nginx
```

## Verify

```bash
sudo systemctl status autorig --no-pager
curl -sI https://autorig.online/developers | head -5
```
