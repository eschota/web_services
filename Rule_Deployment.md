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

## Одна команда: push в GitHub + деплой в `/opt` + nginx + restart

Из корня репозитория (например `/root`), где есть `autorig-online/`:

```bash
/root/autorig-online/deploy/push-and-deploy.sh
# или с сообщением коммита:
/root/autorig-online/deploy/push-and-deploy.sh "fix: описание"
```

Скрипт делает: `git add -A` → commit (если есть изменения) → `git push` → `rsync` `backend/` (без `db/`) и `static/` (без `tasks/`, `glb_cache/`) в `PROD_ROOT` → копирует `deploy/nginx.conf` → `nginx -t` + reload → `systemctl restart autorig`.

Без обновления nginx: `SKIP_NGINX=1 /root/autorig-online/deploy/push-and-deploy.sh`

### Статика и `rsync --delete`

По умолчанию **`static/` синхронизируется без `--delete`**: новые и изменённые файлы из репозитория попадают на прод, но **файлы, которых нет в git, не стираются**. Иначе любой деплой зеркалит только репозиторий и **удаляет** на сервере всё «лишнее» (логотипы, JPG, QR и т.д., если их забыли закоммитить).

Точное зеркало `static/` как в git (включая удаление лишнего на проде):  
`RSYNC_STATIC_DELETE=1 /root/autorig-online/deploy/push-and-deploy.sh`

**Правило:** всё, что нужно сайту в проде (картинки, иконки, шрифты-файлы), должно лежать в **`autorig-online/static/`** и быть в **git**. Не полагаться на ручную заливку только на сервер.

## Deploy только на сервере (без push)

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

## Автоочистка БД (галерея / мёртвые таски на воркерах)

В **`main.py`** фоновый воркер каждые ~30 с (с блокировкой между процессами uvicorn) чистит: строки без путей к постеру в JSON, и задачи, у которых постер/видео **404** на воркере. Несколько раундов подряд: `GALLERY_UPSTREAM_PURGE_ROUNDS` × `GALLERY_UPSTREAM_PURGE_BATCH` за цикл.

Дополнительно на проде можно включить **timer** (раз в 15 минут тот же код через отдельный процесс):

```bash
sudo cp /root/autorig-online/deploy/autorig-gallery-purge.service /etc/systemd/system/
sudo cp /root/autorig-online/deploy/autorig-gallery-purge.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now autorig-gallery-purge.timer
```

Ручной запуск: `sudo systemctl start autorig-gallery-purge.service` или  
`/opt/autorig-online/venv/bin/python /opt/autorig-online/backend/scripts/run_task_cleanup.py`.
