# AutoRig Online — Deployment Rules

## Server Directory Structure

```
/opt/autorig-online/
├── venv/                    # Python virtual environment
└── autorig-online/          # ← Application working directory
    ├── backend/
    │   └── main.py          # FastAPI application
    └── static/              # ← DEPLOY STATIC FILES HERE!
        ├── index.html
        ├── task.html
        ├── css/
        ├── js/
        └── i18n/
            ├── en.json
            └── ru.json
```

## Important!

FastAPI reads static files from:
```
/opt/autorig-online/autorig-online/static/
```

**NOT from** `/opt/autorig-online/static/` (this directory also exists but is NOT used).

## Deployment Commands

### Static files (HTML, CSS, JS, JSON):
```bash
# Copy from local development directory
sudo cp /root/autorig-online/static/FILE /opt/autorig-online/autorig-online/static/FILE

# Example for task.html and localizations:
sudo cp /root/autorig-online/static/task.html /opt/autorig-online/autorig-online/static/
sudo cp /root/autorig-online/static/i18n/*.json /opt/autorig-online/autorig-online/static/i18n/
```

Static files apply **instantly** — no restart required.

### Backend (Python code):
```bash
# Copy backend files
sudo cp /root/autorig-online/backend/*.py /opt/autorig-online/autorig-online/backend/

# Restart service
sudo systemctl restart autorig
```

### Check service status:
```bash
sudo systemctl status autorig --no-pager
```

## Verify Deployment

```bash
# Check that file contains changes
grep "search_text" /opt/autorig-online/autorig-online/static/task.html

# Check that server returns updated version
curl -s "https://autorig.online/task?id=test" | grep "search_text"
```
