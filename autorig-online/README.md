# AutoRig Online

Automatic 3D model rigging service. Upload GLB, FBX, or OBJ models and get them rigged with 50+ animations.

## Features

- Upload files or paste links
- Automatic worker selection (least busy)
- Real-time progress tracking
- Google OAuth2 authentication
- Free tier: 3 anonymous + 7 after login (10 total)
- Admin panel for balance management
- Dark/Light themes
- English/Russian localization

## Tech Stack

- **Backend**: Python 3.11+, FastAPI, SQLAlchemy, SQLite
- **Frontend**: Vanilla HTML/CSS/JS
- **Auth**: Google OAuth2
- **Server**: Nginx + Let's Encrypt

## Project Structure

```
autorig-online/
├── backend/
│   ├── main.py          # FastAPI application
│   ├── config.py        # Configuration
│   ├── database.py      # SQLAlchemy models
│   ├── models.py        # Pydantic schemas
│   ├── auth.py          # Google OAuth2
│   ├── workers.py       # Worker integration
│   ├── tasks.py         # Task logic
│   └── requirements.txt
├── static/
│   ├── css/styles.css   # Styles with themes
│   ├── js/
│   │   ├── app.js       # Main app logic
│   │   ├── i18n.js      # Localization
│   │   └── admin.js     # Admin panel
│   ├── i18n/            # Translation files
│   ├── index.html       # Landing page
│   ├── task.html        # Task progress page
│   ├── admin.html       # Admin panel
│   └── robots.txt
├── deploy/
│   ├── autorig.service  # systemd unit
│   └── nginx.conf       # Nginx config
└── README.md
```

## Deployment

### 1. Prerequisites

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install Python and dependencies
sudo apt install python3.11 python3.11-venv python3-pip nginx certbot python3-certbot-nginx -y
```

### 2. Setup Application

```bash
# Create directories
sudo mkdir -p /opt/autorig-online
sudo mkdir -p /var/autorig/uploads

# Copy files
sudo cp -r /root/autorig-online/* /opt/autorig-online/

# Create virtual environment
cd /opt/autorig-online
sudo python3.11 -m venv venv
sudo ./venv/bin/pip install -r backend/requirements.txt

# Set permissions
sudo chown -R www-data:www-data /opt/autorig-online
sudo chown -R www-data:www-data /var/autorig
```

### 3. Configure Environment

Create `/opt/autorig-online/backend/.env`:

```env
APP_URL=https://autorig.online
DEBUG=false
SECRET_KEY=your-very-secret-random-key-here

DATABASE_URL=sqlite+aiosqlite:///./db/autorig.db

GOOGLE_CLIENT_ID=your-google-client-id-here
GOOGLE_CLIENT_SECRET=your-google-client-secret-here
GOOGLE_REDIRECT_URI=https://autorig.online/auth/callback

ADMIN_EMAIL=eschota@gmail.com
UPLOAD_DIR=/var/autorig/uploads
```

### 4. Setup systemd Service

```bash
sudo cp /opt/autorig-online/deploy/autorig.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable autorig
sudo systemctl start autorig
```

### 5. Setup Nginx

```bash
# Copy config
sudo cp /opt/autorig-online/deploy/nginx.conf /etc/nginx/sites-available/autorig.online
sudo ln -s /etc/nginx/sites-available/autorig.online /etc/nginx/sites-enabled/

# Get SSL certificate
sudo certbot --nginx -d autorig.online -d www.autorig.online

# Test and reload
sudo nginx -t
sudo systemctl reload nginx
```

### 6. Setup Upload Cleanup Cron

```bash
# Edit crontab
sudo crontab -e

# Add line to clean uploads older than 24 hours
0 */6 * * * find /var/autorig/uploads -type f -mmin +1440 -delete
0 */6 * * * find /var/autorig/uploads -type d -empty -delete
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `APP_URL` | Public URL of the site | `https://autorig.online` |
| `DEBUG` | Enable debug mode | `false` |
| `SECRET_KEY` | Secret key for sessions | Required |
| `DATABASE_URL` | SQLite database path | `sqlite+aiosqlite:///./db/autorig.db` |
| `GOOGLE_CLIENT_ID` | Google OAuth client ID | Required |
| `GOOGLE_CLIENT_SECRET` | Google OAuth client secret | Required |
| `GOOGLE_REDIRECT_URI` | OAuth callback URL | `{APP_URL}/auth/callback` |
| `ADMIN_EMAIL` | Admin user email | `eschota@gmail.com` |
| `UPLOAD_DIR` | Upload directory | `/var/autorig/uploads` |

## API Endpoints

### Public

- `GET /` - Landing page
- `GET /task?id=X` - Task progress page
- `GET /auth/login` - Google OAuth login
- `GET /auth/callback` - OAuth callback
- `GET /auth/logout` - Logout
- `GET /auth/me` - Current user info
- `POST /api/task/create` - Create conversion task
- `GET /api/task/{id}` - Get task status
- `GET /api/history` - User task history

### Admin (requires `eschota@gmail.com`)

- `GET /admin` - Admin panel
- `GET /api/admin/users` - List users
- `POST /api/admin/user/{id}/balance` - Update balance

## Workers

Configured workers:
- `http://5.129.157.224:5132/api-converter-glb`
- `http://5.129.157.224:5279/api-converter-glb`
- `http://5.129.157.224:5131/api-converter-glb`
- `http://5.129.157.224:5533/api-converter-glb`
- `http://5.129.157.224:5267/api-converter-glb`

## Testing

### Test conversion with curl:

```bash
# Create task with link
curl -X POST https://autorig.online/api/task/create \
  -F "source=link" \
  -F "input_url=http://5.129.157.224:5267/converter/glb/56938dbb-7d33-4966-bb09-64e4d1fd9fbf/56938dbb-7d33-4966-bb09-64e4d1fd9fbf.glb" \
  -F "type=t_pose"

# Check task status
curl https://autorig.online/api/task/{task_id}
```

## Monitoring

```bash
# Check service status
sudo systemctl status autorig

# View logs
sudo journalctl -u autorig -f

# Check nginx logs
sudo tail -f /var/log/nginx/access.log
sudo tail -f /var/log/nginx/error.log
```

## License

Proprietary - All rights reserved

