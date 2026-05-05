# Deployment Guide — VO and Visual Extractor Web App

A FastAPI web app that wraps the analyzer pipeline. Designed for a single
VPS (50 GB disk) shared by your team behind HTTPS. No shell scripts —
everything is driven by `systemd` and `uvicorn` directly.

## 1. VPS prerequisites

```bash
sudo apt update
sudo apt install -y python3.10-venv python3-pip ffmpeg nginx certbot python3-certbot-nginx
```

## 2. Place the project

```bash
sudo mkdir -p /opt/vo-visual-extractor
sudo chown $USER:$USER /opt/vo-visual-extractor
# Copy this folder into /opt/vo-visual-extractor (rsync, scp, or git clone)
cd /opt/vo-visual-extractor
```

## 3. Install dependencies

```bash
python3 -m venv .venv
.venv/bin/pip install --upgrade pip
.venv/bin/pip install -r requirements.txt
```

## 4. Configure `.env`

Edit the values in `.env` (already in this repo):

| Variable | Notes |
|---|---|
| `GCP_PROJECT_ID` | Your Google Cloud project |
| `GCS_BUCKET_NAME` | Bucket used for temporary video uploads |
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to service-account JSON (relative to project) |
| `VERTEX_AI_LOCATION` | `us-central1` recommended |
| `APP_USERNAME` / `APP_PASSWORD` | Shared team login. **Must be set in production.** |
| `STORAGE_LIMIT_GB` | Hard cap on disk usage (default 50) |
| `MAX_UPLOAD_GB` | Single-file upload cap (default 5) |
| `HOST` / `PORT` | Bind address (`0.0.0.0:8000` by default) |

## 5. Quick test (foreground)

```bash
.venv/bin/uvicorn server.app:app --host 0.0.0.0 --port 8000
# visit http://VPS_IP:8000 and sign in
```

## 6. Run as a service (systemd)

Edit `vo-visual-extractor.service` — set `User=` and `WorkingDirectory=` — then:

```bash
sudo cp vo-visual-extractor.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now vo-visual-extractor
sudo journalctl -u vo-visual-extractor -f       # live logs
```

## 7. Reverse proxy + TLS (nginx + Let's Encrypt)

```bash
sudo cp nginx.conf.example /etc/nginx/sites-available/vo-visual-extractor
sudo ln -s /etc/nginx/sites-available/vo-visual-extractor /etc/nginx/sites-enabled/
# edit server_name in that file to your domain
sudo nginx -t && sudo systemctl reload nginx
sudo certbot --nginx -d extractor.example.com
```

## 8. Operations

| Action | Command |
|---|---|
| Live logs | `sudo journalctl -u vo-visual-extractor -f` |
| Restart | `sudo systemctl restart vo-visual-extractor` |
| Disk usage | `du -sh /opt/vo-visual-extractor/data` |
| Reset everything | `sudo systemctl stop vo-visual-extractor && rm -rf /opt/vo-visual-extractor/data && sudo systemctl start vo-visual-extractor` |

## 9. Architecture summary

- **Backend** — FastAPI (`server/`), single uvicorn worker, single background thread that processes one analysis at a time (avoids Vertex 429s).
- **Storage** — `data/analyses/<id>/{video.mp4,data.json,exports/…}` plus SQLite metadata (`data/index.db`). Hard cap enforced on every upload.
- **Auth** — shared username + password from `.env`, signed cookie, 30-day session.
- **Existing CLI** — `launcher.py` and `analyzer.py` continue to work unchanged for local debugging if needed.

## 10. Things to know

- The first request after a restart marks any previously running/queued jobs as failed (clean recovery).
- Video files are streamed via HTTP `Range` requests, so seeking works smoothly.
- Per-beat comments are shared across all team members in real time (server-side).
- Continuous play: clicking a beat seeks but does NOT pause at the next beat — the video plays through. The active beat highlights and (optionally) the script auto-scrolls.
