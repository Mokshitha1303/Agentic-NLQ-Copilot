# Deployment Guide (Free)

This project deploys for free with:

- **Render** (Python FastAPI backend)
- **Netlify** (static frontend and `/api/*` proxy)

## 1) Deploy Backend on Render (Free)

1. Open Render dashboard -> **New** -> **Web Service**.
2. Connect your GitHub repository.
3. Render auto-detects `render.yaml` in this repo.
4. Confirm service settings:
   - `plan`: `free`
   - `buildCommand`: `pip install -e .`
   - `startCommand`: `uvicorn copilot.api:app --host 0.0.0.0 --port $PORT`
   - `healthCheckPath`: `/health`
5. Add secret env var:
   - `OPENAI_API_KEY` (required for agent mode)
6. Click **Create Web Service**.

After deploy, confirm:

- `https://<your-render-url>/health`
- `https://<your-render-url>/docs`

## 2) Update Netlify Proxy

Edit `netlify.toml` and set:

`to = "https://<your-render-url>/:splat"`

Commit this change before Netlify deploy.

## 3) Deploy Frontend on Netlify (Free)

1. Netlify dashboard -> **Add new site** -> **Import from Git**.
2. Select this repository.
3. Build settings:
   - Build command: *(leave empty)*
   - Publish directory: `web`
4. Deploy site.

Netlify will serve the UI and forward `/api/*` calls to Render.

## 4) Verify End-to-End

1. Open Netlify URL.
2. Submit a query in the UI.
3. Confirm response JSON appears.

## 5) Optional CORS Setup

If calling Render API directly from other origins, set:

- `API_CORS_ORIGINS=https://<your-netlify-domain>`

on Render environment variables.

## Notes

- Free tiers can sleep after idle; first request may be slower.
- Keep benchmark calls small on free tier.
