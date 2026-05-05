# Deployment Guide — Sentiment Platform (Completely Free, No Credit Card)

This guide covers three free deployment options: Railway (recommended), Render, or Cloud Run.

---

## Option 1: Railway.app (✅ RECOMMENDED - Easiest, Completely Free)

**Why Railway?** No credit card required, free tier includes generous monthly credits, deploy with one click.

### Step 1: Prepare your code
- You already pushed to GitHub ✅
- Make sure `docker-compose.prod.yml` exists (it does) ✅

### Step 2: Deploy to Railway
1. Go to https://railway.app
2. Click **Create New Project** → **Deploy from GitHub repo**
3. Connect your GitHub account and select `social-sentiment-analytics-platform`
4. Railway auto-detects `docker-compose.prod.yml` and deploys all services
5. Wait 3-5 minutes for build/deploy to complete
6. Click the **dashboard** service → **View** to open the app

### Step 3: Verify
- You should see the Streamlit dashboard
- It shows "No events yet" (normal on first load)
- Check the dashboard logs if there are errors (Railway dashboard → Logs)

---

## Option 2: Render.com (Free Tier Alternative)

**Why Render?** Also free, supports Docker Compose, simple deployment.

### Step 1: Deploy to Render
1. Go to https://render.com
2. Sign up (GitHub auth available)
3. Go to **Dashboard** → **New +** → **Web Service**
4. Select **Build and deploy from Git**
5. Authorize GitHub and select your repo
6. Set **Build command**: `docker-compose -f docker-compose.prod.yml build`
7. Set **Start command**: `docker-compose -f docker-compose.prod.yml up`
8. Click **Create Web Service**
9. Wait for deploy (5-10 minutes)

### Step 2: Verify
- Once deployed, click the service URL
- Streamlit dashboard should load
- Check logs if issues arise

---

## Option 3: Cloud Run (Requires Free Google Cloud Account)

**Why Cloud Run?** Managed, auto-scales, pay only if exceeded free tier.

**Note:** Requires phone verification (for free trial), but no automatic charges.

### Step 1: Set up Google Cloud
```bash
gcloud auth login
gcloud config set project YOUR_GCP_PROJECT_ID
gcloud services enable run.googleapis.com artifactregistry.googleapis.com
```

### Step 2: Deploy
```bash
# Build image
docker build -t gcr.io/YOUR_GCP_PROJECT_ID/sentiment-dashboard:latest -f Dockerfile .

# Push to Container Registry
docker push gcr.io/YOUR_GCP_PROJECT_ID/sentiment-dashboard:latest

# Deploy to Cloud Run (dashboard only, Kafka separate)
gcloud run deploy sentiment-dashboard \
  --image gcr.io/YOUR_GCP_PROJECT_ID/sentiment-dashboard:latest \
  --platform managed --region us-central1 --allow-unauthenticated \
  --memory 1Gi --cpu 1
```

**But wait:** This deploys **only the dashboard**. For Kafka + services, use Railway or Render (which support full docker-compose).

---

## Local Testing Before Deploy

To test locally before pushing to cloud:

```bash
# Use production compose
docker compose -f docker-compose.prod.yml down -v
docker compose -f docker-compose.prod.yml up --build

# Open http://localhost:8501 in browser
# Should show dashboard (may take 30s to populate events)
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| "No events yet" persists | Wait 1-2 min for producers to send data, then refresh |
| Kafka connection error | Check service logs (Railway/Render dashboard → Logs) |
| Dashboard crashes | Increase memory allocation in platform settings |
| Port 8501 in use locally | Use `docker compose -f docker-compose.prod.yml up -d` |

---

**👉 Recommended: Start with Railway.app (fastest, most free resources).**
