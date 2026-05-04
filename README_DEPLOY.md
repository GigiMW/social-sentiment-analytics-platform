Deployment guide — Streamlit dashboard

This file explains two deployment paths: quick demo on Streamlit Community Cloud and production container deployment (Google Cloud Run / Render / Fly).

1) Streamlit Community Cloud (quick demo)
- Push to GitHub (you already did).
- Ensure `requirements.txt` at repo root contains minimal packages needed by the dashboard (streamlit, plotly, kafka-python, pandas).
- In Streamlit Cloud: New app → connect GitHub → select `dashboard/app.py`.
- Add Secrets (Manage app → Secrets): `KAFKA_BOOTSTRAP_SERVERS`, `NEWS_API_KEY`, `YOUTUBE_API_KEY`, `HF_TOKEN`.
- Deploy and open the app URL.

Notes:
- Streamlit Cloud cannot reach a local Docker Kafka cluster. Use a managed Kafka or host the dashboard in the same network as Kafka (Cloud Run + VPC connector or host on same cloud provider).
- Keep heavy model deps out of the Streamlit container; run `nlp_service` separately (Cloud Run / VM) and use Kafka to stream enriched events.

2) Production (recommended): Docker → Google Cloud Run (example)
Prereqs:
- Google Cloud project, `gcloud` CLI authenticated
- Docker Hub account (or use GCR/Artifact Registry)

Build & push image (Docker Hub example):
```bash
docker build -t <dockerhub-user>/sentiment-dashboard:latest .
docker login
docker push <dockerhub-user>/sentiment-dashboard:latest
```

Deploy to Cloud Run:
```bash
gcloud auth login
gcloud config set project YOUR_GCP_PROJECT_ID
gcloud run deploy sentiment-dashboard \
  --image docker.io/<dockerhub-user>/sentiment-dashboard:latest \
  --platform managed --region us-central1 --allow-unauthenticated \
  --memory 1Gi --cpu 1
```

Set environment variables in Cloud Run Console (Variables & Secrets) or using Secret Manager.

Networking (if Kafka is private): create a Serverless VPC connector and add `--vpc-connector MY_CONNECTOR` to `gcloud run deploy`.

3) Render / Fly / Railway
- All support Docker images or direct GitHub build + environment secrets.
- Use the same Docker image or point the service to the repo and set the run command to:
  `streamlit run dashboard/app.py --server.port 8501 --server.address 0.0.0.0`

Troubleshooting:
- If `ModuleNotFoundError` occurs on Streamlit Cloud, add the missing package to the root `requirements.txt` and redeploy.
- If models are slow to load, pre-download them in your image build or split model work into a separate service.

