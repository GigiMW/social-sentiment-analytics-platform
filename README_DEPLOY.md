Deployment guide — Streamlit dashboard

This file explains two deployment paths: quick demo on Streamlit Community Cloud and production container deployment (Google Cloud Run / Render / Fly).

1) Streamlit Community Cloud (quick demo with Managed Kafka)

**Step 1: Set up Confluent Cloud (managed Kafka)**
- Go to https://confluent.cloud and sign up (free tier available).
- Create a cluster (choose a region close to Streamlit Cloud US-East).
- In the cluster, create an API key:
  - Go to **Cluster settings** → **API keys** → **Create key**.
  - Save the **API Key** (username) and **Secret** (password).
- Get the **Bootstrap server** URL from cluster overview (e.g., `pkc-xxx.us-east-1.provider.confluent.cloud:9092`).
- Verify access: create topics `enriched.nlp` and `analytics.sentiment` (or leave auto-create enabled).

**Step 2: Deploy to Streamlit Cloud**
- Ensure code is pushed to GitHub.
- Go to https://streamlit.io/cloud and click **New app**.
- Select your repo and `dashboard/app.py`.
- **Important**: Go to **Manage app** (top-right) → **Secrets**.
- Add the following (copy from Confluent Cloud):
  ```toml
  KAFKA_BOOTSTRAP_SERVERS = "pkc-xxx.us-east-1.provider.confluent.cloud:9092"
  KAFKA_USERNAME = "your-api-key-here"
  KAFKA_PASSWORD = "your-api-secret-here"
  ```
- Click **Save** and redeploy.
- Streamlit Cloud will now connect to your managed Kafka cluster.

**Step 3: Verify**
- Open the app URL.
- You should see a loading message or "No events yet" (until producers send data).
- If you see an error, check the logs in Streamlit Cloud (Manage app → Logs).

Notes:
- Streamlit Cloud **cannot** reach localhost Kafka; it must use a managed/cloud Kafka service.
- Keep heavy ML models out of Streamlit; run `nlp_service` separately (e.g., on Cloud Run) and use Kafka to stream enriched events.


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

