# Production Dockerfile for the Streamlit dashboard only
# Builds a small image that runs the dashboard at dashboard/app.py
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1
WORKDIR /app

# Install system deps for common Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    libpq-dev \
    curl \
    netcat \
  && rm -rf /var/lib/apt/lists/*

# Copy only dashboard requirements to speed rebuilds
COPY dashboard/requirements.txt /app/dashboard-requirements.txt
RUN pip install --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r /app/dashboard-requirements.txt

# Copy project
COPY . /app

# Expose the Streamlit port
ENV PORT=8501
EXPOSE 8501

# Run Streamlit
CMD ["streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
