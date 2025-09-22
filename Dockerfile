FROM python:3.11-slim

# Environment
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PREFECT_API_SERVE=false \
    LANDING_DIR=/app/landing_zone \
    ARCHIVE_DIR=/app/archive \
    ERROR_DIR=/app/error \
    LOG_DIR=/app/logs

WORKDIR /app

# System deps (minimal; psycopg2-binary and pandas use wheels)
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . .

# Ensure runtime directories exist
RUN mkdir -p /app/landing_zone /app/archive /app/error /app/logs

# Default command: initialize DB then run the pipeline once over LANDING_DIR
CMD ["sh", "-c", "python -m elt.flow init_db && python -m elt.flow"]


