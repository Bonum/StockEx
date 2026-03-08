# HuggingFace Spaces – StockEx Trading Demo
# Single container: Kafka (KRaft) + Matcher + MD Feeder + Dashboard + FIX OEG + FIX UI Client

FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
# Limit Kafka JVM heap to fit free-tier RAM
ENV KAFKA_HEAP_OPTS="-Xmx400m -Xms256m"

# Install Java (required by Kafka) + wget + nginx + C++ build tools (for QuickFIX)
RUN apt-get update && apt-get install -y --no-install-recommends \
      default-jre-headless \
      wget \
      nginx \
      build-essential \
      libssl-dev \
      zlib1g-dev \
      libbz2-dev \
    && rm -rf /var/lib/apt/lists/*

# Download Apache Kafka 3.7.0 (KRaft mode – no ZooKeeper needed)
ARG KAFKA_VERSION=3.7.0
RUN wget -q \
      "https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz" \
      -O /tmp/kafka.tgz \
    && tar -xzf /tmp/kafka.tgz -C /opt \
    && mv /opt/kafka_2.13-${KAFKA_VERSION} /opt/kafka \
    && rm /tmp/kafka.tgz

# Install Python dependencies (quickfix compiles from source – allow extra time)
RUN pip install --no-cache-dir \
      kafka-python==2.0.2 \
      Flask==2.2.5 \
      requests==2.31.0 \
      quickfix \
      numpy \
      pandas \
      scikit-learn \
      "stable-baselines3[extra]" \
      huggingface_hub

# ── Application code (flat layout matching /app container paths) ──────────────
WORKDIR /app

COPY shared/                          /app/shared/
COPY shared_data/securities.txt       /app/data/securities.txt
COPY shared_data/market_schedule.txt  /app/data/market_schedule.txt
# Also expose schedule path via env so dashboard finds it
ENV SCHEDULE_FILE=/app/data/market_schedule.txt

# Matcher service
COPY matcher/matcher.py               /app/matcher.py
COPY matcher/database.py              /app/database.py

# MD Feeder service
COPY md_feeder/mdf_simulator.py       /app/mdf_simulator.py

# Dashboard service
COPY dashboard/dashboard.py           /app/dashboard.py
COPY dashboard/templates/             /app/templates/

# Frontend (order entry) service
COPY frontend/frontend.py             /app/frontend.py
COPY frontend/templates/              /app/frontend_templates/

# FIX OEG (Order Entry Gateway)
COPY fix_oeg/fix_oeg_server.py        /app/fix_oeg/fix_oeg_server.py
COPY fix_oeg/FIX44.xml                /app/fix_oeg/FIX44.xml
# Use HF-specific config (localhost paths instead of container service names)
COPY fix_server_hf.cfg                /app/fix_oeg/fix_server.cfg

# FIX UI Client
COPY fix-ui-client/fix-ui-client.py   /app/fix_ui/fix_ui_client.py
COPY fix-ui-client/templates/         /app/fix_ui/templates/
COPY client_hf.cfg                    /app/fix_ui/client_hf.cfg

# AI Analyst service
COPY ai_analyst/ai_analyst.py         /app/ai_analyst.py

# Clearing House service
COPY clearing_house/                  /app/clearing_house/

# ── Kafka KRaft configuration ─────────────────────────────────────────────────
COPY kafka-kraft.properties           /opt/kafka/config/kraft/server.properties

# ── nginx configuration ───────────────────────────────────────────────────────
COPY nginx.conf                       /etc/nginx/nginx.conf

# ── Startup script ────────────────────────────────────────────────────────────
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 7860

CMD ["/entrypoint.sh"]
