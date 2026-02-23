#!/bin/bash
set -e

KAFKA_DIR=/opt/kafka

export PYTHONPATH=/app
export KAFKA_BOOTSTRAP=localhost:9092
export MATCHER_URL=http://localhost:6000
export PORT=5000
export FIX_CONFIG=/app/fix_ui/client_hf.cfg
export UI_PORT=5002

# ── Kafka (KRaft) ─────────────────────────────────────────────────────────────
echo "[startup] Formatting Kafka storage (KRaft)..."
CLUSTER_ID=$($KAFKA_DIR/bin/kafka-storage.sh random-uuid)
$KAFKA_DIR/bin/kafka-storage.sh format \
  -t "$CLUSTER_ID" \
  -c $KAFKA_DIR/config/kraft/server.properties \
  --ignore-formatted

echo "[startup] Starting Kafka..."
$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/kraft/server.properties &
KAFKA_PID=$!

echo "[startup] Waiting for Kafka to be ready..."
RETRIES=30
until $KAFKA_DIR/bin/kafka-topics.sh \
      --list --bootstrap-server localhost:9092 &>/dev/null 2>&1; do
  RETRIES=$((RETRIES - 1))
  if [ $RETRIES -le 0 ]; then
    echo "[startup] ERROR: Kafka did not start in time"
    exit 1
  fi
  sleep 3
  echo "[startup] Still waiting for Kafka... ($RETRIES retries left)"
done
echo "[startup] Kafka ready."

# Create topics
for TOPIC in orders trades snapshots control; do
  $KAFKA_DIR/bin/kafka-topics.sh \
    --create --if-not-exists \
    --topic "$TOPIC" \
    --partitions 1 \
    --replication-factor 1 \
    --bootstrap-server localhost:9092
done
echo "[startup] Topics created."

# ── Python services ────────────────────────────────────────────────────────────
echo "[startup] Starting Matcher on port 6000..."
python3 /app/matcher.py &
sleep 8

echo "[startup] Starting MD Feeder..."
python3 /app/mdf_simulator.py &

echo "[startup] Starting FIX OEG on port 5001..."
(cd /app/fix_oeg && python3 /app/fix_oeg/fix_oeg_server.py) &
sleep 3

echo "[startup] Starting FIX UI Client on port 5002..."
python3 /app/fix_ui/fix_ui_client.py &
sleep 2

# ── nginx (reverse proxy: port 7860 → dashboard:5000 + fix-ui:5002) ───────────
echo "[startup] Starting nginx on port 7860..."
nginx

echo "[startup] Starting Dashboard on port 5000..."
exec python3 /app/dashboard.py
