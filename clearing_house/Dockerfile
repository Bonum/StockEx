FROM python:3.11-slim

RUN pip install --no-cache-dir flask kafka-python requests

WORKDIR /app

# shared/ is mounted as /app/shared at runtime (via docker-compose volume)
# Copy clearing house service files
COPY clearing_house/ /app/clearing_house/

RUN mkdir -p /app/data

WORKDIR /app/clearing_house

ENV PYTHONPATH=/app

CMD ["python", "app.py"]
