FROM python:3.11-slim

RUN pip install --no-cache-dir kafka-python==2.0.2 requests==2.31.0

WORKDIR /app
COPY ai_analyst.py .

ENV PYTHONPATH=/app

CMD ["python", "-u", "ai_analyst.py"]
