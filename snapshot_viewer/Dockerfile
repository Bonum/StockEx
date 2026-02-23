FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir kafka-python

COPY snapshot_viewer.py .

CMD ["python", "snapshot_viewer.py"]
