FROM python:3.11-slim

WORKDIR /app
COPY mdf_simulator.py .

RUN pip install --no-cache-dir kafka-python

CMD ["python", "-u", "mdf_simulator.py"]
