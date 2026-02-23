FROM python:3.11-slim

WORKDIR /app
COPY oeg_simulator.py .

# Install requests and any other deps
RUN pip install --no-cache-dir requests
RUN pip install --no-cache-dir kafka-python

CMD ["python", "oeg_simulator.py"]
