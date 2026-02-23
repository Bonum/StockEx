FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY frontend.py .
COPY templates templates
CMD ["python", "frontend.py"]
