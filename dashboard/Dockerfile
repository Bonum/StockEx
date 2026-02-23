FROM fix-base

WORKDIR /app

# Install dashboard-specific deps
RUN pip install --no-cache-dir kafka-python requests

COPY . /app

CMD ["python", "dashboard.py"]
