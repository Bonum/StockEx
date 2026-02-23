FROM fix-base

WORKDIR /app
COPY . /app

# Make sure store/ and log/ exist
RUN mkdir -p /app/store /app/log

# Install specific deps
RUN pip install --no-cache-dir kafka-python requests

CMD ["python", "fix-ui-client.py"]
