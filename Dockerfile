FROM python:3.11-slim

# Install boto3 for S3 access
RUN pip install --no-cache-dir boto3

# Copy scripts
COPY download.py /app/download.py
COPY upload.py /app/upload.py

WORKDIR /app

# Default: run the download script (overridden by Step Function for upload)
CMD ["python", "download.py"]
