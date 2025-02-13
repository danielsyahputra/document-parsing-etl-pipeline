FROM python:3.10-slim

# Install dependencies
WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python packages
COPY . .

RUN pip install -r requirements.txt


CMD ["python", "pdf_processor.py"]