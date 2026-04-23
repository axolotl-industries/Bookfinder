FROM python:3.9-slim

WORKDIR /app

# Install basic tools needed for dependency resolution
RUN apt-get update && apt-get install -y \
    wget \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Chromium and then let Playwright install ALL necessary system dependencies
RUN playwright install chromium
RUN playwright install-deps chromium

COPY . .

# Ensure the downloads directory exists inside the container
RUN mkdir -p /app/downloads

EXPOSE 80

CMD ["python", "app.py"]
