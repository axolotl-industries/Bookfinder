FROM python:3.9-slim

WORKDIR /app

# Install basic tools needed
RUN apt-get update && apt-get install -y \
    wget \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set Playwright path so it's accessible to non-root users
ENV PLAYWRIGHT_BROWSERS_PATH=/app/.cache/ms-playwright
RUN mkdir -p /app/.cache/ms-playwright && chmod -R 777 /app

# Install Playwright browsers
RUN playwright install chromium
RUN playwright install-deps chromium

COPY . .

# Ensure the downloads directory and specific cache paths are writable
RUN mkdir -p /app/downloads /app/.cache && chmod 777 /app/downloads /app/.cache

EXPOSE 80

CMD ["python", "app.py"]
