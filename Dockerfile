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

# Ensure the downloads directory and app files are writable by anyone
RUN mkdir -p /app/downloads && chmod -R 777 /app

EXPOSE 80

# Run as root if needed for port 80, but drop to user via compose,
# OR change port to 8080. Given port 80 is used, we'll keep CMD simple.
CMD ["python", "app.py"]
