# Stage 1: Build the frontend
FROM node:20-alpine AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ ./
RUN npm run build

# Stage 2: Final image with Python 3.12, Chromium/Playwright, and Cron
FROM python:3.12-slim-bookworm

# Install system dependencies including cron
RUN apt-get update && apt-get install -y cron curl && rm -rf /var/lib/apt/lists/*

# Install uv for fast python package management
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.cargo/bin:/root/.local/bin:${PATH}"

WORKDIR /app

# Copy backend files
COPY backend/ /app/backend/

# Install python dependencies using uv
WORKDIR /app/backend
RUN uv sync

# Install playwright system dependencies and browsers
RUN uv run playwright install --with-deps chromium

# Copy built frontend from Stage 1
COPY --from=frontend-builder /app/frontend/dist /app/frontend/dist

# Setup daily cron job for scraper
RUN echo "0 0 * * * cd /app/backend && PATH=\$PATH:/usr/local/bin:/root/.local/bin /root/.local/bin/uv run python main.py >> /var/log/cron.log 2>&1" > /etc/cron.d/scraper-cron
RUN chmod 0644 /etc/cron.d/scraper-cron
RUN crontab /etc/cron.d/scraper-cron
RUN touch /var/log/cron.log

# Create start script that starts cron locally and then starts uvicorn server
RUN echo '#!/bin/bash\n\ncron\n\ncd /app/backend\n# Run backend uvicorn server\n/root/.local/bin/uv run uvicorn api:app --host 0.0.0.0 --port 8123\n' > /app/start.sh
RUN chmod +x /app/start.sh

# Default env variables for scraping (changeable at runtime)
ENV RENTAL_ZIPCODES="95050,95051,94040,94041,94043,94085,94086,94087,94089,95008,95131,95132,95133,95134,95070,95071,95030,95031,95032,95033"
ENV PLATFORMS="zillow,redfin"

EXPOSE 8123
CMD ["/app/start.sh"]
