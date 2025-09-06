# Main Dockerfile for Cohere RAG Image Search System
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create non-root user
RUN useradd --create-home --shell /bin/bash app \
    && chown -R app:app /app
USER app

# Set environment variables
ENV PYTHONPATH=/app
ENV ENVIRONMENT=production

# Expose port (Cloud Run uses PORT environment variable, defaults to 8080)
EXPOSE 8080

# Default command (FastAPI server)
CMD ["python", "main.py", "--module", "api"]