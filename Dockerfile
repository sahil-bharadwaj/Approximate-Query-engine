# Python Flask AQE Application
FROM python:3.11-slim

WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY *.py ./
COPY templates/ ./templates/
COPY static/ ./static/

# Create data directory
RUN mkdir -p /data

# Environment variables
ENV PYTHONUNBUFFERED=1
ENV AQE_DB_PATH=/data/aqe.sqlite
ENV PORT=8080
ENV HOST=0.0.0.0

# Expose port
EXPOSE 8080

# Run the application
CMD ["python", "app.py"]
