FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy codebase
COPY . .

# Set environment
ENV PYTHONPATH=/app

# Start pipeline
CMD ["python", "pipeline/main.py"]
