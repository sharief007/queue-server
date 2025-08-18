FROM python:3.11-slim

WORKDIR /app

# Copy source code
COPY . .

# Create storage directories
RUN mkdir -p storage/data storage/snapshots

# Set environment variables
ENV QB_SERVER_HOST=0.0.0.0
ENV QB_SERVER_PORT=9999

# Expose port
EXPOSE 9999

# Run the server
CMD ["python", "server.py"]
