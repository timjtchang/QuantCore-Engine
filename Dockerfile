# Start from the official image
FROM apache/spark:3.5.0

# Switch to root to install system packages
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    python3-dev \
    python3-pip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# 2. Copy requirements
COPY requirements.txt /app/requirements.txt

# 3. Install Python libraries
RUN pip3 install --no-cache-dir --upgrade pip setuptools wheel && \
    pip3 install --no-cache-dir --only-binary :all: -r /app/requirements.txt || \
    pip3 install --no-cache-dir -r /app/requirements.txt

# Switch back to default user for security
USER spark