# Start from the official image
FROM apache/spark:3.5.0

# Switch to root to install system packages
USER root

# 1. Update OS and install Pip (Safety Check)
# We install python3-pip just in case the base image didn't include it.
RUN apt-get update && \
    apt-get install -y python3-pip && \
    apt-get clean

# 2. Copy requirements
COPY requirements.txt /app/requirements.txt

# 3. Install Python libraries
# --no-cache-dir keeps the image small
# --break-system-packages is needed on newer Debian/Ubuntu versions to allow pip to write to /usr/lib
RUN pip3 install --no-cache-dir -r /app/requirements.txt 

# Switch back to default user for security
USER spark