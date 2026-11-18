FROM apache/spark:3.5.3-python3
USER root
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install --no-cache-dir numpy && rm -rf /var/lib/apt/lists/*