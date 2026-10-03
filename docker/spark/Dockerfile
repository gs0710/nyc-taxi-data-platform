FROM python:3.12-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN pip install --no-cache-dir "pyspark>=4.0,<5" pyarrow pandas

# Run as a normal user (uid 1000 = your host user) so files we write
# into ./data belong to you, not to root. A real passwd entry is also
# required because Spark/Hadoop look up the current user's name.
RUN useradd -m -u 1000 spark
USER spark
WORKDIR /app
