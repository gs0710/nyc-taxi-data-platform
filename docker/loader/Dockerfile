FROM python:3.12-slim-bookworm

RUN pip install --no-cache-dir "psycopg[binary]==3.3.6" pyarrow==25.0.1

# Same non-root user as the Spark image (uid 1000 = your host user).
RUN useradd -m -u 1000 loader
USER loader
WORKDIR /app
