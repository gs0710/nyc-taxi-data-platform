FROM python:3.12-slim-bookworm

RUN pip install --no-cache-dir "psycopg[binary]>=3.2,<4" pyarrow

RUN useradd -m -u 1000 loader
USER loader
WORKDIR /app
