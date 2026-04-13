FROM python:3.11-slim

ARG FEAST_VERSION=0.55.0

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DEFAULT_TIMEOUT=120 \
    PIP_RETRIES=10 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FEAST_USAGE=False

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gcc g++ libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/feast/repo

RUN python -m pip install --upgrade pip setuptools wheel \
    && pip install --prefer-binary --default-timeout=120 --retries 10 "feast[postgres]==${FEAST_VERSION}" pandas pyarrow psycopg2-binary grpcio

CMD ["python", "-m", "feast", "--help"]
