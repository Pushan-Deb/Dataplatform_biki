FROM python:3.11-slim

ENV PIP_NO_CACHE_DIR=1 \
    PIP_REQUIRE_HASHES=0

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gcc g++ libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app/ /app/

RUN mkdir -p /app/state /app/feast_repo

CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]
