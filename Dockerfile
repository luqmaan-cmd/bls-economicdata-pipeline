FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bls_unified_pipeline.py .
COPY cpi_schema.sql .
COPY ppi_schema.sql .
COPY ce_schema.sql .
COPY la_schema.sql .
COPY jt_schema.sql .
COPY sa_schema.sql .
COPY oe_schema.sql .
COPY ci_schema.sql .
COPY mp_schema.sql .
COPY sm_schema.sql .

ENTRYPOINT ["python", "bls_unified_pipeline.py"]
