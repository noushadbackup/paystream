FROM python:3.12-slim
WORKDIR /app
COPY schemas /app/schemas
COPY generator /app/generator
RUN pip install --no-cache-dir "/app/generator[kafka,ledger]"
ENV PAYSTREAM_SCHEMA_DIR=/app/schemas
ENTRYPOINT ["paystream-gen"]
