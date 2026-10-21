FROM python:3.12-slim
WORKDIR /app
COPY . /app/decision-service
RUN pip install --no-cache-dir "/app/decision-service[kafka]"
EXPOSE 8095
# Workers = cores given to the container; each worker keeps its own Redis connection pool.
ENV WORKERS=2 PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus
CMD ["sh", "-c", "rm -rf $PROMETHEUS_MULTIPROC_DIR && mkdir -p $PROMETHEUS_MULTIPROC_DIR && exec uvicorn paystream_decision.api:app_from_env --factory --host 0.0.0.0 --port 8095 --workers $WORKERS --no-access-log"]
