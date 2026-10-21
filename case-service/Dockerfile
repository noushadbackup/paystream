FROM python:3.12-slim
WORKDIR /app
COPY schemas /app/schemas
COPY case-service /app/case-service
RUN pip install --no-cache-dir "/app/case-service[kafka,postgres]"
ENV PAYSTREAM_LABEL_SCHEMA=/app/schemas/fraud_label.avsc
EXPOSE 8097
# The same image runs the API (default) and the stream consumer (`paystream-cases consume`).
CMD ["uvicorn", "paystream_cases.api:app_from_env", "--factory", "--host", "0.0.0.0", "--port", "8097"]
