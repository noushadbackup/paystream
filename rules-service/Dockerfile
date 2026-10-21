FROM python:3.12-slim
WORKDIR /app
COPY rules /app/rules
COPY rules-service /app/rules-service
RUN pip install --no-cache-dir "/app/rules-service[kafka,postgres,lake]"
ENV PAYSTREAM_RULE_SCHEMA=/app/rules/schema/rule.schema.json
EXPOSE 8090
# Publish the reviewed library on first start (no-op afterwards), re-send live versions, then serve.
CMD ["sh", "-c", "paystream-rules seed /app/rules/library && exec uvicorn paystream_rules.api:app_from_env --factory --host 0.0.0.0 --port 8090"]
