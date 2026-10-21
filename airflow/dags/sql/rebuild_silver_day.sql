INSERT INTO lake.silver.payments
SELECT txn_id, event_time, txn_type, sender_id, receiver_id, amount_paisa, currency, device_id,
       district, status, channel, ingest_time, event_date
FROM (
    SELECT *, row_number() OVER (PARTITION BY txn_id ORDER BY ingest_time, kafka_partition, kafka_offset) AS copy
    FROM lake.bronze.payment_events
    WHERE event_date = DATE '{day}'
)
WHERE copy = 1
