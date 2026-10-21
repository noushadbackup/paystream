-- PayStream core ledger: the OLTP system of record.
-- Double-entry: every transfer posts one debit and one credit that sum to zero.
-- Debezium streams these tables to Kafka (logical replication, pgoutput).

CREATE SCHEMA IF NOT EXISTS ledger;

CREATE TABLE ledger.transfers (
    txn_id        TEXT PRIMARY KEY,           -- the gateway's idempotency key
    txn_type      TEXT        NOT NULL CHECK (txn_type IN ('P2P', 'CASH_IN', 'CASH_OUT', 'MERCHANT_PAY', 'AIRTIME')),
    amount_paisa  BIGINT      NOT NULL CHECK (amount_paisa > 0),
    event_time    TIMESTAMPTZ NOT NULL,
    posted_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE ledger.entries (
    txn_id        TEXT    NOT NULL REFERENCES ledger.transfers (txn_id),
    leg           CHAR(1) NOT NULL CHECK (leg IN ('D', 'C')),
    account_id    TEXT    NOT NULL,
    amount_paisa  BIGINT  NOT NULL,           -- negative = debit, positive = credit
    posted_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (txn_id, leg),
    CHECK ((leg = 'D' AND amount_paisa < 0) OR (leg = 'C' AND amount_paisa > 0))
);

CREATE INDEX entries_account_idx ON ledger.entries (account_id, posted_at);

-- Money is never created or destroyed: checked at COMMIT, after both legs are in.
CREATE FUNCTION ledger.check_balanced() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    total BIGINT;
    legs  INT;
BEGIN
    SELECT coalesce(sum(amount_paisa), 0), count(*) INTO total, legs
    FROM ledger.entries WHERE txn_id = NEW.txn_id;
    IF legs <> 2 OR total <> 0 THEN
        RAISE EXCEPTION 'transfer % is unbalanced: % legs, sum % paisa', NEW.txn_id, legs, total;
    END IF;
    RETURN NULL;
END $$;

CREATE CONSTRAINT TRIGGER entries_balanced
    AFTER INSERT OR UPDATE ON ledger.entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION ledger.check_balanced();

-- Debezium needs full row images for updates/deletes and a publication to read from.
ALTER TABLE ledger.transfers REPLICA IDENTITY FULL;
ALTER TABLE ledger.entries   REPLICA IDENTITY FULL;
CREATE PUBLICATION paystream_ledger FOR TABLE ledger.transfers, ledger.entries;
