-- Initial ledger schema. Balances are cached and must match ledger entries.
-- Sign convention: balance_minor = SUM(credits) - SUM(debits).
-- Every financial transaction inserts exactly one debit and one credit of the same amount.

CREATE TABLE IF NOT EXISTS accounts (
    id             VARCHAR(36)  NOT NULL,
    kind           VARCHAR(16)  NOT NULL,
    currency       CHAR(3)      NOT NULL,
    balance_minor  BIGINT       NOT NULL DEFAULT 0,
    version        BIGINT       NOT NULL DEFAULT 0,
    created_at     DATETIME(6)  NOT NULL,
    updated_at     DATETIME(6)  NOT NULL,
    PRIMARY KEY (id),
    KEY idx_accounts_kind_currency (kind, currency),
    CONSTRAINT chk_accounts_kind CHECK (kind IN ('user', 'system')),
    CONSTRAINT chk_accounts_currency CHECK (CHAR_LENGTH(currency) = 3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS transactions (
    id               VARCHAR(36)  NOT NULL,
    type             VARCHAR(16)  NOT NULL,
    currency         CHAR(3)      NOT NULL,
    amount_minor     BIGINT       NOT NULL,
    from_account_id  VARCHAR(36)  NOT NULL,
    to_account_id    VARCHAR(36)  NOT NULL,
    idempotency_key  VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    created_at       DATETIME(6)  NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_transactions_idempotency (idempotency_key),
    KEY idx_transactions_created (created_at),
    CONSTRAINT chk_transactions_type CHECK (type IN ('transfer', 'deposit', 'withdrawal')),
    CONSTRAINT chk_transactions_amount CHECK (amount_minor > 0),
    CONSTRAINT fk_transactions_from FOREIGN KEY (from_account_id) REFERENCES accounts (id),
    CONSTRAINT fk_transactions_to FOREIGN KEY (to_account_id) REFERENCES accounts (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS ledger_entries (
    id              VARCHAR(36) NOT NULL,
    transaction_id  VARCHAR(36) NOT NULL,
    account_id      VARCHAR(36) NOT NULL,
    currency        CHAR(3)     NOT NULL,
    direction       VARCHAR(8)  NOT NULL,
    amount_minor    BIGINT      NOT NULL,
    entry_index     TINYINT     NOT NULL,
    created_at      DATETIME(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_entries_tx_index (transaction_id, entry_index),
    KEY idx_entries_account_created (account_id, created_at, id),
    KEY idx_entries_currency (currency),
    CONSTRAINT chk_entries_direction CHECK (direction IN ('debit', 'credit')),
    CONSTRAINT chk_entries_amount CHECK (amount_minor > 0),
    CONSTRAINT chk_entries_index CHECK (entry_index IN (0, 1)),
    CONSTRAINT fk_entries_transaction FOREIGN KEY (transaction_id) REFERENCES transactions (id),
    CONSTRAINT fk_entries_account FOREIGN KEY (account_id) REFERENCES accounts (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Claimed inside the same database transaction as the ledger write.
-- A committed row always points at the transaction that won the key.
CREATE TABLE IF NOT EXISTS idempotency_keys (
    idempotency_key VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    request_hash    CHAR(64)     CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
    transaction_id  VARCHAR(36)  NULL,
    created_at      DATETIME(6)  NOT NULL,
    PRIMARY KEY (idempotency_key),
    CONSTRAINT fk_idempotency_transaction FOREIGN KEY (transaction_id) REFERENCES transactions (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Written in the same transaction as the ledger post. The relay publishes
-- unpublished rows and then sets published_at. Delivery is at-least-once.
CREATE TABLE IF NOT EXISTS outbox (
    id            BIGINT       NOT NULL AUTO_INCREMENT,
    event_id      VARCHAR(36)  NOT NULL,
    event_type    VARCHAR(64)  NOT NULL,
    aggregate_id  VARCHAR(36)  NOT NULL,
    payload       JSON         NOT NULL,
    created_at    DATETIME(6)  NOT NULL,
    published_at  DATETIME(6)  NULL,
    attempts      INT          NOT NULL DEFAULT 0,
    locked_until  DATETIME(6)  NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_outbox_event_id (event_id),
    KEY idx_outbox_pending (published_at, locked_until, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
