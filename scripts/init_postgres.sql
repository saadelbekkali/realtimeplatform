-- PostgreSQL arranca con POSTGRES_DB=crypto ya creada.
-- Este script corre dentro de esa DB automáticamente.

CREATE TABLE IF NOT EXISTS trades (
    trade_id       BIGINT         PRIMARY KEY,
    symbol         VARCHAR(20)    NOT NULL,
    price          NUMERIC(18, 8) NOT NULL,
    quantity       NUMERIC(18, 8) NOT NULL,
    quote_qty      NUMERIC(18, 8) NOT NULL,
    is_buyer_maker BOOLEAN        NOT NULL,
    trade_time     TIMESTAMP      NOT NULL,
    ingested_at    TIMESTAMP      DEFAULT NOW()
);

ALTER TABLE trades REPLICA IDENTITY FULL;

CREATE INDEX IF NOT EXISTS idx_trades_symbol     ON trades (symbol);
CREATE INDEX IF NOT EXISTS idx_trades_trade_time ON trades (trade_time);
