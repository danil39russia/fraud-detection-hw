-- Витрина результатов скоринга
CREATE TABLE IF NOT EXISTS fraud_scores (
    id             BIGSERIAL PRIMARY KEY,
    transaction_id TEXT             NOT NULL UNIQUE,
    score          DOUBLE PRECISION NOT NULL,
    fraud_flag     SMALLINT         NOT NULL CHECK (fraud_flag IN (0, 1)),
    created_at     TIMESTAMPTZ      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fraud_scores_created_at ON fraud_scores (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_scores_fraud ON fraud_scores (fraud_flag, created_at DESC);
