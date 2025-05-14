CREATE TABLE IF NOT EXISTS fraud_predictions (
    trans_date_trans_time VARCHAR(255),
    cc_num BIGINT,
    amt DOUBLE PRECISION,
    merchant VARCHAR(255),
    category VARCHAR(255),
    is_fraud INTEGER,
    prediction DOUBLE PRECISION,
    prob_0 DOUBLE PRECISION,
    prob_1 DOUBLE PRECISION
);