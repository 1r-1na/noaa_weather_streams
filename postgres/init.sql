CREATE TABLE IF NOT EXISTS aggregated_data (
   country                VARCHAR(255),
   measurement_type       VARCHAR(255),
   aggregation_type       VARCHAR(255),
   "value"                NUMERIC(16,2),
   start_ts               TIMESTAMP,
   duration_days          SMALLINT,
   PRIMARY KEY(country, measurement_type, aggregation_type, start_ts, duration_days)
);

CREATE TABLE IF NOT EXISTS quality_codes (
    wban                VARCHAR(5),
    code                CHAR(1),
    amount              BIGINT,
    start_ts            TIMESTAMP,
    duration_days       SMALLINT,
    PRIMARY KEY(wban, code, start_ts, duration_days)
);

CREATE TABLE IF NOT EXISTS live_values(
    wban                VARCHAR(5),
    measurement_type    VARCHAR(255)
    code                CHAR(1),
    "value"             NUMERIC(16,2),
    timestamp           TIMESTAMP,
    lat                 VARCHAR(255),
    lon                 VARCHAR(255),
    PRIMARY KEY(wban, measurement_type)
);