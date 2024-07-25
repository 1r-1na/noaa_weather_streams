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
    measurement_type    VARCHAR(255),
    code                CHAR(1),
    amount              BIGINT,
    start_ts            TIMESTAMP,
    duration_days       SMALLINT,
    PRIMARY KEY(wban, measurement_type, code, start_ts, duration_days)
);

CREATE TABLE IF NOT EXISTS live_values(
    wban                VARCHAR(5),
    measurement_type    VARCHAR(255),
    code                CHAR(1),
    "value"             NUMERIC(16,2),
    timestamp           TIMESTAMP,
    lat                 VARCHAR(255),
    lon                 VARCHAR(255),
    country             VARCHAR(255),
    PRIMARY KEY(wban, measurement_type)
);

CREATE OR REPLACE FUNCTION notify_channel()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('custom_channel', 'Data has changed.');
RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER custom_trigger AFTER INSERT OR UPDATE OR DELETE ON aggregated_data EXECUTE PROCEDURE notify_channel();
CREATE TRIGGER custom_trigger AFTER INSERT OR UPDATE OR DELETE ON live_values EXECUTE PROCEDURE notify_channel();
CREATE TRIGGER custom_trigger AFTER INSERT OR UPDATE OR DELETE ON quality_codes EXECUTE PROCEDURE notify_channel();
