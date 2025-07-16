--TRUNCATE TABLE logs.etl_logs;
--
--SELECT pg_get_serial_sequence('logs.etl_logs', 'log_id');

TRUNCATE TABLE logs.etl_logs RESTART IDENTITY CASCADE;


DROP TABLE IF EXISTS logs.etl_logs;


CREATE SCHEMA IF NOT EXISTS logs;

CREATE TABLE IF NOT EXISTS logs.etl_logs (
    log_id SERIAL PRIMARY KEY,
    process_name VARCHAR(100) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status VARCHAR(20),
    rows_affected INTEGER,
    message TEXT,
    duration INTERVAL GENERATED ALWAYS AS (end_time - start_time) STORED
);



select *
FROM logs.etl_logs
order by log_id;