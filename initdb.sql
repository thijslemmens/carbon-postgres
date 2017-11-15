\c timescale

CREATE EXTENSION timescaledb CASCADE;

CREATE TABLE records (
  time TIMESTAMP NOT NULL ,
  metric TEXT NOT NULL ,
  VALUE FLOAT NOT NULL
);

SELECT create_hypertable('records', 'time', 'metric', 4);