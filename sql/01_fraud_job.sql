-- Job de detecção de fraude: make job
SET 'pipeline.name' = 'fraud-surveillance';
SET 'parallelism.default' = '2';
SET 'table.exec.source.idle-timeout' = '5 s';

CREATE TABLE market_quotes (
  quote_id    STRING,
  security_id STRING,
  bid_price   DECIMAL(10,2),
  ask_price   DECIMAL(10,2),
  quote_time  TIMESTAMP(3),
  WATERMARK FOR quote_time AS quote_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'market_quotes',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-quotes',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE trade_executions (
  execution_id    STRING,
  security_id     STRING,
  execution_price DECIMAL(10,2),
  execution_time  TIMESTAMP(3),
  WATERMARK FOR execution_time AS execution_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'trade_executions',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'flink-trades',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

CREATE TABLE fraud_alerts (
  event_id    STRING,
  security_id STRING,
  fraud_type  STRING,
  severity    STRING,
  details     STRING,
  event_time  TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'fraud_alerts',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

EXECUTE STATEMENT SET
BEGIN

-- Regra 1: preço executado desvia >1% do mid da cotação (janela de 100 ms)
INSERT INTO fraud_alerts
SELECT
  CONCAT('PDEV-', e.execution_id),
  e.security_id,
  'PRICE_DEVIATION',
  CASE WHEN ABS(CAST(e.execution_price AS DOUBLE) - (CAST(q.bid_price AS DOUBLE) + CAST(q.ask_price AS DOUBLE)) / 2)
            / ((CAST(q.bid_price AS DOUBLE) + CAST(q.ask_price AS DOUBLE)) / 2) > 0.03
       THEN 'HIGH' ELSE 'MEDIUM' END,
  CONCAT('Execution price ', CAST(e.execution_price AS STRING),
         ' vs quote mid ', CAST(ROUND((CAST(q.bid_price AS DOUBLE) + CAST(q.ask_price AS DOUBLE)) / 2, 2) AS STRING)),
  e.execution_time
FROM trade_executions e
JOIN market_quotes q
  ON e.security_id = q.security_id
 AND q.quote_time BETWEEN e.execution_time - INTERVAL '0.100' SECOND AND e.execution_time
WHERE ABS(CAST(e.execution_price AS DOUBLE) - (CAST(q.bid_price AS DOUBLE) + CAST(q.ask_price AS DOUBLE)) / 2)
      / ((CAST(q.bid_price AS DOUBLE) + CAST(q.ask_price AS DOUBLE)) / 2) > 0.01;

-- Regra 2: execução sem nenhuma cotação nos 100 ms anteriores (LEFT JOIN -> NULL)
INSERT INTO fraud_alerts
SELECT
  CONCAT('NOQ-', e.execution_id),
  e.security_id,
  'NO_RECENT_QUOTE',
  'HIGH',
  'Execution without a quote in the previous 100 ms',
  e.execution_time
FROM trade_executions e
LEFT JOIN market_quotes q
  ON e.security_id = q.security_id
 AND q.quote_time BETWEEN e.execution_time - INTERVAL '0.100' SECOND AND e.execution_time
WHERE q.quote_id IS NULL;

-- Regra 3: pico de volume (>20 execuções por ativo em janela tumbling de 10 s)
INSERT INTO fraud_alerts
SELECT
  CONCAT('VOL-', security_id, '-', CAST(window_end AS STRING)),
  security_id,
  'VOLUME_SPIKE',
  'MEDIUM',
  CONCAT(CAST(cnt AS STRING), ' executions in a 10s window'),
  window_end
FROM (
  SELECT window_start, window_end, security_id, COUNT(*) AS cnt
  FROM TABLE(TUMBLE(TABLE trade_executions, DESCRIPTOR(execution_time), INTERVAL '10' SECOND))
  GROUP BY window_start, window_end, security_id
)
WHERE cnt > 20;

END;
