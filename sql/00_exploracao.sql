-- Exploração interativa (cole no SQL Client: make sql). Usa datagen, sem Kafka.
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'parallelism.default' = '2';

CREATE TABLE market_quotes (
  quote_id    STRING,
  security_id STRING,
  bid_price   DECIMAL(10,2),
  ask_price   DECIMAL(10,2),
  quote_time  TIMESTAMP(3),
  WATERMARK FOR quote_time AS quote_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'datagen',
  'rows-per-second' = '5',
  'fields.security_id.length' = '1'
);

-- Tumbling de 10 SEGUNDOS (a de 10 min só emitiria depois de 10 min)
SELECT window_start, window_end, security_id, COUNT(*) AS quote_count
FROM TABLE(TUMBLE(TABLE market_quotes, DESCRIPTOR(quote_time), INTERVAL '10' SECOND))
GROUP BY window_start, window_end, security_id;

-- Plano de execução (procure GroupAggregate e Exchange)
EXPLAIN PLAN FOR
SELECT security_id, COUNT(*) FROM market_quotes GROUP BY security_id;

-- Batch vs streaming: com tabela finita o job termina sozinho
-- CREATE TABLE q_bounded (security_id STRING) WITH ('connector'='datagen','number-of-rows'='1000','fields.security_id.length'='1');
-- SET 'execution.runtime-mode' = 'batch';
-- SELECT security_id, COUNT(*) FROM q_bounded GROUP BY security_id;
