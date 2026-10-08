.RECIPEPREFIX = >
PY = .venv/bin/python
JOB_ID = $$(docker compose exec -T jobmanager ./bin/flink list -r 2>/dev/null | grep fraud-surveillance | awk '{print $$4}')

up:
> docker compose up -d --build
> @$(MAKE) topics

topics:
> @until docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; do echo "aguardando Kafka..."; sleep 3; done
> @for t in market_quotes trade_executions fraud_alerts; do docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic $$t --partitions 1 --replication-factor 1; done

sql:
> docker compose exec jobmanager ./bin/sql-client.sh

job:
> docker compose exec -T jobmanager ./bin/sql-client.sh -f /opt/sql/01_fraud_job.sql

# Para o job tirando um savepoint (use antes de mudar o SQL / fazer deploy)
savepoint:
> docker compose exec -T jobmanager ./bin/flink stop --savepointPath /opt/flink/savepoints $(JOB_ID)

savepoints:
> docker compose exec -T jobmanager ls -1 /opt/flink/savepoints

# Retoma o job de um savepoint: make restore SP=/opt/flink/savepoints/savepoint-XXXXXX-YYYYYY
restore:
> @test -n "$(SP)" || (echo "Uso: make restore SP=/opt/flink/savepoints/savepoint-XXXX"; exit 1)
> echo "SET 'execution.savepoint.path' = '$(SP)';" | docker compose exec -T jobmanager sh -c "cat > /tmp/init.sql"
> docker compose exec -T jobmanager ./bin/sql-client.sh -i /tmp/init.sql -f /opt/sql/01_fraud_job.sql

producer:
> $(PY) app/producer.py

console:
> docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic fraud_alerts --isolation-level read_committed

api:
> $(PY) app/alert_api.py

agent:
> $(PY) app/agent_workflow.py --loop 15

down:
> docker compose down -v
