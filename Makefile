.RECIPEPREFIX = >
PY = .venv/bin/python

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

producer:
> $(PY) app/producer.py

console:
> docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 --topic fraud_alerts

api:
> $(PY) app/alert_api.py

agent:
> $(PY) app/agent_workflow.py --loop 15

down:
> docker compose down -v