# Flink + Kafka + MCP + LangGraph — Fraud Surveillance

## Uma vez
    ./setup.sh && code .

## Ordem de execução (um terminal do VS Code para cada)
1. `make up`        — sobe Kafka + Flink e cria os tópicos (dashboard: http://localhost:8081)
2. `make job`       — submete o job SQL (3 regras de fraude)
3. `make producer`  — gera cotações/execuções (rajada de volume a cada 40 s)
4. `make console`   — (opcional) vê os eventos em `fraud_alerts` (aparecem após ~10 s, por causa do watermark de 5 s)
5. `make api`       — API de alertas; teste no Postman: POST http://localhost:8000/api/alerts
6. `make agent`     — agente LangGraph (lê via MCP, envia à API a cada 15 s)

Exploração do PDF (datagen, EXPLAIN, batch): `make sql` e cole `sql/00_exploracao.sql`.
Limpeza: `make down`.

## Problemas comuns
- `permission denied` no docker: `sudo usermod -aG docker $USER` e relogar.
- Sem alertas: inicie o `job` ANTES do `producer` (o job lê de `latest-offset`).
