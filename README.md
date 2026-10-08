# Flink + Kafka + MCP + LangGraph — Fraud Surveillance

Branch `main` = demo (v1). Branch `feature/producao-checkpoints` = evolução para produção (esta).

## O que mudou nesta branch
| Área | Mudança |
|---|---|
| Estado | RocksDB com checkpoints incrementais a cada 10 s (EXACTLY_ONCE), retidos ao cancelar |
| Falhas | restart `fixed-delay` (10 tentativas, 10 s) e volumes para checkpoints/savepoints |
| Deploy | `make savepoint` / `make restore SP=...` para atualizar o job sem perder estado |
| Kafka source | `group-offsets` (retoma de onde parou) em vez de `latest-offset` |
| Kafka sink | `exactly-once` (transações) — consumidores devem usar `read_committed` |
| MCP/Agente | offsets só são confirmados DEPOIS de enviar os alertas (`ack_fraud_events`); retry com backoff |
| API | idempotente por `alert_id`; resumos MEDIUM com id determinístico |

## Uma vez
    ./setup.sh && code .

## Ordem de execução (um terminal do VS Code para cada)
1. `make up`        — Kafka + Flink + tópicos (dashboard: http://localhost:8081)
2. `make job`       — submete o job SQL
3. `make producer`  — gera dados (rajada de volume a cada 40 s)
4. `make console`   — vê `fraud_alerts`. Com exactly-once, os eventos aparecem só após o checkpoint (~10 s) + watermark (5 s)
5. `make api`       — API de alertas (Postman: POST http://localhost:8000/api/alerts)
6. `make agent`     — agente (MCP + LangGraph), a cada 15 s

## Testes de resiliência
1. **Checkpoints:** dashboard → job → aba *Checkpoints*: devem aparecer concluídos a cada ~10 s.
2. **Queda de TaskManager:** `docker compose kill taskmanager`; o job entra em RESTARTING. Depois `docker compose up -d taskmanager`: volta a RUNNING a partir do último checkpoint.
3. **Deploy com savepoint:** `make savepoint` (anota o caminho impresso) → edite o SQL → `make restore SP=<caminho>`.
   Se a topologia mudar muito, o restore pode falhar; nesse caso rode sem SP e aceite perder o estado.
4. **Falha da API:** pare `make api`, deixe o agente rodar (ele loga "offsets NÃO confirmados"), suba a API de novo: os alertas são reentregues sem duplicar.

## Notas
- `transaction.timeout.ms` do sink (10 min) precisa ser menor que `transaction.max.timeout.ms` do broker (15 min).
- O state TTL não foi configurado de propósito: interval join e janelas limpam o estado pelo watermark.
- Ainda NÃO coberto (ver lista de produção): Kafka multi-broker/KRaft, Schema Registry, TLS/SASL, autenticação da API, métricas Prometheus, HA do JobManager.

## Problemas comuns
- `permission denied` no docker: `sudo usermod -aG docker $USER` e relogar.
- Sem alertas: inicie o `job` ANTES do `producer` na primeira execução.
- Após mudar o Dockerfile/compose: `make down && make up` (os volumes são recriados).
