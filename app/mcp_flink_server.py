"""Servidor MCP (stdio): expõe os eventos de fraude do tópico Kafka fraud_alerts.

Entrega at-least-once: get_latest_fraud_events NÃO confirma offsets.
O cliente chama ack_fraud_events(ack_token) somente depois de processar com sucesso.
"""
import json
import os
import time

from confluent_kafka import Consumer, TopicPartition
from mcp.server.fastmcp import FastMCP

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("FRAUD_TOPIC", "fraud_alerts")
GROUP = "mcp-fraud-reader"

mcp = FastMCP("FlinkFraudMonitor")


def _consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",  # só lê transações do sink exactly-once já confirmadas
    })


@mcp.tool()
def get_latest_fraud_events(max_events: int = 50) -> str:
    """Retorna (JSON) eventos de fraude ainda não confirmados e um ack_token para confirmá-los."""
    consumer = _consumer()
    events, errors, next_offsets = [], [], {}
    try:
        consumer.subscribe([TOPIC])
        deadline = time.time() + 8
        while len(events) < max_events and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                if events:
                    break
                continue
            if msg.error():
                errors.append(str(msg.error()))
                continue
            events.append(json.loads(msg.value()))
            part = str(msg.partition())
            next_offsets[part] = max(next_offsets.get(part, 0), msg.offset() + 1)
    except Exception as e:  # noqa: BLE001
        return json.dumps({"events": [], "error": f"Falha ao ler Kafka: {e}"})
    finally:
        consumer.close()  # sem commit: se o cliente não der ack, os eventos são reentregues
    return json.dumps({"events": events, "errors": errors, "ack_token": next_offsets}, ensure_ascii=False)


@mcp.tool()
def ack_fraud_events(ack_token: str) -> str:
    """Confirma (commit) os offsets do ack_token retornado por get_latest_fraud_events."""
    consumer = _consumer()
    try:
        offsets = [TopicPartition(TOPIC, int(p), int(o)) for p, o in json.loads(ack_token).items()]
        consumer.commit(offsets=offsets, asynchronous=False)
        return json.dumps({"acked": True})
    except Exception as e:  # noqa: BLE001
        return json.dumps({"acked": False, "error": str(e)})
    finally:
        consumer.close()


if __name__ == "__main__":
    mcp.run()  # stdout é do protocolo MCP: nunca use print() aqui
