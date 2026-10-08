"""Servidor MCP (stdio): expõe os eventos de fraude do tópico Kafka fraud_alerts."""
import json
import os
import time

from confluent_kafka import Consumer
from mcp.server.fastmcp import FastMCP

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("FRAUD_TOPIC", "fraud_alerts")

mcp = FastMCP("FlinkFraudMonitor")


@mcp.tool()
def get_latest_fraud_events(max_events: int = 50) -> str:
    """Retorna (JSON) os eventos de fraude novos detectados pelo Flink desde a última leitura."""
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "mcp-fraud-reader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    events, errors = [], []
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
        if events:
            consumer.commit(asynchronous=False)
    except Exception as e:  # noqa: BLE001
        return json.dumps({"events": [], "error": f"Falha ao ler Kafka: {e}"})
    finally:
        consumer.close()
    return json.dumps({"events": events, "errors": errors}, ensure_ascii=False)


if __name__ == "__main__":
    mcp.run()  # stdout é do protocolo MCP: nunca use print() aqui
