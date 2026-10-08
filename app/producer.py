"""Gera cotações e execuções sintéticas (com anomalias) nos tópicos Kafka."""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import Producer

MIDS = {"PETR4": 38.50, "VALE3": 62.00, "ITUB4": 33.20}


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="milliseconds")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--anomaly-rate", type=float, default=0.05, help="preço fora do mercado")
    ap.add_argument("--late-rate", type=float, default=0.08, help="execução sem cotação recente")
    ap.add_argument("--burst-every", type=int, default=40, help="segundos entre rajadas (0 = desliga)")
    ap.add_argument("--burst-security", default="PETR4")
    ap.add_argument("--burst-size", type=int, default=30)
    args = ap.parse_args()

    p = Producer({"bootstrap.servers": args.bootstrap})

    def send(topic: str, key: str, payload: dict) -> None:
        p.produce(topic, key=key, value=json.dumps(payload).encode())
        p.poll(0)

    def send_quote(sec: str) -> float:
        MIDS[sec] = round(max(1.0, MIDS[sec] * (1 + random.gauss(0, 0.0005))), 2)
        mid = MIDS[sec]
        send("market_quotes", sec, {
            "quote_id": str(uuid.uuid4())[:8], "security_id": sec,
            "bid_price": round(mid - 0.01, 2), "ask_price": round(mid + 0.01, 2),
            "quote_time": now_iso(),
        })
        return mid

    def send_trade(sec: str, price: float) -> None:
        send("trade_executions", sec, {
            "execution_id": str(uuid.uuid4())[:8], "security_id": sec,
            "execution_price": round(price, 2), "execution_time": now_iso(),
        })

    next_burst = time.time() + args.burst_every if args.burst_every else None
    print("Produzindo... Ctrl+C para parar")
    try:
        while True:
            sec = random.choice(list(MIDS))
            mid = send_quote(sec)

            if random.random() < 0.6:
                late = random.random() < args.late_rate
                time.sleep(random.uniform(0.25, 0.40) if late else random.uniform(0.01, 0.06))
                if random.random() < args.anomaly_rate:
                    price = mid * (1 + random.choice([-1, 1]) * random.uniform(0.015, 0.06))
                else:
                    price = mid * (1 + random.uniform(-0.0005, 0.0005))
                send_trade(sec, price)

            if next_burst and time.time() >= next_burst:
                print(f"[burst] {args.burst_size} execuções em {args.burst_security}")
                for _ in range(args.burst_size):
                    m = send_quote(args.burst_security)
                    time.sleep(0.02)
                    send_trade(args.burst_security, m)
                    time.sleep(0.05)
                next_burst = time.time() + args.burst_every

            time.sleep(random.uniform(0.1, 0.3))
    except KeyboardInterrupt:
        pass
    finally:
        p.flush(5)


if __name__ == "__main__":
    main()
