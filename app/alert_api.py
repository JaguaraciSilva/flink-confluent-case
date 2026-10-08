"""API mock do sistema corporativo de alertas (estilo PagerDuty). Idempotente por alert_id."""
from datetime import datetime

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Sistema de Monitoramento de Fraudes (Mock)")
ALERTS: dict[str, dict] = {}


class Alert(BaseModel):
    alert_id: str
    severity: str
    message: str
    security_id: str


@app.post("/api/alerts")
def receive_alert(alert: Alert):
    if alert.alert_id in ALERTS:  # reentrega (at-least-once): não processa de novo
        return {"status": "success", "alert_id": alert.alert_id, "dispatched": False, "duplicate": True}
    ALERTS[alert.alert_id] = {**alert.model_dump(), "received_at": datetime.now().isoformat()}
    print(f"\n[ALERTA RECEBIDO] {alert.alert_id} | {alert.severity} | {alert.security_id}")
    print(f"  Detalhes: {alert.message}")
    return {"status": "success", "alert_id": alert.alert_id, "dispatched": True, "duplicate": False}


@app.get("/api/alerts")
def list_alerts():
    return {"count": len(ALERTS), "alerts": list(ALERTS.values())}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
