"""Agente LangGraph: lê eventos via MCP, classifica, envia alertas e SÓ ENTÃO confirma os offsets."""
import argparse
import asyncio
import hashlib
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import List, TypedDict

import requests
from langchain_core.tools import tool
from langgraph.graph import END, StateGraph
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

API_URL = "http://localhost:8000/api/alerts"
MCP_SERVER = Path(__file__).with_name("mcp_flink_server.py")


@tool
def send_alert_to_monitoring_system(alert_id: str, severity: str, message: str, security_id: str) -> str:
    """Envia o alerta formatado para a API do sistema de monitoramento."""
    payload = {"alert_id": alert_id, "severity": severity, "message": message, "security_id": security_id}
    last_err = ""
    for attempt in range(3):  # retry com backoff simples
        try:
            r = requests.post(API_URL, json=payload, timeout=5)
            if r.status_code == 200:
                return f"Alerta {alert_id} enviado"
            last_err = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last_err = str(e)
        time.sleep(2 ** attempt)
    return f"Falha ao enviar {alert_id}: {last_err}"


class AgentState(TypedDict):
    events: List[dict]
    urgent: List[dict]
    digest: List[dict]
    actions: List[str]
    ack_token: str
    all_ok: bool


async def _call_mcp(tool_name: str, args: dict) -> dict:
    params = StdioServerParameters(command=sys.executable, args=[str(MCP_SERVER)])
    async with stdio_client(params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            res = await session.call_tool(tool_name, args)
            return json.loads(res.content[0].text)


def fetch_events(state: AgentState):
    data = asyncio.run(_call_mcp("get_latest_fraud_events", {"max_events": 50}))
    if data.get("error"):
        print("[mcp]", data["error"])
    return {"events": data.get("events", []), "ack_token": json.dumps(data.get("ack_token", {}))}


def triage(state: AgentState):
    seen, urgent, digest = set(), [], []
    for ev in state["events"]:
        if ev["event_id"] in seen:
            continue
        seen.add(ev["event_id"])
        (urgent if ev["severity"] == "HIGH" else digest).append(ev)
    return {"urgent": urgent, "digest": digest}


def route(state: AgentState) -> str:
    return "notify" if state["events"] else "end"


def notify(state: AgentState):
    actions = []
    for ev in state["urgent"]:  # HIGH: um alerta por evento
        actions.append(send_alert_to_monitoring_system.invoke({
            "alert_id": f"ALERT-{ev['event_id']}", "severity": "HIGH",
            "message": f"{ev['fraud_type']}: {ev['details']}", "security_id": ev["security_id"],
        }))
    by_sec = defaultdict(list)  # demais: um resumo por ativo, com id determinístico (idempotente)
    for ev in state["digest"]:
        by_sec[ev["security_id"]].append(ev)
    for sec, evs in by_sec.items():
        ids = ",".join(sorted(e["event_id"] for e in evs))
        digest_id = hashlib.sha1(ids.encode()).hexdigest()[:8]
        types = sorted({e["fraud_type"] for e in evs})
        actions.append(send_alert_to_monitoring_system.invoke({
            "alert_id": f"DIGEST-{sec}-{digest_id}", "severity": "MEDIUM",
            "message": f"{len(evs)} eventos ({', '.join(types)})", "security_id": sec,
        }))
    return {"actions": actions, "all_ok": all(a.startswith("Alerta") for a in actions)}


def ack(state: AgentState):
    if not state["all_ok"]:
        print("[agent] houve falha no envio: offsets NÃO confirmados; eventos serão reentregues")
        return {}
    res = asyncio.run(_call_mcp("ack_fraud_events", {"ack_token": state["ack_token"]}))
    if not res.get("acked"):
        print("[agent] falha ao confirmar offsets:", res.get("error"))
    return {}


def build_graph():
    g = StateGraph(AgentState)
    g.add_node("fetch_events", fetch_events)
    g.add_node("triage", triage)
    g.add_node("notify", notify)
    g.add_node("ack", ack)
    g.set_entry_point("fetch_events")
    g.add_edge("fetch_events", "triage")
    g.add_conditional_edges("triage", route, {"notify": "notify", "end": END})
    g.add_edge("notify", "ack")
    g.add_edge("ack", END)
    return g.compile()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--loop", type=int, default=0, help="repete a cada N segundos (0 = uma vez)")
    args = ap.parse_args()
    graph = build_graph()
    while True:
        out = graph.invoke({"events": [], "urgent": [], "digest": [], "actions": [], "ack_token": "{}", "all_ok": False})
        print(f"Eventos: {len(out['events'])} | Ações: {len(out.get('actions', []))}")
        for a in out.get("actions", []):
            print(" -", a)
        if not args.loop:
            break
        time.sleep(args.loop)
