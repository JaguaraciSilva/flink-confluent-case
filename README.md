# Flink + Kafka + MCP + LangGraph — Fraud Surveillance

Este projeto implementa uma plataforma de monitoramento de mercado financeiro em tempo real. O sistema processa fluxos contínuos de cotações e execuções de ordens usando Apache Flink SQL, expõe anomalias via Model Context Protocol (MCP), orquestra tomadas de decisão inteligentes com LangChain/LangGraph e despacha alertas automatizados para uma API corporativa testável via Postman.

🏗️ Arquitetura do Sistema

```mermaid ... 
graph TD
    %% Fontes de Dados e Streaming
    subgraph Streaming ["Camada de Streaming & Processamento (Docker)"]
        Kafka["Apache Kafka<br/>(Tópicos: market_quotes, trade_executions, fraud_alerts)"] -->|Stream de Dados| Flink["Apache Flink SQL<br/>(Watermarks, Interval Joins, Tumbling Windows)"]
        Flink -->|Eventos de Fraude| Kafka
    end

    %% Protocolo MCP
    subgraph MCP_Layer ["Camada de Contexto (MCP)"]
        Kafka -->|Consome fraud_alerts| MCPServer["Servidor MCP<br/>(mcp_flink_server.py / Stdio)"]
    end

    %% Agente Inteligente
    subgraph Agent_Layer ["Camada de Inteligência & Agente"]
        MCPServer -->|Fornece Dados via Ferramenta| Agent["Agente LangGraph / LangChain<br/>(Triagem: HIGH / MEDIUM)"]
    end

    %% API e Destino
    subgraph Destination ["Camada de Notificação & Testes"]
        Agent -->|Dispara Alerta (POST)| FastAPI["API de Alertas (FastAPI)<br/>(localhost:8000/api/alerts)"]
        FastAPI -->|Testado por| Postman["Postman / Desenvolvedor"]
    end

    %% Estilos visuais
    style Streaming fill:#f9f,stroke:#333,stroke-width:2px
    style MCP_Layer fill:#bbf,stroke:#333,stroke-width:2px
    style Agent_Layer fill:#bfb,stroke:#333,stroke-width:2px
    style Destination fill:#ff9,stroke:#333,stroke-width:2px
```

🧠 Conceitos de Engenharia de Streams Explorados

O projeto serve como um laboratório prático para dominar conceitos avançados de processamento de fluxos e arquiteturas orientadas a eventos:

    Watermarks e Bounded Out-of-Orderness:

        Conceito: Permitem que o Flink gerencie eventos que chegam fora de ordem devido a latências de rede. Definimos uma tolerância (ex: 5 segundos) para avançar o tempo do evento sem descartar dados legítimos.

    Interval Joins Temporais:

        Conceito: Cruzamento eficiente de dois fluxos baseado em uma janela de tempo restrita (ex: garantir que uma execução de ordem ocorra entre 0 e 100 milissegundos após a cotação). O Flink aguarda o avanço do watermark para emitir resultados de Left Joins caso o evento correspondente atrase.

    Janelas Tumbling (Tumbling Windows):

        Conceito: Agrupamentos de tempo fixo, sem sobreposição, que emitem os resultados consolidados (como contagens de transações ou volume) estritamente no fechamento de cada período (ex: janelas de 10 minutos).

    Model Context Protocol (MCP):

        Conceito: Protocolo aberto que padroniza a forma como aplicações de IA (como agentes LangGraph) expõem ou consomem fontes de dados contextuais locais (neste caso, lendo diretamente os alertas gerados pelo pipeline de streaming).

# 🚀 Guia Rápido de Execução

## Configuração Inicial (Uma vez) 
    > chmod 555 ./setup.sh
    > ./setup.sh && code .

## Ordem de execução (um terminal do VS Code para cada)
1. `make up`        — sobe Kafka + Flink e cria os tópicos (dashboard: http://localhost:8081)

    O que faz: Sobe o Zookeeper, Kafka, Flink JobManager e TaskManager.
    Cria automaticamente os tópicos necessários (market_quotes, trade_executions, fraud_alerts).

    Validação: Acesse http://localhost:8081 para confirmar 1 TaskManager com 4 slots ativos.

2. `make job`       — submete o job SQL (3 regras de fraude)

    O que faz: Envia o script SQL contendo as regras de detecção (sql/01_fraud_job.sql) para o Flink.

    Validação: O job fraud-surveillance aparecerá como RUNNING no dashboard do Flink.

3. `make producer`  — gera cotações/execuções (rajada de volume a cada 40 s)

    O que faz: Simula transações contínuas e injeta rajadas periódicas de volume em ativos específicos (PETR4, VALE3).

4. `make console`   — (opcional) vê os eventos em `fraud_alerts` (aparecem após ~10 s, por causa do watermark de 5 s)

    O que faz: Abre um consumidor no Kafka para inspecionar os eventos em fraud_alerts (os alertas aparecem após ~10s devido ao atraso configurado no watermark).

5. `make api`       — API de alertas; teste no Postman: POST http://localhost:8000/api/alerts

    O que faz: Inicializa o microsserviço de monitoramento na porta 8000.

    Teste via Postman: Envie um POST para http://localhost:8000/api/alerts com o corpo JSON:
    JSON

    {
      "alert_id": "ALT-001",
      "severity": "HIGH",
      "message": "Possível fraude detectada fora da janela de 100ms",
      "security_id": "PETR4"
    }

6. `make agent`     — agente LangGraph (lê via MCP, envia à API a cada 15 s)

    O que faz: O agente conecta-se ao servidor MCP, recolhe os eventos de fraude mais recentes, faz a triagem por criticidade (HIGH vs MEDIUM) e despacha automaticamente para a API de alertas.

Exploração (datagen, EXPLAIN, batch): `make sql` e cole `sql/00_exploracao.sql`.


7. `make down`      - Limpeza do ambiente 


## 🧪 Cenários de Teste e Validação Prática

    Exploração SQL e Otimização (EXPLAIN):
    Execute make sql e interaja com o cliente Flink colando o conteúdo de sql/00_exploracao.sql linha a linha para analisar planos físicos de execução e alternar entre modos Batch e Streaming.

    Simulação de Data Skew (Assimetria de Dados):
    Teste o comportamento do cluster sob forte desbalanceamento de chaves executando:
    

    > python app/producer.py --burst-every 5 --burst-size 200

        O que observar: Verifique no dashboard do Flink (http://localhost:8081) o impacto nas métricas de Busy e Backpressure por subtask.


## Problemas comuns
- `permission denied` no docker: `sudo usermod -aG docker $USER` e relogar.
- Sem alertas: inicie o `job` ANTES do `producer` (o job lê de `latest-offset`).


## 🛠️ Resolução de Problemas Comuns

    1. Permissão negada no Docker:
        > sudo usermod -aG docker $USER

        (Feche a sessão e volte a entrar para aplicar as permissões).

    2. Erro ClassNotFound do Kafka ao rodar o job:

        Reinicie o ambiente para forçar o build correto do conector:
        > make down && make up

    3. Nenhum evento em fraud_alerts:
    
        Certifique-se de que iniciou o job (make job) antes de iniciar o produtor (make producer), pois o job consome as mensagens a partir do offset mais recente (latest-offset).

    4. Para encerrar todos os processos em execução, utilize Ctrl+C nos terminais abertos e execute:
        > make down