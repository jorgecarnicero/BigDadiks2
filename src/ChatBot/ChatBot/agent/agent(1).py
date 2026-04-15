"""
Strands Agent con Bedrock AgentCore Runtime.
Expone /invocations (POST) y /ping (GET) como requiere AgentCore.
Usa Amazon Nova 2 Lite en eu-central-1 con RAG via Knowledge Base.
"""

import json
import os
import logging
from contextlib import ExitStack
from contextvars import ContextVar
from decimal import Decimal
from typing import Any, Dict, List

import boto3
from strands import Agent, tool
from strands.models import BedrockModel
from bedrock_agentcore.runtime import BedrockAgentCoreApp

try:
    from strands.tools.mcp import MCPClient
    from mcp.client.streamable_http import streamablehttp_client
except Exception:
    MCPClient = None
    streamablehttp_client = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------
bedrock_agent_runtime = boto3.client("bedrock-agent-runtime")

# ---------------------------------------------------------------------------
# Configuration via environment variables (set by CloudFormation)
# ---------------------------------------------------------------------------
knowledge_base_id = os.environ.get("KNOWLEDGE_BASE_ID", "")
model_id = os.environ.get("MODEL_ID", "eu.amazon.nova-2-lite-v1:0")

# ---------------------------------------------------------------------------
# App & Model
# ---------------------------------------------------------------------------
app = BedrockAgentCoreApp()
model = BedrockModel(model_id=model_id)

# Per-request permissions context
CURRENT_PERMISSIONS: ContextVar[Dict[str, bool]] = ContextVar(
    "current_permissions",
    default={"kb_access": True},
)


def decimal_to_float(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, dict):
        return {k: decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [decimal_to_float(item) for item in obj]
    return obj


def deny(message: str) -> Dict[str, Any]:
    return {"success": False, "error": message}


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------
@tool
def search_knowledge_base(query: str) -> Dict[str, Any]:
    """Busca información relevante en el Knowledge Base usando RAG."""
    if not CURRENT_PERMISSIONS.get().get("kb_access", False):
        return deny("No tienes acceso a la Knowledge Base.")
    if not knowledge_base_id:
        return deny("Knowledge Base no configurada.")
    try:
        response = bedrock_agent_runtime.retrieve(
            knowledgeBaseId=knowledge_base_id,
            retrievalQuery={"text": query},
            retrievalConfiguration={
                "vectorSearchConfiguration": {"numberOfResults": 5}
            },
        )
        chunks = []
        for item in response.get("retrievalResults", []):
            chunks.append({
                "score": item.get("score", 0.0),
                "text": item.get("content", {}).get("text", ""),
                "location": item.get("location", {}),
            })
        return {
            "success": True,
            "knowledge_base_id": knowledge_base_id,
            "results": chunks,
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}


# ---------------------------------------------------------------------------
# Agent factory
# ---------------------------------------------------------------------------
def create_agent(tools: List[Any]) -> Agent:
    system_prompt = """Eres un asistente útil que responde preguntas de forma clara y concisa en español.
No uses formato markdown en tus respuestas: nada de **, ##, ```, ni listas con - o *. Responde en texto plano.

Tienes acceso a una Knowledge Base con documentos. Usa la herramienta
search_knowledge_base para buscar información relevante antes de responder
preguntas sobre temas específicos.

Si la Knowledge Base no tiene información relevante, responde con tu
conocimiento general."""

    return Agent(
        system_prompt=system_prompt,
        model=model,
        tools=tools,
        name="ChatbotAgent",
    )


# ---------------------------------------------------------------------------
# Entrypoint (AgentCore Runtime)
# ---------------------------------------------------------------------------
@app.entrypoint
async def invoke(payload: Dict[str, Any] = None):
    try:
        payload = payload or {}
        query = payload.get("prompt", payload.get("message", "Hola"))

        tools = [search_knowledge_base]
        agent = create_agent(tools=tools)
        result = agent(query)
        response_text = result.message["content"][0]["text"]

        return {
            "status": "success",
            "response": response_text,
            "has_context": bool(knowledge_base_id),
        }
    except Exception as exc:
        logger.error(f"Agent error: {exc}")
        return {"status": "error", "error": str(exc)}


if __name__ == "__main__":
    app.run()
