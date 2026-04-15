"""
Frontend Flask que se comunica con el agente Strands en AgentCore Runtime.
Si AGENTCORE_ENDPOINT no está configurado, llama directamente a Bedrock.
"""

import os
import logging

import boto3
import requests
from flask import Flask, render_template, request, jsonify

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

REGION = os.environ.get("AWS_REGION", "eu-central-1")
AGENTCORE_ENDPOINT = os.environ.get("AGENTCORE_ENDPOINT", "")
KB_ID = os.environ.get("KNOWLEDGE_BASE_ID", "")
MODEL_ID = os.environ.get("MODEL_ID", "eu.amazon.nova-2-lite-v1:0")

bedrock_runtime = boto3.client("bedrock-runtime", region_name=REGION)
bedrock_agent_runtime = boto3.client("bedrock-agent-runtime", region_name=REGION)


def retrieve_from_kb(query: str, max_results: int = 5) -> str:
    if not KB_ID:
        return ""
    try:
        response = bedrock_agent_runtime.retrieve(
            knowledgeBaseId=KB_ID,
            retrievalQuery={"text": query},
            retrievalConfiguration={
                "vectorSearchConfiguration": {"numberOfResults": max_results}
            },
        )
        chunks = [
            r.get("content", {}).get("text", "")
            for r in response.get("retrievalResults", [])
            if r.get("content", {}).get("text")
        ]
        return "\n\n---\n\n".join(chunks)
    except Exception as e:
        logger.warning(f"KB retrieval error: {e}")
        return ""


def call_bedrock_direct(message: str) -> dict:
    context = retrieve_from_kb(message)
    system_text = "Eres un asistente útil que responde preguntas de forma clara y concisa en español."
    if context:
        system_text += (
            "\n\nUsa el siguiente contexto para responder. "
            "Si no es relevante, usa tu conocimiento general.\n\n"
            f"CONTEXTO:\n{context}"
        )

    resp = bedrock_runtime.converse(
        modelId=MODEL_ID,
        system=[{"text": system_text}],
        messages=[{"role": "user", "content": [{"text": message}]}],
        inferenceConfig={"maxTokens": 2048},
    )
    output = resp["output"]["message"]["content"][0]["text"]
    return {"response": output, "has_context": bool(context)}


def call_agentcore(message: str) -> dict:
    try:
        resp = requests.post(
            f"{AGENTCORE_ENDPOINT}/invocations",
            json={"message": message},
            timeout=120,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"AgentCore call failed, falling back to direct: {e}")
        return call_bedrock_direct(message)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/health")
def health():
    return jsonify({"status": "healthy"})


@app.route("/chat", methods=["POST"])
def chat():
    try:
        data = request.get_json()
        message = data.get("message", "").strip()
        if not message:
            return jsonify({"error": "Mensaje vacío"}), 400

        if AGENTCORE_ENDPOINT:
            result = call_agentcore(message)
        else:
            result = call_bedrock_direct(message)

        return jsonify(result)
    except Exception as e:
        logger.error(f"Chat error: {e}")
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
