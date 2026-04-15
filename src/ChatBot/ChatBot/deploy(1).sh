#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# CONFIGURACION
# ============================================================================
REGION="eu-central-1"
PROJECT="bedrock-chatbot"
STACK_NAME="${PROJECT}-stack"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_BASE="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
AGENT_REPO="${PROJECT}-agent"
FRONTEND_REPO="${PROJECT}-frontend"

echo "============================================"
echo "  Desplegando Chatbot Bedrock AgentCore"
echo "  Region: ${REGION} (Frankfurt)"
echo "  Cuenta: ${ACCOUNT_ID}"
echo "============================================"

# ============================================================================
# 1. LOGIN EN ECR
# ============================================================================
echo ""
echo "[1/5] Login en ECR..."
aws ecr get-login-password --region "${REGION}" | \
  docker login --username AWS --password-stdin "${ECR_BASE}"

# ============================================================================
# 2. CREAR REPOSITORIOS ECR (si no existen)
# ============================================================================
echo ""
echo "[2/5] Creando repositorios ECR..."

for REPO in "${AGENT_REPO}" "${FRONTEND_REPO}"; do
  if ! aws ecr describe-repositories --repository-names "${REPO}" --region "${REGION}" &>/dev/null; then
    aws ecr create-repository \
      --repository-name "${REPO}" \
      --region "${REGION}" \
      --image-scanning-configuration scanOnPush=true \
      --output text
    echo "  ✓ Repositorio ${REPO} creado"
  else
    echo "  ✓ Repositorio ${REPO} ya existe"
  fi
done

# ============================================================================
# 3. BUILD & PUSH IMAGENES DOCKER
# ============================================================================
echo ""
echo "[3/5] Construyendo y subiendo imágenes Docker..."

AGENT_URI="${ECR_BASE}/${AGENT_REPO}:latest"
FRONTEND_URI="${ECR_BASE}/${FRONTEND_REPO}:latest"

echo "  → Construyendo imagen del agente (ARM64 para AgentCore)..."
docker buildx build --platform linux/arm64 -t "${AGENT_URI}" --push ./agent/

echo "  → Construyendo imagen del frontend (AMD64 para Fargate)..."
docker buildx build --platform linux/amd64 -t "${FRONTEND_URI}" --push ./frontend/

echo "  ✓ Imágenes subidas a ECR"

# ============================================================================
# 4. DESPLEGAR CLOUDFORMATION
# ============================================================================
echo ""
echo "[4/5] Desplegando stack CloudFormation: ${STACK_NAME}..."

aws cloudformation deploy \
  --template-file cloudformation/chatbot-stack.yaml \
  --stack-name "${STACK_NAME}" \
  --region "${REGION}" \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides \
    ProjectName="${PROJECT}" \
    FrontendImageUri="${FRONTEND_URI}" \
    AgentImageUri="${AGENT_URI}" \
  --no-fail-on-empty-changeset

echo "  ✓ Stack desplegado"

# ============================================================================
# 5. MOSTRAR OUTPUTS
# ============================================================================
echo ""
echo "[5/5] Obteniendo información del despliegue..."
echo ""

CHATBOT_URL=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" --region "${REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='ChatbotURL'].OutputValue" --output text)

KB_ID=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" --region "${REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='KnowledgeBaseId'].OutputValue" --output text)

DS_ID=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" --region "${REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='DataSourceId'].OutputValue" --output text)

KB_BUCKET=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}" --region "${REGION}" \
  --query "Stacks[0].Outputs[?OutputKey=='KBDataBucketName'].OutputValue" --output text)

echo "============================================"
echo "  ✅ DESPLIEGUE COMPLETADO"
echo "============================================"
echo ""
echo "  🌐 Chatbot URL:      ${CHATBOT_URL}"
echo "  🧠 Knowledge Base:   ${KB_ID}"
echo "  📦 Data Source:       ${DS_ID}"
echo "  📁 Bucket datos:     ${KB_BUCKET}"
echo ""
echo "  Para subir documentos al Knowledge Base:"
echo "    aws s3 cp mis-docs/ s3://${KB_BUCKET}/ --recursive --region ${REGION}"
echo ""
echo "  Para sincronizar el Knowledge Base:"
echo "    aws bedrock-agent start-ingestion-job \\"
echo "      --knowledge-base-id ${KB_ID} \\"
echo "      --data-source-id ${DS_ID} \\"
echo "      --region ${REGION}"
echo ""
echo "  ⏳ El ALB puede tardar 1-2 minutos en estar disponible."
echo "============================================"
