#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# CONFIGURACION
# ============================================================================
REGION="eu-central-1"
PROJECT="bedrock-chatbot"
STACK_NAME="${PROJECT}-stack"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
AGENT_REPO="${PROJECT}-agent"
FRONTEND_REPO="${PROJECT}-frontend"

echo "============================================"
echo "  ⚠️  DESTRUYENDO TODOS LOS RECURSOS"
echo "  Region: ${REGION}"
echo "  Stack:  ${STACK_NAME}"
echo "============================================"
echo ""
read -p "¿Estás seguro? Esto eliminará TODOS los recursos. (y/N): " CONFIRM
if [[ "${CONFIRM}" != "y" && "${CONFIRM}" != "Y" ]]; then
  echo "Cancelado."
  exit 0
fi

# ============================================================================
# 1. VACIAR BUCKET S3 DE DATOS
# ============================================================================
echo ""
echo "[1/4] Vaciando bucket S3 de datos..."
KB_BUCKET="${PROJECT}-kb-data-${ACCOUNT_ID}"
if aws s3api head-bucket --bucket "${KB_BUCKET}" --region "${REGION}" 2>/dev/null; then
  aws s3 rm "s3://${KB_BUCKET}" --recursive --region "${REGION}" || true
  echo "  ✓ Bucket ${KB_BUCKET} vaciado"
else
  echo "  ⏭ Bucket no existe, saltando"
fi

# ============================================================================
# 2. ELIMINAR STACK CLOUDFORMATION
# ============================================================================
echo ""
echo "[2/4] Eliminando stack CloudFormation: ${STACK_NAME}..."
if aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --region "${REGION}" &>/dev/null; then
  aws cloudformation delete-stack --stack-name "${STACK_NAME}" --region "${REGION}"
  echo "  Esperando a que se elimine (puede tardar varios minutos)..."
  aws cloudformation wait stack-delete-complete --stack-name "${STACK_NAME}" --region "${REGION}"
  echo "  ✓ Stack eliminado"
else
  echo "  ⏭ Stack no existe, saltando"
fi

# ============================================================================
# 3. ELIMINAR REPOSITORIOS ECR
# ============================================================================
echo ""
echo "[3/4] Eliminando repositorios ECR..."
for REPO in "${AGENT_REPO}" "${FRONTEND_REPO}"; do
  if aws ecr describe-repositories --repository-names "${REPO}" --region "${REGION}" &>/dev/null; then
    aws ecr delete-repository --repository-name "${REPO}" --region "${REGION}" --force --output text
    echo "  ✓ Repositorio ${REPO} eliminado"
  else
    echo "  ⏭ Repositorio ${REPO} no existe, saltando"
  fi
done

# ============================================================================
# 4. VERIFICACION
# ============================================================================
echo ""
echo "[4/4] Verificando limpieza..."
REMAINING=0
if aws cloudformation describe-stacks --stack-name "${STACK_NAME}" --region "${REGION}" &>/dev/null; then
  echo "  ⚠️  Stack aún existe"; REMAINING=$((REMAINING + 1))
fi
for REPO in "${AGENT_REPO}" "${FRONTEND_REPO}"; do
  if aws ecr describe-repositories --repository-names "${REPO}" --region "${REGION}" &>/dev/null; then
    echo "  ⚠️  ECR ${REPO} aún existe"; REMAINING=$((REMAINING + 1))
  fi
done

echo ""
if [ "${REMAINING}" -eq 0 ]; then
  echo "============================================"
  echo "  ✅ TODOS LOS RECURSOS ELIMINADOS"
  echo "============================================"
else
  echo "  ⚠️  ${REMAINING} recurso(s) pendiente(s) - revisa la consola AWS"
fi
