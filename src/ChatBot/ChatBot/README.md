# 🤖 Chatbot con Bedrock AgentCore + Strands Agents + RAG

Aplicación de chatbot inteligente desplegada en AWS `eu-central-1` (Frankfurt), la región europea con soporte completo para Bedrock AgentCore.

---

## Arquitectura

```
                         ┌─────────────┐
                         │  Internet   │
                         └──────┬──────┘
                                │ HTTP :80
                    ┌───────────▼───────────┐
                    │   Application Load    │
                    │      Balancer (ALB)   │  ← Subnets públicas
                    └───────────┬───────────┘
                                │ :8080
                    ┌───────────▼───────────┐
                    │   ECS Fargate         │
                    │   (Frontend Flask)    │  ← Subnets privadas + NAT Gateway
                    └───────────┬───────────┘
                                │ HTTPS
                    ┌───────────▼───────────┐
                    │  Bedrock AgentCore    │
                    │  Runtime (Strands     │  ← Serverless, gestionado por AWS
                    │  Agent + RAG tools)   │
                    └──────┬────────┬───────┘
                           │        │
              ┌────────────▼──┐  ┌──▼─────────────────┐
              │  Bedrock LLM  │  │  Bedrock Knowledge  │
              │  Amazon Nova  │  │  Base (RAG)         │
              │  2 Lite       │  │                     │
              └───────────────┘  └──────┬────────┬─────┘
                                        │        │
                              ┌─────────▼──┐  ┌──▼──────────┐
                              │ S3 Vectors │  │  S3 Bucket   │
                              │ (embeddings│  │  (documentos │
                              │  index)    │  │   fuente)    │
                              └────────────┘  └──────────────┘
```

### Componentes

| Componente | Servicio AWS | Descripción |
|---|---|---|
| Frontend | ECS Fargate + ALB | Aplicación Flask con UI de chat, corre en subnets privadas detrás de un ALB público |
| Agente | Bedrock AgentCore Runtime | Contenedor Docker con Strands Agents SDK, expone endpoints `/invocations` y `/ping` |
| LLM | Bedrock - Amazon Nova 2 Lite | Inference profile `eu.amazon.nova-2-lite-v1:0` para generación de respuestas |
| Embeddings | Bedrock - Titan Embeddings V2 | Modelo `amazon.titan-embed-text-v2:0` (1024 dims) para vectorizar documentos |
| Knowledge Base | Bedrock Knowledge Base | RAG: recupera contexto relevante de tus documentos antes de responder |
| Vector Store | S3 Vectors | Almacena los embeddings de los documentos con búsqueda por similitud |
| Documentos | S3 Bucket | Bucket donde subes los PDFs/documentos que alimentan el Knowledge Base |
| Red | VPC + NAT Gateway | VPC con subnets públicas (ALB) y privadas (Fargate), NAT para acceso a internet saliente |

### Flujo de una pregunta

1. El usuario escribe un mensaje en la UI del chat
2. El frontend (Flask en Fargate) envía la petición al AgentCore Runtime
3. El agente Strands usa la tool `search_knowledge_base` para buscar contexto en el Knowledge Base
4. El Knowledge Base busca en S3 Vectors los fragmentos más relevantes
5. El agente envía el contexto + la pregunta a Amazon Nova 2 Lite
6. La respuesta vuelve al frontend y se muestra en la UI
7. Si AgentCore no responde, el frontend tiene fallback directo a Bedrock

---

## Estructura del proyecto

```
├── cloudformation/
│   └── chatbot-stack.yaml        # Template CloudFormation (toda la infra)
├── agent/
│   ├── Dockerfile                # Imagen ARM64 para AgentCore Runtime
│   ├── agent.py                  # Agente Strands con BedrockAgentCoreApp
│   └── requirements.txt
├── frontend/
│   ├── Dockerfile                # Imagen AMD64 para ECS Fargate
│   ├── app.py                    # Aplicación Flask
│   ├── requirements.txt
│   └── templates/
│       └── index.html            # UI del chatbot
├── deploy.sh                     # Script de despliegue completo
├── destroy.sh                    # Script de destrucción total
└── test.txt                      # Ejemplo de un vector almacenado en S3 vectors
└── README.md
```

---

## Requisitos previos

### 1. AWS CLI

Necesitas AWS CLI instalado y configurado con credenciales que tengan permisos de administrador (o al menos permisos para crear VPC, ECS, IAM, Bedrock, S3, ECR, CloudFormation).

```bash
# Verificar que está instalado y configurado
aws sts get-caller-identity
```

Si no lo tienes: https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

### 2. Docker Desktop

El proyecto construye dos imágenes Docker:
- **Agente** (ARM64): para AgentCore Runtime, que requiere arquitectura ARM64
- **Frontend** (AMD64): para ECS Fargate

Necesitas Docker Desktop con soporte `buildx` para construir imágenes multi-arquitectura.

**Instalar Docker Desktop:**

- **macOS**: Descarga desde https://www.docker.com/products/docker-desktop/ o con Homebrew:
  ```bash
  brew install --cask docker
  ```
- **Windows**: Descarga desde https://www.docker.com/products/docker-desktop/
- **Linux**: Sigue las instrucciones de https://docs.docker.com/engine/install/

Después de instalar, abre Docker Desktop y espera a que el icono de la ballena esté estable (indica que el daemon está listo).

```bash
# Verificar que Docker está corriendo
docker info
```

### 3. Habilitar modelos en Bedrock

Antes de desplegar, debes habilitar el acceso a los modelos en la consola de AWS Bedrock en la región `eu-central-1`:

1. Ve a la [consola de Amazon Bedrock](https://eu-central-1.console.aws.amazon.com/bedrock/home?region=eu-central-1#/modelaccess)
2. Haz clic en "Model access" en el menú lateral
3. Haz clic en "Manage model access"
4. Habilita estos dos modelos:
   - **Amazon → Nova 2 Lite** (`eu.amazon.nova-2-lite-v1:0` — inference profile cross-region)
   - **Amazon → Titan Text Embeddings V2** (`amazon.titan-embed-text-v2:0`)
5. Guarda los cambios y espera a que el estado sea "Access granted"

---

## Despliegue paso a paso

### Paso 1: Clonar/descargar el proyecto

Asegúrate de tener todos los archivos del proyecto en tu máquina local.

### Paso 2: Dar permisos de ejecución a los scripts

```bash
chmod +x deploy.sh destroy.sh
```

### Paso 3: Asegurarte de que Docker está corriendo

```bash
# En macOS
open -a Docker
# Espera ~20 segundos a que arranque

# Verificar
docker info
```

### Paso 4: Ejecutar el despliegue

```bash
./deploy.sh
```

El script hace lo siguiente automáticamente:

1. **Login en ECR** — Se autentica en el registro de contenedores de AWS
2. **Crea repositorios ECR** — Uno para el agente y otro para el frontend (si no existen)
3. **Construye y sube las imágenes Docker** — Compila las dos imágenes y las sube a ECR
4. **Despliega el stack CloudFormation** — Crea todos los recursos AWS (~10-15 minutos)
5. **Muestra los outputs** — URL del chatbot, IDs del Knowledge Base, etc.

### Paso 5: Esperar al despliegue

El stack de CloudFormation tarda entre 10 y 15 minutos en crear todos los recursos. Al terminar verás algo como:

```
============================================
  ✅ DESPLIEGUE COMPLETADO
============================================

  🌐 Chatbot URL:      http://bedrock-chatbot-alb-XXXXXXXXX.eu-central-1.elb.amazonaws.com
  🧠 Knowledge Base:   XXXXXXXXXX
  📦 Data Source:       XXXXXXXXXX
  📁 Bucket datos:     bedrock-chatbot-kb-data-XXXXXXXXXXXX
```

### Paso 6: Acceder al chatbot

Abre la URL del chatbot en tu navegador. El ALB puede tardar 1-2 minutos adicionales en estar completamente disponible después del despliegue.

---

## Subir documentos al Knowledge Base

El chatbot funciona desde el primer momento con el conocimiento general de Nova 2 Lite. Para añadir tus propios documentos (PDFs, TXT, HTML, DOCX, CSV, etc.):

### 1. Subir documentos al bucket S3

```bash
# Un solo archivo
aws s3 cp mi-documento.pdf s3://bedrock-chatbot-kb-data-<TU_ACCOUNT_ID>/ --region eu-central-1

# Una carpeta entera
aws s3 cp mis-documentos/ s3://bedrock-chatbot-kb-data-<TU_ACCOUNT_ID>/ --recursive --region eu-central-1
```

### 2. Sincronizar el Knowledge Base

Después de subir documentos, lanza un job de ingesta para que Bedrock los procese y vectorice:

```bash
aws bedrock-agent start-ingestion-job \
  --knowledge-base-id <KB_ID> \
  --data-source-id <DS_ID> \
  --region eu-central-1
```

Los valores de `KB_ID` y `DS_ID` se muestran al final del despliegue.

---

## Destrucción total

Para eliminar TODOS los recursos creados:

```bash
./destroy.sh
```

El script:
1. Vacía el bucket S3 de datos (necesario para poder eliminarlo)
2. Elimina el stack de CloudFormation completo (VPC, ECS, ALB, Knowledge Base, S3 Vectors, AgentCore Runtime, IAM roles, etc.)
3. Elimina los repositorios ECR con todas las imágenes
4. Verifica que no quede nada

Te pedirá confirmación antes de proceder. La eliminación tarda unos 5-10 minutos.

---

## Consultar vectores en S3 Vectors

Los embeddings generados por el Knowledge Base se almacenan en S3 Vectors. Puedes consultarlos directamente con la CLI:

### Listar vectores

```bash
aws s3vectors list-vectors \
  --vector-bucket-name bedrock-chatbot-vectors-911167893020 \
  --index-name bedrock-chatbot-index \
  --max-results 5 \
  --region eu-central-1
```

### Ver un vector en particular con su metadata y embedding

```bash
aws s3vectors get-vectors \
  --vector-bucket-name bedrock-chatbot-vectors-911167893020 \
  --index-name bedrock-chatbot-index \
  --keys '["ba3a1871-dfc5-4dc8-7134-66feeb9a335d0"]' \
  --return-data \
  --return-metadata \
  --region eu-central-1 > test.txt
```

---

## Notas

- **Región**: `eu-central-1` (Frankfurt) es la región europea más cercana a España con soporte para Bedrock AgentCore Runtime. `eu-south-2` (España) no soporta AgentCore a día de hoy.
- **Costes**: El NAT Gateway y el ALB tienen coste por hora (~0.05$/h cada uno). El resto es pay-per-use. Recuerda ejecutar `./destroy.sh` cuando no lo necesites.
- **Fallback**: Si AgentCore Runtime no responde, el frontend llama directamente a Bedrock como fallback, así el chatbot siempre funciona.
