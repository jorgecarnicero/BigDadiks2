import boto3
import sys
import os 

# ==========================================
# 🛑 CONFIGURACIÓN DINÁMICA
# ==========================================
# Boto3 coge las credenciales (KEYS) automáticamente del entorno.
# Solo necesitamos leer la Región y el ARN del Rol de las variables de entorno.

AWS_REGION = os.environ.get("AWS_REGION", "eu-south-2") # Por defecto us-east-1 si no lo pones
GLUE_ROLE_ARN = os.environ.get("GLUE_ROLE_ARN") 

def run_glue_process(group_id: str, bucket_name: str):
    
    # 1. Validación de seguridad
    if not GLUE_ROLE_ARN:
        print("❌ ERROR CRÍTICO: No has definido la variable de entorno 'GLUE_ROLE_ARN'.")
        print("   Necesito saber qué Rol de IAM debe usar el Crawler.")
        print("   Ejecuta en tu terminal: export GLUE_ROLE_ARN='arn:aws:iam::...TuRol...'")
        sys.exit(1)

    # 2. Inicializamos cliente (Boto3 usa tus credenciales de la terminal automágicamente)
    try:
        glue_client = boto3.client('glue', region_name=AWS_REGION)
    except Exception as e:
        print(f"❌ Error conectando con AWS. Verifica tus AWS_ACCESS_KEY_ID y AWS_SECRET_ACCESS_KEY. Detalles: {e}")
        sys.exit(1)

    # Nombres normalizados
    db_name = f"trade_data_{group_id}".replace("-", "_")
    crawler_name = f"crawler_{group_id}_trading"
    s3_target_path = f"s3://{bucket_name}/"

    print(f"🔄 [GLUE] Iniciando gestión en Región: {AWS_REGION}")
    print(f"   Using Role: {GLUE_ROLE_ARN.split('/')[-1]}") # Solo mostramos el nombre final para log

    # ---------------------------------------------------------
    # PASO A: Base de Datos
    # ---------------------------------------------------------
    try:
        glue_client.create_database(
            DatabaseInput={'Name': db_name, 'Description': 'Auto-generated'}
        )
        print(f"✅ [GLUE] DB '{db_name}' creada.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️ [GLUE] DB '{db_name}' ya existe.")

    # ---------------------------------------------------------
    # PASO B: Crawler (Crear o Actualizar)
    # ---------------------------------------------------------
    targets = {'S3Targets': [{'Path': s3_target_path}]}
    
    try:
        glue_client.get_crawler(Name=crawler_name)
        # Si existe, actualizamos
        glue_client.update_crawler(
            Name=crawler_name,
            Role=GLUE_ROLE_ARN,
            DatabaseName=db_name,
            Targets=targets
        )
        print(f"ℹ️ [GLUE] Crawler actualizado.")
    except glue_client.exceptions.EntityNotFoundException:
        # Si no existe, creamos
        print(f"🔨 [GLUE] Creando Crawler...")
        glue_client.create_crawler(
            Name=crawler_name,
            Role=GLUE_ROLE_ARN,
            DatabaseName=db_name,
            Targets=targets,
            SchemaChangePolicy={'DeleteBehavior': 'DEPRECATE_IN_DATABASE', 'UpdateBehavior': 'UPDATE_IN_DATABASE'}
        )

    # ---------------------------------------------------------
    # PASO C: Ejecutar
    # ---------------------------------------------------------
    try:
        glue_client.start_crawler(Name=crawler_name)
        print(f"🚀 [GLUE] Crawler '{crawler_name}' lanzado con éxito.")
    except glue_client.exceptions.CrawlerRunningException:
        print("⚠️ [GLUE] El crawler ya estaba corriendo.")
    except Exception as e:
        print(f"❌ [GLUE] Fallo al arrancar crawler: {e}")

if __name__ == "__main__":
    
    MI_GRUPO_REAL = "imat3a05"  
    MI_BUCKET_REAL = "trade-data-big-daddyks-trading"
    
    # Esto leerá las credenciales de la terminal y usará estos nombres
    run_glue_process(MI_GRUPO_REAL, MI_BUCKET_REAL)