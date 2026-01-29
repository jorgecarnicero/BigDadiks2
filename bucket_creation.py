import boto3
from botocore.exceptions import ClientError
 
# --- DATOS A PERSONALIZAR ---
GRUPO_ID = "big-daddyks"     # <--- VUESTRO NOMBRE DE GRUPO
REGION = "eu-south-2"    # Región España (Madrid)
ACTIVO = "SOLUSD"        # Criptomoneda
ANIOS = [2020, 2021, 2022, 2023] # Se creará un bucket por cada año de esta lista
 
def create_year_buckets_spain():
    # Verificación rápida de credenciales
    try:
        boto3.client('sts').get_caller_identity()
    except:
        print("ERROR: No se detectan las credenciales.")
        print("Ejecuta 'set AWS_ACCESS_KEY_ID=...' en la terminal antes de lanzar el script.")
        return
 
    s3_client = boto3.client('s3', region_name=REGION)
 
    print(f"Iniciando creación de arquitectura Multibucket en {REGION}...\n")
 
    for anio in ANIOS:
        # 1. Definimos el nombre del bucket INCLUYENDO EL AÑO
        bucket_name = f"trade-data-{GRUPO_ID}-raw-{anio}"
        print(f"--- Procesando Año {anio} ---")
        # 2. CREAR EL BUCKET DEL AÑO
        try:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': REGION}
            )
            print(f"Bucket creado: {bucket_name}")
        except ClientError as e:
            code = e.response['Error']['Code']
            if code == 'BucketAlreadyOwnedByYou':
                print(f"El bucket {bucket_name} ya existe y es tuyo. Seguimos.")
            elif code == 'BucketAlreadyExists':
                print(f"ERROR: El nombre '{bucket_name}' ya está ocupado. Cambia GRUPO_ID.")
                continue # Saltamos al siguiente año
            elif code == 'InvalidLocationConstraint':
                print("ERROR DE REGIÓN: Tu cuenta no admite España. Cambia a 'us-east-1'.")
                return
            else:
                print(f"Error creando bucket: {e}")
                continue
 
        # 3. CREAR LAS CARPETAS DE MESES DENTRO
        print(f"Creando carpetas mensuales...")
        for mes in range(1, 13):
            # Ruta limpia: SOLUSD/01/
            ruta_carpeta = f"{ACTIVO}/{mes:02d}/"
            try:
                s3_client.put_object(Bucket=bucket_name, Key=ruta_carpeta)
            except Exception as e:
                print(f"Error carpeta {ruta_carpeta}: {e}")
        print(f"Estructura lista en {bucket_name}")
 
    print("\n¡Proceso finalizado! Tenéis 4 buckets listos.")
 
if __name__ == '__main__':
    create_year_buckets_spain()



