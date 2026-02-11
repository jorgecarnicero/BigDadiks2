# bootstrap_pipeline.py
import time
import boto3
from botocore.exceptions import ClientError

REGION = "eu-south-2"  # <-- CAMBIA si procede

glue = boto3.client("glue", region_name=REGION)

# =========================
# CAMBIA AQUÍ (NOMBRES/RUTAS)
# =========================

# Bucket y rutas S3
DATA_BUCKET = "trade-data-big-daddykds-main"  # <-- CAMBIA
BRONZE_PREFIX = f"s3://{DATA_BUCKET}/bronze/"  # <-- CAMBIA si tu bronze cuelga de otro prefijo
SILVER_PREFIX = f"s3://{DATA_BUCKET}/silver/prices/"  # <-- CAMBIA (recomendado meter dataset)
GOLD_PREFIX   = f"s3://{DATA_BUCKET}/gold/indicators/"  # <-- CAMBIA

# Scripts del Job (ubicación en S3)
SCRIPTS_PREFIX = f"s3://{DATA_BUCKET}/scripts/"  # <-- CAMBIA
SCRIPT_BRONZE_TO_SILVER = SCRIPTS_PREFIX + "bronze_to_silver.py"
SCRIPT_SILVER_TO_GOLD   = SCRIPTS_PREFIX + "silver_to_gold.py"
SCRIPT_RUN_CRAWLER       = SCRIPTS_PREFIX + "run_crawler.py"

# DB de Glue (la que ya tienes)
SRC_DB = "trade_data_imat3a05"  # <-- ya la tienes

# Tabla bronze exacta en Glue Data Catalog (tienes que ponerla tal cual)
SRC_TABLE = "TU_TABLA_BRONZE_EXACTA"  # <-- CAMBIA

# Crawler único
CRAWLER_NAME = "lake_trade_data_imat3a05"  # <-- CAMBIA
CRAWLER_DB = SRC_DB  # o una db separada: trade_data_imat3a05_lake
CRAWLER_ROLE_ARN = "arn:aws:iam::<account-id>:role/<ROL_CRAWLER>"  # <-- CAMBIA
CRAWLER_TABLE_PREFIX = "lake_"  # opcional

# Roles de los Glue Jobs (puede ser el mismo para los 3)
JOB_ROLE_ARN = "arn:aws:iam::<account-id>:role/<ROL_GLUE_JOB>"  # <-- CAMBIA

# Glue version / worker config (ajusta a vuestro entorno)
GLUE_VERSION = "4.0"
WORKER_TYPE = "G.1X"
NUMBER_OF_WORKERS = 5

# Particiones (según vuestro catálogo: "asset,year,month" o "Asset,year,month")
PARTITION_COLS = "asset,year,month"  # <-- CAMBIA si en catálogo es Asset

# (Opcional) pushdown para probar una partición concreta
PUSH_DOWN = ""  # Ej: "(asset=='SOLUSD' and year==2022 and month==2)"

# =========================
# Helpers
# =========================

def ensure_job(job_name: str, script_location: str, default_args: dict):
    """
    Crea el Job si no existe. Si existe, lo actualiza (script/args).
    """
    job_command = {
        "Name": "glueetl",
        "ScriptLocation": script_location,
        "PythonVersion": "3",
    }

    job_def = {
        "Role": JOB_ROLE_ARN,
        "Command": job_command,
        "DefaultArguments": default_args,
        "GlueVersion": GLUE_VERSION,
        "WorkerType": WORKER_TYPE,
        "NumberOfWorkers": NUMBER_OF_WORKERS,
        # Puedes activar JobBookmarks aquí por defecto:
        # "ExecutionProperty": {"MaxConcurrentRuns": 1},
    }

    try:
        glue.get_job(JobName=job_name)
        glue.update_job(JobName=job_name, JobUpdate=job_def)
        print(f"[OK] Job actualizado: {job_name}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(Name=job_name, **job_def)
        print(f"[OK] Job creado: {job_name}")

def ensure_crawler():
    targets = {"S3Targets": [{"Path": BRONZE_PREFIX}, {"Path": SILVER_PREFIX}, {"Path": GOLD_PREFIX}]}

    try:
        glue.get_crawler(Name=CRAWLER_NAME)
        # Si quieres que el bootstrap también lo “mantenga” actualizado:
        glue.update_crawler(
            Name=CRAWLER_NAME,
            Role=CRAWLER_ROLE_ARN,
            DatabaseName=CRAWLER_DB,
            TablePrefix=CRAWLER_TABLE_PREFIX,
            Targets=targets,
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "DEPRECATE_IN_DATABASE"
            },
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"}
        )
        print(f"[OK] Crawler actualizado: {CRAWLER_NAME}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_crawler(
            Name=CRAWLER_NAME,
            Role=CRAWLER_ROLE_ARN,
            DatabaseName=CRAWLER_DB,
            TablePrefix=CRAWLER_TABLE_PREFIX,
            Targets=targets,
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "DEPRECATE_IN_DATABASE"
            },
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"}
        )
        print(f"[OK] Crawler creado: {CRAWLER_NAME}")

def start_job_and_wait(job_name: str, args: dict):
    run_id = glue.start_job_run(JobName=job_name, Arguments=args)["JobRunId"]
    print(f"[RUN] {job_name} -> {run_id}")

    while True:
        jr = glue.get_job_run(JobName=job_name, RunId=run_id, PredecessorsIncluded=False)["JobRun"]
        state = jr["JobRunState"]  # STARTING, RUNNING, SUCCEEDED, FAILED, STOPPED, TIMEOUT
        if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
            print(f"[END] {job_name} -> {state}")
            if state != "SUCCEEDED":
                raise RuntimeError(f"Job {job_name} terminó en estado {state}: {jr.get('ErrorMessage')}")
            return
        time.sleep(20)

# =========================
# 1) Asegurar recursos: 3 jobs + crawler
# =========================

JOB1 = "bronze_to_silver_imat3a05"
JOB2 = "silver_to_gold_imat3a05"
JOB3 = "run_crawler_imat3a05"

# DefaultArguments que “inyectan” parámetros sin tener que ponerlos en consola cada vez
ensure_job(
    JOB1,
    SCRIPT_BRONZE_TO_SILVER,
    default_args={
        "--SRC_DB": SRC_DB,
        "--SRC_TABLE": SRC_TABLE,
        "--SILVER_TARGET_PATH": SILVER_PREFIX,
        "--WRITE_MODE": "append",
        "--PUSH_DOWN": PUSH_DOWN,
        "--PARTITION_COLS": PARTITION_COLS,
    }
)

ensure_job(
    JOB2,
    SCRIPT_SILVER_TO_GOLD,
    default_args={
        "--SILVER_SOURCE_PATH": SILVER_PREFIX,
        "--GOLD_TARGET_PATH": GOLD_PREFIX,
        "--WRITE_MODE": "append",
        "--ASSET_COL": "asset",  # <-- CAMBIA si es Asset
        "--TIME_COL": "time",    # <-- CAMBIA si en SILVER se llama Date/timestamp
        "--CLOSE_COL": "close",  # <-- CAMBIA si Close
        "--PARTITION_COLS": PARTITION_COLS,
    }
)

ensure_job(
    JOB3,
    SCRIPT_RUN_CRAWLER,
    default_args={
        "--CRAWLER_NAME": CRAWLER_NAME,
    }
)

ensure_crawler()

# =========================
# 2) Ejecutar flujo
# =========================
start_job_and_wait(JOB1, {})  # usa DefaultArguments
start_job_and_wait(JOB2, {})
start_job_and_wait(JOB3, {})  # lanza crawler único (desde el script run_crawler.py)

print("[DONE] Pipeline completo.")
