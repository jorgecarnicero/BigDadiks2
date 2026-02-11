# bronze_to_silver.py
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# =========================
# CAMBIOS IMPORTANTES AQUÍ:
# =========================
# Lo ideal es NO tocar el código y pasar argumentos al job.
# Aun así, lo que depende de vuestros nombres es:
# - SRC_DB (ya lo sabes: trade_data_imat3a05)
# - SRC_TABLE (nombre exacto de la tabla bronze en Glue)
# - SILVER_TARGET_PATH (ruta S3 destino para silver)
# - PARTITION_COLS (asset vs Asset y demás)
# - Nombre columna temporal (time/date/Date/...) si quieres castear timestamp

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "SRC_DB",
    "SRC_TABLE",
    "SILVER_TARGET_PATH",
    "WRITE_MODE",
    "PUSH_DOWN",
    "PARTITION_COLS"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

src_db = args["SRC_DB"]  # <-- EJ: trade_data_imat3a05
src_table = args["SRC_TABLE"]  # <-- CAMBIA: nombre exacto tabla bronze
silver_target = args["SILVER_TARGET_PATH"].rstrip("/") + "/"  # <-- CAMBIA: s3://.../silver/<dataset>/
write_mode = (args["WRITE_MODE"] or "append").lower()
push_down = (args.get("PUSH_DOWN") or "").strip()
partition_cols = [c.strip() for c in (args.get("PARTITION_COLS") or "asset,year,month").split(",") if c.strip()]

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# 1) Leer BRONZE desde el catálogo (tabla creada por crawler)
read_kwargs = {"database": src_db, "table_name": src_table}
if push_down:
    # Ejemplo pushdown: "(asset=='SOLUSD' and year==2022 and month==2)"
    read_kwargs["push_down_predicate"] = push_down

dyf = glueContext.create_dynamic_frame_from_catalog(**read_kwargs)
df = dyf.toDF().dropna(how="all")

# 2) Casts mínimos para que SILVER quede “usable”
#    ⚠️ CAMBIA "time" si tu columna temporal se llama distinto:
TIME_COL_CANDIDATES = ["time", "timestamp", "date", "datetime", "Date"]
time_col = next((c for c in TIME_COL_CANDIDATES if c in df.columns), None)
if time_col:
    df = df.withColumn(time_col, F.to_timestamp(F.col(time_col)))

# OHLCV típicos (ajusta a nombres reales si difieren)
for c in ["open", "high", "low", "close", "volume"]:
    if c in df.columns:
        df = df.withColumn(c, F.col(c).cast("double"))

# year/month como int (si existen)
for p in ["year", "month"]:
    if p in df.columns:
        df = df.withColumn(p, F.col(p).cast("int"))

# metadata
df = df.withColumn("_ingest_ts", F.current_timestamp())

# 3) Escribir SILVER en Parquet particionado
#    ⚠️ IMPORTANTE: partition_cols debe coincidir con las columnas reales del DF (asset vs Asset)
(df.write
  .format("parquet")
  .mode(write_mode)
  .partitionBy(*partition_cols)
  .save(silver_target))

job.commit()
