import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F


_REQUIRED = ["JOB_NAME", "SRC_DB", "SRC_TABLE", "SILVER_TARGET_PATH",
             "WRITE_MODE", "PARTITION_COLS", "ASSET_DEFAULT"]
# PUSH_DOWN is optional; only resolve it if actually passed on the command line
_OPTIONAL = [k for k in ["PUSH_DOWN"] if f"--{k}" in sys.argv]
args = getResolvedOptions(sys.argv, _REQUIRED + _OPTIONAL)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Defaults (same values as constants.py but inlined so the script is self-contained in Glue)
_ASSET_COL = "asset"
_TIME_COL = "datetime"
_CLOSE_COL = "close"
_PARTITION_COLS = "asset,year,month"
_DEFAULT_ASSET = "SOLUSD"

src_db = args["SRC_DB"]
src_table = args["SRC_TABLE"]
silver_target = args["SILVER_TARGET_PATH"].rstrip("/") + "/"
write_mode = (args.get("WRITE_MODE") or "append").lower()
push_down = (args.get("PUSH_DOWN") or "").strip()
partition_cols = [c.strip() for c in (args.get("PARTITION_COLS") or _PARTITION_COLS).split(",") if c.strip()]
asset_default = (args.get("ASSET_DEFAULT") or _DEFAULT_ASSET).strip()

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

read_kwargs = {"database": src_db, "table_name": src_table}
if push_down:
    read_kwargs["push_down_predicate"] = push_down

# Load bronze table from Glue Catalog
dyf = glueContext.create_dynamic_frame_from_catalog(**read_kwargs)
df = dyf.toDF().dropna(how="all")

# Normalize datetime column
time_col_candidates = [_TIME_COL, "time", "timestamp", "date", "datetime", "Date"]
time_col = next((c for c in time_col_candidates if c in df.columns), None)
if time_col:
    df = df.withColumn(_TIME_COL, F.to_timestamp(F.col(time_col)))
    if _TIME_COL != time_col:
        df = df.drop(time_col)
else:
    raise Exception(f"No se encontró columna temporal en BRONZE. Busca alguna de {time_col_candidates}.")

# Normalize close price column
close_col_candidates = [_CLOSE_COL, "Close", "close"]
close_col = next((c for c in close_col_candidates if c in df.columns), None)
if close_col:
    df = df.withColumn(_CLOSE_COL, F.col(close_col).cast("double"))
    if _CLOSE_COL != close_col:
        df = df.drop(close_col)
else:
    raise Exception(f"No se encontró columna de cierre en BRONZE. Busca alguna de {close_col_candidates}.")

# Normalize asset (from column or default fallback)
if _ASSET_COL not in df.columns:
    if "asset" in df.columns:
        df = df.withColumnRenamed("asset", _ASSET_COL)
    elif "Asset" in df.columns:
        df = df.withColumn(_ASSET_COL, F.col("Asset"))
    else:
        df = df.withColumn(_ASSET_COL, F.lit(asset_default))

for p in ["year", "month"]:
    if p in df.columns:
        df = df.withColumn(p, F.col(p).cast("int"))

df = df.withColumn("_ingest_ts", F.current_timestamp())

(df.write
  .format("parquet")
  .mode(write_mode)
  .partitionBy(*partition_cols)
  .save(silver_target))

job.commit()
