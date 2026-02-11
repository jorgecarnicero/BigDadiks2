import sys
import pandas as pd
import numpy as np

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import pandas_udf, PandasUDFType

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "SILVER_SOURCE_PATH",
        "GOLD_TARGET_PATH",
        "WRITE_MODE",
        "ASSET_COL",
        "TIME_COL",
        "CLOSE_COL",
        "PARTITION_COLS",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

silver_path = args["SILVER_SOURCE_PATH"].rstrip("/") + "/"
gold_path = args["GOLD_TARGET_PATH"].rstrip("/") + "/"
write_mode = (args.get("WRITE_MODE") or "append").lower()

# Defaults inlined so the script is self-contained in Glue
asset_col = args.get("ASSET_COL") or "asset"
time_col = args.get("TIME_COL") or "datetime"
close_col = args.get("CLOSE_COL") or "close"
partition_cols = [c.strip() for c in (args.get("PARTITION_COLS") or "asset,year,month").split(",") if c.strip()]

spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

df = spark.read.parquet(silver_path)

missing = [c for c in [asset_col, time_col, close_col] if c not in df.columns]
if missing:
    raise Exception(f"Faltan columnas en SILVER: {missing}. Ajusta ASSET_COL/TIME_COL/CLOSE_COL.")

df = (
    df.withColumn(time_col, F.to_timestamp(F.col(time_col)))
      .withColumn(close_col, F.col(close_col).cast("double"))
)

out_schema = StructType(df.schema.fields + [
    StructField("sma_200", DoubleType()),
    StructField("ema_50", DoubleType()),
    StructField("rsi_14", DoubleType()),
    StructField("macd", DoubleType()),
    StructField("macd_signal", DoubleType()),
    StructField("macd_hist", DoubleType()),
])


@pandas_udf(out_schema, PandasUDFType.GROUPED_MAP)
def add_kpis(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values(time_col).copy()
    close = pdf[close_col].astype(float)

    pdf["sma_200"] = close.rolling(window=200, min_periods=200).mean()
    pdf["ema_50"] = close.ewm(span=50, adjust=False, min_periods=50).mean()

    delta = close.diff()
    gain = np.where(delta > 0, delta, 0.0)
    loss = np.where(delta < 0, -delta, 0.0)

    gain_ewm = pd.Series(gain).ewm(alpha=1/14, adjust=False, min_periods=14).mean()
    loss_ewm = pd.Series(loss).ewm(alpha=1/14, adjust=False, min_periods=14).mean()

    rs = gain_ewm / (loss_ewm.replace(0, np.nan))
    pdf["rsi_14"] = 100 - (100 / (1 + rs))

    ema12 = close.ewm(span=12, adjust=False, min_periods=26).mean()
    ema26 = close.ewm(span=26, adjust=False, min_periods=26).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False, min_periods=35).mean()
    hist = macd - signal

    pdf["macd"] = macd
    pdf["macd_signal"] = signal
    pdf["macd_hist"] = hist

    return pdf


out = df.groupBy(asset_col).apply(add_kpis)

(out.write
    .format("parquet")
    .mode(write_mode)
    .partitionBy(*partition_cols)
    .save(gold_path))

job.commit()
