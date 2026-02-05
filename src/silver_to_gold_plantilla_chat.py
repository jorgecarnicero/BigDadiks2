from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, DoubleType
)

# ============================================================
# PLACEHOLDERS (buscar y reemplazar cuando tengáis el esquema)
# ============================================================
DB_NAME = "trade_data_[RELLENAR:grupo]"     # HU3: trade_data_<grupo>
TABLE_NAME = "[RELLENAR:tabla_sol]"         # HU3: nombre tabla con referencia a la cripto (ej. sol / solusd)

# Columnas "súper comunes" (placeholders):
SYMBOL_COL = "symbol"
DATE_COL = "date"        # fecha (diaria) o timestamp -> lo normalizamos a date
PRICE_COL = "close"      # precio usado para KPIs

# Opcional: si existiera una columna de ingesta/actualización para resolver duplicados
INGEST_TS_COL = "ingest_ts"  # placeholder (si no existe, se ignora)

# Constante del proyecto (solo SOLUSD)
SYMBOL_VALUE = "SOLUSD"

# Gestión de huecos:
FILL_MISSING_DAYS = False         # True si queréis "crear" días faltantes
FORWARD_FILL_PRICE = False        # True si además queréis arrastrar el último close conocido (si FILL_MISSING_DAYS=True)

# ============================================================
# (3) KPIs: Parámetros
# HU4 fija SMA200 y EMA50.
# RSI y MACD: NO están parametrizados en el PDF => placeholders.
# Defaults "típicos" (contexto general, NO confirmado por materiales):
#   RSI(14), MACD(12,26,9)
# Cambiadlos si seguís otra convención: [RELLENAR: parámetros RSI/MACD]
# ============================================================
SMA_N = 200
EMA_N = 50

RSI_PERIOD = 14          # [RELLENAR: parámetros RSI]
MACD_FAST = 12           # [RELLENAR: parámetros MACD]
MACD_SLOW = 26
MACD_SIGNAL = 9

# ============================================================
# (4) ORO: Escritura a S3
# HU4 pide almacenar los indicadores en la capa Oro (carpeta /oro/). :contentReference[oaicite:1]{index=1}
# ============================================================
S3_ORO_PREFIX = "s3://[RELLENAR:bucket_name]/[RELLENAR:prefijo_base]/oro/"  # placeholder

WRITE_MODE = "overwrite"  # "overwrite" o "append" (decisión de equipo)
PARTITION_BY_SYMBOL = True
PARTITION_BY_YEAR_MONTH = True   # recomendado para consultas (Athena/Catalog), pero no obligatorio en HU4

# ============================================================


def ensure_symbol(df):
    """Garantiza una columna symbol (aunque solo haya una cripto)."""
    if SYMBOL_COL not in df.columns:
        df = df.withColumn(SYMBOL_COL, F.lit(SYMBOL_VALUE))
    else:
        df = df.filter(F.col(SYMBOL_COL) == SYMBOL_VALUE)
    return df


def cast_date(df):
    """
    Normaliza la columna de fecha a tipo date.
    Si DATE_COL viene en un formato no parseable, ajustad el parseo.
    """
    return df.withColumn(DATE_COL, F.to_date(F.col(DATE_COL)))


def deduplicate(df):
    """
    Elimina duplicados por (symbol, date).
    - si existe INGEST_TS_COL: nos quedamos con el registro más reciente
    - si no: dropDuplicates(symbol,date)
    """
    key_cols = [SYMBOL_COL, DATE_COL]

    if INGEST_TS_COL in df.columns:
        w = Window.partitionBy(*key_cols).orderBy(F.col(INGEST_TS_COL).desc())
        df = (
            df.withColumn("_rn", F.row_number().over(w))
              .filter(F.col("_rn") == 1)
              .drop("_rn")
        )
    else:
        df = df.dropDuplicates(key_cols)

    return df


def fill_missing_days(df):
    """
    (Opcional) Rellena huecos de días creando filas para fechas faltantes.
    - Crea calendario diario entre min y max.
    - Left join con datos reales.
    - Si FORWARD_FILL_PRICE=True: forward fill del close.
    """
    bounds = df.groupBy(SYMBOL_COL).agg(
        F.min(DATE_COL).alias("min_date"),
        F.max(DATE_COL).alias("max_date")
    )

    cal = bounds.select(
        SYMBOL_COL,
        F.explode(F.sequence(F.col("min_date"), F.col("max_date"), F.expr("interval 1 day"))).alias(DATE_COL)
    )

    df2 = cal.join(df, on=[SYMBOL_COL, DATE_COL], how="left")

    if FORWARD_FILL_PRICE:
        w = Window.partitionBy(SYMBOL_COL).orderBy(F.col(DATE_COL)).rowsBetween(Window.unboundedPreceding, 0)
        df2 = df2.withColumn(
            PRICE_COL,
            F.last(F.col(PRICE_COL), ignorenulls=True).over(w)
        )

    return df2


# =========================
# (1) LECTURA DESDE PLATA (CATALOG)
# =========================
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

dyf_plata = glueContext.create_dynamic_frame.from_catalog(
    database=DB_NAME,
    table_name=TABLE_NAME
)
df_plata = dyf_plata.toDF()

# ==========================================
# (2) PREPARACIÓN PARA SERIES TEMPORALES
# ==========================================
df_ts = ensure_symbol(df_plata)
df_ts = cast_date(df_ts)

required = [SYMBOL_COL, DATE_COL, PRICE_COL]
missing = [c for c in required if c not in df_ts.columns]
if missing:
    raise ValueError(
        f"Faltan columnas mínimas en Plata (placeholders): {missing}. "
        f"Actualiza SYMBOL_COL/DATE_COL/PRICE_COL al esquema real cuando lo tengáis."
    )

df_ts = deduplicate(df_ts)

if FILL_MISSING_DAYS:
    df_ts = fill_missing_days(df_ts)

df_ts = df_ts.orderBy(SYMBOL_COL, DATE_COL)

# =========================
# (3) CÁLCULO DE KPIs
# =========================

# 3.1 SMA200 en Spark (ventana por symbol ordenada por date)
w_sma = Window.partitionBy(SYMBOL_COL).orderBy(F.col(DATE_COL)).rowsBetween(-(SMA_N - 1), 0)
df_kpi_base = df_ts.select(SYMBOL_COL, DATE_COL, PRICE_COL).withColumn(
    f"sma_{SMA_N}",
    F.avg(F.col(PRICE_COL).cast("double")).over(w_sma)
)

# 3.2 EMA50 + RSI + MACD con applyInPandas (recursivos / ewm)
def indicators_pandas(pdf):
    import pandas as pd
    import numpy as np

    pdf = pdf.sort_values(DATE_COL)
    close = pd.to_numeric(pdf[PRICE_COL], errors="coerce")

    # EMA(EMA_N)
    pdf[f"ema_{EMA_N}"] = close.ewm(span=EMA_N, adjust=False).mean()

    # RSI(RSI_PERIOD) - suavizado tipo Wilder
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)

    alpha = 1.0 / float(RSI_PERIOD)
    avg_gain = gain.ewm(alpha=alpha, adjust=False).mean()
    avg_loss = loss.ewm(alpha=alpha, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    pdf["rsi"] = 100.0 - (100.0 / (1.0 + rs))

    # MACD(MACD_FAST, MACD_SLOW, MACD_SIGNAL)
    ema_fast = close.ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow = close.ewm(span=MACD_SLOW, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    macd_signal = macd_line.ewm(span=MACD_SIGNAL, adjust=False).mean()
    macd_hist = macd_line - macd_signal

    pdf["macd"] = macd_line
    pdf["macd_signal"] = macd_signal
    pdf["macd_hist"] = macd_hist

    out_cols = [
        SYMBOL_COL, DATE_COL, PRICE_COL,
        f"sma_{SMA_N}", f"ema_{EMA_N}",
        "rsi", "macd", "macd_signal", "macd_hist"
    ]
    return pdf[out_cols]

schema_out = StructType([
    StructField(SYMBOL_COL, StringType(), True),
    StructField(DATE_COL, DateType(), True),
    StructField(PRICE_COL, DoubleType(), True),
    StructField(f"sma_{SMA_N}", DoubleType(), True),
    StructField(f"ema_{EMA_N}", DoubleType(), True),
    StructField("rsi", DoubleType(), True),
    StructField("macd", DoubleType(), True),
    StructField("macd_signal", DoubleType(), True),
    StructField("macd_hist", DoubleType(), True),
])

df_kpi = df_kpi_base.groupBy(SYMBOL_COL).applyInPandas(indicators_pandas, schema=schema_out)

# =========================
# (4) ESCRITURA A CAPA ORO
# =========================
# Dataset final con claves + precio + KPIs (tal como pide HU4). :contentReference[oaicite:2]{index=2}
df_out = df_kpi.select(
    SYMBOL_COL,
    DATE_COL,
    PRICE_COL,
    f"sma_{SMA_N}",
    f"ema_{EMA_N}",
    "rsi",
    "macd",
    "macd_signal",
    "macd_hist"
)

# Particionado recomendado (no obligatorio en HU4): symbol y/o year-month
partition_cols = []
if PARTITION_BY_SYMBOL:
    partition_cols.append(SYMBOL_COL)

if PARTITION_BY_YEAR_MONTH:
    df_out = df_out.withColumn("year_month", F.date_format(F.col(DATE_COL), "yyyy-MM"))
    partition_cols.append("year_month")

writer = df_out.write.mode(WRITE_MODE).format("parquet")

if partition_cols:
    writer = writer.partitionBy(*partition_cols)

# Escritura en S3 (capa Oro) - S3 almacena objetos en buckets :contentReference[oaicite:3]{index=3}
writer.save(S3_ORO_PREFIX)

# df_out ya está en Oro (S3). Si luego queréis consultarlo con Athena, normalmente catalogáis Oro con un Crawler (HU3).
