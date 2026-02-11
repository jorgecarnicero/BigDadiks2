# Pipeline HU4 (opencode)

Objetivo: leer CSV de bronze (catalogados), escribir Parquet en silver con misma partición `asset/year/month`, calcular KPIs y escribir Parquet en gold, con un crawler único.

## Estructura
- `settings.py`: parámetros centrales (bucket, DB, nombres de jobs/crawler, prefijos, columnas, particiones). ARN se leen de env.
- `job_bronze_to_silver.py`: lee tabla bronze del catálogo, normaliza `datetime` y `close`, deriva `asset` si falta, escribe Parquet particionado en silver.
- `job_silver_to_gold_kpis.py`: lee Parquet silver, valida columnas, calcula KPIs (SMA200, EMA50, RSI14, MACD) por asset, escribe Parquet particionado en gold.
- `job_run_crawler.py`: asegura/lanza crawler único con targets bronze/silver/gold.
- `pipeline_launcher.py`: crea/actualiza jobs y crawler y ejecuta el flujo completo.
- `requisitos.txt`: variables de entorno necesarias.
- `setup_bucket_and_scripts.py`: crea bucket (si falta), siembra prefijos y sube scripts a S3.

## Requisitos de entorno
Ver `requisitos.txt`. Subir los scripts a `s3://trade-data-big-daddyks-main/scripts/opencode/` o ajusta `SCRIPTS_PREFIX` en `settings.py`.

## Orden de ejecución (Glue)
1. Ejecuta `pipeline_launcher.py` (Job Glue o local con creds) para crear/actualizar recursos.
2. Flujo automático en launcher: crawler → job_bronze_to_silver → crawler → job_silver_to_gold_kpis → crawler.

## Args clave (jobs)
- `job_bronze_to_silver`: `SRC_DB`, `SRC_TABLE` (ej. `lake_bronze`), `SILVER_TARGET_PATH`, `WRITE_MODE`, `PUSH_DOWN`, `PARTITION_COLS`.
- `job_silver_to_gold_kpis`: `SILVER_SOURCE_PATH`, `GOLD_TARGET_PATH`, `WRITE_MODE`, `ASSET_COL`, `TIME_COL`, `CLOSE_COL`, `PARTITION_COLS`.
- `job_run_crawler`: `CRAWLER_NAME`, `CRAWLER_DB`, `CRAWLER_ROLE_ARN`, `S3_TARGETS`, `TABLE_PREFIX`, `WAIT`.

## Supuestos
- Crawler único `lake_imat3a05`, DB `trade_data_imat3a05`, TablePrefix `lake_`.
- Prefijos: bronze/, silver/prices/, gold/indicators/.
- Particiones: `asset/year/month` en silver y gold.
- Columna tiempo: `datetime`; cierre: `close`; `asset` se deriva si no viene en el CSV (se espera partición `asset=` en bronze).
