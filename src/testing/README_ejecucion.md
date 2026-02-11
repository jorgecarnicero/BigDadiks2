# End-to-end Runbook (opencode)

This complements README.md with clearer execution steps and what each part does.

## Prerequisites
- Env vars: see `requisitos.txt` (`AWS_REGION`, `GLUE_JOB_ROLE_ARN`, `GLUE_CRAWLER_ROLE_ARN`).
- AWS credentials available in your environment.
- Scripts uploaded to S3 at `SCRIPTS_PREFIX` (default `s3://trade-data-big-daddyks-main/scripts/opencode/`).

## Step 0: Bucket and scripts
- Run `setup_bucket_and_scripts.py` locally (uses boto3 + creds):
  - Creates bucket if missing.
  - Seeds visual prefixes: bronze/, silver/, gold/, scripts/, scripts/opencode/, silver/prices/, gold/indicators/.
  - Uploads local job scripts to S3 `scripts/opencode/`.

## Step 1: Ingest bronze (CSV with cache/retention)
- Use your existing `complete.py` to download SOLUSD and upload CSVs to `bronze/asset=SOLUSD/year=YYYY/month=MM/data.csv`.
- Keep the cache/retention behavior; ensure partition prefix is `asset=` (lowercase).

## Step 2: Create/refresh Glue resources and run pipeline
### Recommended: `pipeline_launcher.py` (one command/job)
- Ensures/updates Glue jobs and crawler, then runs in order:
  1) Crawler (bronze/silver/gold)
  2) job_bronze_to_silver (CSV → Parquet silver)
  3) Crawler
  4) job_silver_to_gold_kpis (KPIs → Parquet gold)
  5) Crawler

### Manual alternative
1) job_run_crawler.py (targets bronze/silver/gold; TablePrefix=lake_)
2) job_bronze_to_silver.py (SRC_DB=trade_data_imat3a05, SRC_TABLE=lake_bronze, PARTITION_COLS asset,year,month, ASSET_DEFAULT=SOLUSD)
3) job_run_crawler.py
4) job_silver_to_gold_kpis.py (SILVER_SOURCE_PATH=s3://.../silver/prices/, GOLD_TARGET_PATH=s3://.../gold/indicators/)
5) job_run_crawler.py

## Outputs
- Silver: Parquet partitioned by asset/year/month.
- Gold: Parquet with KPIs (SMA200, EMA50, RSI14, MACD) partitioned by asset/year/month.
- Glue Catalog tables (TablePrefix=lake_): lake_bronze, lake_silver, lake_gold in DB `trade_data_imat3a05`.
