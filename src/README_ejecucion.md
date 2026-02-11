# End-to-end Runbook (src)

One-command flow: ensure bucket/prefixes, upload scripts, ingest bronze (complete.py), run Glue pipeline with 3 separate crawlers.

## Quick start
1) Set env vars:
   - AWS_REGION=eu-south-2
   - GLUE_JOB_ROLE_ARN=arn:aws:iam::<account-id>:role/<GlueJobRole>
   - GLUE_CRAWLER_ROLE_ARN=arn:aws:iam::<account-id>:role/<CrawlerRole>
   - AWS credentials in your session/profile.
2) Run from `src/`:
   ```
   python opencode_run_all.py
   ```

## What happens
1) Bucket/prefixes: ensure bucket exists, seed visual prefixes (bronze/silver/gold/scripts).
2) Upload scripts: job_bronze_to_silver.py, job_silver_to_gold_kpis.py, job_run_crawler.py, pipeline_launcher.py to `SCRIPTS_PREFIX` (see constants.py).
3) Ingest bronze: runs local `complete.py` (cache/retention) -> CSV to `bronze/Asset=SOLUSD/year=YYYY/month=MM/data.csv`.
4) Pipeline: runs local `pipeline_launcher.py` which creates/updates jobs + 3 crawlers and executes:
   crawler_bronze -> bronze_to_silver -> crawler_silver -> silver_to_gold_kpis -> crawler_gold.

## Customizing names/paths
- Edit `constants.py`:
  - Bucket/region: `BUCKET`, `REGION`
  - Prefixes: `BRONZE_PREFIX` (`bronze/`), `SILVER_PREFIX` (`silver/`), `GOLD_PREFIX` (`gold/`)
  - Crawlers: `CRAWLER_BRONZE`, `CRAWLER_SILVER`, `CRAWLER_GOLD`
  - Glue IDs: `GLUE_DB`, `TABLE_PREFIX`, job names
  - Columns/partition: `ASSET_COL`, `TIME_COL`, `CLOSE_COL`, `PARTITION_COLS`, `DEFAULT_ASSET`
- Env-only: `GLUE_JOB_ROLE_ARN`, `GLUE_CRAWLER_ROLE_ARN` (do not hardcode ARNs).

## Manual alternative (if you don't run opencode_run_all)
1) Run `complete.py` to ingest bronze.
2) Run `pipeline_launcher.py` to do crawler_bronze -> bronze_to_silver -> crawler_silver -> silver_to_gold_kpis -> crawler_gold.

## Outputs
- Silver: Parquet partitioned asset/year/month in `s3://BUCKET/silver/`.
- Gold: Parquet with KPIs (SMA200, EMA50, RSI14, MACD) partitioned asset/year/month in `s3://BUCKET/gold/`.
- Glue Catalog: lake_bronze, lake_silver, lake_gold in DB `GLUE_DB` (default trade_data_imat3a05) with prefix `TABLE_PREFIX` (default lake_).
