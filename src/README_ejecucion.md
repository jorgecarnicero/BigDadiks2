# End-to-end Runbook (testing/opencode)

One-command flow: ensure bucket/prefixes, upload scripts, ingest bronze (complete.py), run Glue pipeline (crawler -> bronze_to_silver -> silver_to_gold_kpis -> crawler).

## Quick start
1) Set env vars:
   - AWS_REGION=eu-south-2
   - GLUE_JOB_ROLE_ARN=arn:aws:iam::<account-id>:role/<GlueJobRole>
   - GLUE_CRAWLER_ROLE_ARN=arn:aws:iam::<account-id>:role/<CrawlerRole>
   - AWS credentials in your session/profile.
2) Run:
   ```
   cd testing/opencode
   python opencode_run_all.py
   ```

## What happens
1) Bucket/prefixes: ensure bucket exists, seed visual prefixes (bronze/silver/gold/scripts).
2) Upload scripts: job_bronze_to_silver.py, job_silver_to_gold_kpis.py, job_run_crawler.py, pipeline_launcher.py to `SCRIPTS_PREFIX` (see constants.py).
3) Ingest bronze: runs local `complete.py` (cache/retention) -> CSV to `bronze/asset=SOLUSD/year=YYYY/month=MM/data.csv`.
4) Pipeline: runs local `pipeline_launcher.py` which creates/updates jobs + crawler and executes: crawler -> bronze_to_silver -> crawler -> silver_to_gold_kpis -> crawler.

## Customizing names/paths
- Edit `constants.py` in this folder:
  - Bucket/region: `BUCKET`, `REGION`
  - Prefixes: `BRONZE_PREFIX`, `SILVER_PREFIX`, `GOLD_PREFIX`, `SCRIPTS_PREFIX`
  - Glue IDs: `GLUE_DB`, `CRAWLER_NAME`, `TABLE_PREFIX`, job names
  - Columns/partition: `ASSET_COL`, `TIME_COL`, `CLOSE_COL`, `PARTITION_COLS`, `DEFAULT_ASSET`
- Env-only: `GLUE_JOB_ROLE_ARN`, `GLUE_CRAWLER_ROLE_ARN` (do not hardcode ARNs).

## Manual alternative (if you don’t run opencode_run_all)
1) Run `complete.py` to ingest bronze.
2) Run `pipeline_launcher.py` to do crawler -> bronze_to_silver -> crawler -> silver_to_gold_kpis -> crawler.

## Outputs
- Silver: Parquet partitioned asset/year/month.
- Gold: Parquet with KPIs (SMA200, EMA50, RSI14, MACD) partitioned asset/year/month.
- Glue Catalog: lake_bronze, lake_silver, lake_gold in DB `GLUE_DB` (default trade_data_imat3a05) with prefix `TABLE_PREFIX` (default lake_).
