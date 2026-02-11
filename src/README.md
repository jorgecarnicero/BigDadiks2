# src (one-click pipeline)

One-command pipeline: bucket setup, upload scripts to S3, ingest bronze (complete.py with cache/retention), run Glue pipeline with 3 separate crawlers (one per layer).

## How to run
1) Export env vars:
   - AWS_REGION=eu-south-2
   - GLUE_JOB_ROLE_ARN=arn:aws:iam::<account-id>:role/<GlueJobRole>
   - GLUE_CRAWLER_ROLE_ARN=arn:aws:iam::<account-id>:role/<CrawlerRole>
   - AWS credentials in your session/profile.
2) From `src/`:
   ```
   python opencode_run_all.py
   ```

## What it does
1) Ensures bucket and prefixes (bronze/silver/gold/scripts) exist.
2) Uploads job scripts to `SCRIPTS_PREFIX` (see constants.py).
3) Runs `complete.py` (ingest bronze CSV with cache/retention) -> `bronze/Asset=SOLUSD/...`.
4) Runs `pipeline_launcher.py`:
   - Creates 3 crawlers (one per layer: bronze, silver, gold).
   - Executes: crawler_bronze -> bronze_to_silver -> crawler_silver -> silver_to_gold_kpis -> crawler_gold.

## Customize names/paths
Edit `constants.py`:
- Bucket/region: BUCKET, REGION
- Prefixes: BRONZE_PREFIX (`silver/`), SILVER_PREFIX (`silver/`), GOLD_PREFIX (`gold/`)
- Crawlers: CRAWLER_BRONZE, CRAWLER_SILVER, CRAWLER_GOLD
- Glue IDs: GLUE_DB, TABLE_PREFIX, job names
- Columns/partition: ASSET_COL, TIME_COL, CLOSE_COL, PARTITION_COLS, DEFAULT_ASSET
Env only: GLUE_JOB_ROLE_ARN, GLUE_CRAWLER_ROLE_ARN (do not hardcode ARNs).

## Files in `src/`
- constants.py: all config knobs
- opencode_run_all.py: one-click runner
- job_bronze_to_silver.py: bronze -> silver (Parquet) with ASSET fallback
- job_silver_to_gold_kpis.py: KPIs -> gold
- job_run_crawler.py: ensure/run a single crawler (one per layer)
- pipeline_launcher.py: ensure jobs + 3 crawlers and execute pipeline
- complete.py: TradingView ingestion to bronze

## Outputs
- Silver: Parquet partitioned by asset/year/month in `s3://BUCKET/silver/`
- Gold: Parquet with KPIs (SMA200, EMA50, RSI14, MACD) partitioned by asset/year/month in `s3://BUCKET/gold/`
- Glue Catalog: lake_bronze, lake_silver, lake_gold in DB GLUE_DB (default trade_data_imat3a05), prefix TABLE_PREFIX (default lake_)
