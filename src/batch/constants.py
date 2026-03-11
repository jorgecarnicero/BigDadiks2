"""
Central constants for the end-to-end pipeline.
Adjust values here to change names/paths without touching the rest of the code.
"""

import os

# Region and bucket
REGION = "eu-south-2"
BUCKET = "trade-data-big-daddyks-main"

# Prefixes for layers
BRONZE_PREFIX = f"s3://{BUCKET}/bronze/"
SILVER_PREFIX = f"s3://{BUCKET}/silver/"
GOLD_PREFIX = f"s3://{BUCKET}/gold/"

# Database
GLUE_DB = "trade_data_imat3a05"
TABLE_PREFIX = "lake_"

# Crawlers (one per layer)
CRAWLER_BRONZE = "lake_bronze_imat3a05"
CRAWLER_SILVER = "lake_silver_imat3a05"
CRAWLER_GOLD = "lake_gold_imat3a05"

# Jobs
JOB_BRONZE_TO_SILVER = "bronze_to_silver_imat3a05"
JOB_SILVER_TO_GOLD = "silver_to_gold_imat3a05"
JOB_RUN_CRAWLER = "run_crawler_imat3a05"

# Scripts location in S3
SCRIPTS_PREFIX = f"s3://{BUCKET}/scripts/"
SCRIPT_BRONZE_TO_SILVER = SCRIPTS_PREFIX + "job_bronze_to_silver.py"
SCRIPT_SILVER_TO_GOLD = SCRIPTS_PREFIX + "job_silver_to_gold_kpis.py"
SCRIPT_RUN_CRAWLER = SCRIPTS_PREFIX + "job_run_crawler.py"

# Columns and partitions
ASSET_COL = "asset"
TIME_COL = "datetime"
CLOSE_COL = "close"
PARTITION_COLS = "asset,year,month"
DEFAULT_ASSET = "SOLUSD"

# Roles from environment (do not hardcode ARNs)
GLUE_JOB_ROLE_ARN = os.environ.get("GLUE_JOB_ROLE_ARN", "")
GLUE_CRAWLER_ROLE_ARN = os.environ.get("GLUE_CRAWLER_ROLE_ARN", "")
