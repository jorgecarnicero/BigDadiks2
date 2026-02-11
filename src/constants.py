"""
Central constants for the end-to-end opencode pipeline (testing sandbox).
Adjust values here to change names/paths without touching the rest of the code.
"""

import os

# Region and bucket
REGION = "eu-south-2"
BUCKET = "trade-data-big-daddyks-main"

# Prefixes for layers
BRONZE_PREFIX = f"s3://{BUCKET}/bronze/"
SILVER_PREFIX = f"s3://{BUCKET}/silver/prices/"
GOLD_PREFIX = f"s3://{BUCKET}/gold/indicators/"

# Database and crawler
GLUE_DB = "trade_data_imat3a05"
CRAWLER_NAME = "lake_imat3a05"
TABLE_PREFIX = "lake_"

# Jobs
JOB_BRONZE_TO_SILVER = "bronze_to_silver_imat3a05"
JOB_SILVER_TO_GOLD = "silver_to_gold_imat3a05"
JOB_RUN_CRAWLER = "run_crawler_imat3a05"

# Scripts location in S3
SCRIPTS_PREFIX = f"s3://{BUCKET}/scripts/"
SCRIPT_BRONZE_TO_SILVER = SCRIPTS_PREFIX + "job_bronze_to_silver.py"
SCRIPT_SILVER_TO_GOLD = SCRIPTS_PREFIX + "job_silver_to_gold_kpis.py"
SCRIPT_RUN_CRAWLER = SCRIPTS_PREFIX + "job_run_crawler.py"
SCRIPT_PIPELINE_LAUNCHER = SCRIPTS_PREFIX + "pipeline_launcher.py"

# Columns and partitions
ASSET_COL = "asset"
TIME_COL = "datetime"
CLOSE_COL = "close"
PARTITION_COLS = "asset,year,month"
DEFAULT_ASSET = "SOLUSD"

# Roles from environment (do not hardcode ARNs)
GLUE_JOB_ROLE_ARN = os.environ.get("GLUE_JOB_ROLE_ARN", "")
GLUE_CRAWLER_ROLE_ARN = os.environ.get("GLUE_CRAWLER_ROLE_ARN", "")
