#!/usr/bin/env python3
import boto3
import sys
import os 

# ==========================================
# 🛑 DYNAMIC CONFIGURATION
# ==========================================
# Boto3 retrieves credentials (KEYS) automatically from the environment.
# We only need to read the Region and the Role ARN from environment variables.

AWS_REGION = os.environ.get("AWS_REGION", "eu-south-2") 
GLUE_ROLE_ARN = os.environ.get("GLUE_ROLE_ARN") 

def run_glue_process(group_id: str, bucket_name: str):
    
    # 1. Security Validation
    if not GLUE_ROLE_ARN:
        print("❌ CRITICAL ERROR: Environment variable 'GLUE_ROLE_ARN' is not defined.")
        print("   I need to know which IAM Role the Crawler should use.")
        print("   Run in your terminal: export GLUE_ROLE_ARN='arn:aws:iam::...YourRole...'")
        sys.exit(1)

    # 2. Initialize Client
    try:
        glue_client = boto3.client('glue', region_name=AWS_REGION)
    except Exception as e:
        print(f"❌ Error connecting to AWS. Check your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY. Details: {e}")
        sys.exit(1)

    # Normalized Names
    # We keep the DB name generic so all layers (bronze/silver/gold) can live in the same DB later
    db_name = f"trade_data_{group_id}".replace("-", "_")
    
    # The crawler is specific to the Bronze layer
    crawler_name = f"crawler_{group_id}_bronze"
    
    # IMPORTANT: Target specifically the 'bronze' folder
    s3_target_path = f"s3://{bucket_name}/bronze/"

    print(f"🔄 [GLUE] Starting management in Region: {AWS_REGION}")
    print(f"   Using Role: ...{GLUE_ROLE_ARN.split('/')[-1]}") 
    print(f"   Target Path: {s3_target_path}")

    # ---------------------------------------------------------
    # STEP A: Database
    # ---------------------------------------------------------
    try:
        glue_client.create_database(
            DatabaseInput={'Name': db_name, 'Description': 'Data Lakehouse - Trade Data'}
        )
        print(f"✅ [GLUE] Database '{db_name}' created.")
    except glue_client.exceptions.AlreadyExistsException:
        print(f"ℹ️ [GLUE] Database '{db_name}' already exists.")

    # ---------------------------------------------------------
    # STEP B: Crawler (Create or Update)
    # ---------------------------------------------------------
    targets = {'S3Targets': [{'Path': s3_target_path}]}
    
    try:
        glue_client.get_crawler(Name=crawler_name)
        # If it exists, update it
        glue_client.update_crawler(
            Name=crawler_name,
            Role=GLUE_ROLE_ARN,
            DatabaseName=db_name,
            Targets=targets
        )
        print(f"ℹ️ [GLUE] Crawler '{crawler_name}' updated.")
    except glue_client.exceptions.EntityNotFoundException:
        # If it doesn't exist, create it
        print(f"🔨 [GLUE] Creating Crawler '{crawler_name}'...")
        glue_client.create_crawler(
            Name=crawler_name,
            Role=GLUE_ROLE_ARN,
            DatabaseName=db_name,
            Targets=targets,
            # Policy to update tables if new partitions (months) are added
            SchemaChangePolicy={'DeleteBehavior': 'DEPRECATE_IN_DATABASE', 'UpdateBehavior': 'UPDATE_IN_DATABASE'},
            RecrawlPolicy={'RecrawlBehavior': 'CRAWL_EVERYTHING'}
        )

    # ---------------------------------------------------------
    # STEP C: Execution
    # ---------------------------------------------------------
    try:
        glue_client.start_crawler(Name=crawler_name)
        print(f"🚀 [GLUE] Crawler '{crawler_name}' started successfully.")
        print("   Check the AWS Glue Console in a few minutes to see the new tables.")
    except glue_client.exceptions.CrawlerRunningException:
        print("⚠️ [GLUE] The crawler is already running.")
    except Exception as e:
        print(f"❌ [GLUE] Failed to start crawler: {e}")

if __name__ == "__main__":
    
    # Configuration matching your Ingestion Script
    ACTUAL_GROUP_ID = "imat3a05"  
    
    # Constructing the Single Bucket Name
    ACTUAL_BUCKET_NAME = f"trade-data-{ACTUAL_GROUP_ID}-main"
    
    run_glue_process(ACTUAL_GROUP_ID, ACTUAL_BUCKET_NAME)