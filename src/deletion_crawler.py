#!/usr/bin/env python3
import boto3
import sys
import os
import time

# ==========================================
# 🧹 CONFIGURATION
# ==========================================
AWS_REGION = os.environ.get("AWS_REGION", "eu-south-2")

def delete_glue_resources(group_id: str):
    
    try:
        glue_client = boto3.client('glue', region_name=AWS_REGION)
    except Exception as e:
        print(f"❌ Error connecting to AWS: {e}")
        sys.exit(1)

    # Naming conventions (Must match your creation script)
    db_name = f"trade_data_{group_id}".replace("-", "_")
    crawler_name = f"crawler_{group_id}_bronze"

    print(f"🔥 [CLEANUP] Starting cleanup for Group ID: {group_id}")
    print(f"   Target DB: {db_name}")
    print(f"   Target Crawler: {crawler_name}")

    # ---------------------------------------------------------
    # STEP 1: Stop and Delete Crawler
    # ---------------------------------------------------------
    try:
        # Check if crawler exists
        crawler = glue_client.get_crawler(Name=crawler_name)
        status = crawler['Crawler']['State']
        
        # If running or stopping, we must stop/wait before deleting
        if status in ['RUNNING', 'STOPPING']:
            print(f"⚠️  Crawler is {status}. Sending STOP signal...")
            try:
                glue_client.stop_crawler(Name=crawler_name)
            except:
                pass # It might have stopped in the meantime
            
            print("⏳ Waiting for Crawler to stop (this might take a moment)...")
            while status != 'READY':
                time.sleep(2)
                crawler = glue_client.get_crawler(Name=crawler_name)
                status = crawler['Crawler']['State']
                print(f"   Status: {status}...")
        
        # Now delete it
        glue_client.delete_crawler(Name=crawler_name)
        print(f"✅ Crawler '{crawler_name}' DELETED.")

    except glue_client.exceptions.EntityNotFoundException:
        print(f"ℹ️  Crawler '{crawler_name}' not found (already clean).")
    except Exception as e:
        print(f"❌ Error deleting crawler: {e}")

    # ---------------------------------------------------------
    # STEP 2: Delete Database (Cascades to Tables)
    # ---------------------------------------------------------
    # Deleting the database automatically removes all tables inside it from the Catalog.
    try:
        glue_client.delete_database(Name=db_name)
        print(f"✅ Database '{db_name}' DELETED.")
    except glue_client.exceptions.EntityNotFoundException:
        print(f"ℹ️  Database '{db_name}' not found (already clean).")
    except Exception as e:
        print(f"❌ Error deleting database: {e}")

    print("✨ Cleanup complete. You are ready to re-run the creation script.")

if __name__ == "__main__":
    
    # Must match your project ID
    ACTUAL_GROUP_ID = "imat3a05"  
    
    delete_glue_resources(ACTUAL_GROUP_ID)