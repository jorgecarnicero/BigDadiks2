"""
Setup helper to ensure bucket and script prefixes exist.

Actions:
- Create bucket if it does not exist.
- Seed visual prefixes for bronze/silver/gold/scripts in S3 (empty objects).
- Upload local job scripts to the configured S3 scripts prefix.

This is a local utility; requires boto3 and AWS credentials.
"""

import os
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

import settings


SCRIPTS_LOCAL = [
    "job_bronze_to_silver.py",
    "job_silver_to_gold_kpis.py",
    "job_run_crawler.py",
    "pipeline_launcher.py",
]

PREFIXES = [
    "bronze/",
    "silver/",
    "gold/",
    "scripts/",
    "scripts/opencode/",
    "silver/prices/",
    "gold/indicators/",
]


def ensure_bucket_exists(s3_client, bucket: str, region: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket)
        return
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("403", "AccessDenied"):
            raise RuntimeError(f"Bucket '{bucket}' exists but is not accessible. Check ownership or choose another name.") from e

    # Create bucket
    if region == "us-east-1":
        s3_client.create_bucket(Bucket=bucket)
    else:
        s3_client.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})


def seed_prefixes(s3_client, bucket: str) -> None:
    for prefix in PREFIXES:
        try:
            s3_client.put_object(Bucket=bucket, Key=prefix, Body=b"")
        except Exception:
            # Non-fatal; continue
            continue


def upload_scripts(s3_client, bucket: str, prefix: str, base_dir: Path) -> None:
    for name in SCRIPTS_LOCAL:
        path = base_dir / name
        if not path.exists():
            print(f"[WARN] Local script not found: {path}")
            continue
        key = prefix.lstrip("s3://").split("/", 1)[1] if prefix.startswith("s3://") else prefix
        key = key.rstrip("/") + "/" + name
        with path.open("rb") as fh:
            s3_client.put_object(Bucket=bucket, Key=key, Body=fh.read())
        print(f"[OK] Uploaded {name} -> s3://{bucket}/{key}")


def main() -> int:
    bucket = settings.BUCKET
    region = settings.REGION
    scripts_prefix = settings.SCRIPTS_PREFIX

    s3 = boto3.client("s3", region_name=region)

    print(f"Ensuring bucket '{bucket}' in region '{region}'...")
    ensure_bucket_exists(s3, bucket, region)
    print("Bucket ok.")

    print("Seeding prefixes (visual folders)...")
    seed_prefixes(s3, bucket)
    print("Prefixes ok.")

    print("Uploading scripts to S3...")
    base_dir = Path(__file__).parent
    upload_scripts(s3, bucket, scripts_prefix, base_dir)
    print("Scripts upload ok.")

    return 0


if __name__ == "__main__":
    sys.exit(main())
