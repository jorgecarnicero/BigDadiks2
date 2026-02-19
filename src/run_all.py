"""
One-click runner for the pipeline.

Phases:
1) Ensure bucket exists and seed visual prefixes (bronze/silver/gold/scripts).
2) Upload Glue job scripts to S3 (SCRIPTS_PREFIX).
3) Run ingestion (complete.py) to load bronze CSVs with cache/retention.
4) Create/update Glue jobs and crawlers, then run the pipeline:
   crawler_bronze -> bronze_to_silver -> crawler_silver -> silver_to_gold_kpis -> crawler_gold.

Assumptions:
- Env vars: AWS creds, AWS_REGION, GLUE_JOB_ROLE_ARN, GLUE_CRAWLER_ROLE_ARN.
- Bucket/prefix names set in constants.py.
- Scripts live locally in src/ (job_*.py, pipeline_launcher.py, complete.py).
"""

import subprocess
import sys
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

import constants


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
]


def ensure_bucket_exists(s3_client, bucket: str, region: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket)
        print(f"[bucket] exists: {bucket}")
        return
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("403", "AccessDenied"):
            raise RuntimeError(f"Bucket '{bucket}' exists but is not accessible.") from e

    if region == "us-east-1":
        s3_client.create_bucket(Bucket=bucket)
    else:
        s3_client.create_bucket(Bucket=bucket, CreateBucketConfiguration={"LocationConstraint": region})
    print(f"[bucket] created: {bucket}")


def seed_prefixes(s3_client, bucket: str) -> None:
    for prefix in PREFIXES:
        try:
            s3_client.put_object(Bucket=bucket, Key=prefix, Body=b"")
        except Exception:
            continue
    print("[bucket] prefixes seeded")


def upload_scripts(s3_client, bucket: str, prefix: str, base_dir: Path) -> None:
    if prefix.startswith("s3://"):
        _, _, rest = prefix.partition("s3://")
        bucket_from_prefix, _, key_root = rest.partition("/")
        if bucket_from_prefix and bucket_from_prefix != bucket:
            raise RuntimeError(f"SCRIPTS_PREFIX bucket mismatch: {bucket_from_prefix} vs {bucket}")
    else:
        key_root = prefix
    key_root = key_root.rstrip("/")

    for name in SCRIPTS_LOCAL:
        path = base_dir / name
        if not path.exists():
            print(f"[warn] local script not found: {path}")
            continue
        key = f"{key_root}/{name}"
        with path.open("rb") as fh:
            s3_client.put_object(Bucket=bucket, Key=key, Body=fh.read())
        print(f"[upload] {name} -> s3://{bucket}/{key}")


def run_complete_py(base_dir: Path) -> None:
    script = base_dir / "complete.py"
    if not script.exists():
        raise FileNotFoundError("complete.py not found in src/")
    print("[ingest] running complete.py (bronze ingest with cache/retention)...")
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise RuntimeError(f"complete.py failed with code {result.returncode}")
    print("[ingest] complete")


def run_pipeline_launcher(base_dir: Path) -> None:
    script = base_dir / "pipeline_launcher.py"
    if not script.exists():
        raise FileNotFoundError("pipeline_launcher.py not found in src/")
    print("[pipeline] running pipeline_launcher.py ...")
    result = subprocess.run([sys.executable, str(script)], capture_output=True, text=True)
    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise RuntimeError(f"pipeline_launcher.py failed with code {result.returncode}")
    print("[pipeline] complete")


def main() -> int:
    base_dir = Path(__file__).parent
    bucket = constants.BUCKET
    region = constants.REGION
    scripts_prefix = constants.SCRIPTS_PREFIX

    s3 = boto3.client("s3", region_name=region)

    ensure_bucket_exists(s3, bucket, region)
    seed_prefixes(s3, bucket)
    upload_scripts(s3, bucket, scripts_prefix, base_dir)

    run_complete_py(base_dir)
    run_pipeline_launcher(base_dir)

    print("[done] end-to-end run finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
