import time
import boto3

import constants

glue = boto3.client("glue", region_name=constants.REGION)


def ensure_job(job_name: str, script_location: str, default_args: dict):
    job_command = {
        "Name": "glueetl",
        "ScriptLocation": script_location,
        "PythonVersion": "3",
    }

    job_def = {
        "Role": constants.GLUE_JOB_ROLE_ARN,
        "Command": job_command,
        "DefaultArguments": default_args,
        "GlueVersion": "4.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 5,
    }

    try:
        glue.get_job(JobName=job_name)
        glue.delete_job(JobName=job_name)
    except glue.exceptions.EntityNotFoundException:
        pass
    glue.create_job(Name=job_name, **job_def)
    print(f"[OK] Job creado: {job_name}")


def ensure_crawler(crawler_name: str, s3_path: str, table_prefix: str):
    """Create or update a single crawler for one layer (bronze/silver/gold)."""
    targets = {"S3Targets": [{"Path": s3_path}]}

    crawler_kwargs = dict(
        Role=constants.GLUE_CRAWLER_ROLE_ARN,
        DatabaseName=constants.GLUE_DB,
        TablePrefix=table_prefix,
        Targets=targets,
        SchemaChangePolicy={
            "UpdateBehavior": "LOG",
            "DeleteBehavior": "LOG",
        },
        RecrawlPolicy={"RecrawlBehavior": "CRAWL_EVERYTHING"},
    )

    try:
        glue.get_crawler(Name=crawler_name)
        glue.update_crawler(Name=crawler_name, **crawler_kwargs)
        print(f"[OK] Crawler actualizado: {crawler_name}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_crawler(Name=crawler_name, **crawler_kwargs)
        print(f"[OK] Crawler creado: {crawler_name}")


def start_job_and_wait(job_name: str, args: dict):
    run_id = glue.start_job_run(JobName=job_name, Arguments=args)["JobRunId"]
    print(f"[RUN] {job_name} -> {run_id}")

    while True:
        jr = glue.get_job_run(JobName=job_name, RunId=run_id, PredecessorsIncluded=False)["JobRun"]
        state = jr["JobRunState"]
        if state in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
            print(f"[END] {job_name} -> {state}")
            if state != "SUCCEEDED":
                raise RuntimeError(f"Job {job_name} terminó en estado {state}: {jr.get('ErrorMessage')}")
            return
        time.sleep(20)


def run_crawler_via_job(crawler_name: str, s3_path: str):
    """Run the run_crawler Glue job for a specific crawler/path."""
    start_job_and_wait(constants.JOB_RUN_CRAWLER, {
        "--CRAWLER_NAME": crawler_name,
        "--CRAWLER_DB": constants.GLUE_DB,
        "--CRAWLER_ROLE_ARN": constants.GLUE_CRAWLER_ROLE_ARN,
        "--S3_TARGET": s3_path,
        "--TABLE_PREFIX": constants.TABLE_PREFIX,
        "--WAIT": "true",
        "--REGION": constants.REGION,
    })


def main():
    # --- Create/update jobs ---
    ensure_job(
        constants.JOB_BRONZE_TO_SILVER,
        constants.SCRIPT_BRONZE_TO_SILVER,
        default_args={
            "--SRC_DB": constants.GLUE_DB,
            "--SRC_TABLE": f"{constants.TABLE_PREFIX}bronze",
            "--SILVER_TARGET_PATH": constants.SILVER_PREFIX,
            "--WRITE_MODE": "append",
            "--PARTITION_COLS": constants.PARTITION_COLS,
            "--ASSET_DEFAULT": constants.DEFAULT_ASSET,
        },
    )

    ensure_job(
        constants.JOB_SILVER_TO_GOLD,
        constants.SCRIPT_SILVER_TO_GOLD,
        default_args={
            "--SILVER_SOURCE_PATH": constants.SILVER_PREFIX,
            "--GOLD_TARGET_PATH": constants.GOLD_PREFIX,
            "--WRITE_MODE": "append",
            "--ASSET_COL": constants.ASSET_COL,
            "--TIME_COL": constants.TIME_COL,
            "--CLOSE_COL": constants.CLOSE_COL,
            "--PARTITION_COLS": constants.PARTITION_COLS,
        },
    )

    ensure_job(
        constants.JOB_RUN_CRAWLER,
        constants.SCRIPT_RUN_CRAWLER,
        default_args={
            "--CRAWLER_NAME": constants.CRAWLER_BRONZE,
            "--CRAWLER_DB": constants.GLUE_DB,
            "--CRAWLER_ROLE_ARN": constants.GLUE_CRAWLER_ROLE_ARN,
            "--S3_TARGET": constants.BRONZE_PREFIX,
            "--TABLE_PREFIX": constants.TABLE_PREFIX,
            "--WAIT": "true",
            "--REGION": constants.REGION,
        },
    )

    # --- Create/update crawlers (one per layer) ---
    ensure_crawler(constants.CRAWLER_BRONZE, constants.BRONZE_PREFIX, constants.TABLE_PREFIX)
    ensure_crawler(constants.CRAWLER_SILVER, constants.SILVER_PREFIX, constants.TABLE_PREFIX)
    ensure_crawler(constants.CRAWLER_GOLD, constants.GOLD_PREFIX, constants.TABLE_PREFIX)

    # --- Pipeline execution ---
    # 1) Crawl bronze
    run_crawler_via_job(constants.CRAWLER_BRONZE, constants.BRONZE_PREFIX)

    # 2) Bronze -> Silver
    start_job_and_wait(constants.JOB_BRONZE_TO_SILVER, {})

    # 3) Crawl silver
    run_crawler_via_job(constants.CRAWLER_SILVER, constants.SILVER_PREFIX)

    # 4) Silver -> Gold
    start_job_and_wait(constants.JOB_SILVER_TO_GOLD, {})

    # 5) Crawl gold
    run_crawler_via_job(constants.CRAWLER_GOLD, constants.GOLD_PREFIX)

    print("[DONE] Pipeline completo.")


if __name__ == "__main__":
    main()
