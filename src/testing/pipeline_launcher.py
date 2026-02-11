import time
import boto3

import settings

glue = boto3.client("glue", region_name=settings.REGION)


def ensure_job(job_name: str, script_location: str, default_args: dict):
    job_command = {
        "Name": "glueetl",
        "ScriptLocation": script_location,
        "PythonVersion": "3",
    }

    job_def = {
        "Role": settings.GLUE_JOB_ROLE_ARN,
        "Command": job_command,
        "DefaultArguments": default_args,
        "GlueVersion": "4.0",
        "WorkerType": "G.1X",
        "NumberOfWorkers": 5,
    }

    try:
        glue.get_job(JobName=job_name)
        glue.update_job(JobName=job_name, JobUpdate=job_def)
        print(f"[OK] Job actualizado: {job_name}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_job(Name=job_name, **job_def)
        print(f"[OK] Job creado: {job_name}")


def ensure_crawler():
    targets = {"S3Targets": [{"Path": settings.BRONZE_PREFIX}, {"Path": settings.SILVER_PREFIX}, {"Path": settings.GOLD_PREFIX}]}

    try:
        glue.get_crawler(Name=settings.CRAWLER_NAME)
        glue.update_crawler(
            Name=settings.CRAWLER_NAME,
            Role=settings.GLUE_CRAWLER_ROLE_ARN,
            DatabaseName=settings.GLUE_DB,
            TablePrefix=settings.TABLE_PREFIX,
            Targets=targets,
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "DEPRECATE_IN_DATABASE",
            },
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"},
        )
        print(f"[OK] Crawler actualizado: {settings.CRAWLER_NAME}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_crawler(
            Name=settings.CRAWLER_NAME,
            Role=settings.GLUE_CRAWLER_ROLE_ARN,
            DatabaseName=settings.GLUE_DB,
            TablePrefix=settings.TABLE_PREFIX,
            Targets=targets,
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "DEPRECATE_IN_DATABASE",
            },
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"},
        )
        print(f"[OK] Crawler creado: {settings.CRAWLER_NAME}")


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


def main():
    ensure_job(
        settings.JOB_BRONZE_TO_SILVER,
        settings.SCRIPT_BRONZE_TO_SILVER,
        default_args={
            "--SRC_DB": settings.GLUE_DB,
            "--SRC_TABLE": f"{settings.TABLE_PREFIX}bronze",
            "--SILVER_TARGET_PATH": settings.SILVER_PREFIX,
            "--WRITE_MODE": "append",
            "--PUSH_DOWN": "",
            "--PARTITION_COLS": settings.PARTITION_COLS,
            "--ASSET_DEFAULT": settings.DEFAULT_ASSET,
        },
    )

    ensure_job(
        settings.JOB_SILVER_TO_GOLD,
        settings.SCRIPT_SILVER_TO_GOLD,
        default_args={
            "--SILVER_SOURCE_PATH": settings.SILVER_PREFIX,
            "--GOLD_TARGET_PATH": settings.GOLD_PREFIX,
            "--WRITE_MODE": "append",
            "--ASSET_COL": settings.ASSET_COL,
            "--TIME_COL": settings.TIME_COL,
            "--CLOSE_COL": settings.CLOSE_COL,
            "--PARTITION_COLS": settings.PARTITION_COLS,
        },
    )

    ensure_job(
        settings.JOB_RUN_CRAWLER,
        settings.SCRIPT_RUN_CRAWLER,
        default_args={
            "--CRAWLER_NAME": settings.CRAWLER_NAME,
            "--CRAWLER_DB": settings.GLUE_DB,
            "--CRAWLER_ROLE_ARN": settings.GLUE_CRAWLER_ROLE_ARN,
            "--S3_TARGETS": ",".join([settings.BRONZE_PREFIX, settings.SILVER_PREFIX, settings.GOLD_PREFIX]),
            "--TABLE_PREFIX": settings.TABLE_PREFIX,
            "--WAIT": "true",
        },
    )

    ensure_crawler()

    start_job_and_wait(settings.JOB_RUN_CRAWLER, {})
    start_job_and_wait(settings.JOB_BRONZE_TO_SILVER, {})
    start_job_and_wait(settings.JOB_RUN_CRAWLER, {})
    start_job_and_wait(settings.JOB_SILVER_TO_GOLD, {})
    start_job_and_wait(settings.JOB_RUN_CRAWLER, {})

    print("[DONE] Pipeline completo.")


if __name__ == "__main__":
    main()
