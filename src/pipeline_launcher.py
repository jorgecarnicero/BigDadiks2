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
        # Delete and recreate to guarantee DefaultArguments are fully replaced
        glue.delete_job(JobName=job_name)
    except glue.exceptions.EntityNotFoundException:
        pass
    glue.create_job(Name=job_name, **job_def)
    print(f"[OK] Job creado: {job_name}")


def ensure_crawler():
    targets = {"S3Targets": [{"Path": constants.BRONZE_PREFIX}, {"Path": constants.SILVER_PREFIX}, {"Path": constants.GOLD_PREFIX}]}

    try:
        glue.get_crawler(Name=constants.CRAWLER_NAME)
        glue.update_crawler(
            Name=constants.CRAWLER_NAME,
            Role=constants.GLUE_CRAWLER_ROLE_ARN,
            DatabaseName=constants.GLUE_DB,
            TablePrefix=constants.TABLE_PREFIX,
            Targets=targets,
            SchemaChangePolicy={
                "UpdateBehavior": "LOG",
                "DeleteBehavior": "LOG",
            },
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"},
        )
        print(f"[OK] Crawler actualizado: {constants.CRAWLER_NAME}")
    except glue.exceptions.EntityNotFoundException:
        glue.create_crawler(
            Name=constants.CRAWLER_NAME,
            Role=constants.GLUE_CRAWLER_ROLE_ARN,
            DatabaseName=constants.GLUE_DB,
            TablePrefix=constants.TABLE_PREFIX,
            Targets=targets,
            SchemaChangePolicy={
                "UpdateBehavior": "LOG",
                "DeleteBehavior": "LOG",
            },
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"},
        )
        print(f"[OK] Crawler creado: {constants.CRAWLER_NAME}")


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
            "--CRAWLER_NAME": constants.CRAWLER_NAME,
            "--CRAWLER_DB": constants.GLUE_DB,
            "--CRAWLER_ROLE_ARN": constants.GLUE_CRAWLER_ROLE_ARN,
            "--S3_TARGETS": ",".join([constants.BRONZE_PREFIX, constants.SILVER_PREFIX, constants.GOLD_PREFIX]),
            "--TABLE_PREFIX": constants.TABLE_PREFIX,
            "--WAIT": "true",
            "--REGION": constants.REGION,
        },
    )

    ensure_crawler()

    start_job_and_wait(constants.JOB_RUN_CRAWLER, {})
    start_job_and_wait(constants.JOB_BRONZE_TO_SILVER, {})
    start_job_and_wait(constants.JOB_RUN_CRAWLER, {})
    start_job_and_wait(constants.JOB_SILVER_TO_GOLD, {})
    start_job_and_wait(constants.JOB_RUN_CRAWLER, {})

    print("[DONE] Pipeline completo.")


if __name__ == "__main__":
    main()
