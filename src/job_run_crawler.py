import sys
import time
import boto3
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "CRAWLER_NAME",
        "CRAWLER_DB",
        "CRAWLER_ROLE_ARN",
        "S3_TARGETS",
        "TABLE_PREFIX",
        "WAIT",
        "REGION",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

crawler_name = args["CRAWLER_NAME"]
crawler_db = args["CRAWLER_DB"]
crawler_role_arn = args["CRAWLER_ROLE_ARN"]
table_prefix = (args.get("TABLE_PREFIX") or "").strip()
wait = (args.get("WAIT") or "true").lower() == "true"
region = args.get("REGION") or "eu-south-2"

s3_targets = [p.strip() for p in args["S3_TARGETS"].split(",") if p.strip()]
glue = boto3.client("glue", region_name=region)


def ensure_crawler_exists():
    try:
        glue.get_crawler(Name=crawler_name)
        glue.update_crawler(
            Name=crawler_name,
            Role=crawler_role_arn,
            DatabaseName=crawler_db,
            TablePrefix=table_prefix,
            Targets={"S3Targets": [{"Path": p} for p in s3_targets]},
            SchemaChangePolicy={
                "UpdateBehavior": "LOG",
                "DeleteBehavior": "LOG",
            },
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"},
        )
        return "updated"
    except glue.exceptions.EntityNotFoundException:
        glue.create_crawler(
            Name=crawler_name,
            Role=crawler_role_arn,
            DatabaseName=crawler_db,
            TablePrefix=table_prefix,
            Targets={"S3Targets": [{"Path": p} for p in s3_targets]},
            SchemaChangePolicy={
                "UpdateBehavior": "LOG",
                "DeleteBehavior": "LOG",
            },
            RecrawlPolicy={"RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"},
        )
        return "created"


def start_crawler_if_ready():
    c = glue.get_crawler(Name=crawler_name)["Crawler"]
    state = c["State"]
    if state != "READY":
        return "already_running"
    glue.start_crawler(Name=crawler_name)
    return "started"


def wait_until_ready(poll_seconds=15):
    while True:
        state = glue.get_crawler(Name=crawler_name)["Crawler"]["State"]
        if state == "READY":
            break
        time.sleep(poll_seconds)


status = ensure_crawler_exists()
start_status = start_crawler_if_ready()

if wait and start_status == "started":
    wait_until_ready()

job.commit()
