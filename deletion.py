import boto3
from botocore.exceptions import ClientError

def delete_buckets_by_prefix(
    prefix: str,
    region: str,
    dry_run: bool = True,
) -> None:
    s3 = boto3.client("s3", region_name=region)

    resp = s3.list_buckets()
    buckets = [b["Name"] for b in resp.get("Buckets", []) if b["Name"].startswith(prefix)]

    if not buckets:
        print(f"No buckets found with prefix: {prefix}")
        return

    print(f"Buckets matching prefix '{prefix}':")
    for name in buckets:
        print(f"  - {name}")

    for bucket in buckets:
        print(f"\nDeleting bucket: {bucket}")

        if dry_run:
            print("[DRY_RUN] Would empty and delete this bucket.")
            continue

        # Delete all object versions + delete markers (works for both versioned and non-versioned buckets)
        try:
            paginator = s3.get_paginator("list_object_versions")
            for page in paginator.paginate(Bucket=bucket):
                to_delete = []

                for v in page.get("Versions", []):
                    to_delete.append({"Key": v["Key"], "VersionId": v["VersionId"]})

                for m in page.get("DeleteMarkers", []):
                    to_delete.append({"Key": m["Key"], "VersionId": m["VersionId"]})

                if to_delete:
                    # Batch in chunks of 1000
                    for i in range(0, len(to_delete), 1000):
                        chunk = to_delete[i : i + 1000]
                        s3.delete_objects(Bucket=bucket, Delete={"Objects": chunk, "Quiet": True})

            # Also handle non-versioned buckets where list_object_versions may return nothing
            paginator2 = s3.get_paginator("list_objects_v2")
            for page in paginator2.paginate(Bucket=bucket):
                objs = [{"Key": o["Key"]} for o in page.get("Contents", [])]
                if objs:
                    for i in range(0, len(objs), 1000):
                        chunk = objs[i : i + 1000]
                        s3.delete_objects(Bucket=bucket, Delete={"Objects": chunk, "Quiet": True})

            s3.delete_bucket(Bucket=bucket)
            print("Deleted.")
        except ClientError as e:
            print(f"ERROR deleting {bucket}: {e}")


GROUP_ID = "big-daddyks"
REGION = "eu-south-2"

# 1) Preview
delete_buckets_by_prefix(prefix=f"trade-data-{GROUP_ID}-", region=REGION, dry_run=True)

# 2) Actually delete (only after you’ve checked the list)
delete_buckets_by_prefix(prefix=f"trade-data-{GROUP_ID}-", region=REGION, dry_run=False)
