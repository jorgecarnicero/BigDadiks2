#!/usr/bin/env python3
"""
TradeData - Batch ingestion (TradingView -> CSV -> S3 multi-bucket by year)

- Downloads ~4 years of daily historical data for crypto assets from TradingView (recommended exchange: BINANCE).
- Partitions data by year/month.
- Uploads CSV partitions to S3 using a multi-bucket design (one bucket per year).

Naming convention:
- Bucket: trade-data-{GROUP_ID}-raw-{YYYY}
- Key:    {ASSET}/{MM}/{ASSET}_{YYYY}-{MM}.csv

Notes:
- S3 has no real folders; "folders" are just prefixes. This script uploads objects with the desired prefix.
- Bucket creation is region-aware: CreateBucketConfiguration is used outside us-east-1.
- No hardcoded credentials: relies on AWS env vars / config files / profiles.
"""

from __future__ import annotations

import io
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple

import boto3
import pandas as pd
from botocore.exceptions import ClientError

# TradingView-Data library imports (package name may vary depending on installation)
# The user's starter code uses: from TradingviewData import TradingViewData, Interval
try:
    from TradingviewData import TradingViewData, Interval  # type: ignore
except Exception as e:
    print("ERROR: Could not import TradingviewData. Make sure TradingView-Data is installed.")
    print(f"Import error details: {e}")
    sys.exit(1)

# -------------------------------------------------------------------
# CONSTANTS (PARAMETRIZE HERE)
# -------------------------------------------------------------------
GROUP_ID: str = "big-daddyks"
REGION: str = "eu-south-2"
EXCHANGE: str = "BINANCE"

ASSETS: List[str] = ["SOLUSD"]  # Ready to scale to the full list if needed

DAYS_BACK: int = 4 * 365  # 1460 days (simple approach as requested)
USE_CALENDAR_YEARS: bool = False
# If True, the start date is computed as "today - 4 years" using pandas DateOffset
# (more robust around leap years vs fixed 1460 days).

# TradingView request sizing:
# We request slightly more than DAYS_BACK to avoid edge cases (missing days).
N_BARS_BUFFER: int = 15

# Upload behavior
S3_CONTENT_TYPE: str = "text/csv"
DRY_RUN: bool = False  # If True, does not create buckets or upload files (prints what it would do)

# If a bucket name is already taken by someone else (global S3 namespace),
# you must change GROUP_ID or set a deterministic suffix here.
BUCKET_NAME_SUFFIX: str = ""  # e.g., "g1" -> trade-data-...-raw-2024-g1

# Optional: If you want to create empty "folder marker" objects (not required)
CREATE_PREFIX_MARKERS: bool = False

# -------------------------------------------------------------------
# DATA CLASSES
# -------------------------------------------------------------------
@dataclass(frozen=True)
class DateRange:
    start: date
    end: date
    years: List[int]


# -------------------------------------------------------------------
# DATE RANGE
# -------------------------------------------------------------------
def compute_date_range(days_back: int, use_calendar_years: bool = False) -> DateRange:
    """
    Computes start/end dates and the list of years involved.

    - Default: start = today - timedelta(days_back) (fixed 1460 days)
    - If use_calendar_years=True: start = today - 4 years using pandas DateOffset (leap-year robust)
    """
    today = datetime.now(timezone.utc).date()

    if use_calendar_years:
        # Leap-year robust "4 years back" using pandas DateOffset
        start_dt = (pd.Timestamp(today) - pd.DateOffset(years=4)).date()
    else:
        start_dt = (datetime.now(timezone.utc) - timedelta(days=days_back)).date()

    years = list(range(start_dt.year, today.year + 1))
    return DateRange(start=start_dt, end=today, years=years)


# -------------------------------------------------------------------
# TRADINGVIEW DOWNLOAD
# -------------------------------------------------------------------
def fetch_tradingview_daily_history(
    asset: str,
    exchange: str,
    start_date: date,
    end_date: date,
    n_bars: int,
) -> pd.DataFrame:
    """
    Downloads historical daily data from TradingView-Data and returns a DataFrame.
    Normalizes datetime to timezone-aware UTC and filters [start_date, end_date].
    """
    tv = TradingViewData()

    # Optional search (do not fail ingestion if it errors)
    try:
        tv.search(asset, exchange)
    except Exception:
        pass

    try:
        raw = tv.get_hist(
            symbol=asset,
            exchange=exchange,
            interval=Interval.daily,
            n_bars=n_bars,
        )
    except Exception as e:
        raise RuntimeError(f"TradingView download failed for {asset} on {exchange}: {e}") from e

    if raw is None:
        raise RuntimeError(f"TradingView returned None for {asset} on {exchange}.")

    # Convert to DataFrame if needed
    df = raw.copy() if isinstance(raw, pd.DataFrame) else pd.DataFrame(raw)

    if df.empty:
        raise RuntimeError(f"TradingView returned an empty dataset for {asset} on {exchange}.")

    # --- Normalize datetime column/index ---
    if "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], errors="coerce")
    elif isinstance(df.index, pd.DatetimeIndex):
        dt = pd.to_datetime(df.index, errors="coerce")
        df = df.reset_index().rename(columns={"index": "datetime"})
    else:
        # Common alternatives
        for candidate in ("time", "date", "timestamp"):
            if candidate in df.columns:
                df = df.rename(columns={candidate: "datetime"})
                dt = pd.to_datetime(df["datetime"], errors="coerce")
                break
        else:
            raise RuntimeError(
                "Could not find a datetime field in TradingView result. "
                "Print df.columns / df.head() and adapt the normalization."
            )

    # Force UTC tz-aware to avoid naive/aware comparison errors
    # If dt is naive -> localize; if aware -> convert.
    try:
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize("UTC")
        else:
            dt = dt.dt.tz_convert("UTC")
    except Exception:
        # If dt isn't a Series with .dt (edge cases), fallback:
        dt = pd.to_datetime(dt, utc=True, errors="coerce")

    df["datetime"] = dt
    df = df.dropna(subset=["datetime"]).copy()
    df = df.sort_values("datetime")

    # Filter by date range (UTC)
    start_ts = pd.Timestamp(start_date).tz_localize("UTC")
    end_ts = pd.Timestamp(end_date).tz_localize("UTC") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    df = df[(df["datetime"] >= start_ts) & (df["datetime"] <= end_ts)].copy()

    if df.empty:
        raise RuntimeError(
            f"After filtering [{start_date}..{end_date}], no rows remain for {asset}. "
            "Increase n_bars or verify symbol/exchange."
        )

    return df


# -------------------------------------------------------------------
# S3 HELPERS
# -------------------------------------------------------------------
def build_bucket_name(group_id: str, year: int, suffix: str = "") -> str:
    base = f"trade-data-{group_id}-raw-{year}"
    return f"{base}-{suffix}" if suffix else base


def ensure_bucket_exists(s3_client, bucket_name: str, region: str) -> None:
    """
    Idempotent bucket creation:
    - If bucket exists and is owned by you: continue.
    - If bucket exists but not owned by you: raise with strategy.
    - If it doesn't exist: create it (region-aware).
    """
    # First, try head_bucket (works if you own it or have access).
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        # If head_bucket succeeds, we can continue (owned or accessible).
        return
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        # 404 / NoSuchBucket -> can create
        # 403 -> exists but not accessible (likely not yours)
        if code in ("403", "AccessDenied"):
            raise RuntimeError(
                f"S3 bucket name '{bucket_name}' seems to exist but is not accessible/owned by you.\n"
                f"Strategy: change GROUP_ID or set BUCKET_NAME_SUFFIX to a deterministic unique value."
            ) from e

    # Not accessible via head_bucket; attempt to create
    if DRY_RUN:
        print(f"[DRY_RUN] Would create bucket: {bucket_name} in region {region}")
        return

    try:
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
    except ClientError as e:
        err = e.response.get("Error", {})
        code = err.get("Code", "")
        if code == "BucketAlreadyOwnedByYou":
            return
        if code == "BucketAlreadyExists":
            raise RuntimeError(
                f"S3 bucket name '{bucket_name}' is already taken in the global namespace.\n"
                f"Strategy: change GROUP_ID or set BUCKET_NAME_SUFFIX (deterministic) and re-run."
            ) from e
        if code == "InvalidLocationConstraint":
            raise RuntimeError(
                f"InvalidLocationConstraint for region '{region}'. "
                f"Check REGION or your account/endpoint configuration."
            ) from e
        raise


def ensure_prefix_markers(s3_client, bucket_name: str, prefixes: Iterable[str]) -> None:
    """
    Optional: create empty objects ending with '/' to visually show prefixes as "folders".
    Not required for S3 to work, but sometimes requested for demos.
    """
    if not CREATE_PREFIX_MARKERS:
        return

    for p in prefixes:
        key = p if p.endswith("/") else f"{p}/"
        if DRY_RUN:
            print(f"[DRY_RUN] Would create prefix marker: s3://{bucket_name}/{key}")
            continue
        s3_client.put_object(Bucket=bucket_name, Key=key, Body=b"")


def upload_csv_to_s3(
    s3_client,
    bucket: str,
    key: str,
    csv_text: str,
    content_type: str = "text/csv",
) -> None:
    if DRY_RUN:
        print(f"[DRY_RUN] Would upload: s3://{bucket}/{key} ({len(csv_text)} bytes)")
        return

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_text.encode("utf-8"),
        ContentType=content_type,
    )


# -------------------------------------------------------------------
# PARTITION + UPLOAD
# -------------------------------------------------------------------
def partition_by_year_month(df: pd.DataFrame) -> Dict[Tuple[int, int], pd.DataFrame]:
    """
    Returns a dict mapping (year, month) -> partition DataFrame.
    """
    tmp = df.copy()
    tmp["year"] = tmp["datetime"].dt.year
    tmp["month"] = tmp["datetime"].dt.month

    partitions: Dict[Tuple[int, int], pd.DataFrame] = {}
    for (y, m), g in tmp.groupby(["year", "month"], sort=True):
        # Keep the data columns + datetime, drop helper columns
        part = g.drop(columns=["year", "month"]).copy()
        partitions[(int(y), int(m))] = part
    return partitions


def dataframe_to_csv_text(df: pd.DataFrame) -> str:
    """
    Converts DataFrame to CSV string.
    """
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def upload_asset_partitions_to_s3(
    s3_client,
    asset: str,
    partitions: Dict[Tuple[int, int], pd.DataFrame],
    group_id: str,
    region: str,
) -> List[Tuple[str, str]]:
    """
    Uploads monthly partitions to year-buckets.

    Returns list of (bucket, key) uploaded.
    """
    uploaded: List[Tuple[str, str]] = []

    # Ensure all relevant buckets exist
    years_needed = sorted({y for (y, _) in partitions.keys()})
    for year in years_needed:
        bucket = build_bucket_name(group_id, year, BUCKET_NAME_SUFFIX)
        ensure_bucket_exists(s3_client, bucket, region)

    # Upload per month
    for (year, month), part_df in sorted(partitions.items()):
        bucket = build_bucket_name(group_id, year, BUCKET_NAME_SUFFIX)

        month_str = f"{month:02d}"
        prefix = f"{asset}/{month_str}/"
        key = f"{prefix}{asset}_{year}-{month_str}.csv"

        ensure_prefix_markers(s3_client, bucket, [prefix])

        csv_text = dataframe_to_csv_text(part_df)
        upload_csv_to_s3(s3_client, bucket, key, csv_text, content_type=S3_CONTENT_TYPE)

        uploaded.append((bucket, key))

    return uploaded


# -------------------------------------------------------------------
# MAIN ORCHESTRATION
# -------------------------------------------------------------------
def main() -> int:
    date_range = compute_date_range(DAYS_BACK, use_calendar_years=USE_CALENDAR_YEARS)

    print("=== TradeData Batch Ingestion ===")
    print(f"Region: {REGION}")
    print(f"Exchange: {EXCHANGE}")
    print(f"Assets: {ASSETS}")
    print(f"Date range (UTC): {date_range.start} -> {date_range.end}")
    print(f"Years involved: {date_range.years}")
    print(f"DRY_RUN: {DRY_RUN}")
    print("")

    # Create S3 client (credentials come from env/profile/config)
    s3_client = boto3.client("s3", region_name=REGION)

    for asset in ASSETS:
        print(f"--- Processing asset: {asset} ---")

        n_bars = DAYS_BACK + N_BARS_BUFFER
        try:
            df = fetch_tradingview_daily_history(
                asset=asset,
                exchange=EXCHANGE,
                start_date=date_range.start,
                end_date=date_range.end,
                n_bars=n_bars,
            )
        except Exception as e:
            print(f"ERROR: Failed to download data for {asset}: {e}")
            continue

        print(f"Downloaded rows: {len(df)}")
        partitions = partition_by_year_month(df)
        print(f"Monthly partitions: {len(partitions)}")

        try:
            uploaded = upload_asset_partitions_to_s3(
                s3_client=s3_client,
                asset=asset,
                partitions=partitions,
                group_id=GROUP_ID,
                region=REGION,
            )
        except Exception as e:
            print(f"ERROR: Failed uploading partitions for {asset}: {e}")
            continue

        print(f"Uploaded objects: {len(uploaded)}")
        # Print a small sample of uploaded paths
        for b, k in uploaded[:5]:
            print(f"  - s3://{b}/{k}")
        if len(uploaded) > 5:
            print(f"  ... ({len(uploaded) - 5} more)")
        print("")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
