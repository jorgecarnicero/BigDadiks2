#!/usr/bin/env python3
from __future__ import annotations

import io
import json
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import boto3
import pandas as pd
from botocore.exceptions import ClientError

try:
    from TradingviewData import TradingViewData, Interval  # type: ignore
except Exception as e:
    print("ERROR: Could not import TradingviewData. Make sure TradingView-Data is installed.")
    print(f"Import error details: {e}")
    sys.exit(1)


GROUP_ID: str = "big-daddyks"
REGION: str = "eu-south-2"
EXCHANGE: str = "BINANCE"

ASSETS: List[Dict[str, str]] = [
    {"symbol": "SOLUSD", "slug": "SOLUSD"},
]

DAYS_BACK: int = 4 * 365
USE_CALENDAR_YEARS: bool = False
N_BARS_BUFFER: int = 20

DRY_RUN: bool = False
BUCKET_NAME_SUFFIX: str = ""
S3_CONTENT_TYPE: str = "text/csv"

STATE_PREFIX: str = "_state"


@dataclass(frozen=True)
class RunWindow:
    start_needed: date
    end_needed: date
    start_fetch: date


def utc_today() -> date:
    return datetime.now(timezone.utc).date()


def first_day_of_month(d: date) -> date:
    return date(d.year, d.month, 1)


def build_bucket_name(group_id: str, suffix: str = "") -> str:
    base = f"trade-data-{group_id}-trading"
    return f"{base}-{suffix}" if suffix else base


def ensure_bucket_exists(s3_client, bucket_name: str, region: str) -> None:
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        return
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("403", "AccessDenied"):
            raise RuntimeError(
                f"S3 bucket '{bucket_name}' exists but is not accessible/owned by you. "
                f"Change GROUP_ID or BUCKET_NAME_SUFFIX."
            ) from e

    if DRY_RUN:
        print(f"[DRY_RUN] Would create bucket: {bucket_name} ({region})")
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
        code = e.response.get("Error", {}).get("Code", "")
        if code == "BucketAlreadyOwnedByYou":
            return
        if code == "BucketAlreadyExists":
            raise RuntimeError(
                f"S3 bucket name '{bucket_name}' is already taken. Change GROUP_ID or BUCKET_NAME_SUFFIX."
            ) from e
        raise


def state_key(asset_slug: str) -> str:
    return f"{STATE_PREFIX}/{asset_slug}/last_ingested.json"


def read_last_ingested_date(s3_client, bucket: str, asset_slug: str) -> Optional[date]:
    key = state_key(asset_slug)
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        payload = obj["Body"].read().decode("utf-8")
        data = json.loads(payload)
        iso = data.get("last_ingested_date")
        if not iso:
            return None
        return date.fromisoformat(iso)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "")
        if code in ("NoSuchKey", "404"):
            return None
        raise
    except Exception:
        return None


def write_last_ingested_date(s3_client, bucket: str, asset_slug: str, d: date) -> None:
    key = state_key(asset_slug)
    body = json.dumps(
        {"last_ingested_date": d.isoformat(), "updated_at_utc": datetime.now(timezone.utc).isoformat()},
        ensure_ascii=False,
    )
    if DRY_RUN:
        print(f"[DRY_RUN] Would write state: s3://{bucket}/{key} -> {d.isoformat()}")
        return

    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/json",
    )


def compute_run_window(last_ingested: Optional[date], days_back: int, use_calendar_years: bool) -> RunWindow:
    end_needed = utc_today()

    if last_ingested is None:
        if use_calendar_years:
            start_needed = (pd.Timestamp(end_needed) - pd.DateOffset(years=4)).date()
        else:
            start_needed = end_needed - timedelta(days=days_back)
    else:
        start_needed = last_ingested + timedelta(days=1)

    if start_needed > end_needed:
        start_needed = end_needed

    start_fetch = first_day_of_month(start_needed)
    return RunWindow(start_needed=start_needed, end_needed=end_needed, start_fetch=start_fetch)


def fetch_tradingview_daily_history(
    symbol: str,
    exchange: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    tv = TradingViewData()

    try:
        tv.search(symbol, exchange)
    except Exception:
        pass

    days = (end_date - start_date).days + 1
    n_bars = max(days + N_BARS_BUFFER, 50)

    try:
        raw = tv.get_hist(symbol=symbol, exchange=exchange, interval=Interval.daily, n_bars=n_bars)
    except Exception as e:
        raise RuntimeError(f"TradingView download failed for {symbol} on {exchange}: {e}") from e

    if raw is None:
        raise RuntimeError(f"TradingView returned None for {symbol} on {exchange}.")

    df = raw.copy() if isinstance(raw, pd.DataFrame) else pd.DataFrame(raw)
    if df.empty:
        raise RuntimeError(f"TradingView returned empty dataset for {symbol} on {exchange}.")

    if "datetime" in df.columns:
        dt = pd.to_datetime(df["datetime"], errors="coerce")
    elif isinstance(df.index, pd.DatetimeIndex):
        dt = pd.to_datetime(df.index, errors="coerce")
        df = df.reset_index().rename(columns={"index": "datetime"})
    else:
        for candidate in ("time", "date", "timestamp"):
            if candidate in df.columns:
                df = df.rename(columns={candidate: "datetime"})
                dt = pd.to_datetime(df["datetime"], errors="coerce")
                break
        else:
            raise RuntimeError("No datetime column/index found in TradingView result.")

    try:
        if getattr(dt.dt, "tz", None) is None:
            dt = dt.dt.tz_localize("UTC")
        else:
            dt = dt.dt.tz_convert("UTC")
    except Exception:
        dt = pd.to_datetime(dt, utc=True, errors="coerce")

    df["datetime"] = dt
    df = df.dropna(subset=["datetime"]).copy()
    df = df.sort_values("datetime")

    start_ts = pd.Timestamp(start_date).tz_localize("UTC")
    end_ts = pd.Timestamp(end_date).tz_localize("UTC") + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
    df = df[(df["datetime"] >= start_ts) & (df["datetime"] <= end_ts)].copy()

    if df.empty:
        raise RuntimeError(f"No rows after filtering [{start_date}..{end_date}] for {symbol}.")

    return df


def partition_by_year_month(df: pd.DataFrame) -> Dict[Tuple[int, int], pd.DataFrame]:
    tmp = df.copy()
    tmp["year"] = tmp["datetime"].dt.year
    tmp["month"] = tmp["datetime"].dt.month

    out: Dict[Tuple[int, int], pd.DataFrame] = {}
    for (y, m), g in tmp.groupby(["year", "month"], sort=True):
        out[(int(y), int(m))] = g.drop(columns=["year", "month"]).copy()
    return out


def dataframe_to_csv_text(df: pd.DataFrame) -> str:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue()


def data_key(asset_slug: str, symbol: str, year: int, month: int) -> str:
    mm = f"{month:02d}"
    return f"{asset_slug}/{year}/{mm}/{symbol}_{year}-{mm}.csv"


def upload_csv(s3_client, bucket: str, key: str, csv_text: str) -> None:
    if DRY_RUN:
        print(f"[DRY_RUN] Would upload: s3://{bucket}/{key}")
        return
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_text.encode("utf-8"),
        ContentType=S3_CONTENT_TYPE,
    )


def main() -> int:
    s3_client = boto3.client("s3", region_name=REGION)
    bucket = build_bucket_name(GROUP_ID, BUCKET_NAME_SUFFIX)
    ensure_bucket_exists(s3_client, bucket, REGION)

    print("=== TradeData Batch Ingestion ===")
    print(f"Bucket: {bucket}")
    print(f"Region: {REGION}")
    print(f"Exchange: {EXCHANGE}")
    print(f"DRY_RUN: {DRY_RUN}")
    print("")

    for asset in ASSETS:
        symbol = asset["symbol"]
        slug = asset["slug"]

        last_ingested = read_last_ingested_date(s3_client, bucket, slug)
        window = compute_run_window(last_ingested, DAYS_BACK, USE_CALENDAR_YEARS)

        if last_ingested is not None and window.start_needed == window.end_needed and window.start_needed > last_ingested:
            pass

        if last_ingested is not None and window.start_needed > window.end_needed:
            print(f"{symbol}: nothing to do.")
            continue

        if last_ingested is not None and window.start_needed == window.end_needed and window.start_needed == last_ingested:
            print(f"{symbol}: nothing to do.")
            continue

        print(f"--- Asset: {symbol} ({slug}) ---")
        print(f"Last ingested: {last_ingested.isoformat() if last_ingested else 'None'}")
        print(f"Need: {window.start_needed} -> {window.end_needed}")
        print(f"Fetch from: {window.start_fetch} -> {window.end_needed}")

        try:
            df = fetch_tradingview_daily_history(
                symbol=symbol,
                exchange=EXCHANGE,
                start_date=window.start_fetch,
                end_date=window.end_needed,
            )
        except Exception as e:
            print(f"ERROR: download failed for {symbol}: {e}")
            continue

        partitions = partition_by_year_month(df)

        affected_keys: List[Tuple[int, int]] = []
        for (y, m) in partitions.keys():
            month_start = date(y, m, 1)
            if month_start >= first_day_of_month(window.start_needed) and month_start <= first_day_of_month(window.end_needed):
                affected_keys.append((y, m))

        affected_keys = sorted(set(affected_keys))
        if not affected_keys:
            print(f"{symbol}: nothing to upload.")
            continue

        uploaded = 0
        for (y, m) in affected_keys:
            part_df = partitions[(y, m)]
            key = data_key(slug, symbol, y, m)
            csv_text = dataframe_to_csv_text(part_df)
            try:
                upload_csv(s3_client, bucket, key, csv_text)
                uploaded += 1
            except Exception as e:
                print(f"ERROR: upload failed s3://{bucket}/{key}: {e}")
                continue

        if uploaded > 0:
            write_last_ingested_date(s3_client, bucket, slug, window.end_needed)

        print(f"Uploaded partitions: {uploaded}")
        print("")

    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
