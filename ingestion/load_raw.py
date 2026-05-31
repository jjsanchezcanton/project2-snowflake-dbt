#!/usr/bin/env python3
"""
load_raw.py - Project 2 (Snowflake + dbt)

Re-downloads NYC TLC yellow-taxi parquet + taxi zone lookup from source,
PUTs them to a Snowflake internal stage, and COPYs into RAW tables.
Auth: key-pair (env vars from .env).

Usage (from .venv-dbt):  python ingestion/load_raw.py
"""
from __future__ import annotations

import os
import sys
import urllib.request
from pathlib import Path

import snowflake.connector

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # env vars may be exported directly

# --- Config -----------------------------------------------------------
TLC_BASE = "https://d37ci6vzurychx.cloudfront.net"
MONTHS = ["2024-01", "2024-02", "2024-03"]   # 2-3 months -> safe on trial credits
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
STAGE = "RAW.STG_TLC"

REQUIRED_ENV = [
    "SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_FILE",
    "SNOWFLAKE_ROLE", "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA",
]


def download(url: str, dest: Path) -> None:
    if dest.exists():
        print(f"  cached    {dest.name}")
        return
    print(f"  downloading {url}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(url, dest)
    print(f"  saved     {dest.name} ({dest.stat().st_size/1e6:.1f} MB)")


def fetch_sources() -> None:
    for m in MONTHS:
        fname = f"yellow_tripdata_{m}.parquet"
        download(f"{TLC_BASE}/trip-data/{fname}", DATA_DIR / fname)
    download(f"{TLC_BASE}/misc/taxi_zone_lookup.csv", DATA_DIR / "taxi_zone_lookup.csv")


def get_conn():
    missing = [v for v in REQUIRED_ENV if not os.environ.get(v)]
    if missing:
        sys.exit(f"ERROR: missing env vars: {', '.join(missing)}")
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
        private_key_file_pwd=os.environ.get("SNOWFLAKE_PRIVATE_KEY_FILE_PWD"),
        role=os.environ["SNOWFLAKE_ROLE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )


def run(cur, sql: str) -> None:
    print(f"  > {sql.strip().splitlines()[0][:80]}")
    cur.execute(sql)


def main() -> None:
    print("1) Fetching source files from NYC TLC ...")
    fetch_sources()

    print("2) Connecting to Snowflake (key-pair) ...")
    conn = get_conn()
    cur = conn.cursor()
    try:
        print("3) Clean stage + PUT files ...")
        run(cur, f"REMOVE @{STAGE}")
        for p in sorted(DATA_DIR.glob("yellow_tripdata_*.parquet")):
            run(cur, f"PUT file://{p} @{STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        zone = DATA_DIR / "taxi_zone_lookup.csv"
        run(cur, f"PUT file://{zone} @{STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")

        print("4) COPY INTO RAW tables ...")
        run(cur, "TRUNCATE TABLE IF EXISTS RAW.YELLOW_TRIPDATA_RAW")
        run(cur, """
            COPY INTO RAW.YELLOW_TRIPDATA_RAW (record, _source_file)
            FROM (SELECT $1, METADATA$FILENAME FROM @RAW.STG_TLC)
            FILE_FORMAT = (FORMAT_NAME = RAW.FF_PARQUET)
            PATTERN = '.*yellow_tripdata_.*[.]parquet'
            ON_ERROR = 'ABORT_STATEMENT'
        """)
        run(cur, "TRUNCATE TABLE IF EXISTS RAW.TAXI_ZONE_LOOKUP")
        run(cur, """
            COPY INTO RAW.TAXI_ZONE_LOOKUP (locationid, borough, zone, service_zone)
            FROM (SELECT $1, $2, $3, $4 FROM @RAW.STG_TLC)
            FILE_FORMAT = (FORMAT_NAME = RAW.FF_CSV)
            PATTERN = '.*taxi_zone_lookup[.]csv'
            ON_ERROR = 'ABORT_STATEMENT'
        """)

        print("5) Row counts:")
        for tbl in ["RAW.YELLOW_TRIPDATA_RAW", "RAW.TAXI_ZONE_LOOKUP"]:
            cur.execute(f"SELECT COUNT(*) FROM {tbl}")
            print(f"   {tbl:30s} {cur.fetchone()[0]:>12,}")
    finally:
        cur.close()
        conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
