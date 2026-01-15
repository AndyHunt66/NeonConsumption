#!/usr/bin/env python3
import os
import sys
import requests
import psycopg2
import datetime

# ----------------------------------------------------------------------
# Config – set these before running
# ----------------------------------------------------------------------
NEON_API_TOKEN = os.getenv("NEON_API_TOKEN")          # ← put your API key here
NEON_API_URL  = "https://console.neon.tech/api/v2/consumption_history"
DB_CONN_STR   = os.getenv("POSTGRES_CONN")           # e.g. "postgresql://..."

# ----------------------------------------------------------------------
# Helper – call the Neon API
# ----------------------------------------------------------------------
def neon_get(endpoint: str, params: dict) -> dict:
    headers = {"Authorization": f"Bearer {NEON_API_TOKEN}"}
    r = requests.get(f"{NEON_API_URL}{endpoint}", headers=headers, params=params)
    r.raise_for_status()
    return r.json()

# ----------------------------------------------------------------------
# Generic paginated fetch
# ----------------------------------------------------------------------
def fetch_all(endpoint: str, params: dict) -> list:
    """
    Retrieve all items from a paginated Neon API endpoint.
    The endpoint must return a dict with a `projects` key and a
    `pagination.cursor` value for the next page.
    """
    items: list = []
    while True:
        resp = neon_get(endpoint, params)
        items.extend(resp.get("projects", []))
        cursor = resp.get("pagination", {}).get("cursor")
        if not cursor:
            break
        params["cursor"] = cursor
    return items

# ----------------------------------------------------------------------
# Build the three request payloads
# ----------------------------------------------------------------------
def fetch_daily() -> list:
    """Daily consumption for the last 60 days, per project."""
    end = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT00:00:00Z")
    start = (datetime.datetime.utcnow() - datetime.timedelta(days=59)).strftime(
        "%Y-%m-%dT00:00:00Z"
    )
    return fetch_all(
        "/projects",
        {"limit": 100, "from": start, "to": end, "granularity": "daily"},
    )

def fetch_hourly() -> list:
    """Hourly consumption for the last 168 hours, per project."""
    end = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    start = (datetime.datetime.utcnow() - datetime.timedelta(hours=167)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    return fetch_all(
        "/projects",
        {"limit": 100, "from": start, "to": end, "granularity": "hourly"},
    )

def fetch_monthly() -> list:
    """Monthly consumption for the last 12 months, per project."""
    end = datetime.datetime.utcnow().date().replace(day=1).strftime(
        "%Y-%m-%dT00:00:00Z"
    )
    start = (
        datetime.datetime.utcnow()
        .date()
        .replace(day=1)
        . - datetime.timedelta(days=335)
    ).strftime("%Y-%m-%dT00:00:00Z")
    return fetch_all(
        "/projects",
        {"limit": 100, "from": start, "to": end, "granularity": "monthly"},
    )

# ----------------------------------------------------------------------
# Insert into PostgreSQL
# ----------------------------------------------------------------------
def insert_rows(conn, table: str, rows: list, columns: list):
    """
    Bulk‑insert rows into the specified table.
    `rows` must be a list of tuples that match `columns`.
    """
    if not rows:
        return
    cols = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({placeholders}) ON CONFLICT DO NOTHING;"
    with conn.cursor() as cur:
        cur.executemany(sql, rows)
    conn.commit()

# ----------------------------------------------------------------------
# Main driver
# ----------------------------------------------------------------------
def main():
    # open DB connection
    conn = psycopg2.connect(DB_CONN_STR)

    # 1. Daily
    daily_rows = []
    for proj in fetch_daily():
        proj_id = proj["project_id"]
        for period in proj["periods"]:
            for rec in period["consumption"]:
                daily_rows.append(
                    (
                        proj_id,
                        rec["timeframe_start"],
                        rec["timeframe_end"],
                        rec["active_time_seconds"],
                        rec["compute_time_seconds"],
                        rec["written_data_bytes"],
                        rec["synthetic_storage_size_bytes"],
                    )
                )
    insert_rows(
        conn,
        "consumption_daily",
        daily_rows,
        [
            "project_id",
            "timeframe_start",
            "timeframe_end",
            "active_time_seconds",
            "compute_time_seconds",
            "written_data_bytes",
            "synthetic_storage_size_bytes",
        ],
    )

    # 2. Hourly
    hourly_rows = []
    for proj in fetch_hourly():
        proj_id = proj["project_id"]
        for period in proj["periods"]:
            for rec in period["consumption"]:
                hourly_rows.append(
                    (
                        proj_id,
                        rec["timeframe_start"],
                        rec["timeframe_end"],
                        rec["active_time_seconds"],
                        rec["compute_time_seconds"],
                        rec["written_data_bytes"],
                        rec["synthetic_storage_size_bytes"],
                    )
                )
    insert_rows(
        conn,
        "consumption_hourly",
        hourly_rows,
        [
            "project_id",
            "timeframe_start",
            "timeframe_end",
            "active_time_seconds",
            "compute_time_seconds",
            "written_data_bytes",
            "synthetic_storage_size_bytes",
        ],
    )

    # 3. Monthly
    monthly_rows = []
    for proj in fetch_monthly():
        proj_id = proj["project_id"]
        for period in proj["periods"]:
            for rec in period["consumption"]:
                monthly_rows.append(
                    (
                        proj_id,
                        rec["timeframe_start"],
                        rec["timeframe_end"],
                        rec["active_time_seconds"],
                        rec["compute_time_seconds"],
                        rec["written_data_bytes"],
                        rec["synthetic_storage_size_bytes"],
                    )
                )
    insert_rows(
        conn,
        "consumption_monthly",
        monthly_rows,
        [
            "project_id",
            "timeframe_start",
            "timeframe_end",
            "active_time_seconds",
            "compute_time_seconds",
            "written_data_bytes",
            "synthetic_storage_size_bytes",
        ],
    )

    conn.close()

if __name__ == "__main__":
    if not NEON_API_TOKEN or not DB_CONN_STR:
        print("Set NEON_API_TOKEN and POSTGRES_CONN environment variables.", file=sys.stderr)
        sys.exit(1)
    main()