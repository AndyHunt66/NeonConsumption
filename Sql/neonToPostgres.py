#!/usr/bin/env python3
import os
import sys
import requests
import psycopg2
import datetime 

# ------------------------------------------------------------------
# Config – set these before running
# ------------------------------------------------------------------
NEON_API_TOKEN = os.getenv("NEON_API_TOKEN")          # ← put your API key here
NEON_API_URL  = "https://console.neon.tech/api/v2/consumption_history"
DB_CONN_STR   = os.getenv("POSTGRES_CONN")           # e.g. "postgresql://neondb_owner:npg_abcdefghijk@ep-bright-sugar-xxxxxxxxx-pooler.eu-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"

# ------------------------------------------------------------------
# Helper – call the Neon API
# ------------------------------------------------------------------
def neon_get(endpoint: str, params: dict) -> dict:
    headers = {"Authorization": f"Bearer {NEON_API_TOKEN}"}
    r = requests.get(f"{NEON_API_URL}{endpoint}", headers=headers, params=params)
    r.raise_for_status()
    return r.json()

# ------------------------------------------------------------------
# Build the three request payloads
# ------------------------------------------------------------------
def fetch_daily() -> list:
    """Daily consumption for the last 60 days, per project."""
    end = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%dT00:00:00Z')
    start = datetime.datetime.today()  - datetime.timedelta(days=59)
    start = start.strftime('%Y-%m-%dT00:00:00Z')
    return neon_get("/projects",
                    {
                     "limit": 100,
                     "from": start,
                     "to": end,
                     "granularity": "daily"})["projects"]

def fetch_hourly() -> list:
    """Hourly consumption for the last 168 hours, per project."""
    end = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    start = datetime.datetime.utcnow() - datetime.timedelta(hours=167)
    start = start.strftime('%Y-%m-%dT%H:%M:%SZ')
    return neon_get("/projects",
                    {
                    "limit": 100,
                     "from": start,
                     "to": end,
                     "granularity": "hourly"})["projects"]

def fetch_monthly() -> list:
    """Monthly consumption for the last 12 months, per project."""
    end = datetime.datetime.utcnow().date().replace(day=1).strftime('%Y-%m-%dT00:00:00Z')
    start = ( datetime.datetime.utcnow().date() - datetime.timedelta(days=335)).replace(day=1).strftime('%Y-%m-%dT00:00:00Z')
    return neon_get("/projects",
                       {
                    "limit": 100,
                     "from": start,
                     "to": end,
                     "granularity": "monthly"})["projects"]

# ------------------------------------------------------------------
# Insert into PostgreSQL
# ------------------------------------------------------------------
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

# ------------------------------------------------------------------
# Main driver
# ------------------------------------------------------------------
def main():
    # open DB connection
    conn = psycopg2.connect(DB_CONN_STR)

    # ------------------------------------------------------------------
    # 1. Daily
    # ------------------------------------------------------------------
    daily_rows = []
    for proj in fetch_daily():
        proj_id = proj["project_id"]
        for period in proj["periods"]:
            for rec in period["consumption"]:
                daily_rows.append((
                    proj_id,
                    rec["timeframe_start"],                     # YYYY-MM-DD
                    rec["timeframe_end"],                     # YYYY-MM-DD
                    rec["active_time_seconds"],                   # numeric
                    rec["compute_time_seconds"],                   # numeric
                    rec["written_data_bytes"],                  # text
                    rec["synthetic_storage_size_bytes"]
                ))
    insert_rows(conn,
                "consumption_daily",
                daily_rows,
                ["project_id", "timeframe_start","timeframe_end", "active_time_seconds", "compute_time_seconds", "written_data_bytes", "synthetic_storage_size_bytes"])

    # ------------------------------------------------------------------
    # 2. Hourly
    # ------------------------------------------------------------------
    hourly_rows = []
    for proj in fetch_hourly():
        proj_id = proj["project_id"]
        for period in proj["periods"]:
            for rec in period["consumption"]:
                hourly_rows.append((
                    proj_id,
                    rec["timeframe_start"],                     # YYYY-MM-DD
                    rec["timeframe_end"],                     # YYYY-MM-DD
                    rec["active_time_seconds"],                   # numeric
                    rec["compute_time_seconds"],                   # numeric
                    rec["written_data_bytes"],                  # text
                    rec["synthetic_storage_size_bytes"]
                ))
    insert_rows(conn,
                "consumption_hourly",
                hourly_rows,
                ["project_id", "timeframe_start","timeframe_end", "active_time_seconds", "compute_time_seconds", "written_data_bytes", "synthetic_storage_size_bytes"])
    # ------------------------------------------------------------------
    # 3. Monthly
    # ------------------------------------------------------------------
    monthly_rows = []
    for proj in fetch_monthly():
        proj_id = proj["project_id"]
        for period in proj["periods"]:
            for rec in period["consumption"]:
                monthly_rows.append((
                    proj_id,
                    rec["timeframe_start"],                     # YYYY-MM-DD
                    rec["timeframe_end"],                     # YYYY-MM-DD
                    rec["active_time_seconds"],                   # numeric
                    rec["compute_time_seconds"],                   # numeric
                    rec["written_data_bytes"],                  # text
                    rec["synthetic_storage_size_bytes"]
                ))
    insert_rows(conn,
                "consumption_monthly",
                monthly_rows,
                ["project_id", "timeframe_start","timeframe_end", "active_time_seconds", "compute_time_seconds", "written_data_bytes", "synthetic_storage_size_bytes"])
    conn.close()

if __name__ == "__main__":
    if not NEON_API_TOKEN or not DB_CONN_STR:
        print("Set NEON_API_TOKEN and POSTGRES_CONN environment variables.", file=sys.stderr)
        sys.exit(1)
    main()