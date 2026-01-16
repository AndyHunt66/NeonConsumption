import os
from flask import Flask, render_template, jsonify, request
import psycopg2
import psycopg2.extras

app = Flask(__name__)

# Connection setup – read from env or hard‑code temporarily
# Example:""postgresql://neondb_owner:npg_abcdefg@ep-gold-treasure-abcdefgh-pooler.eu-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://neondb_owner:npg_abcdefg@ep-gold-treasure-abcdefgh-pooler.eu-west-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

# Helper to run a query and return JSON‑serialisable rows
def run_query(sql, params=None):
    with get_conn() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchall()

# API endpoint
@app.route("/api/consumption/<granularity>")
def api_consumption(granularity):
    project_id = request.args.get("project_id")
    sql = f"SELECT timeframe_start, active_time_seconds, compute_time_seconds, written_data_bytes, synthetic_storage_size_bytes FROM consumption_{granularity} WHERE project_id = %s ORDER BY timeframe_start "
    rows = run_query(sql, (project_id,))
    return jsonify(rows)

@app.route("/api/projectids")
def api_project_ids():
    project_id = request.args.get("project_id")
    sql = "SELECT distinct(project_id) FROM consumption_monthly ORDER BY project_id "
    rows = run_query(sql, (project_id,))
    projects = []
    for row in rows:
        projects.append(row["project_id"])

    return jsonify(projects)

# Main page
@app.route("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)