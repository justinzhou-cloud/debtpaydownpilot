"""
Databricks Apps entrypoint: serve the static dashboard under ./dashboard.

Run locally: pip install flask && flask --app app run --debug
Databricks: see app.yaml (gunicorn + DATABRICKS_APP_PORT).
"""
from __future__ import annotations

from pathlib import Path

from flask import Flask, abort, send_from_directory

ROOT = Path(__file__).resolve().parent
DASH = ROOT / "dashboard"

app = Flask(__name__)


@app.get("/")
def index():
    if not (DASH / "index.html").is_file():
        return (
            "<h1>Dashboard files missing</h1>"
            "<p>Expected <code>dashboard/index.html</code> next to <code>app.py</code>.</p>",
            500,
        )
    return send_from_directory(DASH, "index.html")


@app.get("/<path:name>")
def dashboard_assets(name: str):
    if ".." in name or name.startswith(("/", "\\")):
        abort(404)
    target = (DASH / name).resolve()
    try:
        target.relative_to(DASH.resolve())
    except ValueError:
        abort(404)
    if not target.is_file():
        abort(404)
    return send_from_directory(DASH, name)
