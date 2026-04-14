"""Shared Snowflake session helpers (SSO or password)."""

from __future__ import annotations

import os
from pathlib import Path

import snowflake.connector

ROOT = Path(__file__).resolve().parent


def load_env_file(path: str | Path | None = None) -> None:
    """Load key=value pairs from .env into os.environ (does not override existing)."""
    path = Path(path) if path else ROOT / ".env"
    if not path.is_file():
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v


def connect_snowflake():
    """Open a Snowflake connection. Use SNOWFLAKE_AUTHENTICATOR=EXTERNALBROWSER for SSO."""
    load_env_file()
    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    user = os.environ.get("SNOWFLAKE_USER")
    password = os.environ.get("SNOWFLAKE_PASSWORD", "")
    authenticator = os.environ.get("SNOWFLAKE_AUTHENTICATOR", "snowflake").upper()

    if not account or not user:
        raise ValueError("Set SNOWFLAKE_ACCOUNT and SNOWFLAKE_USER in .env")

    kwargs: dict = {
        "account": account,
        "user": user,
    }
    wh = os.environ.get("SNOWFLAKE_WAREHOUSE")
    db = os.environ.get("SNOWFLAKE_DATABASE")
    sc = os.environ.get("SNOWFLAKE_SCHEMA")
    role = os.environ.get("SNOWFLAKE_ROLE")
    if wh:
        kwargs["warehouse"] = wh
    if db:
        kwargs["database"] = db
    if sc:
        kwargs["schema"] = sc
    if role:
        kwargs["role"] = role

    if authenticator == "EXTERNALBROWSER":
        kwargs["authenticator"] = "externalbrowser"
    else:
        if not password:
            raise ValueError("Set SNOWFLAKE_PASSWORD or SNOWFLAKE_AUTHENTICATOR=EXTERNALBROWSER")
        kwargs["password"] = password

    return snowflake.connector.connect(**kwargs)


def query_to_dataframe(conn, sql: str):
    import pandas as pd

    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetch_pandas_all()
    except Exception:
        raise
    finally:
        cur.close()
