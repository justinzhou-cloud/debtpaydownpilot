#!/usr/bin/env python3
"""
CLI connection test. For SSO set SNOWFLAKE_AUTHENTICATOR=EXTERNALBROWSER in .env

  .venv/bin/python connect.py
"""

from __future__ import annotations

import sys

from snowflake_client import connect_snowflake, load_env_file


def main() -> None:
    load_env_file()
    try:
        conn = connect_snowflake()
    except ValueError as e:
        print(e, file=sys.stderr)
        sys.exit(1)
    try:
        cur = conn.cursor()
        cur.execute("SELECT current_version(), current_user(), current_role()")
        row = cur.fetchone()
        print("Connected.")
        print(f"  version: {row[0]}")
        print(f"  user:    {row[1]}")
        print(f"  role:    {row[2]}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
